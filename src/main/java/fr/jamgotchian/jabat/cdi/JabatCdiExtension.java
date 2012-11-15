/*
 * Copyright 2012 Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.jamgotchian.jabat.cdi;

import com.google.common.collect.Sets;
import fr.jamgotchian.jabat.context.JabatThreadContext;
import fr.jamgotchian.jabat.job.Artifact;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.batch.annotation.BatchContext;
import javax.batch.annotation.BatchProperty;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.InjectionException;
import javax.enterprise.inject.spi.AnnotatedField;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.enterprise.inject.spi.InjectionTarget;
import javax.enterprise.inject.spi.ProcessInjectionTarget;
import javax.inject.Named;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
class JabatCdiExtension implements Extension {

    private static final List<Class<? extends Annotation>> ARTIFACT_ANNOTATIONS
            = Collections.unmodifiableList(Arrays.asList(javax.batch.annotation.Batchlet.class,
                                                         javax.batch.annotation.ItemReader.class,
                                                         javax.batch.annotation.ItemProcessor.class,
                                                         javax.batch.annotation.ItemWriter.class,
                                                         javax.batch.annotation.JobListener.class,
                                                         javax.batch.annotation.StepListener.class,
                                                         javax.batch.annotation.CheckpointAlgorithm.class));

    private static final List<Class<?>> ARTIFACT_INTERFACES
            = Collections.unmodifiableList(Arrays.asList(javax.batch.api.Batchlet.class,
                                                         javax.batch.api.ItemReader.class,
                                                         javax.batch.api.ItemProcessor.class,
                                                         javax.batch.api.ItemWriter.class,
                                                         javax.batch.api.JobListener.class,
                                                         javax.batch.api.StepListener.class,
                                                         javax.batch.api.CheckpointAlgorithm.class));

    private static final Set<Class<? extends javax.batch.runtime.context.BatchContext>> CONTEXT_CLASSES
            = Collections.unmodifiableSet(Sets.newHashSet(JobContext.class,
                                                          StepContext.class));

    public static BeanManager BEAN_MANAGER;

    JabatCdiExtension() {
    }

    public void init(@Observes BeforeBeanDiscovery bbd, BeanManager beanManager) {
        BEAN_MANAGER = beanManager;
    }

    private <X> boolean isArtifact(AnnotatedType<X> at) {
        for (Class<? extends Annotation> artifactAnnotation : ARTIFACT_ANNOTATIONS) {
            if (at.isAnnotationPresent(artifactAnnotation)) {
                return true;
            }
        }
        for (Class<?> artifactInterface : ARTIFACT_INTERFACES) {
            if (artifactInterface.isAssignableFrom(at.getJavaClass())) {
                return true;
            }
        }
        return false;
    }
    
    public <X> void injectContext(@Observes ProcessInjectionTarget<X> pit) {
        final InjectionTarget<X> it = pit.getInjectionTarget();
        final AnnotatedType<X> at = pit.getAnnotatedType();
        if (isArtifact(at)) {
            final List<Field> contextFields = new ArrayList<Field>();
            final List<Field> propertyFields = new ArrayList<Field>();
            for (AnnotatedField<? super X> annotatedField : at.getFields()) {
                BatchContext batchContext = annotatedField.getAnnotation(BatchContext.class);
                if  (batchContext != null) {
                    Field field = annotatedField.getJavaMember();
                    if (CONTEXT_CLASSES.contains(field.getType())) {
                        contextFields.add(field);
                    } else {
                        pit.addDefinitionError( new InjectionException("Field annotated with "
                                + BatchContext.class + " should be of type " + CONTEXT_CLASSES));
                    }
                }
                BatchProperty batchProperty = annotatedField.getAnnotation(BatchProperty.class);
                if (batchProperty != null) {
                    Field field = annotatedField.getJavaMember();
                    // TODO the field should not be final?
                    if (field.getType() == String.class) {
                        propertyFields.add(field);
                    } else {
                        pit.addDefinitionError( new InjectionException("Field annotated with "
                                + BatchProperty.class + " should be of type " + String.class));
                    }
                }
            }

            InjectionTarget<X> wrapped = new InjectionTarget<X>() {

                @Override
                public void inject(X instance, CreationalContext<X> ctx) {
                    it.inject(instance, ctx);
                    try {
                        for (Field field : contextFields) {
                            Class<?> contextClass = field.getType();
                            field.setAccessible(true);
                            if (contextClass == JobContext.class) {
                                field.set(instance, JabatThreadContext.getInstance().getActiveJobContext());
                            } else if (contextClass == StepContext.class) {
                                field.set(instance, JabatThreadContext.getInstance().getActiveStepContext());
                            } else {
                                throw new InternalError();
                            }
                        }
                        for (Field field : propertyFields) {
                            // get the bean name
                            String name = at.getAnnotation(Named.class).value();
                            Artifact artifact = JabatThreadContext.getInstance().getActiveStepContext().getNode().getArtifact(name);
                            Properties properties = artifact.getProperties();
                            // TODO support the name parameter of @BatchProperty
                            String value = properties.getProperty(field.getName());
                            if (value != null) {
                                field.setAccessible(true);
                                field.set(instance, value);
                            }
                        }
                    } catch (IllegalAccessException e) {
                        throw new InjectionException(e);
                    }
                }

                @Override
                public void postConstruct(X instance) {
                    it.postConstruct(instance);
                }

                @Override
                public void preDestroy(X instance) {
                    it.dispose(instance);
                }

                @Override
                public void dispose(X instance) {
                    it.dispose(instance);
                }

                @Override
                public Set<InjectionPoint> getInjectionPoints() {
                    return it.getInjectionPoints();
                }

                @Override
                public X produce(CreationalContext<X> ctx) {
                    return it.produce(ctx);
                }

            };

            pit.setInjectionTarget(wrapped);
        }
    }

}
