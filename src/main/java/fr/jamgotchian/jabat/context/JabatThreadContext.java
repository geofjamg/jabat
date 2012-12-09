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
package fr.jamgotchian.jabat.context;

import fr.jamgotchian.jabat.job.Artifact;
import fr.jamgotchian.jabat.job.Flow;
import fr.jamgotchian.jabat.job.Job;
import fr.jamgotchian.jabat.job.Split;
import fr.jamgotchian.jabat.job.Step;
import fr.jamgotchian.jabat.repository.JabatJobExecution;
import fr.jamgotchian.jabat.repository.JabatJobInstance;
import fr.jamgotchian.jabat.repository.JabatStepExecution;
import fr.jamgotchian.jabat.util.JabatException;
import java.io.Externalizable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import javax.batch.annotation.BatchContext;
import javax.batch.annotation.BatchProperty;
import javax.batch.runtime.context.FlowContext;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.SplitContext;
import javax.batch.runtime.context.StepContext;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatThreadContext {

    private static final JabatThreadContext INSTANCE = new JabatThreadContext();

    public static JabatThreadContext getInstance() {
        return INSTANCE;
    }

    private final ThreadLocal<JabatJobContext<Object>> jobContext
            = new ThreadLocal<JabatJobContext<Object>>();

    private final ThreadLocal<JabatStepContext<Object, Externalizable>> stepContext
            = new ThreadLocal<JabatStepContext<Object, Externalizable>>();

    private final ThreadLocal<JabatFlowContext<Object>> flowContext
            = new ThreadLocal<JabatFlowContext<Object>>();

    private final ThreadLocal<JabatSplitContext<Object>> splitContext
            = new ThreadLocal<JabatSplitContext<Object>>();

    public JabatJobContext<Object> getJobContext() {
        return jobContext.get();
    }

    public void createJobContext(Job job, JabatJobInstance jobInstance, JabatJobExecution jobExecution) {
        jobContext.set(new JabatJobContext<Object>(job, jobInstance, jobExecution));
    }

    public void destroyJobContext() {
        jobContext.remove();
    }

    public JabatStepContext<Object, Externalizable> getStepContext() {
        return stepContext.get();
    }

    public void createStepContext(Step step, JabatStepExecution stepExecution) {
        stepContext.set(new JabatStepContext<Object, Externalizable>(step, stepExecution));
    }

    public void destroyStepContext() {
        stepContext.remove();
    }

    public JabatFlowContext<Object> getFlowContext() {
        return flowContext.get();
    }

    public void createFlowContext(Flow flow) {
        flowContext.set(new JabatFlowContext<Object>(flow));
    }

    public void destroyFlowContext() {
        flowContext.remove();
    }

    public JabatSplitContext<Object> getSplitContext() {
        return splitContext.get();
    }

    public void createSplitContext(Split split) {
        splitContext.set(new JabatSplitContext<Object>(split));
    }

    public void destroySplitContext() {
        splitContext.remove();
    }

    public void inject(Object instance, String name) throws IllegalAccessException {
        Class<?> clazz = instance.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            // context injection
            if (field.isAnnotationPresent(BatchContext.class)) {
                Class<?> fieldType = field.getType();
                Object fieldValue;
                if (fieldType == JobContext.class) {
                    fieldValue = jobContext.get();
                } else if (fieldType == StepContext.class) {
                    fieldValue = stepContext.get();
                } else if (fieldType == SplitContext.class) {
                    fieldValue = splitContext.get();
                } else if (fieldType == FlowContext.class) {
                    fieldValue = flowContext.get();
                } else {
                    throw new JabatException("Field annotated with "
                            + BatchContext.class.getName()
                            + " should have one of the following type: "
                            + JobContext.class.getName() + ", " + StepContext.class.getName() + ", "
                            + SplitContext.class.getName() + "or " + FlowContext.class.getName());
                }
                if (!field.isAccessible()) {
                    field.setAccessible(true);
                }
                field.set(instance, fieldValue);
            }
            // property injection
            BatchProperty batchProperty = field.getAnnotation(BatchProperty.class);
            if (batchProperty != null) {
                if (Modifier.isFinal(field.getModifiers())) {
                    throw new JabatException("Field annotated with "
                            + BatchProperty.class.getName() + " should not be final");
                }
                if (field.getType() == String.class) {
                    // get the current artifact
                    JabatStepContext<?, ?> context = stepContext.get();
                    if (context == null) {
                        throw new JabatException("Step context is not set");
                    }
                    Artifact artifact = context.getNode().getArtifact(name);
                    String propertyName = batchProperty.name().isEmpty()
                            ? field.getName() : batchProperty.name();
                    String value = artifact.getSubstitutedProperties().getProperty(propertyName);
                    if (value != null) {
                        if (!field.isAccessible()) {
                            field.setAccessible(true);
                        }
                        field.set(instance, value);
                    }
                } else {
                    throw new JabatException("Field annotated with "
                            + BatchProperty.class.getName() + " should be of type "
                            + String.class.getName());
                }
            }
        }
    }
}
