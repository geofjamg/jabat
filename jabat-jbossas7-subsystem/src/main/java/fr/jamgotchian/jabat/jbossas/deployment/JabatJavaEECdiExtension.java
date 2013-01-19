/*
 * Copyright 2013 Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>.
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
package fr.jamgotchian.jabat.jbossas.deployment;

import fr.jamgotchian.jabat.api.annotation.BatchRuntime;
import fr.jamgotchian.jabat.cdi.ForwardingInjectionTarget;
import fr.jamgotchian.jabat.cdi.JabatCdiExtension;
import fr.jamgotchian.jabat.runtime.JobOperatorImpl;
import fr.jamgotchian.jabat.runtime.JobContainer;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Set;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.InjectionException;
import javax.enterprise.inject.spi.AnnotatedField;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.InjectionTarget;
import javax.enterprise.inject.spi.ProcessInjectionTarget;
import org.jboss.as.server.CurrentServiceContainer;
import org.jboss.msc.service.ServiceContainer;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatJavaEECdiExtension extends JabatCdiExtension {

    public JabatJavaEECdiExtension() {
    }

    public <X> void injectJobOperator(@Observes ProcessInjectionTarget<X> pit) {
        final InjectionTarget<X> it = pit.getInjectionTarget();
        final AnnotatedType<X> at = pit.getAnnotatedType();

        final Set<Field> fieldsToInject = new HashSet<Field>();
        for (AnnotatedField<? super X> af : at.getFields()) {
            if (af.isAnnotationPresent(BatchRuntime.class)) {
                Field field = af.getJavaMember();
                field.setAccessible(true);
                fieldsToInject.add(field);
            }
        }

        if (fieldsToInject.size() > 0) {
            InjectionTarget<X> wrapped = new ForwardingInjectionTarget<X>(it) {

                @Override
                public void inject(X instance, CreationalContext<X> ctx) {
                    it.inject(instance, ctx);
                    try {

                        JobContainerService service = (JobContainerService) getCurrentServiceContainer()
                                .getService(JobContainerService.NAME).getService();
                        JobContainer jobContainer = service.getValue().getJobContainer();

                        for (Field field : fieldsToInject) {
                            field.set(instance, new JobOperatorImpl(jobContainer));
                        }
                    } catch (Throwable t) {
                        throw new InjectionException(t);
                    }
                }
            };

            pit.setInjectionTarget(wrapped);
        }
    }

    public static ServiceContainer getCurrentServiceContainer() {
        PrivilegedAction<ServiceContainer> action = new PrivilegedAction<ServiceContainer>() {
            @Override
            public ServiceContainer run() {
                return CurrentServiceContainer.getServiceContainer();
            }
        };
        return AccessController.doPrivileged(action);
    }
}
