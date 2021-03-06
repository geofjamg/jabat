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

import fr.jamgotchian.jabat.runtime.context.ThreadContext;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.InjectionException;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.InjectionTarget;
import javax.enterprise.inject.spi.ProcessInjectionTarget;
import javax.inject.Named;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatCdiExtension implements Extension {

    public static BeanManager BEAN_MANAGER;

    public JabatCdiExtension() {
    }

    public void setBeanManager(@Observes BeforeBeanDiscovery bbd, BeanManager beanManager) {
        BEAN_MANAGER = beanManager;
    }

    public <X> void processBatchArtifact(@Observes ProcessInjectionTarget<X> pit) {
        final InjectionTarget<X> it = pit.getInjectionTarget();
        final AnnotatedType<X> at = pit.getAnnotatedType();

        // TODO check the type is a batch artifact
        if (at.isAnnotationPresent(Named.class)) {
            InjectionTarget<X> wrapped = new ForwardingInjectionTarget<X>(it) {

                @Override
                public void inject(X instance, CreationalContext<X> ctx) {
                    it.inject(instance, ctx);
                    try {
                        // get the bean name
                        String name = at.getAnnotation(Named.class).value();
                        ThreadContext.getInstance().inject(instance, name);
                    } catch (Throwable t) {
                        throw new InjectionException(t);
                    }
                }
            };

            pit.setInjectionTarget(wrapped);
        }
    }

}
