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
package fr.jamgotchian.jabat.cdi;

import java.util.Set;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.enterprise.inject.spi.InjectionTarget;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ForwardingInjectionTarget<X> implements InjectionTarget<X> {

    private final InjectionTarget<X> delegate;

    public ForwardingInjectionTarget(InjectionTarget<X> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void inject(X instance, CreationalContext<X> ctx) {
        delegate.inject(instance, ctx);
    }

    @Override
    public void postConstruct(X instance) {
        delegate.postConstruct(instance);
    }

    @Override
    public void preDestroy(X instance) {
        delegate.preDestroy(instance);
    }

    @Override
    public X produce(CreationalContext<X> ctx) {
        return delegate.produce(ctx);
    }

    @Override
    public void dispose(X instance) {
        delegate.dispose(instance);
    }

    @Override
    public Set<InjectionPoint> getInjectionPoints() {
        return delegate.getInjectionPoints();
    }

}
