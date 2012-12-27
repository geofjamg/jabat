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
package fr.jamgotchian.jabat.artifact.annotation;

import java.io.Externalizable;
import java.lang.reflect.InvocationTargetException;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
abstract class ResourceProxy {

    protected final Object object;

    protected ResourceProxy(Object object) {
        this.object = object;
    }

    protected abstract ResourceAnnotatedClass getAnnotatedClass();

    public void open(Externalizable checkpoint) throws Exception {
        try {
            getAnnotatedClass().getOpenMethod().invoke(object, checkpoint);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

    public void close() throws Exception {
        try {
            if (getAnnotatedClass().getCloseMethod() != null) {
                getAnnotatedClass().getCloseMethod().invoke(object);
            }
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

    public Externalizable checkpointInfo() throws Exception {
        try {
            return (Externalizable) getAnnotatedClass().getCheckpointInfoMethod().invoke(object);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

}
