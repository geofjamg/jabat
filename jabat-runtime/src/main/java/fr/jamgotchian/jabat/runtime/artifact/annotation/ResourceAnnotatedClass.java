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
package fr.jamgotchian.jabat.runtime.artifact.annotation;

import com.google.common.base.Predicate;
import static fr.jamgotchian.jabat.runtime.util.MethodUtil.*;
import java.io.Externalizable;
import java.lang.reflect.Method;
import javax.batch.annotation.CheckpointInfo;
import javax.batch.annotation.Close;
import javax.batch.annotation.Open;

/**
 * @Open void <method-name>(Externalizable checkpoint) throws Exception
 * [@Close void <method-name>() throws Exception]
 * @CheckpointInfo Externalizable <method-name> () throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public abstract class ResourceAnnotatedClass {

    private final Method openMethod;

    private final Method closeMethod;

    private final Method checkpointInfoMethod;

    protected ResourceAnnotatedClass(Class<?> clazz) {
        openMethod = findAnnotatedMethod(clazz, Open.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return m.getReturnType() == Void.TYPE
                        && hasOneParameter(m, Externalizable.class)
                        && throwsOneException(m, Exception.class);
            }
        });
        openMethod.setAccessible(true);
        closeMethod = findAnnotatedMethod(clazz, Close.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return m.getReturnType() == Void.TYPE
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (closeMethod != null) {
            closeMethod.setAccessible(true);
        }
        checkpointInfoMethod = findAnnotatedMethod(clazz, CheckpointInfo.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return Externalizable.class.isAssignableFrom(m.getReturnType())
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        checkpointInfoMethod.setAccessible(true);
    }

    public Method getOpenMethod() {
        return openMethod;
    }

    public Method getCloseMethod() {
        return closeMethod;
    }

    public Method getCheckpointInfoMethod() {
        return checkpointInfoMethod;
    }

}
