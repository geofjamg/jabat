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

import static fr.jamgotchian.jabat.util.MethodUtil.*;
import com.google.common.base.Predicate;
import java.lang.reflect.Method;
import javax.batch.annotation.AfterJob;
import javax.batch.annotation.BeforeJob;

/**
 * [@BeforeJob void <method-name> () throws Exception]
 * [@AfterJob void <method-name> () throws Exception]
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobListenerAnnotatedClass {

    private final Method beforeJobMethod;

    private final Method afterJobMethod;

    public JobListenerAnnotatedClass(Class<?> clazz) {
        beforeJobMethod = findAnnotatedMethod(clazz, BeforeJob.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (beforeJobMethod != null) {
            beforeJobMethod.setAccessible(true);
        }
        afterJobMethod = findAnnotatedMethod(clazz, AfterJob.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (afterJobMethod != null) {
            afterJobMethod.setAccessible(true);
        }
    }

    public Method getBeforeJobMethod() {
        return beforeJobMethod;
    }

    public Method getAfterJobMethod() {
        return afterJobMethod;
    }

}
