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
package fr.jamgotchian.jabat.artifact.annotated;

import static fr.jamgotchian.jabat.util.MethodUtil.*;
import com.google.common.base.Predicate;
import java.lang.reflect.Method;
import javax.batch.annotation.AfterStep;
import javax.batch.annotation.BeforeStep;

/**
 * [@BeforeStep void <method-name> () throws Exception]
 * [@AfterStep void <method-name> () throws Exception ]
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class StepListenerAnnotatedClass {
       
    private final Method beforeStepMethod;

    private final Method afterStepMethod;

    public StepListenerAnnotatedClass(Class<?> clazz) {
        beforeStepMethod = findAnnotatedMethod(clazz, BeforeStep.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (beforeStepMethod != null) {
            beforeStepMethod.setAccessible(true);
        }
        afterStepMethod = findAnnotatedMethod(clazz, AfterStep.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (afterStepMethod != null) {
            afterStepMethod.setAccessible(true);
        }
    }

    public Method getBeforeStepMethod() {
        return beforeStepMethod;
    }

    public Method getAfterStepMethod() {
        return afterStepMethod;
    }
    
}
