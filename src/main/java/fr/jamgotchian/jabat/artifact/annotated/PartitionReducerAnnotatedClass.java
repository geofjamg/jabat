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

import com.google.common.base.Predicate;
import static fr.jamgotchian.jabat.util.MethodUtil.*;
import java.lang.reflect.Method;
import javax.batch.annotation.AfterPartitionedStepCompletion;
import javax.batch.annotation.BeforePartitionedStepCompletion;
import javax.batch.annotation.BeginPartitionedStep;
import javax.batch.annotation.RollbackPartitionedStep;

/**
 * [@BeginPartitionedStep void <method-name>() throws Exception]
 * [@BeforePartitionedStepCompletion void <method-name>() throws Exception]
 * [@RollbackPartitionedStep void <method-name>() throws Exception]
 * [@AfterPartitionedStepCompletion void <method-name>(String status) throws Exception]
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class PartitionReducerAnnotatedClass {

    private final Method beginPartitionedStepMethod;

    private final Method beforePartitionedStepCompletionMethod;

    private final Method rollbackPartitionedStepMethod;

    private final Method afterPartitionedStepCompletionMethod;

    public PartitionReducerAnnotatedClass(Class<?> clazz) {
        beginPartitionedStepMethod = findAnnotatedMethod(clazz, BeginPartitionedStep.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (beginPartitionedStepMethod != null) {
            beginPartitionedStepMethod.setAccessible(true);
        }
        beforePartitionedStepCompletionMethod = findAnnotatedMethod(clazz, BeforePartitionedStepCompletion.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (beforePartitionedStepCompletionMethod != null) {
            beforePartitionedStepCompletionMethod.setAccessible(true);
        }
        rollbackPartitionedStepMethod = findAnnotatedMethod(clazz, RollbackPartitionedStep.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (rollbackPartitionedStepMethod != null) {
            rollbackPartitionedStepMethod.setAccessible(true);
        }
        afterPartitionedStepCompletionMethod = findAnnotatedMethod(clazz, AfterPartitionedStepCompletion.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasOneParameter(m, String.class)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (afterPartitionedStepCompletionMethod != null) {
            afterPartitionedStepCompletionMethod.setAccessible(true);
        }
    }

    public Method getBeginPartitionedStepMethod() {
        return beginPartitionedStepMethod;
    }

    public Method getBeforePartitionedStepCompletionMethod() {
        return beforePartitionedStepCompletionMethod;
    }

    public Method getRollbackPartitionedStepMethod() {
        return rollbackPartitionedStepMethod;
    }

    public Method getAfterPartitionedStepCompletionMethod() {
        return afterPartitionedStepCompletionMethod;
    }

}
