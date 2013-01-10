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
package fr.jamgotchian.jabat.runtime.artifact.annotated;

import com.google.common.base.Predicate;
import static fr.jamgotchian.jabat.runtime.util.MethodUtil.*;
import java.lang.reflect.Method;
import javax.batch.annotation.BeginCheckpoint;
import javax.batch.annotation.CheckpointTimeout;
import javax.batch.annotation.EndCheckpoint;
import javax.batch.annotation.IsReadyToCheckpoint;

/**
 * @CheckpointTimeout int <method-name> (int timeout) throws Exception
 * @BeginCheckpoint void <method-name> () throws Exception
 * @IsReadyToCheckpoint boolean <method-name> () throws Exception
 * @EndCheckpoint void <method-name> () throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class CheckpointAlgorithmAnnotatedClass {

    private final Method checkpointTimeoutMethod;

    private final Method beginCheckpointMethod;

    private final Method isReadyToCheckpointMethod;

    private final Method endCheckpointMethod;

    public CheckpointAlgorithmAnnotatedClass(Class<?> clazz) {
        checkpointTimeoutMethod = findAnnotatedMethod(clazz, CheckpointTimeout.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Integer.TYPE)
                        && hasOneParameter(m, Integer.TYPE)
                        && throwsOneException(m, Exception.class);
            }
        });
        checkpointTimeoutMethod.setAccessible(true);
        beginCheckpointMethod = findAnnotatedMethod(clazz, BeginCheckpoint.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        beginCheckpointMethod.setAccessible(true);
        isReadyToCheckpointMethod = findAnnotatedMethod(clazz, IsReadyToCheckpoint.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Boolean.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        isReadyToCheckpointMethod.setAccessible(true);
        endCheckpointMethod = findAnnotatedMethod(clazz, EndCheckpoint.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        endCheckpointMethod.setAccessible(true);
    }

    public Method getCheckpointTimeoutMethod() {
        return checkpointTimeoutMethod;
    }

    public Method getBeginCheckpointMethod() {
        return beginCheckpointMethod;
    }

    public Method getIsReadyToCheckpointMethod() {
        return isReadyToCheckpointMethod;
    }

    public Method getEndCheckpointMethod() {
        return endCheckpointMethod;
    }

}
