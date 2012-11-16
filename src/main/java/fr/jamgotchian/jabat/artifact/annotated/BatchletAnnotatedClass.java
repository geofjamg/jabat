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
import javax.batch.annotation.Process;
import javax.batch.annotation.Stop;

/**
 * @Process String <method-name> () throws Exception
 * @Stop void <method-name> () throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchletAnnotatedClass extends AnnotatedClass {

    private final Method processMethod;

    private final Method stopMethod;

    public BatchletAnnotatedClass(Class<?> clazz) {
        super(clazz);
        processMethod = findAnnotatedMethod(clazz, Process.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, String.class)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        processMethod.setAccessible(true);
        stopMethod = findAnnotatedMethod(clazz, Stop.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        stopMethod.setAccessible(true);
    }

    public Method getProcessMethod() {
        return processMethod;
    }

    public Method getStopMethod() {
        return stopMethod;
    }
    
}
