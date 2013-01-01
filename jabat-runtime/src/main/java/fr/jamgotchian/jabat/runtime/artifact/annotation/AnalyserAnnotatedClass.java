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
import javax.batch.annotation.AnalyzeCollectorData;
import javax.batch.annotation.AnalyzeStatus;

/**
 * [@AnalyzeCollectorData void <method-name>(Externalizable data) throws Exception]
 * [@AnalyzeStatus void <method-name>(String batchStatus, String exitStatus) throws Exception]
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class AnalyserAnnotatedClass {

    private final Method analyseCollectorDataMethod;

    private final Method analyseStatusMethod;

    public AnalyserAnnotatedClass(Class<?> clazz) {
        analyseCollectorDataMethod = findAnnotatedMethod(clazz, AnalyzeCollectorData.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasOneParameter(m, Externalizable.class)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (analyseCollectorDataMethod != null) {
            analyseCollectorDataMethod.setAccessible(true);
        }
        analyseStatusMethod = findAnnotatedMethod(clazz, AnalyzeStatus.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasTwoParameters(m, String.class, String.class)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (analyseStatusMethod != null) {
            analyseStatusMethod.setAccessible(true);
        }
    }

    public Method getAnalyseCollectorDataMethod() {
        return analyseCollectorDataMethod;
    }

    public Method getAnalyseStatusMethod() {
        return analyseStatusMethod;
    }

}
