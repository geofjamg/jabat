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
package fr.jamgotchian.jabat.util;

import com.google.common.base.Predicate;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class MethodUtil {

    private MethodUtil() {
    }

    public static boolean throwsOneException(Method m, Class<?> type) {
        return m.getExceptionTypes().length == 1
                && m.getExceptionTypes()[0] == type;
    }

    public static boolean hasZeroParameter(Method m) {
        return m.getParameterTypes().length == 0;
    }

    public static boolean hasOneParameter(Method m, Class<?> type) {
        return m.getParameterTypes().length == 1
                && type.isAssignableFrom(m.getParameterTypes()[0]);
    }

    public static boolean hasReturnType(Method m, Class<?> type) {
        return m.getReturnType() == type;
    }

    public static Method findAnnotatedMethod(Class<?> clazz,
                                             Class<? extends Annotation> annotationClass,
                                             boolean optional,
                                             Predicate<Method> predicate) {
        for (Method m : clazz.getDeclaredMethods()) {
            if (m.isAnnotationPresent(annotationClass)) {
                if (predicate.apply(m)) {
                    return m;
                } else {
                    throw new JabatException("Bad signature for " + annotationClass.getName()
                            + " annotated method of class " + clazz.getName());
                }
            }
        }
        if (optional) {
            return null;
        } else {
            throw new JabatException("Cannot find " + annotationClass.getName()
                    + " annotated method");
        }
    }

}
