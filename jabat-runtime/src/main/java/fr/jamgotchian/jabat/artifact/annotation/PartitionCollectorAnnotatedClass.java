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

import com.google.common.base.Predicate;
import static fr.jamgotchian.jabat.util.MethodUtil.*;
import java.io.Externalizable;
import java.lang.reflect.Method;
import javax.batch.annotation.CollectPartitionData;

/**
 * @CollectPartitionData Externalizable <method-name>() throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class PartitionCollectorAnnotatedClass {

    private final Method collectPartitionDataMethod;

    public PartitionCollectorAnnotatedClass(Class<?> clazz) {
        collectPartitionDataMethod = findAnnotatedMethod(clazz, CollectPartitionData.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Externalizable.class)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        collectPartitionDataMethod.setAccessible(true);
    }

    public Method getCollectPartitionDataMethod() {
        return collectPartitionDataMethod;
    }

}