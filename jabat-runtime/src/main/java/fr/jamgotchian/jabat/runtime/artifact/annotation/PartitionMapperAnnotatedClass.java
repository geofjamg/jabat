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

import static fr.jamgotchian.jabat.runtime.util.MethodUtil.*;
import com.google.common.base.Predicate;
import java.lang.reflect.Method;
import javax.batch.annotation.MapPartitions;
import javax.batch.api.parameters.PartitionPlan;

/**
 * @MapPartitions PartitionPlan <method-name>( ) throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class PartitionMapperAnnotatedClass {

    private final Method mapPartitionsMethod;

    public PartitionMapperAnnotatedClass(Class<?> clazz) {
        mapPartitionsMethod = findAnnotatedMethod(clazz, MapPartitions.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, PartitionPlan.class)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        mapPartitionsMethod.setAccessible(true);
    }

    public Method getMapPartitionsMethod() {
        return mapPartitionsMethod;
    }

}