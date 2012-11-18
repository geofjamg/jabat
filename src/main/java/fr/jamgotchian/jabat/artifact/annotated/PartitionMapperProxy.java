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

import java.lang.reflect.InvocationTargetException;
import javax.batch.api.PartitionMapper;
import javax.batch.api.parameters.PartitionPlan;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class PartitionMapperProxy implements PartitionMapper {

    private final Object object;

    private final PartitionMapperAnnotatedClass annotatedClass;

    public PartitionMapperProxy(Object object) {
        this.object = object;
        annotatedClass = new PartitionMapperAnnotatedClass(object.getClass());
    }

    @Override
    public PartitionPlan mapPartitions() throws Exception {
        PartitionPlan plan = null;
        try {
            plan = (PartitionPlan) annotatedClass.getMapPartitionsMethod().invoke(object);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
        return plan;
    }

}
