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

import java.lang.reflect.InvocationTargetException;
import javax.batch.api.PartitionReducer;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class PartitionReducerProxy implements PartitionReducer {

    private final Object object;

    private final PartitionReducerAnnotatedClass annotatedClass;

    public PartitionReducerProxy(Object object) {
        this.object = object;
        annotatedClass = new PartitionReducerAnnotatedClass(object.getClass());
    }

    @Override
    public void beginPartitionedStep() throws Exception {
        try {
            if (annotatedClass.getBeginPartitionedStepMethod() != null) {
                annotatedClass.getBeginPartitionedStepMethod().invoke(object);
            }
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

    @Override
    public void beforePartitionedStepCompletion() throws Exception {
        try {
            if (annotatedClass.getBeforePartitionedStepCompletionMethod() != null) {
                annotatedClass.getBeforePartitionedStepCompletionMethod().invoke(object);
            }
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

    @Override
    public void rollbackPartitionedStep() throws Exception {
        try {
            if (annotatedClass.getRollbackPartitionedStepMethod() != null) {
                annotatedClass.getRollbackPartitionedStepMethod().invoke(object);
            }
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

    @Override
    public void afterPartitionedStepCompletion(String status) throws Exception {
        try {
            if (annotatedClass.getAfterPartitionedStepCompletionMethod() != null) {
                annotatedClass.getAfterPartitionedStepCompletionMethod().invoke(object, status);
            }
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

}
