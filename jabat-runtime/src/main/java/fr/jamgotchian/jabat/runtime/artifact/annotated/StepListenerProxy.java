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
import javax.batch.api.StepListener;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class StepListenerProxy implements StepListener {

    private final Object object;

    private final StepListenerAnnotatedClass annotatedClass;

    public StepListenerProxy(Object object) {
        this.object = object;
        annotatedClass = new StepListenerAnnotatedClass(object.getClass());
    }

    @Override
    public void beforeStep() throws Exception {
        try {
            if (annotatedClass.getBeforeStepMethod() != null) {
                annotatedClass.getBeforeStepMethod().invoke(object);
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
    public void afterStep() throws Exception {
        try {
            if (annotatedClass.getAfterStepMethod() != null) {
                annotatedClass.getAfterStepMethod().invoke(object);
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
