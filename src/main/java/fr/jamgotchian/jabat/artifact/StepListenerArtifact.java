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
package fr.jamgotchian.jabat.artifact;

import com.google.common.base.Predicate;
import static fr.jamgotchian.jabat.util.MethodUtil.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import javax.batch.annotation.AfterStep;
import javax.batch.annotation.BeforeStep;

/**
 * @BeforeStep void <method-name> () throws Exception
 * @AfterStep void <method-name> () throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class StepListenerArtifact extends Artifact {

    private final Method beforeStepMethod;

    private final Method afterStepMethod;

    public StepListenerArtifact(Object object) {
        super(object);
        beforeStepMethod = findAnnotatedMethod(object.getClass(), BeforeStep.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (beforeStepMethod != null) {
            beforeStepMethod.setAccessible(true);
        }
        afterStepMethod = findAnnotatedMethod(object.getClass(), AfterStep.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (afterStepMethod != null) {
            afterStepMethod.setAccessible(true);
        }
    }

    public void beforeStep() throws Exception {
        try {
            if (beforeStepMethod != null) {
                beforeStepMethod.invoke(object);
            }
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

    public void afterStep() throws Exception {
        try {
            if (afterStepMethod != null) {
                afterStepMethod.invoke(object);
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
