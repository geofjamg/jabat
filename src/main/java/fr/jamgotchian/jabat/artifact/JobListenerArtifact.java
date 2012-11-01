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
import javax.batch.annotation.AfterJob;
import javax.batch.annotation.BeforeJob;

/**
 * @BeforeJob void <method-name> () throws Exception
 * @AfterJob void <method-name> () throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobListenerArtifact extends Artifact {

    private Method beforeJobMethod;

    private Method afterJobMethod;

    public JobListenerArtifact(Object object, String name) {
        super(object, name);
    }

    @Override
    public void initialize() {
        beforeJobMethod = findAnnotatedMethod(object.getClass(), BeforeJob.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (beforeJobMethod != null) {
            beforeJobMethod.setAccessible(true);
        }
        afterJobMethod = findAnnotatedMethod(object.getClass(), AfterJob.class, true, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        if (afterJobMethod != null) {
            afterJobMethod.setAccessible(true);
        }
    }

    public void beforeJob() throws Exception {
        try {
            if (beforeJobMethod != null) {
                beforeJobMethod.invoke(object);
            }
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

    public void afterJob() throws Exception {
        try {
            if (afterJobMethod != null) {
                afterJobMethod.invoke(object);
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
