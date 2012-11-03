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
import javax.batch.annotation.Process;
import javax.batch.annotation.Stop;

/**
 * @Process String <method-name> () throws Exception
 * @Stop void <method-name> () throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchletArtifactInstance extends ArtifactInstance {

    private Method processMethod;

    private Method stopMethod;

    public BatchletArtifactInstance(Object object, String ref) {
        super(object, ref);
    }

    @Override
    public void initialize() {
        processMethod = findAnnotatedMethod(object.getClass(), Process.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, String.class)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        processMethod.setAccessible(true);
        stopMethod = findAnnotatedMethod(object.getClass(), Stop.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return hasReturnType(m, Void.TYPE)
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        stopMethod.setAccessible(true);
    }

    public String process() throws Exception {
        String exitStatus = null;
        try {
            exitStatus = (String) processMethod.invoke(object);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
        return exitStatus;
    }

    public void stop() throws Exception {
        try {
            stopMethod.invoke(object);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

}
