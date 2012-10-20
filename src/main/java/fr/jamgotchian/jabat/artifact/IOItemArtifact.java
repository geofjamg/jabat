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
import java.io.Externalizable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import javax.batch.annotation.Close;
import javax.batch.annotation.Open;

/**
 * @Open void <method-name>(Externalizable checkpoint) throws Exception
 * @Close void <method-name>() throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class IOItemArtifact {

    protected final Object artifact;

    private final Method openMethod;

    private final Method closeMethod;

    public IOItemArtifact(Object artifact) {
        this.artifact = artifact;
        openMethod = findAnnotatedMethod(artifact.getClass(), Open.class, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return m.getReturnType() == Void.TYPE
                        && hasOneParameter(m, Externalizable.class)
                        && throwsOneException(m, Exception.class);
            }
        });
        openMethod.setAccessible(true);
        closeMethod = findAnnotatedMethod(artifact.getClass(), Close.class, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return m.getReturnType() == Void.TYPE
                        && hasZeroParameter(m)
                        && throwsOneException(m, Exception.class);
            }
        });
        closeMethod.setAccessible(true);
    }

    public void open(Externalizable checkpoint) throws Exception {
        try {
            openMethod.invoke(artifact, checkpoint);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

    public void close() throws Exception {
        try {
            closeMethod.invoke(artifact);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

}
