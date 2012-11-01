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
import javax.batch.annotation.ProcessItem;

/**
 * @ProcessItem <output-item-type> <method-name>(<item-type> item) throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ProcessItemArtifact extends Artifact {

    private Method processItemMethod;

    private Class<?> itemType;

    public ProcessItemArtifact(Object object, String ref, final Class<?> itemType) {
        super(object, ref);
        this.itemType = itemType;
    }

    @Override
    public void initialize() {
        processItemMethod = findAnnotatedMethod(object.getClass(), ProcessItem.class, false, new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return m.getReturnType() != Void.TYPE
                        && hasOneParameter(m, itemType)
                        && throwsOneException(m, Exception.class);
            }
        });
        processItemMethod.setAccessible(true);
    }

    public Class<?> getOutputItemType() {
        return processItemMethod.getReturnType();
    }

    public Object processItem(Object item) throws Exception {
        // TODO check item has itemType type
        try {
            return processItemMethod.invoke(object, item);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

}
