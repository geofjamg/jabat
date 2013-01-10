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

import fr.jamgotchian.jabat.runtime.util.JabatRuntimeException;
import java.lang.reflect.InvocationTargetException;
import javax.batch.api.ItemProcessor;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ItemProcessorProxy implements ItemProcessor<Object, Object> {

    private final Object object;

    private final ItemProcessorAnnotatedClass annotatedClass;

    public ItemProcessorProxy(Object object) {
        this.object = object;
        annotatedClass = new ItemProcessorAnnotatedClass(object.getClass());
    }

    @Override
    public Object processItem(Object item) throws Exception {
        Class<?> expectedType = annotatedClass.getItemType();
        if (item.getClass() != expectedType) {
            throw new JabatRuntimeException("Bad item type " + item.getClass()
                    + ", expected " + expectedType);
        }
        try {
            return annotatedClass.getProcessItemMethod().invoke(object, item);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

}
