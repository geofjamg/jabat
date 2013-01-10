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
import java.lang.reflect.Type;
import java.util.List;
import javax.batch.api.ItemWriter;

/**
 * @WriteItems void <method-name> (List<item-type> items) throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ItemWriterProxy extends ResourceProxy implements ItemWriter<Object> {

    private final ItemWriterAnnotatedClass annotatedClass;

    public ItemWriterProxy(Object object) {
        super(object);
        annotatedClass = new ItemWriterAnnotatedClass(object.getClass());
    }

    @Override
    protected ResourceAnnotatedClass getAnnotatedClass() {
        return annotatedClass;
    }

    @Override
    public void writeItems(List<Object> items) throws Exception {
        Type expectedType = annotatedClass.getItemType();
        for (Object item : items) {
            if (item.getClass() != expectedType) {
                throw new JabatRuntimeException("Bad item type " + item.getClass()
                        + ", expected " + expectedType);
            }
        }
        try {
            annotatedClass.getWriteItemsMethod().invoke(object, items);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

}
