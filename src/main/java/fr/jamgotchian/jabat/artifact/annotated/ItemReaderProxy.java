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
import javax.batch.api.ItemReader;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ItemReaderProxy extends ResourceProxy implements ItemReader<Object> {

    private final ItemReaderAnnotatedClass annotatedClass;
    
    public ItemReaderProxy(Object object) {
        super(object);
        annotatedClass = new ItemReaderAnnotatedClass(object.getClass());
    }

    @Override
    protected ResourceAnnotatedClass getAnnotatedClass() {
        return annotatedClass;
    }

    @Override
    public Object readItem() throws Exception {
        try {
            return annotatedClass.getReadItemMethod().invoke(object);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

}
