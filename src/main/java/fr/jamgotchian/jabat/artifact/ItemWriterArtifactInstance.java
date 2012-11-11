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

import fr.jamgotchian.jabat.artifact.annotated.ItemWriterAnnotatedClass;
import fr.jamgotchian.jabat.artifact.annotated.ResourceAnnotatedClass;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * @WriteItems void <method-name> (List<item-type> items) throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ItemWriterArtifactInstance extends ResourceArtifactInstance {

    private final Class<?> outputItemType;
    
    private final ItemWriterAnnotatedClass annotatedClass;

    public ItemWriterArtifactInstance(Object object, Class<?> outputItemType) {
        super(object);
        this.outputItemType = outputItemType;
        annotatedClass = new ItemWriterAnnotatedClass(object.getClass(), outputItemType);
    }

    @Override
    protected ResourceAnnotatedClass getAnnotatedClass() {
        return annotatedClass;
    }

    public void writeItems(List<Object> items) throws Exception {
        // TODO check items have itemType type
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
