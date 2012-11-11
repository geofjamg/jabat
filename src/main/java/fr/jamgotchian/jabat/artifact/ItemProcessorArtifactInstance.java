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

import fr.jamgotchian.jabat.artifact.annotated.ItemProcessorAnnotatedClass;
import java.lang.reflect.InvocationTargetException;

/**
 * @ProcessItem <output-item-type> <method-name>(<item-type> item) throws Exception
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ItemProcessorArtifactInstance extends ArtifactInstance {

    private final Class<?> inputItemType;

    private final ItemProcessorAnnotatedClass annotatedClass;
    
    public ItemProcessorArtifactInstance(Object object, Class<?> inputItemType) {
        super(object);
        this.inputItemType = inputItemType;
        annotatedClass = new ItemProcessorAnnotatedClass(object.getClass(), inputItemType);
    }

    public Class<?> getOutputItemType() {
        return annotatedClass.getProcessItemMethod().getReturnType();
    }

    public Object processItem(Object item) throws Exception {
        // TODO check item has itemType type
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
