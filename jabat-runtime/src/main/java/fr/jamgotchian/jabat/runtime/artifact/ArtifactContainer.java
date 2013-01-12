/*
 * Copyright 2013 Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>.
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
package fr.jamgotchian.jabat.runtime.artifact;

import com.google.common.collect.Iterables;
import fr.jamgotchian.jabat.runtime.context.ThreadContext;
import fr.jamgotchian.jabat.runtime.util.JabatRuntimeException;
import java.util.ArrayList;
import java.util.List;

/**
 * A container for managing artifacts lifecycle.
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ArtifactContainer {

    private final BatchXml batchXml;

    private final ArtifactFactory factory;

    private final List<Object> managedObjects = new ArrayList<Object>();

    private final List<Object> objects = new ArrayList<Object>();

    public ArtifactContainer(BatchXml batchXml, ArtifactFactory factory) {
        this.batchXml = batchXml;
        this.factory = factory;
    }

    private Object createFromBatchXml(String name, Class<?> type) {
        String className = batchXml.getArtifactClass(name);
        Object obj = null;
        if (className != null) {
            Class<?> clazz;
            try {
                clazz = Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new JabatRuntimeException("Batch artifact class '"
                        + className + "' not found");
            }
            if (!type.isAssignableFrom(clazz)) {
                throw new JabatRuntimeException("Expected artifact type is "
                        + type.getName() + ", instead of " + clazz.getName());
            }
            try {
                obj = clazz.newInstance();
                objects.add(obj);
                ThreadContext.getInstance().inject(obj, name);
            } catch (ReflectiveOperationException e) {
                throw new JabatRuntimeException(e);
            }
        }
        return obj;
    }

    private Object createFromDiFramework(String name, Class<?> type) {
        Object obj = factory.create(name);
        if (obj != null) {
            managedObjects.add(obj);
            if (!type.isAssignableFrom(obj.getClass())) {
                throw new JabatRuntimeException("Expected artifact type is "
                        + type.getName() + ", instead of " + obj.getClass().getName());
            }
        }
        return obj;
    }

    public <T> T create(String name, Class<T> type) throws Exception {
        if (!ArtifactType.isArtifactType(type)) {
            throw new JabatRuntimeException(type.getName()
                    + " is not a batch artifact type");
        }
        Object obj;
        if (factory != null) {
            obj = createFromDiFramework(name, type);
            if (obj == null) {
                obj = createFromBatchXml(name, type);
            }
        } else {
            obj = createFromBatchXml(name, type);
        }
        if (obj == null) {
            throw new JabatRuntimeException("Batch artifact '" + name
                    + "' not found");
        }
        return (T) obj;
    }

    public <T> Iterable<T> get(Class<T> type) {
        List<T> result = new ArrayList<T>();
        for (Object obj : Iterables.concat(objects, managedObjects)) {
            if (type.isAssignableFrom(obj.getClass())) {
                result.add((T) obj);
            }
        }
        return result;
    }

    public void release() throws Exception {
        for (Object obj : managedObjects) {
            factory.destroy(obj);
        }
        objects.clear();
        managedObjects.clear();
    }

}
