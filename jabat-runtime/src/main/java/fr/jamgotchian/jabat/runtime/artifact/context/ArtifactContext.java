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
package fr.jamgotchian.jabat.runtime.artifact.context;

import fr.jamgotchian.jabat.runtime.artifact.ArtifactFactory;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public abstract class ArtifactContext {

    private final ArtifactFactory factory;

    private final List<Object> objects = new ArrayList<Object>();

    public ArtifactContext(ArtifactFactory factory) {
        this.factory = factory;
    }

    protected Object create(String ref) throws Exception {
        Object obj = factory.create(ref);
        objects.add(obj);
        return obj;
    }

    public void release() throws Exception {
        for (Object obj : objects) {
            factory.destroy(obj);
        }
        objects.clear();
    }
}
