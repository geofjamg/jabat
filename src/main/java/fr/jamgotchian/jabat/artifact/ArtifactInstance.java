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

/**
 * This is the base class for all artifact instance wrapper.
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public abstract class ArtifactInstance {

    protected final Object object;

    protected final String ref;

    protected ArtifactInstance(Object object, String ref) {
        this.object = object;
        this.ref = ref;
    }

    public abstract void initialize();

    public Object getObject() {
        return object;
    }

    public String getRef() {
        return ref;
    }

}