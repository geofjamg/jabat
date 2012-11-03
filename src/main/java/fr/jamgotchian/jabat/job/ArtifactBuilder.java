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
package fr.jamgotchian.jabat.job;

import fr.jamgotchian.jabat.util.JabatException;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ArtifactBuilder<P> {

    private final P parent;

    private final Setter<Artifact> setter;
    
    private String ref;
        
    private final Properties properties = new Properties();

    public ArtifactBuilder(P parent, Setter<Artifact> setter) {
        this.parent = parent;
        this.setter = setter;
    }

    public ArtifactBuilder<P> setRef(String ref) {
        this.ref = ref;
        return this;
    }

    public ArtifactBuilder<P> setProperty(String name, String value) {
        properties.setProperty(name, value);
        return this;
    }

    public P build() {
        if (ref == null) {
            throw new JabatException("Artifact ref is not set");
        }
        setter.set(new Artifact(ref, properties));
        return parent;
    }
}
