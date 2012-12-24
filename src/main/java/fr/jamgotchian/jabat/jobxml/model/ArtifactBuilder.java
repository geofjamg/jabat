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
package fr.jamgotchian.jabat.jobxml.model;

import fr.jamgotchian.jabat.util.JabatException;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ArtifactBuilder {

    private String ref;

    private final Properties properties = new Properties();

    public ArtifactBuilder() {
    }

    public ArtifactBuilder setRef(String ref) {
        this.ref = ref;
        return this;
    }

    public ArtifactBuilder addProperty(String name, String value) {
        properties.setProperty(name, value);
        return this;
    }

    public ArtifactBuilder addProperties(Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    public Artifact build() {
        if (ref == null) {
            throw new JabatException("Artifact ref is not set");
        }
        if (ref.isEmpty()) {
            throw new JabatException("Artifact ref is an empty string");
        }
        return new Artifact(ref, properties);
    }
}
