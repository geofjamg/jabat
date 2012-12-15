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
public class DecisionBuilder {

    private String id;

    private Properties properties = new Properties();

    private Artifact artifact;

    public DecisionBuilder() {
    }

    public DecisionBuilder setId(String id) {
        this.id = id;
        return this;
    }

    public DecisionBuilder setProperty(String name, String value) {
        properties.setProperty(name, value);
        return this;
    }

    public DecisionBuilder setArtifact(Artifact artifact) {
        this.artifact = artifact;
        return this;
    }

    public Decision build() {
        if (id == null) {
            throw new JabatException("Decision id is not set");
        }
        if (artifact == null) {
            throw new JabatException("Decision artifact is not set");
        }
        return new Decision(id, properties, artifact);
    }
}
