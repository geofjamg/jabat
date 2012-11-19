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
import fr.jamgotchian.jabat.util.Setter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobBuilder {

    private String id;

    private final Properties properties = new Properties();

    private final List<Artifact> listeners = new ArrayList<Artifact>();

    public JobBuilder setId(String id) {
        this.id = id;
        return this;
    }

    public JobBuilder setProperty(String name, String value) {
        properties.setProperty(name, value);
        return this;
    }

    public ArtifactBuilder<JobBuilder> newListener() {
        return new ArtifactBuilder<JobBuilder>(this, new Setter<Artifact>() {
            @Override
            public void set(Artifact listener) {
                JobBuilder.this.listeners.add(listener);
            }
        });
    }

    public Job build() {
        if (id == null) {
            throw new JabatException("Job id is not set");
        }
        return new Job(id, properties, listeners);
    }
}
