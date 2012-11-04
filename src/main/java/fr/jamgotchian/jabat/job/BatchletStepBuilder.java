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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchletStepBuilder<P> {

    private final P parent;
    
    private final NodeContainer container;

    private final Setter<BatchletStep> setter;
    
    private String id;
    
    private String next;
    
    private final Properties properties = new Properties();

    private final List<Artifact> listenerArtifacts = new ArrayList<Artifact>();
    
    private Artifact artifact;
    
    public BatchletStepBuilder(P parent, NodeContainer container, Setter<BatchletStep> setter) {
        this.parent = parent;
        this.container = container;
        this.setter = setter;
    }

    public BatchletStepBuilder<P> setId(String id) {
        this.id = id;
        return this;
    }

    public BatchletStepBuilder<P> setNext(String next) {
        this.next = next;
        return this;
    }

    public BatchletStepBuilder<P> setProperty(String name, String value) {
        properties.setProperty(name, value);
        return this;
    }

    public ArtifactBuilder<BatchletStepBuilder<P>> newArtifact() {
        return new ArtifactBuilder<BatchletStepBuilder<P>>(this, new Setter<Artifact>() {
            @Override
            public void set(Artifact artifact) {
                BatchletStepBuilder.this.artifact = artifact;
            }
        });
    }
    
    public P build() {
        if (id == null) {
            throw new JabatException("Batchlet id is not set");
        }
        if (artifact == null) {
            throw new JabatException("Batchlet artifact is not set");            
        }
        setter.set(new BatchletStep(id, container, next, properties, listenerArtifacts, artifact));
        return parent;
    }
}
