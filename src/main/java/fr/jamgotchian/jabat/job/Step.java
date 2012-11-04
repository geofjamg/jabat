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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public abstract class Step extends AbstractNode implements Node, Chainable, Listenable {

    private final String next;

    private final List<Artifact> listenerArtifacts;

    Step(String id, NodeContainer container, String next, Properties properties,
            List<Artifact> listenerArtifacts) {
        super(id, properties, container);
        this.next = next;
        this.listenerArtifacts = listenerArtifacts;
    }

    @Override
    public String getNext() {
        return next;
    }

    @Override
    public void addListenerArtifact(Artifact artifact) {
        listenerArtifacts.add(artifact);
    }

    @Override
    public Collection<Artifact> getListenerArtifacts() {
        return listenerArtifacts;
    }
    
    public abstract Artifact getArtifact(String ref);

}
