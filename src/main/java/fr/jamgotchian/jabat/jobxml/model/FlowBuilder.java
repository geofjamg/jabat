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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class FlowBuilder {

    private String id;

    private String next;

    private final Properties properties = new Properties();

    private final List<Artifact> listeners = new ArrayList<Artifact>();

    private final List<AbstractNode> nodes = new ArrayList<AbstractNode>();

    public FlowBuilder() {
    }

    public FlowBuilder setId(String id) {
        this.id = id;
        return this;
    }

    public FlowBuilder setNext(String next) {
        this.next = next;
        return this;
    }

    public FlowBuilder addProperty(String name, String value) {
        properties.setProperty(name, value);
        return this;
    }

    public FlowBuilder addProperties(Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    public FlowBuilder addListener(Artifact listener) {
        listeners.add(listener);
        return this;
    }

    public FlowBuilder addListeners(List<Artifact> listeners) {
        this.listeners.addAll(listeners);
        return this;
    }

    public FlowBuilder addStep(Step step) {
        nodes.add(step);
        return this;
    }

    public FlowBuilder addSplit(Split split) {
        nodes.add(split);
        return this;
    }

    public FlowBuilder addDecision(Decision decision) {
        nodes.add(decision);
        return this;
    }

    public Flow build() {
        if (id == null) {
            throw new JabatException("Flow id is not set");
        }
        JobCheckUtil.checkIdUnicity(nodes);
        JobCheckUtil.checkNotAssociated(nodes);
        return new Flow(id, properties, nodes, next, listeners);
    }
}
