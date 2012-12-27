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
public class JobBuilder {

    private String id;

    private final Properties properties = new Properties();

    private final List<AbstractNode> nodes = new ArrayList<AbstractNode>();

    private final List<Artifact> listeners = new ArrayList<Artifact>();

    private boolean restartable = false;

    public JobBuilder setId(String id) {
        this.id = id;
        return this;
    }

    public JobBuilder addProperty(String name, String value) {
        properties.setProperty(name, value);
        return this;
    }

    public JobBuilder addProperties(Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    public JobBuilder addListener(Artifact listener) {
        listeners.add(listener);
        return this;
    }

    public JobBuilder addListeners(List<Artifact> listeners) {
        this.listeners.addAll(listeners);
        return this;
    }

    public JobBuilder setRestartable(boolean restartable) {
        this.restartable = restartable;
        return this;
    }

    public JobBuilder addStep(Step step) {
        nodes.add(step);
        return this;
    }


    public JobBuilder addFlow(Flow flow) {
        nodes.add(flow);
        return this;
    }

    public JobBuilder addSplit(Split split) {
        nodes.add(split);
        return this;
    }

    public JobBuilder addDecision(Decision decision) {
        nodes.add(decision);
        return this;
    }

    public Job build() {
        if (id == null) {
            throw new JabatException("Job id is not set");
        }
        JobCheckUtil.checkIdUnicity(nodes);
        JobCheckUtil.checkNotAssociated(nodes);
        return new Job(id, properties, nodes, listeners, restartable);
    }
}
