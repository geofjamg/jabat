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

import fr.jamgotchian.jabat.jobxml.util.JobXmlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class SplitBuilder {

    private String id;

    private String next;

    private final Properties properties = new Properties();

    private final List<Artifact> listeners = new ArrayList<Artifact>();

    private final List<Flow> flows = new ArrayList<Flow>();

    public SplitBuilder() {
    }

    public SplitBuilder setId(String id) {
        this.id = id;
        return this;
    }

    public SplitBuilder setNext(String next) {
        this.next = next;
        return this;
    }

    public SplitBuilder addProperty(String name, String value) {
        properties.setProperty(name, value);
        return this;
    }

    public SplitBuilder addProperties(Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    public SplitBuilder addListener(Artifact listener) {
        listeners.add(listener);
        return this;
    }

    public SplitBuilder addListeners(List<Artifact> listeners) {
        this.listeners.addAll(listeners);
        return this;
    }

    public SplitBuilder addFlow(Flow flow) {
        flows.add(flow);
        return this;
    }

    public Split build() {
        if (id == null) {
            throw new JobXmlException("Split id is not set");
        }
        if (flows.size() < 2) {
            throw new JobXmlException("A Split is supposed to contain at least 2 flows");
        }
        JobCheckUtil.checkIdUnicity(flows);
        JobCheckUtil.checkNotAssociated(flows);
        return new Split(id, properties, flows, next, listeners);
    }
}
