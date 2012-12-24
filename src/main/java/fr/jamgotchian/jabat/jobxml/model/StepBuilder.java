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
import javax.batch.api.parameters.PartitionPlan;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public abstract class StepBuilder<B extends StepBuilder, S extends Step> {

    protected String id;

    protected String next;

    protected Properties properties = new Properties();

    protected List<Artifact> listeners = new ArrayList<Artifact>();

    protected PartitionPlan partitionPlan;

    protected Artifact partitionMapper;

    protected Artifact partitionReducer;

    protected Artifact partitionCollector;

    protected Artifact partitionAnalyser;

    protected final List<TerminatingElement> terminatingElements
            = new ArrayList<TerminatingElement>();

    protected StepBuilder() {
    }

    protected abstract B getBuilder();

    public B setId(String id) {
        this.id = id;
        return getBuilder();
    }

    public B setNext(String next) {
        this.next = next;
        return getBuilder();
    }

    public B addProperty(String name, String value) {
        properties.setProperty(name, value);
        return getBuilder();
    }

    public B addProperties(Properties properties) {
        this.properties.putAll(properties);
        return getBuilder();
    }

    public B addListener(Artifact listener) {
        listeners.add(listener);
        return getBuilder();
    }

    public B addListeners(List<Artifact> listeners) {
        this.listeners.addAll(listeners);
        return getBuilder();
    }

    public B setPartitionPlan(PartitionPlan partitionPlan) {
        this.partitionPlan = partitionPlan;
        return getBuilder();
    }

    public B setPartitionMapper(Artifact partitionMapper) {
        this.partitionMapper = partitionMapper;
        return getBuilder();
    }

    public B setPartitionReducer(Artifact partitionReducer) {
        this.partitionReducer = partitionReducer;
        return getBuilder();
    }

    public B setPartitionCollector(Artifact partitionCollector) {
        this.partitionCollector = partitionCollector;
        return getBuilder();
    }

    public B setPartitionAnalyzer(Artifact partitionAnalyser) {
        this.partitionAnalyser = partitionAnalyser;
        return getBuilder();
    }

    public B addTerminatingElement(TerminatingElement terminatingElement) {
        terminatingElements.add(terminatingElement);
        return getBuilder();
    }

    protected void check() {
        if (id == null) {
            throw new JabatException("Chunk id is not set");
        }
    }

    public abstract S build();
}
