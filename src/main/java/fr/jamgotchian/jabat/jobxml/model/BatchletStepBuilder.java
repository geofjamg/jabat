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
public class BatchletStepBuilder {

    private String id;

    private String next;

    private Properties properties = new Properties();

    private List<Artifact> listeners = new ArrayList<Artifact>();

    private PartitionPlan partitionPlan;

    private Artifact partitionMapper;

    private Artifact partitionReducer;

    private Artifact partitionCollector;

    private Artifact partitionAnalyser;

    private Artifact artifact;

    public BatchletStepBuilder() {
    }

    public BatchletStepBuilder setId(String id) {
        this.id = id;
        return this;
    }

    public BatchletStepBuilder setNext(String next) {
        this.next = next;
        return this;
    }

    public BatchletStepBuilder setProperty(String name, String value) {
        properties.setProperty(name, value);
        return this;
    }

    public BatchletStepBuilder setProperties(Properties properties) {
        if (properties == null) {
            this.properties = new Properties();
        } else {
            this.properties = properties;
        }
        return this;
    }

    public BatchletStepBuilder addListener(Artifact listener) {
        listeners.add(listener);
        return this;
    }

    public BatchletStepBuilder setListeners(List<Artifact> listeners) {
        if (listeners == null) {
            this.listeners = new ArrayList<Artifact>();
        } else {
            this.listeners = listeners;
        }
        return this;
    }

    public BatchletStepBuilder setPartitionPlan(PartitionPlan partitionPlan) {
        this.partitionPlan = partitionPlan;
        return this;
    }

    public BatchletStepBuilder setPartitionMapper(Artifact partitionMapper) {
        this.partitionMapper = partitionMapper;
        return this;
    }

    public BatchletStepBuilder setPartitionReducer(Artifact partitionReducer) {
        this.partitionReducer = partitionReducer;
        return this;
    }

    public BatchletStepBuilder setPartitionCollector(Artifact partitionCollector) {
        this.partitionCollector = partitionCollector;
        return this;
    }

    public BatchletStepBuilder setPartitionAnalyser(Artifact partitionAnalyser) {
        this.partitionAnalyser = partitionAnalyser;
        return this;
    }

    public BatchletStepBuilder setArtifact(Artifact artifact) {
        this.artifact = artifact;
        return this;
    }

    public BatchletStep build() {
        if (id == null) {
            throw new JabatException("Batchlet id is not set");
        }
        if (artifact == null) {
            throw new JabatException("Batchlet artifact is not set");
        }
        return new BatchletStep(id, next, properties, partitionPlan, partitionMapper,
                                partitionReducer, partitionCollector, partitionAnalyser,
                                listeners, artifact);
    }
}
