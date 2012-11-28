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
import java.util.List;
import java.util.Properties;
import javax.batch.api.parameters.PartitionPlan;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public abstract class Step extends AbstractNode implements Node, Chainable {

    private final String next;

    private PartitionPlan partitionPlan;

    private Artifact partitionMapper;

    private Artifact partitionReducer;

    private Artifact partitionCollector;

    private Artifact partitionAnalyser;

    private final List<Artifact> listeners;

    private final List<TerminatingElement> terminatingElements = new ArrayList<TerminatingElement>();

    Step(String id, NodeContainer container, String next, Properties properties,
         List<Artifact> listeners) {
        super(id, properties, container);
        this.next = next;
        this.listeners = listeners;
    }

    @Override
    public String getNext() {
        return next;
    }

    public PartitionPlan getPartitionPlan() {
        return partitionPlan;
    }

    public void setPartitionPlan(PartitionPlan partitionPlan) {
        this.partitionPlan = partitionPlan;
    }

    public Artifact getPartitionMapper() {
        return partitionMapper;
    }

    public void setPartitionMapper(Artifact partitionMapper) {
        this.partitionMapper = partitionMapper;
    }

    public Artifact getPartitionReducer() {
        return partitionReducer;
    }

    public void setPartitionReducer(Artifact partitionReducer) {
        this.partitionReducer = partitionReducer;
    }

    public Artifact getPartitionCollector() {
        return partitionCollector;
    }

    public void setPartitionCollector(Artifact partitionCollector) {
        this.partitionCollector = partitionCollector;
    }

    public Artifact getPartitionAnalyser() {
        return partitionAnalyser;
    }

    public void setPartitionAnalyser(Artifact partitionAnalyser) {
        this.partitionAnalyser = partitionAnalyser;
    }

    public void addListener(Artifact listener) {
        listeners.add(listener);
    }

    public List<Artifact> getListeners() {
        return listeners;
    }

    public void addTerminatingElement(TerminatingElement ctrlElt) {
        terminatingElements.add(ctrlElt);
    }

    public List<TerminatingElement> getTerminatingElements() {
        return terminatingElements;
    }

    @Override
    public Artifact getArtifact(String ref) {
        if (partitionMapper != null
                && partitionMapper.getRef().equals(ref)) {
            return partitionMapper;
        } else if (partitionReducer != null
                && partitionReducer.getRef().equals(ref)) {
            return partitionReducer;
        } else if (partitionCollector != null
                && partitionCollector.getRef().equals(ref)) {
            return partitionCollector;
        } else if (partitionAnalyser != null
                && partitionAnalyser.getRef().equals(ref)) {
            return partitionAnalyser;
        } else {
            for (Artifact listener : listeners) {
                if (listener.getRef().equals(ref)) {
                    return listener;
                }
            }
        }
        return null;
    }

    protected void getArtifacts(List<Artifact> artifacts) {
        if (partitionMapper != null) {
            artifacts.add(partitionMapper);
        }
        if (partitionReducer != null) {
            artifacts.add(partitionReducer);
        }
        if (partitionCollector != null) {
            artifacts.add(partitionCollector);
        }
        if (partitionAnalyser != null) {
            artifacts.add(partitionAnalyser);
        }
        artifacts.addAll(listeners);
    }

}
