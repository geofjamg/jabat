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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.batch.api.parameters.PartitionPlan;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public abstract class Step extends AbstractNode implements Chainable {

    private final String next;

    private final PartitionPlan partitionPlan;

    private final Artifact partitionMapper;

    private final Artifact partitionReducer;

    private final Artifact partitionCollector;

    private final Artifact partitionAnalyser;

    private final List<Artifact> listeners;

    private final List<TerminatingElement> terminatingElements = new ArrayList<TerminatingElement>();

    Step(String id, String next, Properties properties, PartitionPlan partitionPlan,
         Artifact partitionMapper, Artifact partitionReducer, Artifact partitionCollector,
         Artifact partitionAnalyser, List<Artifact> listeners) {
        super(id, properties);
        this.next = next;
        this.partitionPlan = partitionPlan;
        this.partitionMapper = partitionMapper;
        this.partitionReducer = partitionReducer;
        this.partitionCollector = partitionCollector;
        this.partitionAnalyser = partitionAnalyser;
        this.listeners = listeners;
    }

    @Override
    public String getNext() {
        return next;
    }

    public PartitionPlan getPartitionPlan() {
        return partitionPlan;
    }

    public Artifact getPartitionMapper() {
        return partitionMapper;
    }

    public Artifact getPartitionReducer() {
        return partitionReducer;
    }

    public Artifact getPartitionCollector() {
        return partitionCollector;
    }

    public Artifact getPartitionAnalyser() {
        return partitionAnalyser;
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
