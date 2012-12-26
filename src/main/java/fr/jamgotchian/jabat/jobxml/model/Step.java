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

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.batch.api.parameters.PartitionPlan;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public abstract class Step extends AbstractNode implements Chainable {

    private final String next;

    private final int startLimit;

    private final boolean allowStartIfComplete;

    private final PartitionPlan partitionPlan;

    private final Artifact partitionMapper;

    private final Artifact partitionReducer;

    private final Artifact partitionCollector;

    private final Artifact partitionAnalyzer;

    private final List<Artifact> listeners;

    private final List<ControlElement> controlElements;

    Step(String id, String next, int startLimit, boolean allowStartIfComplete,
            Properties properties, PartitionPlan partitionPlan, Artifact partitionMapper,
            Artifact partitionReducer, Artifact partitionCollector, Artifact partitionAnalyzer,
            List<Artifact> listeners, List<ControlElement> controlElements) {
        super(id, properties);
        this.next = next;
        this.startLimit = startLimit;
        this.allowStartIfComplete = allowStartIfComplete;
        this.partitionPlan = partitionPlan;
        this.partitionMapper = partitionMapper;
        this.partitionReducer = partitionReducer;
        this.partitionCollector = partitionCollector;
        this.partitionAnalyzer = partitionAnalyzer;
        this.listeners = Collections.unmodifiableList(listeners);
        this.controlElements = Collections.unmodifiableList(controlElements);
    }

    @Override
    public String getNext() {
        return next;
    }

    public int getStartLimit() {
        return startLimit;
    }

    public boolean isAllowStartIfComplete() {
        return allowStartIfComplete;
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

    public Artifact getPartitionAnalyzer() {
        return partitionAnalyzer;
    }

    public List<Artifact> getListeners() {
        return listeners;
    }

    public void addControlElement(TerminatingElement controlElement) {
        controlElements.add(controlElement);
    }

    public List<ControlElement> getControlElements() {
        return controlElements;
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
        } else if (partitionAnalyzer != null
                && partitionAnalyzer.getRef().equals(ref)) {
            return partitionAnalyzer;
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
        if (partitionAnalyzer != null) {
            artifacts.add(partitionAnalyzer);
        }
        artifacts.addAll(listeners);
    }

}
