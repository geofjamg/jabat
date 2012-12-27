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

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchletStepBuilder extends StepBuilder<BatchletStepBuilder, BatchletStep> {

    private Artifact artifact;

    public BatchletStepBuilder() {
    }

    @Override
    protected BatchletStepBuilder getBuilder() {
        return this;
    }

    public BatchletStepBuilder setArtifact(Artifact artifact) {
        this.artifact = artifact;
        return this;
    }

    @Override
    public BatchletStep build() {
        check();
        if (artifact == null) {
            throw new JabatException("Batchlet artifact is not set");
        }
        return new BatchletStep(id, next, startLimit, allowStartIfComplete, properties,
                                partitionPlan, partitionMapper, partitionReducer,
                                partitionCollector, partitionAnalyser,
                                listeners, controlElements, artifact);
    }
}
