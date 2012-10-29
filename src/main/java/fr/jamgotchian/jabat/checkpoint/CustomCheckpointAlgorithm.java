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
package fr.jamgotchian.jabat.checkpoint;

import fr.jamgotchian.jabat.artifact.CheckpointAlgorithmArtifact;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class CustomCheckpointAlgorithm implements CheckpointAlgorithm {

    private final CheckpointAlgorithmArtifact artifact;

    public CustomCheckpointAlgorithm(CheckpointAlgorithmArtifact artifact) {
        this.artifact = artifact;
    }

    @Override
    public int checkpointTimeout(int timeout) throws Exception {
        return artifact.checkpointTimeout(timeout);
    }

    @Override
    public void beginCheckpoint() throws Exception {
        artifact.beginCheckpoint();
    }

    @Override
    public boolean isReadyToCheckpoint() throws Exception {
        return artifact.isReadyToCheckpoint();
    }

    @Override
    public void endCheckpoint() throws Exception {
        artifact.endCheckpoint();
    }

}
