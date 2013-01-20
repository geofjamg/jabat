/*
 * Copyright 2013 Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>.
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
package javax.batch.api;

/**
 * The AbstractCheckpointAlgorithm provides default implementations of optional
 * methods.
 */
public abstract class AbstractCheckpointAlgorithm implements
        CheckpointAlgorithm {

    /**
     * Optional method.
     *
     * Implement this method if the CheckpointAlgorithm establishes a checkpoint
     * timeout. The default implementation returns 0, which means maximum
     * permissible timeout allowed by runtime environment.
     *
     * @return the timeout interval to use for the next checkpoint interval
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public int checkpointTimeout(int timeout) throws Exception {
        return 0;
    }

    /**
     * Optional method.
     *
     * Implement this method for the CheckpointAlgorithm to do something before
     * a checkpoint begins. The default implementation does nothing.
     *
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void beginCheckpoint() throws Exception {
    }

    /**
     * Required method.
     *
     * This method implements the logic to decide if a checkpoint should be
     * taken now.
     *
     * @return boolean indicating whether or not to checkpoint now.
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public abstract boolean isReadyToCheckpoint() throws Exception;

    /**
     * Optional method.
     *
     * Implement this method for the CheckpointAlgorithm to do something after a
     * checkpoint ends. The default implementation does nothing.
     *
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void endCheckpoint() throws Exception {
    }
}