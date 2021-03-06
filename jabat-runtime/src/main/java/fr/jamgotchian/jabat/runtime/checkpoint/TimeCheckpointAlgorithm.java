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
package fr.jamgotchian.jabat.runtime.checkpoint;

import java.util.Date;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class TimeCheckpointAlgorithm extends BuiltInCheckpointAlgorithm {

    private Date startTime;

    public TimeCheckpointAlgorithm(int commitInterval) {
        super(commitInterval);
    }

    @Override
    public int checkpointTimeout(int timeout) throws Exception {
        // TODO
        return timeout;
    }

    @Override
    public void beginCheckpoint() throws Exception {
        startTime = new Date();
    }

    @Override
    public boolean isReadyToCheckpoint() throws Exception {
        return (new Date().getTime() - startTime.getTime()) > commitInterval * 1000;
    }

    @Override
    public void endCheckpoint() throws Exception {
    }

}
