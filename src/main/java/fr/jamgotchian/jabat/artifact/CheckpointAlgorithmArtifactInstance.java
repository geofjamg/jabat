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
package fr.jamgotchian.jabat.artifact;

import fr.jamgotchian.jabat.artifact.annotated.CheckpointAlgorithmAnnotatedClass;
import java.lang.reflect.InvocationTargetException;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class CheckpointAlgorithmArtifactInstance extends ArtifactInstance {

    private final CheckpointAlgorithmAnnotatedClass annotatedClass;
    
    public CheckpointAlgorithmArtifactInstance(Object object) {
        super(object);
        annotatedClass = new CheckpointAlgorithmAnnotatedClass(object.getClass());
    }

    public int checkpointTimeout(int timeout) throws Exception {
        int nextTimeout = timeout;
        try {
            nextTimeout = (Integer) annotatedClass.getCheckpointTimeoutMethod().invoke(object, timeout);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
        return nextTimeout;
    }

    public void beginCheckpoint() throws Exception {
        try {
            annotatedClass.getBeginCheckpointMethod().invoke(object);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

    public boolean isReadyToCheckpoint() throws Exception {
        boolean isReady = false;
        try {
            isReady = (Boolean) annotatedClass.getIsReadyToCheckpointMethod().invoke(object);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
        return isReady;
    }

    public void endCheckpoint() throws Exception {
        try {
            annotatedClass.getEndCheckpointMethod().invoke(object);
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

}
