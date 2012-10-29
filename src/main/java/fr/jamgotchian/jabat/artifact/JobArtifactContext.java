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

import java.util.ArrayList;
import java.util.List;
import javax.batch.spi.ArtifactFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobArtifactContext {

    private final ArtifactFactory factory;

    private final List<JobListenerArtifact> jobListeners = new ArrayList<JobListenerArtifact>();

    public JobArtifactContext(ArtifactFactory factory) {
        this.factory = factory;
    }

    public JobListenerArtifact createJobListener(String ref) throws Exception {
        Object obj = factory.create(ref);
        JobListenerArtifact artifact = new JobListenerArtifact(obj);
        jobListeners.add(artifact);
        return artifact;
    }

    public List<JobListenerArtifact> getJobListeners() {
        return jobListeners;
    }

    public void release() throws Exception {
        for (JobListenerArtifact artifact : jobListeners) {
            factory.destroy(artifact.getObject());
        }
    }

}
