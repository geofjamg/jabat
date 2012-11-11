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
public class JobArtifactContext extends ArtifactContext {

    private final List<JobListenerArtifactInstance> jobListeners 
            = new ArrayList<JobListenerArtifactInstance>();

    public JobArtifactContext(ArtifactFactory factory) {
        super(factory);
    }

    public JobListenerArtifactInstance createJobListener(String ref) throws Exception {
        Object obj = create(ref);
        JobListenerArtifactInstance instance = new JobListenerArtifactInstance(obj);
        jobListeners.add(instance);
        return instance;
    }

    public List<JobListenerArtifactInstance> getJobListeners() {
        return jobListeners;
    }

    @Override
    public void release() throws Exception {
        super.release();
        jobListeners.clear();
    }

}
