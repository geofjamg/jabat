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

import fr.jamgotchian.jabat.artifact.annotated.StepListenerProxy;
import java.util.ArrayList;
import java.util.List;
import javax.batch.spi.ArtifactFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class StepArtifactContext extends ArtifactContext {

    private final List<StepListener> stepListeners = new ArrayList<StepListener>();

    public StepArtifactContext(ArtifactFactory factory) {
        super(factory);
    }

    public StepListener createStepListener(String ref) throws Exception {
        Object obj = create(ref);
        StepListenerProxy proxy = new StepListenerProxy(obj);
        stepListeners.add(proxy);
        return proxy;
    }

    public List<StepListener> getStepListeners() {
        return stepListeners;
    }

    @Override
    public void release() throws Exception {
        super.release();
        stepListeners.clear();
    }

}
