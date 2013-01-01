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
package fr.jamgotchian.jabat.runtime.artifact.context;

import fr.jamgotchian.jabat.runtime.artifact.ArtifactFactory;
import fr.jamgotchian.jabat.runtime.artifact.annotation.BatchletProxy;
import javax.batch.api.Batchlet;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchletArtifactContext extends StepArtifactContext {

    public BatchletArtifactContext(ArtifactFactory factory) {
        super(factory);
    }

    public Batchlet createBatchlet(String ref) throws Exception {
        Object obj = create(ref);
        if (obj instanceof Batchlet) {
            return (Batchlet) obj;
        } else {
            return new BatchletProxy(obj);
        }
    }

}
