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

import fr.jamgotchian.jabat.runtime.ArtifactFactory;
import fr.jamgotchian.jabat.runtime.artifact.annotation.SplitAnalyserProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.SplitCollectorProxy;
import javax.batch.api.SplitAnalyzer;
import javax.batch.api.SplitCollector;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class SplitArtifactContext extends ArtifactContext {

    public SplitArtifactContext(ArtifactFactory factory) {
        super(factory);
    }

    public SplitCollector createSplitCollector(String ref) throws Exception {
        Object obj = create(ref);
        SplitCollector splitCollector;
        if (obj instanceof SplitCollector) {
            splitCollector = (SplitCollector) obj;
        } else {
            splitCollector = new SplitCollectorProxy(obj);
        }
        return splitCollector;
    }

    public SplitAnalyzer createSplitAnalyser(String ref) throws Exception {
        Object obj = create(ref);
        SplitAnalyzer splitAnalyser;
        if (obj instanceof SplitAnalyzer) {
            splitAnalyser = (SplitAnalyzer) obj;
        } else {
            splitAnalyser = new SplitAnalyserProxy(obj);
        }
        return splitAnalyser;
    }

}
