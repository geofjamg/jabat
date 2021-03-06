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

import java.io.Externalizable;

/**
 * PartitionCollector provides a way to pass data from individual partitions to
 * a single point of control running on the step's parent thread. The
 * PartitionAnalyzer is used to receive and process this data.
 *
 */
public interface PartitionCollector {

    /**
     * The collectPartitionData method receives control periodically during
     * partition processing. This method receives control on each thread
     * processing a partition as follows: <p> <ol> <li>for a chunk type step, it
     * receives control after every chunk checkpoint and then one last time at
     * the end of the partition;</li> <li>for a batchlet type step, it receives
     * control once at the end of the batchlet.</li> </ol>
     *
     * @return an Externalizable object to pass to the PartitionAnalyzer.
     * @throws Exception is thrown if an error occurs.
     */
    Externalizable collectPartitionData() throws Exception;
}
