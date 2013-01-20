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
package javax.batch.runtime;

import java.util.Date;
import java.util.Properties;

public interface JobExecution {

    /**
     * Get id for this JobExecution.
     *
     * @return execution id
     */
    long getId();

    /**
     * Get batch status of this execution.
     *
     * @return batch status value.
     */
    BatchStatus getBatchStatus();

    /**
     * Get time execution entered STARTED status.
     *
     * @return date (time)
     */
    Date getStartTime();

    /**
     * Get time execution entered end status: COMPLETED, STOPPED, FAILED
     *
     * @return date (time)
     */
    Date getEndTime();

    /**
     * Get execution exit status.
     *
     * @return exit status.
     */
    String getExitStatus();

    /**
     * Get time execution was created.
     *
     * @return date (time)
     */
    Date getCreateTime();

    /**
     * Get time execution was last updated updated.
     *
     * @return date (time)
     */
    Date getLastUpdatedTime();

    /**
     * Get job parameters for this execution.
     *
     * @return job parameters
     */
    Properties getJobParameters();
}
