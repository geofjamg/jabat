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

import javax.batch.runtime.JobOperator;

/**
 * The BatchRuntime represents the batch runtime environment.
 *
 */
public class BatchRuntime {

    /**
     * The getJobOperator factory method returns an instance of the JobOperator
     * interface. Repeated calls to this method returns the same instance.
     *
     * @return JobOperator instance.
     */
    public static JobOperator getJobOperator() {
        return null;
    }
}
