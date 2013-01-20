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

/**
 * The AbstractJobListener provides default implementations of optional methods.
 */
public abstract class AbstractJobListener implements JobListener {

    /**
     * Optional method.
     *
     * Implement this method if the JobListener will do something before the job
     * begins. The default implementation does nothing.
     *
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void beforeJob() throws Exception {
    }

    /**
     * Optional method.
     *
     * Implement this method if the JobListener will do something after the job
     * ends. The default implementation does nothing.
     *
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void afterJob() throws Exception {
    }

    /**
     * Optional method.
     *
     * Implement this method if the JobListener will do something when an
     * exception is thrown during job processing.
     *
     * @param ex is the exception thrown.
     */
    @Override
    public void onException(Exception ex) {
    }
}
