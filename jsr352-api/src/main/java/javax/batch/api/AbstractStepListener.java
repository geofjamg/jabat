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
 * The AbstractStepListener provides default implementations of optional
 * methods.
 */
public abstract class AbstractStepListener implements StepListener {

    /**
     * Optional method.
     *
     * Implement this method if the StepListener will do something before the
     * step begins. The default implementation does nothing.
     *
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void beforeStep() throws Exception {
    }

    /**
     * Optional method.
     *
     * Implement this method if the StepListener will do something after the
     * step ends. The default implementation does nothing.
     *
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void afterStep() throws Exception {
    }

    /**
     * Optional method.
     *
     * Implement this method if the StepListener will do something when an
     * exception is thrown during step processing.
     *
     * @param ex is the exception thrown.
     */
    @Override
    public void onException(Exception ex) {
    }
}
