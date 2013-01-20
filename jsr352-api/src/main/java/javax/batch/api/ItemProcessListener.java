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
 * ItemProcessListener intercepts item processing.
 *
 * @param <T> specifies the type processed by an item processor.
 * @param <R> specifies the type returned by an item processor.
 */
public interface ItemProcessListener<T, R> {

    /**
     * The beforeProcess method receives control before an item processor is
     * called to process the next item. The method receives the item to be
     * processed as an input.
     *
     * @param item specifies the item about to be processed.
     * @throws Exception if an error occurs.
     */
    void beforeProcess(T item) throws Exception;

    /**
     * The afterProcess method receives control after an item processor
     * processes an item. The method receives the item processed and the result
     * item as an input.
     *
     * @param item specifies the item about to be processed.
     * @param result specifies the item to pass to the item writer.
     * @throws Exception if an error occurs.
     */
    void afterProcess(T item, R result) throws Exception;

    /**
     * The onProcessError a method receives control after an item processor
     * processItem method throws an exception. This method receives the
     * exception and the input item.
     *
     * @param item specifies the item about to be processed.
     * @param ex specifies the exception thrown by the item processor.
     * @throws Exception
     */
    void onProcessError(T item, Exception ex) throws Exception;
}
