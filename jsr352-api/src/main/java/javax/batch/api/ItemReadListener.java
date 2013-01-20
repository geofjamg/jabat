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
 * ItemReadListener intercepts item reader processing.
 *
 * @param <T> specifies the type processed by an item reader.
 */
public interface ItemReadListener<T> {

    /**
     * The beforeRead method receives control before an item reader is called to
     * read the next item.
     *
     * @throws Exception is thrown if an error occurs.
     */
    void beforeRead() throws Exception;

    /**
     * The afterRead method receives control after an item reader reads an item.
     * The method receives the item read as an input.
     *
     * @param item specifies the item read by the item reader.
     * @throws Exception is thrown if an error occurs.
     */
    void afterRead(T item) throws Exception;

    /**
     * The onReadError method receives control after an item reader throws an
     * exception in the readItem method. This method receives the exception as
     * an input.
     *
     * @param ex specifies the exception that occurred in the item reader.
     * @throws Exception is thrown if an error occurs.
     */
    void onReadError(Exception ex) throws Exception;
}
