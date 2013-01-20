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

import java.util.List;

/**
 * The AbstractItemWriteListener provides default implementations of optional
 * methods.
 *
 * @param <T> specifies the item type written by the ItemWriter paired with this
 * ItemWriteListener.
 */
public abstract class AstractItemWriteListener<T> implements
        ItemWriteListener<T> {

    /**
     * Optional method.
     *
     * Implement this method if the ItemWriteListener will do something before
     * the items are written. The default implementation does nothing.
     *
     * @param items specifies the items about to be written.
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void beforeWrite(List<T> items) throws Exception {
    }

    /**
     * Optional method.
     *
     * Implement this method if the ItemWriteListener will do something after
     * the items are written. The default implementation does nothing.
     *
     * @param items specifies the items about to be written.
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void afterWrite(List<T> items) throws Exception {
    }

    /**
     * Optional method.
     *
     * Implement this method if the ItemWriteListener will do something when the
     * ItemWriter writeItems method throws an exception. The default
     * implementation does nothing.
     *
     * @param items specifies the items about to be written.
     * @param ex specifies the exception thrown by the item writer.
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void onWriteError(List<T> items, Exception ex) throws Exception {
    }
}
