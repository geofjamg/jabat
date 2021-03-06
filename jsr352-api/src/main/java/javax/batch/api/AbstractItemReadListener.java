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
 * The AbstractItemReadListener provides default implementations of optional
 * methods.
 *
 * @param <T> specifies the item type read by the ItemReader paired with this
 * ItemReadListener.
 */
public abstract class AbstractItemReadListener<T> implements
        ItemReadListener<T> {

    /**
     * Optional method.
     *
     * Implement this method if the ItemReadListener will do something before
     * the item is read. The default implementation does nothing.
     *
     *
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void beforeRead() throws Exception {
    }

    /**
     * Optional method.
     *
     * Implement this method if the ItemReadListener will do something after the
     * item is read. The default implementation does nothing.
     *
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void afterRead(T item) throws Exception {
    }

    /**
     * Optional method.
     *
     * Implement this method if the ItemReadListener will do something when the
     * ItemReader readItem method throws an exception. The default
     * implementation does nothing.
     *
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void onReadError(Exception ex) throws Exception {
    }
}
