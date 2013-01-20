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
import java.util.List;

/**
 * The AbstractItemWriter provides default implementations of optional methods.
 *
 * @param <T> specifies the item type written by the ItemWriter.
 */
public abstract class AbstractItemWriter<T> implements ItemWriter<T> {

    /**
     * Optional method.
     *
     * Implement this method if the ItemWriter requires any open time
     * processing. The default implementation does nothing.
     *
     * @param last checkpoint for this ItemReader
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void open(Externalizable checkpoint) throws Exception {
    }

    /**
     * Optional method.
     *
     * Implement this method if the ItemWriter requires any close time
     * processing. The default implementation does nothing.
     *
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public void close() throws Exception {
    }

    /**
     * Required method.
     *
     * Implement write logic for the ItemWriter in this method.
     *
     * @param items specifies the list of items to write.
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public abstract void writeItems(List<T> items) throws Exception;

    /**
     * Optional method.
     *
     * Implement this method if the ItemWriter supports checkpoints. The default
     * implementation returns null.
     *
     * @return checkpoint data
     * @throws Exception (or subclass) if an error occurs.
     */
    @Override
    public Externalizable checkpointInfo() throws Exception {
        return null;
    }
}
