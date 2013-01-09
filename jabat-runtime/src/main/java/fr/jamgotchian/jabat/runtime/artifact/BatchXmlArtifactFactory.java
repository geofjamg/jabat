/*
 * Copyright 2012 Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>.
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
package fr.jamgotchian.jabat.runtime.artifact;

import fr.jamgotchian.jabat.runtime.context.JabatThreadContext;
import fr.jamgotchian.jabat.runtime.util.JabatRuntimeException;
import java.io.InputStream;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchXmlArtifactFactory implements ArtifactFactory {

    private BatchXml batchXml;

    @Override
    public void initialize() {
        InputStream is = getClass().getResourceAsStream("/META-INF/batch.xml");
        if (is != null) {
            batchXml = new BatchXmlParser().parse(is);
        } else {
            batchXml = new BatchXml();
        }
    }

    @Override
    public Object create(String name) {
        Class<?> clazz = batchXml.getArtifactClass(name);
        if (clazz == null) {
            throw new JabatRuntimeException("Batch artifact '" + name + "' not found");
        }
        Object instance;
        try {
            instance = clazz.newInstance();
            JabatThreadContext.getInstance().inject(instance, name);
        } catch (ReflectiveOperationException e) {
            throw new JabatRuntimeException(e);
        }
        return instance;
    }

    @Override
    public void destroy(Object instance) {
    }

}
