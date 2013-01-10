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
package fr.jamgotchian.jabat.runtime.artifact;

import fr.jamgotchian.jabat.runtime.util.JabatRuntimeException;
import java.io.InputStream;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchXmlParser {

    public BatchXml parse(InputStream is) {
        final BatchXml batchXml = new BatchXml();
        try {
            XMLInputFactory xmlif = XMLInputFactory.newInstance();
            XMLStreamReader xmlsr = xmlif.createXMLStreamReader(is);
            while (xmlsr.hasNext()) {
                int eventType = xmlsr.next();
                switch (eventType) {
                    case XMLEvent.START_ELEMENT: {
                        String localName = xmlsr.getLocalName();
                        if ("batch-artifact".equals(localName)) {
                            String name = xmlsr.getAttributeValue(null, "name");
                            String className = xmlsr.getAttributeValue(null, "class");
                            try {
                                Class<?> clazz = Class.forName(className);
                                batchXml.addArtifact(name, clazz);
                            } catch (ClassNotFoundException e) {
                                throw new JabatRuntimeException("Batch artifact class '"
                                        + className + "' not found");
                            }
                        }
                    }
                    break;
                }
            };
        } catch (FactoryConfigurationError e) {
            throw new JabatRuntimeException(e);
        } catch (XMLStreamException e) {
            throw new JabatRuntimeException(e);
        }
        return batchXml;
    }

}