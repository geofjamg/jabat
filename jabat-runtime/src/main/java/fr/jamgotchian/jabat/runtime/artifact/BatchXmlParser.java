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
import java.io.IOException;
import java.io.InputStream;
import javax.xml.XMLConstants;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.util.StreamReaderDelegate;
import javax.xml.transform.stax.StAXSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.xml.sax.SAXException;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchXmlParser {

    public BatchXml parse(InputStream is) {
        final BatchXml batchXml = new BatchXml();
        try {
            XMLInputFactory xmlif = XMLInputFactory.newInstance();

            // parse and validate the document at the same time
            XMLStreamReader xmlsr = xmlif.createXMLStreamReader(is);
            XMLStreamReader delegate = new StreamReaderDelegate(xmlsr) {
                @Override
                public int next() throws XMLStreamException {
                    int eventType = super.next();
                    switch (eventType) {
                        case XMLEvent.START_ELEMENT: {
                            String localName = super.getLocalName();
                            if ("batch-artifact".equals(localName)) {
                                String name = super.getAttributeValue(null, "name");
                                String className = super.getAttributeValue(null, "class");
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
                    return eventType;
                }
            };
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = factory.newSchema(getClass().getResource("/batch.xsd"));
            Validator validator = schema.newValidator();
            validator.validate(new StAXSource(delegate));
        } catch (FactoryConfigurationError e) {
            throw new JabatRuntimeException(e);
        } catch (IOException e) {
            throw new JabatRuntimeException(e);
        } catch (SAXException e) {
            throw new JabatRuntimeException(e);
        } catch (XMLStreamException e) {
            throw new JabatRuntimeException(e);
        }
        return batchXml;
    }

}