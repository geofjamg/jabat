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
package fr.jamgotchian.jabat.runtime.artifact.impl;

import fr.jamgotchian.jabat.runtime.artifact.ArtifactFactory;
import fr.jamgotchian.jabat.runtime.context.JabatThreadContext;
import fr.jamgotchian.jabat.runtime.util.JabatRuntimeException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.util.StreamReaderDelegate;
import javax.xml.transform.stax.StAXSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchXmlArtifactFactory implements ArtifactFactory {

    private final Map<String, Class<?>> classes = new HashMap<String, Class<?>>();

    @Override
    public void initialize() throws Exception {
        InputStream is = getClass().getResourceAsStream("/META-INF/batch.xml");
        if (is != null) {
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
                                String id = super.getAttributeValue(null, "id");
                                String className = super.getAttributeValue(null, "class");
                                try {
                                    Class<?> clazz = Class.forName(className);
                                    classes.put(id, clazz);
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
        }
    }

    @Override
    public Object create(String ref) throws Exception {
        Class<?> clazz = classes.get(ref);
        if (clazz == null) {
            throw new JabatRuntimeException("Batch artifact '" + ref + "' not found");
        }
        Object instance = clazz.newInstance();
        JabatThreadContext.getInstance().inject(instance, ref);
        return instance;
    }

    @Override
    public void destroy(Object instance) throws Exception {
    }

}
