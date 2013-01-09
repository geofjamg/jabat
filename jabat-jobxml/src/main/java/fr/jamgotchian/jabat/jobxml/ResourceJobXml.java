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
package fr.jamgotchian.jabat.jobxml;

import fr.jamgotchian.jabat.jobxml.util.JobXmlException;
import java.io.InputStream;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ResourceJobXml implements JobXml {

    private final String resourceName;

    public ResourceJobXml(String resourceName) {
        this.resourceName = resourceName;
    }

    @Override
    public InputStream getInputStream() {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
    }

    private static JobXmlException createCannotFindTopLevelNodeTypeException(Throwable cause) {
        throw new JobXmlException("Cannot find the top level node type", cause);
    }

    @Override
    public TopLevelNodeType getTopLevelNodeType() {
        try {
            XMLInputFactory xmlif = XMLInputFactory.newInstance();
            XMLStreamReader xmlsr = xmlif.createXMLStreamReader(getInputStream());
            while (xmlsr.hasNext()) {
                // stop at the first start element
                if (xmlsr.next() == XMLEvent.START_ELEMENT) {
                    try {
                        String elementName = xmlsr.getLocalName();
                        return TopLevelNodeType.valueOf(elementName.toUpperCase());
                    } catch (IllegalArgumentException e) {
                        throw createCannotFindTopLevelNodeTypeException(e);
                    }
                }
            }
        } catch (XMLStreamException e) {
            throw createCannotFindTopLevelNodeTypeException(e);
        }
        throw createCannotFindTopLevelNodeTypeException(null);
    }

}
