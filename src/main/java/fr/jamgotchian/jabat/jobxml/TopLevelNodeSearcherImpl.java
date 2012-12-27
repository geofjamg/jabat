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
package fr.jamgotchian.jabat.jobxml;

import com.google.common.base.Predicate;
import fr.jamgotchian.jabat.util.JabatException;
import java.io.InputStream;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class TopLevelNodeSearcherImpl implements TopLevelNodeSearcher {

    private final JobPathScanner scanner;

    public TopLevelNodeSearcherImpl(JobPathScanner scanner) {
        this.scanner = scanner;
    }

    public TopLevelNodeSearcherImpl() {
        this(new JobPathScannerImpl());
    }

    @Override
    public InputStream search(final TopLevelNodeType type, final String id) {
        if (type == null) {
            throw new IllegalArgumentException("type is null");
        }
        if (id == null) {
            throw new IllegalArgumentException("id is null");
        }

        return scanner.scan(new Predicate<InputStream>() {

            @Override
            public boolean apply(InputStream is) {
                try {
                    XMLInputFactory xmlif = XMLInputFactory.newInstance();
                    XMLStreamReader xmlsr = xmlif.createXMLStreamReader(is);
                    while (xmlsr.hasNext()) {
                        // stop at the first start element
                        if (xmlsr.next() == XMLEvent.START_ELEMENT) {
                            return type.name().toLowerCase().equals(xmlsr.getLocalName())
                                    && id.equals(xmlsr.getAttributeValue(null, "id"));
                        }
                    }
                } catch (XMLStreamException e) {
                    throw new JabatException(e);
                }
                return false;
            }
        });
    }

}
