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
package fr.jamgotchian.jabat.util;

import javax.xml.stream.XMLStreamReader;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class XmlUtil {

    private XmlUtil() {
    }

    public static int getAttributeIntValue(XMLStreamReader xmlsr, String namespaceURI,
                                           String localName, int defaultValue) {
        String value = xmlsr.getAttributeValue(namespaceURI, localName);
        if (value != null) {
            return Integer.valueOf(value);
        } else {
            return defaultValue;
        }
    }

    public static <E extends Enum<E>> E getAttributeEnumValue(XMLStreamReader xmlsr, String namespaceURI,
                                                              String localName, Class<E> clazz, E defaultValue) {
        String value = xmlsr.getAttributeValue(namespaceURI, localName);
        if (value != null) {
            return Enum.valueOf(clazz, value.toUpperCase());
        } else {
            return defaultValue;
        }
    }

}
