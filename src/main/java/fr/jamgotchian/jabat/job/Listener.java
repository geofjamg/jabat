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
package fr.jamgotchian.jabat.job;

import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class Listener implements Propertiable {

    private final String ref;

    private final Propertiable parent;

    private Properties properties = new Properties();

    public Listener(String ref, Propertiable parent) {
        this.ref = ref;
        this.parent = parent;
    }

    public String getRef() {
        return ref;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public String getProperty(String name) {
        String value = properties.getProperty(name);
        if (value == null) {
            value = parent.getProperty(name);
        }
        return value;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void addProperty(String name, String value) {
        properties.put(name, value);
    }

}
