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
package fr.jamgotchian.jabat.jobxml.model;

import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
abstract class AbstractNode implements Node {

    private final String id;

    private Properties properties;

    private Properties substitutedproperties = new Properties();

    private NodeContainer container;

    AbstractNode(String id, Properties properties) {
        this.id = id;
        this.properties = properties;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public Properties getSubstitutedProperties() {
        return substitutedproperties;
    }

    @Override
    public NodeContainer getContainer() {
        return container;
    }

    void setContainer(NodeContainer container) {
        this.container = container;
    }

}
