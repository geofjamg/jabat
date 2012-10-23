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

import fr.jamgotchian.jabat.util.JabatException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public abstract class AbstractNodeContainer extends AbstractNode {

    protected final Map<String, Node> nodes = new LinkedHashMap<String, Node>();

    public AbstractNodeContainer(String id, NodeContainer container) {
        super(id, container);
    }

    public void addNode(Node node) {
        nodes.put(node.getId(), node);
    }

    public Collection<Node> getNodes() {
        return nodes.values();
    }

    public Node getNode(String id) {
        if (id == null) {
            throw new IllegalArgumentException("Node id is null");
        }
        Node node = nodes.get(id);
        if (node == null) {
            throw new JabatException("Node " + id + " not found");
        }
        return node;
    }

    public Node getFirstChainableNode() {
        for (Node node : nodes.values()) {
            if (node instanceof Chainable) {
                return node;
            }
        }
        return null;
    }

}
