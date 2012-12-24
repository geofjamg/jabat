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

import fr.jamgotchian.jabat.util.JabatException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
class JobCheckUtil {

    private JobCheckUtil() {
    }

    static void checkNotAssociated(Collection<? extends AbstractNode> nodes) {
        for (Node node : nodes) {
            if (node == null) {
                throw new JabatException("Node is null");
            }
            if (node.getContainer() != null) {
                throw new JabatException("Node '" + node.getId()
                        + "' is already in container '" + node.getContainer().getId()
                        + "'");
            }
        }
    }

    static void checkIdUnicity(Collection<? extends Node> nodes) {
        Set<String> ids = new HashSet<String>();
        for (Node node : nodes) {
            if (node == null) {
                throw new JabatException("Node is null");
            }
            if (ids.contains(node.getId())) {
                throw new JabatException("Duplicate node id '" + node.getId() + "'");
            }
            ids.add(node.getId());
        }
    }

}
