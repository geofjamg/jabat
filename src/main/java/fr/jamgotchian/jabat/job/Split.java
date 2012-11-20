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
public class Split extends AbstractNodeContainer implements NodeContainer, Chainable {

    private final String next;

    private final Artifact collector;

    private final Artifact analyser;

    public Split(String id, NodeContainer container, String next,
                 Artifact collector, Artifact analyser) {
        super(id, new Properties(), container);
        this.next = next;
        this.collector = collector;
        this.analyser = analyser;
    }

    @Override
    public String getNext() {
        return next;
    }

    public Artifact getCollector() {
        return collector;
    }

    public Artifact getAnalyser() {
        return analyser;
    }

    @Override
    public <A> void accept(NodeVisitor<A> visitor, A arg) {
        visitor.visit(this, arg);
    }

}
