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

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class AbstractNodeVisitor<A> implements NodeVisitor<A> {

    @Override
    public void visit(Job job, A arg) {
    }

    @Override
    public void visit(BatchletStep step, A arg) {
    }

    @Override
    public void visit(ChunkStep step, A arg) {
    }

    @Override
    public void visit(Flow flow, A arg) {
    }

    @Override
    public void visit(Split split, A arg) {
    }

    @Override
    public void visit(Decision decision, A arg) {
    }

}
