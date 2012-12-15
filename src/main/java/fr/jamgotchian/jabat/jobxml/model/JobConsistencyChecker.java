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
public class JobConsistencyChecker {

    private final Job job;

    public JobConsistencyChecker(Job job) {
        this.job = job;
    }

    private final NodeVisitor<ConsistencyReport> visitor = new AbstractNodeVisitor<ConsistencyReport>() {

        @Override
        public void visit(Job job, ConsistencyReport report) {
            for (Node n : job.getNodes()) {
                n.accept(visitor, report);
            }
        }

        @Override
        public void visit(BatchletStep step, ConsistencyReport report) {
        }

        @Override
        public void visit(ChunkStep step, ConsistencyReport report) {
        }

        @Override
        public void visit(Flow flow, ConsistencyReport report) {
            for (Node n : flow.getNodes()) {
                n.accept(visitor, report);
            }
        }

        @Override
        public void visit(Split split, ConsistencyReport report) {
            for (Node n : split.getNodes()) {
                n.accept(visitor, report);
            }
        }

        @Override
        public void visit(Decision decision, ConsistencyReport report) {
            for (ControlElement ctrlElt : decision.getControlElements()) {
                switch (ctrlElt.getType()) {
                    case NEXT:
                        {
                            NextElement nextElt = (NextElement) ctrlElt;
                            // see jsr352 specification 5.6.4
                            if (job.getNode(nextElt.getTo()) == null) {
                                report.addError("A decision can only transition to a job level step|flow|split: "
                                        + nextElt.toString());
                            }
                            break;
                        }
                }
            }
        }

    };

    public ConsistencyReport check() {
        ConsistencyReport report = new ConsistencyReport();
        visitor.visit(job, report);
        return report;
    }

}
