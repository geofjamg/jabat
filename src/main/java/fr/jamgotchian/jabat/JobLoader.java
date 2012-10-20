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
package fr.jamgotchian.jabat;

import fr.jamgotchian.jabat.job.StepNode;
import fr.jamgotchian.jabat.job.ChunkStepNode;
import fr.jamgotchian.jabat.job.SplitNode;
import fr.jamgotchian.jabat.job.FlowNode;
import fr.jamgotchian.jabat.job.Job;
import fr.jamgotchian.jabat.job.BatchletStepNode;
import fr.jamgotchian.jabat.job.Node;
import fr.jamgotchian.jabat.job.NodeContainer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.batch.runtime.NoSuchJobException;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
class JobLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobLoader.class);

    private static class StepAttributes {

        public String id;

        public String next;

        private StepAttributes(String id, String next) {
            this.id = id;
            this.next = next;
        }
    }

    private final JobPath path = new JobPath();

    private final Map<String, Job> jobs = new HashMap<String, Job>();

    JobLoader() {
        for (File file : path.findJobXml()) {
            readJobXml(file);
        }
    }

    private void readJobXml(File file) {
        LOGGER.debug("Load job xml file " + file);
        try {
            XMLInputFactory xmlif = XMLInputFactory.newInstance();
            XMLStreamReader xmlsr = xmlif.createXMLStreamReader(new FileReader(file));
            Deque<NodeContainer> container = new ArrayDeque<NodeContainer>(1);
            Deque<Node> node = new ArrayDeque<Node>(1);
            Deque<StepAttributes> stepAttrs = new ArrayDeque<StepAttributes>(1);
            while (xmlsr.hasNext()) {
                int eventType = xmlsr.next();
                switch (eventType) {
                    case XMLEvent.START_ELEMENT:
                        {
                            String localName = xmlsr.getLocalName();
                            if ("job".equals(localName)) {
                                String id = xmlsr.getAttributeValue(null, "id");
                                Job job = new Job(id);
                                jobs.put(job.getId(), job);
                                node.push(job);
                                container.push(job);
                            } else if ("step".equals(localName)) {
                                String id = xmlsr.getAttributeValue(null, "id");
                                String next = xmlsr.getAttributeValue(null, "next");
                                stepAttrs.push(new StepAttributes(id, next));
                            } else if ("split".equals(localName)) {
                                String id = xmlsr.getAttributeValue(null, "id");
                                String next = xmlsr.getAttributeValue(null, "next");
                                SplitNode split = new SplitNode(id, container.getFirst(), next);
                                container.getFirst().addNode(split);
                                node.push(split);
                                container.push(split);
                            } else if ("flow".equals(localName)) {
                                String id = xmlsr.getAttributeValue(null, "id");
                                String next = xmlsr.getAttributeValue(null, "next");
                                FlowNode flow = new FlowNode(id, container.getFirst(), next);
                                container.getFirst().addNode(flow);
                                node.push(flow);
                                container.push(flow);
                            } else if ("batchlet".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                StepNode step = new BatchletStepNode(stepAttrs.getFirst().id,
                                                                     container.getFirst(),
                                                                     stepAttrs.getFirst().next,
                                                                     ref);
                                container.getFirst().addNode(step);
                                node.push(step);
                            } else if ("chunk".equals(localName)) {
                                String readerRef = xmlsr.getAttributeValue(null, "reader");
                                String processorRef = xmlsr.getAttributeValue(null, "processor");
                                String writerRef = xmlsr.getAttributeValue(null, "writer");
                                StepNode step = new ChunkStepNode(stepAttrs.getFirst().id,
                                                                  container.getFirst(),
                                                                  stepAttrs.getFirst().next,
                                                                  readerRef,
                                                                  processorRef,
                                                                  writerRef);
                                container.getFirst().addNode(step);
                                node.push(step);
                            } else if ("property".equals(localName)) {
                                String name = xmlsr.getAttributeValue(null, "name");
                                String value = xmlsr.getAttributeValue(null, "value");
                                node.getFirst().getParameters().setProperty(name, value);
                            }
                            break;
                        }

                    case XMLEvent.END_ELEMENT:
                        {
                            String localName = xmlsr.getLocalName();
                            if ("job".equals(localName)) {
                                container.pop();
                                node.pop();
                            } else if ("step".equals(localName)) {
                                stepAttrs.pop();
                            } else if ("split".equals(localName)) {
                                container.pop();
                                node.pop();
                            } else if ("flow".equals(localName)) {
                                container.pop();
                                node.pop();
                            } else if ("batchlet".equals(localName)) {
                                node.pop();
                            } else if ("chunk".equals(localName)) {
                                node.pop();
                            }
                        }
                        break;
                }
            }
        } catch (FactoryConfigurationError e) {
            LOGGER.error(e.toString(), e);
        } catch (FileNotFoundException e) {
            LOGGER.error(e.toString(), e);
        } catch (XMLStreamException e) {
            LOGGER.error(e.toString(), e);
        }
    }

    Job getJob(String id) throws NoSuchJobException {
        Job job = jobs.get(id);
        if (job == null) {
            throw new NoSuchJobException("Job " + id + " not found");
        }
        return job;
    }

    Set<String> getJobIds() {
        return jobs.keySet();
    }

}
