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
import fr.jamgotchian.jabat.job.Listenable;
import fr.jamgotchian.jabat.job.Listener;
import fr.jamgotchian.jabat.job.NodeContainer;
import fr.jamgotchian.jabat.job.Parameterizable;
import fr.jamgotchian.jabat.util.JabatException;
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

    private NodeContainer getContainer(Deque<Object> element) {
        if (element.getFirst() instanceof NodeContainer) {
            return (NodeContainer) element.getFirst();
        } else {
            throw new JabatException("Element is not a node container");
        }
    }

    private Parameterizable getParameterizable(Deque<Object> element) {
        if (element.getFirst() instanceof Parameterizable) {
            return (Parameterizable) element.getFirst();
        } else {
            throw new JabatException("Element is not a parameterizable");
        }
    }

    private Listenable getListenable(Deque<Object> element) {
        if (element.getFirst() instanceof Listenable) {
            return (Listenable) element.getFirst();
        } else {
            throw new JabatException("Element is not a listenable");
        }
    }

    private void readJobXml(File file) {
        LOGGER.debug("Load job xml file " + file);
        try {
            XMLInputFactory xmlif = XMLInputFactory.newInstance();
            XMLStreamReader xmlsr = xmlif.createXMLStreamReader(new FileReader(file));
            Deque<Object> element = new ArrayDeque<Object>(1);
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
                                element.push(job);
                            } else if ("step".equals(localName)) {
                                String id = xmlsr.getAttributeValue(null, "id");
                                String next = xmlsr.getAttributeValue(null, "next");
                                stepAttrs.push(new StepAttributes(id, next));
                            } else if ("split".equals(localName)) {
                                String id = xmlsr.getAttributeValue(null, "id");
                                String next = xmlsr.getAttributeValue(null, "next");
                                NodeContainer container = getContainer(element);
                                SplitNode split = new SplitNode(id, container, next);
                                container.addNode(split);
                                element.push(split);
                            } else if ("flow".equals(localName)) {
                                String id = xmlsr.getAttributeValue(null, "id");
                                String next = xmlsr.getAttributeValue(null, "next");
                                NodeContainer container = getContainer(element);
                                FlowNode flow = new FlowNode(id, container, next);
                                container.addNode(flow);
                                element.push(flow);
                            } else if ("batchlet".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                NodeContainer container = getContainer(element);
                                StepNode step = new BatchletStepNode(stepAttrs.getFirst().id,
                                                                     container,
                                                                     stepAttrs.getFirst().next,
                                                                     ref);
                                container.addNode(step);
                                element.push(step);
                            } else if ("chunk".equals(localName)) {
                                String readerRef = xmlsr.getAttributeValue(null, "reader");
                                String processorRef = xmlsr.getAttributeValue(null, "processor");
                                String writerRef = xmlsr.getAttributeValue(null, "writer");
                                int bufferSize = Integer.valueOf(xmlsr.getAttributeValue(null, "buffer-size"));
                                String value = xmlsr.getAttributeValue(null, "retry-limit");
                                int retryLimit = -1;
                                if (value != null) {
                                    retryLimit = Integer.valueOf(value);
                                }
                                NodeContainer container = getContainer(element);
                                StepNode step = new ChunkStepNode(stepAttrs.getFirst().id,
                                                                  container,
                                                                  stepAttrs.getFirst().next,
                                                                  readerRef,
                                                                  processorRef,
                                                                  writerRef,
                                                                  bufferSize,
                                                                  retryLimit);
                                container.addNode(step);
                                element.push(step);
                            } else if ("property".equals(localName)) {
                                String name = xmlsr.getAttributeValue(null, "name");
                                String value = xmlsr.getAttributeValue(null, "value");
                                getParameterizable(element).getParameters().setProperty(name, value);
                            } else if ("listener".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                getListenable(element).addListener(new Listener(ref));
                            }
                            break;
                        }

                    case XMLEvent.END_ELEMENT:
                        {
                            String localName = xmlsr.getLocalName();
                            if ("job".equals(localName)
                                    || "split".equals(localName)
                                    || "flow".equals(localName)
                                    || "batchlet".equals(localName)
                                    || "chunk".equals(localName)) {
                                element.pop();
                            } else if ("step".equals(localName)) {
                                stepAttrs.pop();
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
