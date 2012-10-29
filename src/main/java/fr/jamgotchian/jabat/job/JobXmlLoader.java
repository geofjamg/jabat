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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;
import javax.batch.runtime.NoSuchJobException;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import org.antlr.runtime.RecognitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobXmlLoader.class);

    private static class StepAttributes {

        public String id;

        public String next;

        private StepAttributes(String id, String next) {
            this.id = id;
            this.next = next;
        }
    }

    private final JobPath path = new JobPath();

    public JobXmlLoader() {
    }

    private static NodeContainer getContainer(Deque<Object> element) {
        if (element.getFirst() instanceof NodeContainer) {
            return (NodeContainer) element.getFirst();
        } else {
            throw new JabatException("Element is not a node container");
        }
    }

    private static Propertiable getPropertiable(Deque<Object> element) {
        if (element.getFirst() instanceof Propertiable) {
            return (Propertiable) element.getFirst();
        } else {
            throw new JabatException("Element is not a parameterizable");
        }
    }

    private static Listenable getListenable(Deque<Object> element) {
        if (element.getFirst() instanceof Listenable) {
            return (Listenable) element.getFirst();
        } else {
            throw new JabatException("Element is not a listenable");
        }
    }

    private Job loadFile(File file, String jobId, Properties parameters) {
        Job job = null;
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
                                if (!id.equals(jobId)) {
                                    return null;
                                }
                                job = new Job(id);
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
                                CheckpointPolicy checkpointPolicy = CheckpointPolicy.ITEM;
                                String value = xmlsr.getAttributeValue(null, "checkpoint-policy");
                                if (value != null) {
                                    checkpointPolicy = CheckpointPolicy.valueOf(value);
                                }
                                value = xmlsr.getAttributeValue(null, "commit-interval");
                                int commitInterval = 10;
                                if (value != null) {
                                    commitInterval = Integer.valueOf(value);
                                }
                                int bufferSize;
                                value = xmlsr.getAttributeValue(null, "buffer-size");
                                if (value != null) {
                                    bufferSize = Integer.valueOf(value);
                                } else {
                                    switch (checkpointPolicy) {
                                        case ITEM:
                                            bufferSize = commitInterval;
                                            break;
                                        case TIME:
                                        case CUSTOM:
                                            bufferSize = 10;
                                            break;
                                        default:
                                            throw new InternalError();
                                    }
                                }
                                value = xmlsr.getAttributeValue(null, "retry-limit");
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
                                                                  checkpointPolicy,
                                                                  commitInterval,
                                                                  bufferSize,
                                                                  retryLimit);
                                container.addNode(step);
                                element.push(step);
                            } else if ("property".equals(localName)) {
                                String name = xmlsr.getAttributeValue(null, "name");
                                String value = xmlsr.getAttributeValue(null, "value");
                                String substitutedValue = JobUtil.substitute(value, parameters, getPropertiable(element));
                                getPropertiable(element).addProperty(name, substitutedValue);
                            } else if ("listener".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                getListenable(element).addListener(new Listener(ref, getPropertiable(element)));
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
        } catch (IOException e) {
            LOGGER.error(e.toString(), e);
        } catch (XMLStreamException e) {
            LOGGER.error(e.toString(), e);
        } catch (RecognitionException e) {
            LOGGER.error(e.toString(), e);
        }
        LOGGER.debug("Load job xml {} file {}", jobId, file);
        return job;
    }

    public Job load(String id, Properties parameters) throws NoSuchJobException {
        for (File file : path.findJobXml()) {
            Job job = loadFile(file, id, parameters);
            if (job != null) {
                return job;
            }
        }
        throw new NoSuchJobException("Job " + id + " not found");
    }

}
