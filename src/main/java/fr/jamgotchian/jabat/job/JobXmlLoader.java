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

    private static class StepElement {

        public String id;

        public String next;

        private final Properties properties = new Properties();

        private StepElement(String id, String next) {
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

    private static void setProperty(Deque<Object> element, StepElement stepElt, Properties parameters,
                                    String name, String value, String applyTo)
            throws IOException, JabatException, RecognitionException {
        Properties properties = null;
        Object first = element.getFirst();
        if (stepElt != null) {
            properties = stepElt.properties;
        } else if (first instanceof Job) {
            properties = ((Job) first).getProperties();
        } else if (first instanceof BatchletStepNode) {
            properties = ((BatchletStepNode) first).getRef().getProperties();
        } else if (first instanceof ChunkStepNode) {
            ChunkStepNode chunk = (ChunkStepNode) first;
            if (applyTo != null) {
                if (applyTo.equals(chunk.getReaderRef().getName())) {
                    properties = chunk.getReaderRef().getProperties();
                } else if (applyTo.equals(chunk.getProcessorRef().getName())) {
                    properties = chunk.getProcessorRef().getProperties();
                } else if (applyTo.equals(chunk.getWriterRef().getName())) {
                    properties = chunk.getWriterRef().getProperties();
                }
            }
        } else if (first instanceof Listener) {
            ((Listener) first).getRef().getProperties();
        } else {
            throw new JabatException("Cannot set the property");
        }
        if (properties != null) {
            String substitutedValue = JobUtil.substitute(value, parameters, getScopeProperties(element));
            properties.setProperty(name, substitutedValue);
        }
    }

    private static Properties getScopeProperties(Deque<Object> element) {
        Properties result = new Properties();
        // accumulate properties in the current scope starting by deeper
        // nesting level
        for (Object o : element) {
            if (o instanceof Propertiable) {
                result.putAll(((Propertiable) o).getProperties());
            }
        }
        return result;
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
            StepElement stepElt = null;
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
                                stepElt = new StepElement(id, next);
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
                                StepNode step = new BatchletStepNode(stepElt.id,
                                                                     container,
                                                                     stepElt.next,
                                                                     stepElt.properties,
                                                                     new ArtifactRef(ref));
                                container.addNode(step);
                                element.push(step);
                                stepElt = null;
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
                                StepNode step = new ChunkStepNode(stepElt.id,
                                                                  container,
                                                                  stepElt.next,
                                                                  stepElt.properties,
                                                                  new ArtifactRef(readerRef),
                                                                  new ArtifactRef(processorRef),
                                                                  new ArtifactRef(writerRef),
                                                                  checkpointPolicy,
                                                                  commitInterval,
                                                                  bufferSize,
                                                                  retryLimit);
                                container.addNode(step);
                                element.push(step);
                                stepElt = null;
                            } else if ("property".equals(localName)) {
                                String name = xmlsr.getAttributeValue(null, "name");
                                String value = xmlsr.getAttributeValue(null, "value");
                                String applyTo = xmlsr.getAttributeValue(null, "applyTo");
                                setProperty(element, stepElt, parameters, name, value, applyTo);
                            } else if ("listener".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                getListenable(element).addListener(new Listener(new ArtifactRef(ref)));
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
