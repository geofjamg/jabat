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

import fr.jamgotchian.jabat.util.Deques;
import fr.jamgotchian.jabat.util.JabatException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
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
public class JobXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobXmlLoader.class);

    private static class StepElement {

        public String id;

        public String next;

        private Properties properties = new Properties();

        private final List<Artifact> listeners = new ArrayList<Artifact>();

        private StepElement(String id, String next) {
            this.id = id;
            this.next = next;
        }

        public void addListener(Artifact listener) {
            listeners.add(listener);
        }

        public List<Artifact> getListeners() {
            return listeners;
        }

        public Properties getProperties() {
            return properties;
        }

        public void setProperty(String name, String value) {
            properties.setProperty(name, value);
        }

    }

    private enum XmlContext {
        JOB,
        STEP,
        BATCHLET,
        CHUNK,
        SPLIT,
        FLOW,
        DECISION,
        CHECKPOINT_ALGORITHM,
        LISTENER
    }

    private final JobPath path = new JobPath();

    public JobXmlLoader() {
    }

    private Job loadFile(File file, String jobId, Properties parameters) {
        Job job = null;
        try {
            XMLInputFactory xmlif = XMLInputFactory.newInstance();
            XMLStreamReader xmlsr = xmlif.createXMLStreamReader(new FileReader(file));

            // xml contextual information
            Deque<XmlContext> xmlContext = new ArrayDeque<XmlContext>(3);
            Deque<Object> xmlElt = new ArrayDeque<Object>(3);

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
                                xmlContext.push(XmlContext.JOB);
                                xmlElt.push(job);
                            } else if ("step".equals(localName)) {
                                String id = xmlsr.getAttributeValue(null, "id");
                                String next = xmlsr.getAttributeValue(null, "next");
                                StepElement stepElt = new StepElement(id, next);
                                xmlContext.push(XmlContext.STEP);
                                xmlElt.push(stepElt);
                            } else if ("split".equals(localName)) {
                                String id = xmlsr.getAttributeValue(null, "id");
                                String next = xmlsr.getAttributeValue(null, "next");
                                NodeContainer container = (NodeContainer) xmlElt.getFirst();
                                Split split = new Split(id, container, next, null, null);
                                container.addNode(split);
                                xmlContext.push(XmlContext.SPLIT);
                                xmlElt.push(split);
                            } else if ("flow".equals(localName)) {
                                String id = xmlsr.getAttributeValue(null, "id");
                                String next = xmlsr.getAttributeValue(null, "next");
                                NodeContainer container = (NodeContainer) xmlElt.getFirst();
                                Flow flow = new Flow(id, container, next);
                                container.addNode(flow);
                                xmlContext.push(XmlContext.FLOW);
                                xmlElt.push(flow);
                            } else if ("batchlet".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                StepElement stepElt = (StepElement) xmlElt.pop();
                                NodeContainer container = (NodeContainer) xmlElt.getFirst();
                                Artifact artifact = new Artifact(ref);
                                BatchletStep batchlet = new BatchletStep(stepElt.id,
                                                                         container,
                                                                         stepElt.next,
                                                                         stepElt.properties,
                                                                         stepElt.listeners,
                                                                         artifact);
                                container.addNode(batchlet);
                                xmlContext.push(XmlContext.BATCHLET);
                                xmlElt.push(batchlet);
                            } else if ("chunk".equals(localName)) {
                                String readerRef = xmlsr.getAttributeValue(null, "reader");
                                String processorRef = xmlsr.getAttributeValue(null, "processor");
                                String writerRef = xmlsr.getAttributeValue(null, "writer");
                                CheckpointPolicy checkpointPolicy = CheckpointPolicy.ITEM;
                                String value = xmlsr.getAttributeValue(null, "checkpoint-policy");
                                if (value != null) {
                                    checkpointPolicy = CheckpointPolicy.valueOf(value.toUpperCase());
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
                                StepElement stepElt = (StepElement) xmlElt.pop();
                                NodeContainer container = (NodeContainer) xmlElt.getFirst();
                                Artifact reader = new Artifact(readerRef);
                                Artifact processor = new Artifact(processorRef);
                                Artifact writer = new Artifact(writerRef);
                                ChunkStep chunk = new ChunkStep(stepElt.id,
                                                                container,
                                                                stepElt.next,
                                                                stepElt.properties,
                                                                stepElt.listeners,
                                                                reader,
                                                                processor,
                                                                writer,
                                                                checkpointPolicy,
                                                                commitInterval,
                                                                bufferSize,
                                                                retryLimit);
                                container.addNode(chunk);
                                xmlContext.push(XmlContext.CHUNK);
                                xmlElt.push(chunk);
                            } else if ("property".equals(localName)) {
                                String name = xmlsr.getAttributeValue(null, "name");
                                String value = xmlsr.getAttributeValue(null, "value");
                                switch (xmlContext.getFirst()) {
                                    case JOB:
                                        ((Job) xmlElt.getFirst()).setProperty(name, value);
                                        break;
                                    case STEP:
                                        ((StepElement) xmlElt.getFirst()).setProperty(name, value);
                                        break;
                                    case BATCHLET:
                                        ((BatchletStep) xmlElt.getFirst()).getArtifact().setProperty(name, value);
                                        break;
                                    case CHUNK:
                                        {
                                            ChunkStep chunk = (ChunkStep) xmlElt.getFirst();
                                            String[] split = name.split(":");
                                            if (split.length != 2) {
                                                throw new JabatException("Chunk property syntax error, it should be <artifact-name:property-name>");
                                            }
                                            String artifactName = split[0];
                                            String propertyName = split[1];
                                            if (chunk.getReader().getRef().equals(artifactName)) {
                                                chunk.getReader().setProperty(propertyName, value);
                                            } else if (chunk.getProcessor().getRef().equals(artifactName)) {
                                                chunk.getProcessor().setProperty(propertyName, value);
                                            } else if (chunk.getWriter().getRef().equals(artifactName)) {
                                                chunk.getWriter().setProperty(propertyName, value);
                                            } else {
                                                throw new JabatException("Artifact " + artifactName + " not found");
                                            }
                                        }
                                        break;
                                    case SPLIT:
                                    case FLOW:
                                    case DECISION:
                                        ((Node) xmlElt.getFirst()).setProperty(name, value);
                                        break;
                                    case CHECKPOINT_ALGORITHM:
                                    case LISTENER:
                                        ((Artifact) xmlElt.getFirst()).setProperty(name, value);
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context "
                                                + xmlContext.getFirst());
                                }
                            } else if ("listener".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                Artifact listener = new Artifact(ref);
                                switch (xmlContext.getFirst()) {
                                    case JOB:
                                        ((Job) xmlElt.getFirst()).addListener(listener);
                                        break;
                                    case STEP:
                                        ((StepElement) xmlElt.getFirst()).addListener(listener);
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context"
                                                + xmlContext.getFirst());
                                }
                                xmlContext.push(XmlContext.LISTENER);
                                xmlElt.push(listener);
                            } else if ("checkpoint-algorithm".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                if (xmlContext.getFirst() == XmlContext.CHUNK) {
                                    ChunkStep chunk = (ChunkStep) xmlElt.getFirst();
                                    if (chunk.getCheckpointPolicy() != CheckpointPolicy.CUSTOM) {
                                        throw new JabatException("Checkpoint algorithm should be only specified in case of custom checkpoint policy");
                                    }
                                    Artifact checkpointAlgo = new Artifact(ref);
                                    chunk.setCheckpointAlgo(checkpointAlgo);
                                    xmlContext.push(XmlContext.CHECKPOINT_ALGORITHM);
                                    xmlElt.push(checkpointAlgo);
                                } else {
                                    throw new JabatException("Unexpected Xml context"
                                            + xmlContext.getFirst());
                                }
                            }
                            break;

                        }

                    case XMLEvent.END_ELEMENT:
                        {
                            String localName = xmlsr.getLocalName();
                            if ("job".equals(localName)
                                    || "split".equals(localName)
                                    || "flow".equals(localName)
                                    || "checkpoint-algorithm".equals(localName)
                                    || "batchlet".equals(localName)
                                    || "chunk".equals(localName)
                                    || "checkpoint-algorithm".equals(localName)
                                    || "listener".equals(localName)) {
                                xmlContext.pop();
                                xmlElt.pop();
                            }
                        }
                        break;
                }
            }
        } catch (FactoryConfigurationError e) {
            throw new JabatException(e);
        } catch (IOException e) {
            throw new JabatException(e);
        } catch (XMLStreamException e) {
            throw new JabatException(e);
        }

        // check job consistency
        ConsistencyReport report = new JobConsistencyChecker(job).check();

        // substitute property values
        new PropertyValueSubstitutor(job, parameters).substitute();

        LOGGER.debug("Load job xml {} file {}", jobId, file);

        return job;
    }

    public Job load(String id, Properties parameters) throws NoSuchJobException {
        for (File file : path.findJobXml()) {
            try {
                Job job = loadFile(file, id, parameters);
                if (job != null) {
                    return job;
                }
            } catch (JabatException e) {
                LOGGER.error(e.toString(), e);
            }
        }
        throw new NoSuchJobException("Job " + id + " not found");
    }

}
