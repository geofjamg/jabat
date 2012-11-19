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
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private static class StepElement implements Listenable, Propertiable {

        public String id;

        public String next;

        private Properties properties = new Properties();

        private final List<Artifact> listeners = new ArrayList<Artifact>();

        private StepElement(String id, String next) {
            this.id = id;
            this.next = next;
        }

        @Override
        public void addListener(Artifact listener) {
            listeners.add(listener);
        }

        @Override
        public Collection<Artifact> getListeners() {
            return listeners;
        }

        @Override
        public Properties getProperties() {
            return properties;
        }

        @Override
        public void setProperty(String name, String value) {
            properties.setProperty(name, value);
        }

    }

    private static class MultipleArtifactsElement {

        private final Map<String, Artifact> artifacts = new HashMap<String, Artifact>();

        private MultipleArtifactsElement(Artifact... artifacts) {
            for (Artifact a : artifacts) {
                this.artifacts.put(a.getRef(), a);
            }
        }

        private Artifact getArtifact(String ref) {
            Artifact a = artifacts.get(ref);
            if (a == null) {
                throw new JabatException("Artifact " + ref + " not found");
            }
            return a;
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
        ARTIFACT,
        MULTIPLE_ARTIFACTS,
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
                                xmlContext.push(XmlContext.ARTIFACT);
                                xmlElt.push(batchlet);
                                xmlElt.push(artifact);
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
                                Artifact readerArtifact = new Artifact(readerRef);
                                Artifact processorArtifact = new Artifact(processorRef);
                                Artifact writerArtifact = new Artifact(writerRef);
                                ChunkStep chunk = new ChunkStep(stepElt.id,
                                                                container,
                                                                stepElt.next,
                                                                stepElt.properties,
                                                                stepElt.listeners,
                                                                readerArtifact,
                                                                processorArtifact,
                                                                writerArtifact,
                                                                checkpointPolicy,
                                                                commitInterval,
                                                                bufferSize,
                                                                retryLimit);
                                container.addNode(chunk);
                                xmlContext.push(XmlContext.CHUNK);
                                xmlElt.push(chunk);
                                xmlContext.push(XmlContext.MULTIPLE_ARTIFACTS);
                                xmlElt.push(new MultipleArtifactsElement(readerArtifact,
                                                                         processorArtifact,
                                                                         writerArtifact));
                            } else if ("property".equals(localName)) {
                                String name = xmlsr.getAttributeValue(null, "name");
                                String value = xmlsr.getAttributeValue(null, "value");
                                switch (xmlContext.getFirst()) {
                                    case JOB:
                                    case STEP:
                                    case ARTIFACT:
                                        ((Propertiable) xmlElt.getFirst()).setProperty(name, value);
                                        break;
                                    case MULTIPLE_ARTIFACTS:
                                        String applyTo = xmlsr.getAttributeValue(null, "applyTo");
                                        if (applyTo != null) {
                                            MultipleArtifactsElement artifacts = (MultipleArtifactsElement) xmlElt.getFirst();
                                            artifacts.getArtifact(applyTo).getProperties().setProperty(name, value);
                                        }
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context"
                                                + xmlContext.getFirst());
                                }
                            } else if ("listener".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                switch (xmlContext.getFirst()) {
                                    case JOB:
                                    case STEP:
                                        Artifact artifact = new Artifact(ref);
                                        ((Listenable) xmlElt.getFirst()).addListener(artifact);
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context"
                                                + xmlContext.getFirst());
                                }
                            } else if ("checkpoint-algorithm".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                if (Deques.getSecond(xmlContext) == XmlContext.CHUNK) {
                                    Artifact checkpointAlgoArtifact = new Artifact(ref);
                                    ChunkStep chunk = (ChunkStep) Deques.getSecond(xmlElt);
                                    if (chunk.getCheckpointPolicy() != CheckpointPolicy.CUSTOM) {
                                        throw new JabatException("Checkpoint algorithm should be only specified in case of custom checkpoint policy");
                                    }
                                    chunk.setCheckpointAlgo(checkpointAlgoArtifact);
                                    xmlContext.push(XmlContext.ARTIFACT);
                                    xmlElt.push(checkpointAlgoArtifact);
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
                                    || "checkpoint-algorithm".equals(localName)) {
                                xmlContext.pop();
                                xmlElt.pop();
                            } else if ("batchlet".equals(localName)
                                    || "chunk".equals(localName)) {
                                xmlContext.pop();
                                xmlContext.pop();
                                xmlElt.pop();
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
