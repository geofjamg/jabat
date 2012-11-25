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
import fr.jamgotchian.jabat.util.XmlUtil;
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

        private final Properties properties = new Properties();

        private final List<Artifact> listeners = new ArrayList<Artifact>();

        private final List<TerminatingElement> terminatingElements
                = new ArrayList<TerminatingElement>();

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

        public void addTerminatingElement(TerminatingElement ctrlElt) {
            terminatingElements.add(ctrlElt);
        }

        public List<TerminatingElement> getTerminatingElements() {
            return terminatingElements;
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
        LISTENER,
        MAPPER,
        REDUCER,
        COLLECTOR,
        ANALYSER,
        PARTITION
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
                                Split split = new Split(id, container, next);
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
                                CheckpointPolicy checkpointPolicy = XmlUtil.getAttributeEnumValue(xmlsr, null, "checkpoint-policy", CheckpointPolicy.class, CheckpointPolicy.ITEM);
                                int commitInterval = XmlUtil.getAttributeIntValue(xmlsr, null, "commit-interval", 10);
                                int defaultBufferSize;
                                switch (checkpointPolicy) {
                                    case ITEM:
                                        defaultBufferSize = commitInterval;
                                        break;
                                    case TIME:
                                    case CUSTOM:
                                        defaultBufferSize = 10;
                                        break;
                                    default:
                                        throw new InternalError();
                                }
                                int bufferSize = XmlUtil.getAttributeIntValue(xmlsr, null, "buffer-size", defaultBufferSize);
                                int retryLimit = XmlUtil.getAttributeIntValue(xmlsr, null, "retry-limit", -1);
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
                            } else if ("decision".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                NodeContainer container = (NodeContainer) xmlElt.getFirst();
                                Decision decision = new Decision(ref, container, new Artifact(ref));
                                container.addNode(decision);
                                xmlContext.push(XmlContext.DECISION);
                                xmlElt.push(decision);
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
                                                throw new JabatException("Artifact " + artifactName
                                                        + " not found");
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
                                    case COLLECTOR:
                                    case ANALYSER:
                                        ((Artifact) xmlElt.getFirst()).setProperty(name, value);
                                        break;
                                    case PARTITION:
                                        throw new JabatException("TODO");
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
                                        throw new JabatException("Unexpected Xml context "
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
                                    throw new JabatException("Unexpected Xml context "
                                            + xmlContext.getFirst());
                                }
                            } else if ("collector".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                Artifact collector = new Artifact(ref);
                                switch (xmlContext.getFirst()) {
                                    case SPLIT:
                                        {
                                            Split split = (Split) xmlElt.getFirst();
                                            split.setCollector(collector);
                                            xmlContext.push(XmlContext.COLLECTOR);
                                            xmlElt.push(collector);
                                        }
                                        break;
                                    case PARTITION:
                                        {
                                            Partition partition = (Partition) xmlElt.getFirst();
                                            partition.setCollector(collector);
                                            xmlContext.push(XmlContext.COLLECTOR);
                                            xmlElt.push(collector);
                                        }
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context "
                                                + xmlContext.getFirst());
                                }
                            } else if ("analyser".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                Artifact analyser = new Artifact(ref);
                                switch (xmlContext.getFirst()) {
                                    case SPLIT:
                                        {
                                            Split split = (Split) xmlElt.getFirst();
                                            split.setAnalyser(analyser);
                                            xmlContext.push(XmlContext.ANALYSER);
                                            xmlElt.push(analyser);
                                        }
                                        break;
                                    case PARTITION:
                                        {
                                            Partition partition = (Partition) xmlElt.getFirst();
                                            partition.setAnalyser(analyser);
                                            xmlContext.push(XmlContext.ANALYSER);
                                            xmlElt.push(analyser);
                                        }
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context "
                                                + xmlContext.getFirst());
                                }
                            } else if ("mapper".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                Artifact mapper = new Artifact(ref);
                                if (xmlContext.getFirst() == XmlContext.PARTITION) {
                                    Partition partition = (Partition) xmlElt.getFirst();
                                    partition.setMapper(mapper);
                                    xmlContext.push(XmlContext.MAPPER);
                                    xmlElt.push(mapper);
                                } else {
                                    throw new JabatException("Unexpected Xml context "
                                            + xmlContext.getFirst());
                                }
                            } else if ("reducer".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                Artifact reducer = new Artifact(ref);
                                if (xmlContext.getFirst() == XmlContext.PARTITION) {
                                    Partition partition = (Partition) xmlElt.getFirst();
                                    partition.setReducer(reducer);
                                    xmlContext.push(XmlContext.REDUCER);
                                    xmlElt.push(reducer);
                                } else {
                                    throw new JabatException("Unexpected Xml context "
                                            + xmlContext.getFirst());
                                }
                            } else if ("end".equals(localName)) {
                                String on = xmlsr.getAttributeValue(null, "on");
                                String exitStatus = xmlsr.getAttributeValue(null, "exit-status");
                                EndElement end = new EndElement(on, exitStatus);
                                switch (xmlContext.getFirst()) {
                                    case STEP:
                                        ((StepElement) xmlElt.getFirst()).addTerminatingElement(end);
                                        break;
                                    case DECISION:
                                        ((Decision) xmlElt.getFirst()).addControlElement(end);
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context "
                                                + xmlContext.getFirst());
                                }
                            } else if ("fail".equals(localName)) {
                                String on = xmlsr.getAttributeValue(null, "on");
                                String exitStatus = xmlsr.getAttributeValue(null, "exit-status");
                                FailElement fail = new FailElement(on, exitStatus);
                                switch (xmlContext.getFirst()) {
                                    case STEP:
                                        ((StepElement) xmlElt.getFirst()).addTerminatingElement(fail);
                                        break;
                                    case DECISION:
                                        ((Decision) xmlElt.getFirst()).addControlElement(fail);
                                    default:
                                        throw new JabatException("Unexpected Xml context "
                                                + xmlContext.getFirst());
                                }
                            } else if ("stop".equals(localName)) {
                                String on = xmlsr.getAttributeValue(null, "on");
                                String exitStatus = xmlsr.getAttributeValue(null, "exit-status");
                                String restart = xmlsr.getAttributeValue(null, "restart");
                                StopElement stop = new StopElement(on, exitStatus, restart);
                                switch (xmlContext.getFirst()) {
                                    case STEP:
                                        ((StepElement) xmlElt.getFirst()).addTerminatingElement(stop);
                                        break;
                                    case DECISION:
                                        ((Decision) xmlElt.getFirst()).addControlElement(stop);
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context "
                                                + xmlContext.getFirst());
                                }
                            } else if ("next".equals(localName)) {
                                String on = xmlsr.getAttributeValue(null, "on");
                                String to = xmlsr.getAttributeValue(null, "to");
                                if (xmlContext.getFirst() == XmlContext.DECISION) {
                                    NextElement next = new NextElement(on, to);
                                    ((Decision) xmlElt.getFirst()).addControlElement(next);
                                } else {
                                    throw new JabatException("Unexpected Xml context "
                                            + xmlContext.getFirst());
                                }
                            } else if ("partition".equals(localName)) {
                                if (xmlContext.getFirst() == XmlContext.BATCHLET
                                        || xmlContext.getFirst() == XmlContext.CHUNK) {
                                    Partition partition = new Partition();
                                    ((Step) xmlElt.getFirst()).setPartition(partition);
                                    xmlContext.push(XmlContext.PARTITION);
                                    xmlElt.push(partition);
                                } else {
                                    throw new JabatException("Unexpected Xml context "
                                            + xmlContext.getFirst());
                                }
                            } else if ("plan".equals(localName)) {
                                int instances = XmlUtil.getAttributeIntValue(xmlsr, null, "instances", 1);
                                int threads = XmlUtil.getAttributeIntValue(xmlsr, null, "threads", instances);
                                PartitionPlanImpl plan = new PartitionPlanImpl(instances, threads);
                                if (xmlContext.getFirst() == XmlContext.PARTITION) {
                                    ((Partition) xmlElt.getFirst()).setPlan(plan);
                                } else {
                                    throw new JabatException("Unexpected Xml context "
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
                                    || "listener".equals(localName)
                                    || "collector".equals(localName)
                                    || "analyser".equals(localName)
                                    || "mapper".equals(localName)
                                    || "reducer".equals(localName)
                                    || "partition".equals(localName)) {
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
