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
package fr.jamgotchian.jabat.jobxml;

import fr.jamgotchian.jabat.jobxml.model.Artifact;
import fr.jamgotchian.jabat.jobxml.model.ArtifactBuilder;
import fr.jamgotchian.jabat.jobxml.model.BatchletStep;
import fr.jamgotchian.jabat.jobxml.model.BatchletStepBuilder;
import fr.jamgotchian.jabat.jobxml.model.CheckpointPolicy;
import fr.jamgotchian.jabat.jobxml.model.ChunkStep;
import fr.jamgotchian.jabat.jobxml.model.ChunkStepBuilder;
import fr.jamgotchian.jabat.jobxml.model.ConsistencyReport;
import fr.jamgotchian.jabat.jobxml.model.Decision;
import fr.jamgotchian.jabat.jobxml.model.DecisionBuilder;
import fr.jamgotchian.jabat.jobxml.model.EndElement;
import fr.jamgotchian.jabat.jobxml.model.FailElement;
import fr.jamgotchian.jabat.jobxml.model.Flow;
import fr.jamgotchian.jabat.jobxml.model.FlowBuilder;
import fr.jamgotchian.jabat.jobxml.model.Job;
import fr.jamgotchian.jabat.jobxml.model.JobBuilder;
import fr.jamgotchian.jabat.jobxml.model.JobConsistencyChecker;
import fr.jamgotchian.jabat.jobxml.model.JobPath;
import fr.jamgotchian.jabat.jobxml.model.NextElement;
import fr.jamgotchian.jabat.jobxml.model.Node;
import fr.jamgotchian.jabat.jobxml.model.NodeContainer;
import fr.jamgotchian.jabat.jobxml.model.PartitionPlanImpl;
import fr.jamgotchian.jabat.jobxml.model.Split;
import fr.jamgotchian.jabat.jobxml.model.SplitBuilder;
import fr.jamgotchian.jabat.jobxml.model.StopElement;
import fr.jamgotchian.jabat.jobxml.model.TerminatingElement;
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
import javax.batch.api.parameters.PartitionPlan;
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

        private String id;

        private String next;

        private PartitionPlan partitionPlan;

        private Artifact partitionMapper;

        private Artifact partitionReducer;

        private Artifact partitionCollector;

        private Artifact partitionAnalyser;

        private final Properties properties = new Properties();

        private final List<Artifact> listeners = new ArrayList<Artifact>();

        private final List<TerminatingElement> terminatingElements
                = new ArrayList<TerminatingElement>();

        private StepElement(String id, String next) {
            this.id = id;
            this.next = next;
        }

        private String getId() {
            return id;
        }

        private String getNext() {
            return next;
        }

        private PartitionPlan getPartitionPlan() {
            return partitionPlan;
        }

        private void setPartitionPlan(PartitionPlan partitionPlan) {
            this.partitionPlan = partitionPlan;
        }

        private Artifact getPartitionReducer() {
            return partitionReducer;
        }

        private void setPartitionMapper(Artifact partitionMapper) {
            this.partitionMapper = partitionMapper;
        }

        private Artifact getPartitionMapper() {
            return partitionMapper;
        }

        private void setPartitionReducer(Artifact partitionReducer) {
            this.partitionReducer = partitionReducer;
        }

        private Artifact getPartitionCollector() {
            return partitionCollector;
        }

        private void setPartitionCollector(Artifact partitionCollector) {
            this.partitionCollector = partitionCollector;
        }

        private Artifact getPartitionAnalyser() {
            return partitionAnalyser;
        }

        private void setPartitionAnalyser(Artifact partitionAnalyser) {
            this.partitionAnalyser = partitionAnalyser;
        }

        private void addListener(Artifact listener) {
            listeners.add(listener);
        }

        private List<Artifact> getListeners() {
            return listeners;
        }

        private Properties getProperties() {
            return properties;
        }

        private void addTerminatingElement(TerminatingElement ctrlElt) {
            terminatingElements.add(ctrlElt);
        }

        private List<TerminatingElement> getTerminatingElements() {
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
        PLAN
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
            int partition = -1;

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
                                job = new JobBuilder()
                                        .setId(id)
                                    .build();
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
                                Split split = new SplitBuilder().setId(id).setNext(next).build();
                                switch (xmlContext.getFirst()) {
                                    case JOB:
                                        ((Job) xmlElt.getFirst()).addSplit(split);
                                        break;
                                    case FLOW:
                                        ((Flow) xmlElt.getFirst()).addSplit(split);
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context "
                                                + xmlContext.getFirst());
                                }
                                xmlContext.push(XmlContext.SPLIT);
                                xmlElt.push(split);
                            } else if ("flow".equals(localName)) {
                                String id = xmlsr.getAttributeValue(null, "id");
                                String next = xmlsr.getAttributeValue(null, "next");
                                Flow flow = new FlowBuilder().setId(id).setNext(next).build();
                                switch (xmlContext.getFirst()) {
                                    case JOB:
                                        ((Job) xmlElt.getFirst()).addFlow(flow);
                                        break;
                                    case FLOW:
                                        ((Flow) xmlElt.getFirst()).addFlow(flow);
                                        break;
                                    case SPLIT:
                                        ((Split) xmlElt.getFirst()).addFlow(flow);
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context "
                                                + xmlContext.getFirst());
                                }
                                xmlContext.push(XmlContext.FLOW);
                                xmlElt.push(flow);
                            } else if ("batchlet".equals(localName)) {
                                StepElement stepElt = (StepElement) xmlElt.pop();
                                xmlContext.pop();
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                Artifact artifact = new ArtifactBuilder().setRef(ref).build();
                                BatchletStep batchlet = new BatchletStepBuilder()
                                        .setId(stepElt.getId())
                                        .setNext(stepElt.getNext())
                                        .setProperties(stepElt.getProperties())
                                        .setPartitionPlan(stepElt.getPartitionPlan())
                                        .setPartitionMapper(stepElt.getPartitionMapper())
                                        .setPartitionReducer(stepElt.getPartitionReducer())
                                        .setPartitionCollector(stepElt.getPartitionCollector())
                                        .setPartitionAnalyser(stepElt.getPartitionAnalyser())
                                        .setListeners(stepElt.getListeners())
                                        .setArtifact(artifact)
                                        .build();
                                switch (xmlContext.getFirst()) {
                                    case JOB:
                                        ((Job) xmlElt.getFirst()).addBatchlet(batchlet);
                                        break;
                                    case FLOW:
                                        ((Flow) xmlElt.getFirst()).addBatchlet(batchlet);
                                        break;
                                    case STEP:
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context "
                                                + xmlContext.getFirst());
                                }
                                xmlContext.push(XmlContext.BATCHLET);
                                xmlElt.push(batchlet);
                            } else if ("chunk".equals(localName)) {
                                StepElement stepElt = (StepElement) xmlElt.pop();
                                xmlContext.pop();
                                String readerRef = xmlsr.getAttributeValue(null, "reader");
                                String processorRef = xmlsr.getAttributeValue(null, "processor");
                                String writerRef = xmlsr.getAttributeValue(null, "writer");
                                CheckpointPolicy checkpointPolicy = XmlUtil.getAttributeEnumValue(xmlsr, null, "checkpoint-policy", CheckpointPolicy.class);
                                Integer commitInterval = XmlUtil.getAttributeIntegerValue(xmlsr, null, "commit-interval");
                                Integer bufferSize = XmlUtil.getAttributeIntegerValue(xmlsr, null, "buffer-size");
                                Integer retryLimit = XmlUtil.getAttributeIntegerValue(xmlsr, null, "retry-limit");
                                Artifact reader = new ArtifactBuilder().setRef(readerRef).build();
                                Artifact processor = new ArtifactBuilder().setRef(processorRef).build();
                                Artifact writer = new ArtifactBuilder().setRef(writerRef).build();
                                ChunkStepBuilder builder = new ChunkStepBuilder();
                                builder.setId(stepElt.getId())
                                        .setNext(stepElt.getNext())
                                        .setProperties(stepElt.getProperties())
                                        .setPartitionPlan(stepElt.getPartitionPlan())
                                        .setPartitionMapper(stepElt.getPartitionMapper())
                                        .setPartitionReducer(stepElt.getPartitionReducer())
                                        .setPartitionCollector(stepElt.getPartitionCollector())
                                        .setPartitionAnalyser(stepElt.getPartitionAnalyser())
                                        .setListeners(stepElt.getListeners())
                                        .setReader(reader)
                                        .setProcessor(processor)
                                        .setWriter(writer);
                                if (checkpointPolicy != null) {
                                    builder.setCheckpointPolicy(checkpointPolicy);
                                }
                                if (commitInterval != null) {
                                    builder.setCommitInterval(commitInterval);
                                }
                                if (bufferSize != null) {
                                    builder.setBufferSize(bufferSize);
                                }
                                if (retryLimit != null) {
                                    builder.setRetryLimit(retryLimit);
                                }
                                ChunkStep chunk = builder.build();
                                switch (xmlContext.getFirst()) {
                                    case JOB:
                                        ((Job) xmlElt.getFirst()).addChunk(chunk);
                                        break;
                                    case FLOW:
                                        ((Flow) xmlElt.getFirst()).addChunk(chunk);
                                        break;
                                    case STEP:
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context "
                                                + xmlContext.getFirst());
                                }
                                xmlContext.push(XmlContext.CHUNK);
                                xmlElt.push(chunk);
                            } else if ("decision".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                NodeContainer container = (NodeContainer) xmlElt.getFirst();
                                Artifact artifact = new ArtifactBuilder().setRef(ref).build();
                                Decision decision = new DecisionBuilder().setArtifact(artifact).build();
                                switch (xmlContext.getFirst()) {
                                    case JOB:
                                        ((Job) xmlElt.getFirst()).addDecision(decision);
                                        break;
                                    case FLOW:
                                        ((Flow) xmlElt.getFirst()).addDecision(decision);
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context "
                                                + xmlContext.getFirst());
                                }
                                xmlContext.push(XmlContext.DECISION);
                                xmlElt.push(decision);
                            } else if ("properties".equals(localName)) {
                                Integer value = XmlUtil.getAttributeIntegerValue(xmlsr, null, "partition");
                                if (value != null) {
                                    partition = value;
                                }
                            } else if ("property".equals(localName)) {
                                String name = xmlsr.getAttributeValue(null, "name");
                                String value = xmlsr.getAttributeValue(null, "value");
                                switch (xmlContext.getFirst()) {
                                    case JOB:
                                        ((Job) xmlElt.getFirst()).getProperties().setProperty(name, value);
                                        break;
                                    case STEP:
                                        ((StepElement) xmlElt.getFirst()).getProperties().setProperty(name, value);
                                        break;
                                    case BATCHLET:
                                        ((BatchletStep) xmlElt.getFirst()).getArtifact().getProperties().setProperty(name, value);
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
                                                chunk.getReader().getProperties().setProperty(propertyName, value);
                                            } else if (chunk.getProcessor().getRef().equals(artifactName)) {
                                                chunk.getProcessor().getProperties().setProperty(propertyName, value);
                                            } else if (chunk.getWriter().getRef().equals(artifactName)) {
                                                chunk.getWriter().getProperties().setProperty(propertyName, value);
                                            } else {
                                                throw new JabatException("Artifact " + artifactName
                                                        + " not found");
                                            }
                                        }
                                        break;
                                    case SPLIT:
                                    case FLOW:
                                    case DECISION:
                                        ((Node) xmlElt.getFirst()).getProperties().setProperty(name, value);
                                        break;
                                    case CHECKPOINT_ALGORITHM:
                                    case LISTENER:
                                    case COLLECTOR:
                                    case ANALYSER:
                                        ((Artifact) xmlElt.getFirst()).getProperties().setProperty(name, value);
                                        break;
                                    case PLAN:
                                        PartitionPlan plan = ((PartitionPlan) xmlElt.getFirst());
                                        if (partition < 0 || partition >= plan.getPartitionCount()) {
                                            throw new JabatException("Inconsistent partition number: " + partition);
                                        }
                                        plan.getPartitionProperties()[partition].setProperty(name, value);
                                        break;
                                    default:
                                        throw new JabatException("Unexpected Xml context "
                                                + xmlContext.getFirst());
                                }
                            } else if ("listener".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                Artifact listener = new ArtifactBuilder().setRef(ref).build();
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
                                    Artifact checkpointAlgo = new ArtifactBuilder().setRef(ref).build();
                                    chunk.setCheckpointAlgo(checkpointAlgo);
                                    xmlContext.push(XmlContext.CHECKPOINT_ALGORITHM);
                                    xmlElt.push(checkpointAlgo);
                                } else {
                                    throw new JabatException("Unexpected Xml context "
                                            + xmlContext.getFirst());
                                }
                            } else if ("collector".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                Artifact collector = new ArtifactBuilder().setRef(ref).build();
                                switch (xmlContext.getFirst()) {
                                    case SPLIT:
                                        {
                                            Split split = (Split) xmlElt.getFirst();
                                            split.setCollector(collector);
                                            xmlContext.push(XmlContext.COLLECTOR);
                                            xmlElt.push(collector);
                                        }
                                        break;
                                    case STEP:
                                        {
                                            StepElement step = (StepElement) xmlElt.getFirst();
                                            step.setPartitionCollector(collector);
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
                                Artifact analyser = new ArtifactBuilder().setRef(ref).build();
                                switch (xmlContext.getFirst()) {
                                    case SPLIT:
                                        {
                                            Split split = (Split) xmlElt.getFirst();
                                            split.setAnalyser(analyser);
                                            xmlContext.push(XmlContext.ANALYSER);
                                            xmlElt.push(analyser);
                                        }
                                        break;
                                    case STEP:
                                        {
                                            StepElement step = (StepElement) xmlElt.getFirst();
                                            step.setPartitionAnalyser(analyser);
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
                                Artifact mapper = new ArtifactBuilder().setRef(ref).build();
                                if (xmlContext.getFirst() == XmlContext.STEP) {
                                    StepElement step = (StepElement) xmlElt.getFirst();
                                    step.setPartitionMapper(mapper);
                                    xmlContext.push(XmlContext.MAPPER);
                                    xmlElt.push(mapper);
                                } else {
                                    throw new JabatException("Unexpected Xml context "
                                            + xmlContext.getFirst());
                                }
                            } else if ("reducer".equals(localName)) {
                                String ref = xmlsr.getAttributeValue(null, "ref");
                                Artifact reducer = new ArtifactBuilder().setRef(ref).build();
                                if (xmlContext.getFirst() == XmlContext.STEP) {
                                    StepElement step = (StepElement) xmlElt.getFirst();
                                    step.setPartitionReducer(reducer);
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
                            } else if ("plan".equals(localName)) {
                                Integer value = XmlUtil.getAttributeIntegerValue(xmlsr, null, "instances");
                                int instances = value != null ? value : 1;
                                value = XmlUtil.getAttributeIntegerValue(xmlsr, null, "threads");
                                int threads = value != null ? value : instances;
                                PartitionPlanImpl plan = new PartitionPlanImpl(instances, threads);
                                if (xmlContext.getFirst() == XmlContext.STEP) {
                                    ((StepElement) xmlElt.getFirst()).setPartitionPlan(plan);
                                    xmlContext.push(XmlContext.PLAN);
                                    xmlElt.push(plan);
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
                                    || "plan".equals(localName)) {
                                xmlContext.pop();
                                xmlElt.pop();
                            } else if ("properties".equals(localName)) {
                                partition = -1;
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
