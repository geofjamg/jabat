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
import fr.jamgotchian.jabat.jobxml.model.BatchletStep;
import fr.jamgotchian.jabat.jobxml.model.ChunkStep;
import fr.jamgotchian.jabat.jobxml.model.ControlElement;
import fr.jamgotchian.jabat.jobxml.model.Decision;
import fr.jamgotchian.jabat.jobxml.model.EndElement;
import fr.jamgotchian.jabat.jobxml.model.ExceptionClassFilter;
import fr.jamgotchian.jabat.jobxml.model.FailElement;
import fr.jamgotchian.jabat.jobxml.model.Flow;
import fr.jamgotchian.jabat.jobxml.model.Job;
import fr.jamgotchian.jabat.jobxml.model.NextElement;
import fr.jamgotchian.jabat.jobxml.model.Node;
import fr.jamgotchian.jabat.jobxml.model.NodeContainer;
import fr.jamgotchian.jabat.jobxml.model.Split;
import fr.jamgotchian.jabat.jobxml.model.Step;
import fr.jamgotchian.jabat.jobxml.model.StopElement;
import java.io.Writer;
import java.util.List;
import java.util.Properties;
import javax.batch.api.parameters.PartitionPlan;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class XMLizer implements JobXmlConstants {

    private void writeProperties(Properties properties, int partition, XMLStreamWriter sw) throws XMLStreamException {
        if (properties.isEmpty()) {
            return;
        }
        sw.writeStartElement(NS_URI, "properties");
        if (partition != -1) {
            sw.writeAttribute("partition", Integer.toString(partition));
        }
        for (String name : properties.stringPropertyNames()) {
            String value = properties.getProperty(name);
            sw.writeStartElement(NS_URI, "property");
            sw.writeAttribute("name", name);
            sw.writeAttribute("value", value);
            sw.writeEndElement();
        }
        sw.writeEndElement();
    }

    private void writerArtifact(Artifact artifact, String artifactName, XMLStreamWriter sw) throws XMLStreamException {
        sw.writeStartElement(NS_URI, artifactName);
        sw.writeAttribute("ref", artifact.getRef());
        writeProperties(artifact.getProperties(), -1, sw);
        sw.writeEndElement();
    }

    private void writeListeners(List<Artifact> listeners, XMLStreamWriter sw) throws XMLStreamException {
        if (listeners.isEmpty()) {
            return;
        }
        sw.writeStartElement(NS_URI, "listeners");
        for (Artifact listener : listeners) {
            writerArtifact(listener, "listener", sw);
        }
        sw.writeEndElement();
    }

    private void writePlan(PartitionPlan plan, XMLStreamWriter sw) throws XMLStreamException {
        sw.writeStartElement(NS_URI, "plan");
        sw.writeAttribute("instances", Integer.toString(plan.getPartitionCount()));
        sw.writeAttribute("threads", Integer.toString(plan.getThreadCount()));
        Properties properties[] = plan.getPartitionProperties();
        for (int i = 0; i < plan.getPartitionCount(); i++) {
            writeProperties(properties[i], i, sw);
        }
        sw.writeEndElement();
    }

    private void writeControlElements(List<? extends ControlElement> controlElements, XMLStreamWriter sw) throws XMLStreamException {
        for (ControlElement controlElement : controlElements) {
            switch (controlElement.getType()) {
                case END:
                    {
                        EndElement endElement = (EndElement) controlElement;
                        sw.writeStartElement(NS_URI, "end");
                        sw.writeAttribute("on", endElement.getOn());
                        if (endElement.getExitStatus() != null) {
                            sw.writeAttribute("exit-status", endElement.getExitStatus());
                        }
                        sw.writeEndElement();
                    }
                    break;
                case FAIL:
                    {
                        FailElement failElement = (FailElement) controlElement;
                        sw.writeStartElement(NS_URI, "fail");
                        sw.writeAttribute("on", failElement.getOn());
                        if (failElement.getExitStatus() != null) {
                            sw.writeAttribute("exit-status", failElement.getExitStatus());
                        }
                        sw.writeEndElement();
                    }
                    break;
                case STOP:
                    {
                        StopElement stopElement = (StopElement) controlElement;
                        sw.writeStartElement(NS_URI, "stop");
                        sw.writeAttribute("on", stopElement.getOn());
                        if (stopElement.getExitStatus() != null) {
                            sw.writeAttribute("exit-status", stopElement.getExitStatus());
                        }
                        if (stopElement.getRestart() != null) {
                            sw.writeAttribute("restart", stopElement.getRestart());
                        }
                        sw.writeEndElement();
                    }
                    break;
                case NEXT:
                    {
                        NextElement nextElement = (NextElement) controlElement;
                        sw.writeStartElement(NS_URI, "next");
                        sw.writeAttribute("on", nextElement.getOn());
                        sw.writeAttribute("to", nextElement.getTo());
                        sw.writeEndElement();
                    }
                    break;
                default:
                    throw new InternalError();
            }
        }
    }

    private void writeStepBefore(Step step, XMLStreamWriter sw) throws XMLStreamException {
        sw.writeStartElement(NS_URI, "step");
        sw.writeAttribute("id", step.getId());
        sw.writeAttribute("start-limit", Integer.toString(step.getStartLimit()));
        sw.writeAttribute("allow-start-if-complete", Boolean.toString(step.isAllowStartIfComplete()));
        if (step.getNext() != null) {
            sw.writeAttribute("next", step.getNext());
        }

        if (step.getPartitionPlan() != null || step.getPartitionMapper() != null) {
            sw.writeStartElement(NS_URI, "partition");
            if (step.getPartitionPlan() != null) {
                writePlan(step.getPartitionPlan(), sw);
            }
            if (step.getPartitionMapper() != null) {
                writerArtifact(step.getPartitionMapper(), "mapper", sw);
            }
            if (step.getPartitionReducer() != null) {
                writerArtifact(step.getPartitionReducer(), "reducer", sw);
            }
            if (step.getPartitionCollector() != null) {
                writerArtifact(step.getPartitionCollector(), "collector", sw);
            }
            if (step.getPartitionAnalyzer() != null) {
                writerArtifact(step.getPartitionAnalyzer(), "analyzer", sw);
            }
            sw.writeEndElement();
        }

        writeProperties(step.getProperties(), -1, sw);

        writeListeners(step.getListeners(), sw);
    }

    private void writeStepAfter(Step step, XMLStreamWriter sw) throws XMLStreamException {
        writeControlElements(step.getControlElements(), sw);
        sw.writeEndElement();
    }

    private void writeBatchletStep(BatchletStep step, XMLStreamWriter sw) throws XMLStreamException {
        writeStepBefore(step, sw);
        writerArtifact(step.getArtifact(), "batchlet", sw);
        writeStepAfter(step, sw);
    }

    private void writeExceptionClassFilter(ExceptionClassFilter filter, String filterName, XMLStreamWriter sw) throws XMLStreamException {
        if (filter.getIncludedClasses().isEmpty()
                && filter.getExcludedClasses().isEmpty()) {
            return;
        }
        sw.writeStartElement(NS_URI, filterName);
        for (Class<?> clazz : filter.getIncludedClasses()) {
            sw.writeStartElement(NS_URI, "include");
            sw.writeAttribute("class", clazz.getName());
            sw.writeEndElement();
        }
        for (Class<?> clazz : filter.getExcludedClasses()) {
            sw.writeStartElement(NS_URI, "exclude");
            sw.writeAttribute("class", clazz.getName());
            sw.writeEndElement();
        }
        sw.writeEndElement();
    }

    private void writeChunkStep(ChunkStep step, XMLStreamWriter sw) throws XMLStreamException {
        writeStepBefore(step, sw);
        sw.writeAttribute("reader", step.getReader().getRef());
        sw.writeAttribute("processor", step.getProcessor().getRef());
        sw.writeAttribute("writer", step.getWriter().getRef());
        sw.writeAttribute("checkpoint-policy", step.getCheckpointPolicy().name().toLowerCase());
        sw.writeAttribute("commit-interval", Integer.toString(step.getCommitInterval()));
        sw.writeAttribute("buffer-size", Integer.toString(step.getBufferSize()));
        if (step.getSkipLimit() != -1) {
            sw.writeAttribute("skip-limit", Integer.toString(step.getSkipLimit()));
        }
        if (step.getRetryLimit() != -1) {
            sw.writeAttribute("retry-limit", Integer.toString(step.getRetryLimit()));
        }

        Properties properties = new Properties();
        for (String propertyName : step.getReader().getProperties().stringPropertyNames()) {
            String value = step.getReader().getProperties().getProperty(propertyName);
            String name = step.getReader().getRef() + ":" + propertyName;
            properties.setProperty(name, value);
        }
        for (String propertyName : step.getProcessor().getProperties().stringPropertyNames()) {
            String value = step.getProcessor().getProperties().getProperty(propertyName);
            String name = step.getProcessor().getRef() + ":" + propertyName;
            properties.setProperty(name, value);
        }
        for (String propertyName : step.getWriter().getProperties().stringPropertyNames()) {
            String value = step.getWriter().getProperties().getProperty(propertyName);
            String name = step.getWriter().getRef() + ":" + propertyName;
            properties.setProperty(name, value);
        }
        writeProperties(properties, -1, sw);

        if (step.getCheckpointAlgo() != null) {
            writerArtifact(step.getCheckpointAlgo(), "checkpoint-algorithm", sw);
        }

        writeExceptionClassFilter(step.getSkippableExceptionClasses(), "skippable-exception-classes", sw);
        writeExceptionClassFilter(step.getRetryableExceptionClasses(), "retryable-exception-classes", sw);
        writeExceptionClassFilter(step.getNoRollbackExceptionClasses(), "no-rollback-exception-classes", sw);

        writeStepAfter(step, sw);
    }

    private void writeSplit(Split split, XMLStreamWriter sw) throws XMLStreamException {
        sw.writeStartElement(NS_URI, "split");
        sw.writeAttribute("id", split.getId());
        if (split.getNext() != null) {
            sw.writeAttribute("next", split.getNext());
        }

        writeProperties(split.getProperties(), -1, sw);

        writeListeners(split.getListeners(), sw);

        writeNodes(split, sw);

        sw.writeEndElement();
    }

    private void writeFlow(Flow flow, XMLStreamWriter sw) throws XMLStreamException {
        sw.writeStartElement(NS_URI, "flow");
        sw.writeAttribute("id", flow.getId());
        if (flow.getNext() != null) {
            sw.writeAttribute("next", flow.getNext());
        }
        writeProperties(flow.getProperties(), -1, sw);

        writeListeners(flow.getListeners(), sw);

        writeNodes(flow, sw);

        sw.writeEndElement();
    }

    private void writeDecision(Decision decision, XMLStreamWriter sw) throws XMLStreamException {
        sw.writeStartElement(NS_URI, "decision");
        sw.writeAttribute("id", decision.getId());
        sw.writeAttribute("ref", decision.getArtifact().getRef());

        writeProperties(decision.getProperties(), -1, sw);

        writeControlElements(decision.getControlElements(), sw);

        sw.writeEndElement();
    }

    private void writeNodes(NodeContainer container, XMLStreamWriter sw) throws XMLStreamException {
        for (Node node : container.getNodes()) {
            switch (node.getType()) {
                case JOB:
                    // nothing
                    break;
                case BATCHLET_STEP:
                    writeBatchletStep((BatchletStep) node, sw);
                    break;
                case CHUNK_STEP:
                    writeChunkStep((ChunkStep) node, sw);
                    break;
                case SPLIT:
                    writeSplit((Split) node, sw);
                    break;
                case FLOW:
                    writeFlow((Flow) node, sw);
                    break;
                case DECISION:
                    writeDecision((Decision) node, sw);
                    break;
                default:
                    throw new InternalError();
            }
        }
    }

    private void writeJob(Job job, XMLStreamWriter sw) throws XMLStreamException {
        sw.writeStartElement(NS_URI, "job");
        sw.writeNamespace(NS_PREFIX, NS_URI);
        sw.writeAttribute("id", job.getId());
        sw.writeAttribute("restartable", Boolean.toString(job.isRestartable()));

        writeProperties(job.getProperties(), -1, sw);

        writeListeners(job.getListeners(), sw);

        writeNodes(job, sw);

        sw.writeEndElement();
    }

    public void process(Job job, Writer writer) throws XMLStreamException {
        XMLOutputFactory output = XMLOutputFactory.newInstance();
        XMLStreamWriter sw = output.createXMLStreamWriter(writer);
        sw.writeStartDocument("UTF-8", "1.0");
        sw.setPrefix(NS_PREFIX, NS_URI);

        writeJob(job, sw);

        sw.writeEndDocument();
        sw.flush();
    }
}
