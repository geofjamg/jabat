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
import fr.jamgotchian.jabat.jobxml.model.BatchletStepBuilder;
import fr.jamgotchian.jabat.jobxml.model.CheckpointPolicy;
import fr.jamgotchian.jabat.jobxml.model.ChunkStepBuilder;
import fr.jamgotchian.jabat.jobxml.model.ConsistencyReport;
import fr.jamgotchian.jabat.jobxml.model.ControlElement;
import fr.jamgotchian.jabat.jobxml.model.Decision;
import fr.jamgotchian.jabat.jobxml.model.DecisionBuilder;
import fr.jamgotchian.jabat.jobxml.model.EndElement;
import fr.jamgotchian.jabat.jobxml.model.FailElement;
import fr.jamgotchian.jabat.jobxml.model.Flow;
import fr.jamgotchian.jabat.jobxml.model.FlowBuilder;
import fr.jamgotchian.jabat.jobxml.model.Job;
import fr.jamgotchian.jabat.jobxml.model.JobBuilder;
import fr.jamgotchian.jabat.jobxml.model.JobConsistencyChecker;
import fr.jamgotchian.jabat.jobxml.model.NextElement;
import fr.jamgotchian.jabat.jobxml.model.PartitionPlanBuilder;
import fr.jamgotchian.jabat.jobxml.model.Split;
import fr.jamgotchian.jabat.jobxml.model.SplitBuilder;
import fr.jamgotchian.jabat.jobxml.model.Step;
import fr.jamgotchian.jabat.jobxml.model.StepBuilder;
import fr.jamgotchian.jabat.jobxml.model.StopElement;
import fr.jamgotchian.jabat.jobxml.util.JobXmlException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.batch.api.parameters.PartitionPlan;
import javax.batch.runtime.NoSuchJobException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;
import org.jdom2.input.sax.XMLReaderSchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobXmlLoader implements JobXmlConstants {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobXmlLoader.class);

    private final TopLevelNodeSearcher searcher = new TopLevelNodeSearcherImpl();

    private static interface ExceptionClassFilterer {

        void include(Class<?> clazz);

        void exclude(Class<?> clazz);

    }

    private static interface ControlElementAdder {

        void addControlElement(ControlElement controlElement);
    }

    /**
     * Create properties from a job xml fragment.
     *
     * <jsl:properties>
     *     <jsl:property name="{property-name}" value="{property-value}"/>
     *     <jsl:property name="{property-name}" value="{property-value}"/>
     * </jsl:properties>
     *
     */
    private Properties createProperties(Element propertiesElem, Namespace ns) {
        Properties properties = new Properties();
        for (Element propertyElem : propertiesElem.getChildren("property", ns)) {
            String name = propertyElem.getAttributeValue("name");
            String value = propertyElem.getAttributeValue("value");
            properties.setProperty(name, value);
        }
        return properties;
    }

    /**
     * Create a batch artifact from a job xml fragment.
     *
     * <jsl:{artifact-type} ref="{artifact-name}">
     *     <jsl:properties>
     *         <jsl:property name="{property-name}" value="{property-value}"/>
     *     </jsl:properties>
     * </jsl:{artifact-type}>
     */
    private Artifact createArtifact(Element artifactElem, Namespace ns) {
        String ref = artifactElem.getAttributeValue("ref");
        ArtifactBuilder builder = new ArtifactBuilder();
        builder.setRef(ref);
        Element propertiesElem = artifactElem.getChild("properties", ns);
        if (propertiesElem != null) {
            builder.addProperties(createProperties(propertiesElem, ns));
        }
        return builder.build();
    }

    /**
     * Create listeners from a job xml fragment.
     *
     * <jsl:listeners>
     *     <jsl:listener ref="{artifact-name}">
     *         <jsl:properties>
     *             <jsl:property name="{property-name}" value="{property-value}"/>
     *         </jsl:properties>
     *     </jsl:listener>
     * </jsl:listeners>
     *
     */
    private List<Artifact> createListeners(Element listenersElem, Namespace ns) {
        List<Artifact> listeners = new ArrayList<Artifact>();
        for (Element listenerElem : listenersElem.getChildren("listener", ns)) {
            listeners.add(createArtifact(listenerElem, ns));
        }
        return listeners;
    }

    /**
     * Create a batchlet step from a job xml fragment.
     *
     * <jsl:batchlet ref="{artifact-name}">
     *     <jsl:properties>
     *         <jsl:property name="{property-name}" value="{property-value}"/>
     *     </jsl:properties>
     * </jsl:batchlet>
     *
     */
    private BatchletStepBuilder createBatchletBuilder(Element batchletElem, Namespace ns) {
        BatchletStepBuilder builder = new BatchletStepBuilder();
        builder.setArtifact(createArtifact(batchletElem, ns));
        return builder;
    }

    /**
     * Create an exception class filter from a job xml fragment.
     *
     * <jsl:{exception-class-filter-type}>
     *     <include class="{class-name}"/>
     *     <exclude class="{class-name}"/>
     * </jsl:{exception-class-filter-type}>
     *
     */
    private void processExceptionClassFilter(Element exceptionClassFilterElem, Namespace ns,
                                             ExceptionClassFilterer filterer) {
        try {
            for (Element includeElem : exceptionClassFilterElem.getChildren("include", ns)) {
                String className = includeElem.getAttributeValue("class");
                filterer.include(Class.forName(className));
            }
            for (Element includeElem : exceptionClassFilterElem.getChildren("exclude", ns)) {
                String className = includeElem.getAttributeValue("class");
                filterer.exclude(Class.forName(className));
            }
        } catch (ClassNotFoundException e) {
            throw new JobXmlException(e);
        }
    }

    /**
     * Create a chunk step from a job xml fragment.
     *
     * <jsl:chunk reader="{artifact-name}" processor="{artifact-name}" writer="{artifact-name}"
     *            checkpoint-policy="{item|time|custom}" commit-interval="{value}"
     *            buffer-size="{value}" skip-limit="{value}" retry-limit="{value}">
     *     <jsl:properties>
     *         <jsl:property name="{artifact-name:property-name}" value="{property-value}"/>
     *     </jsl:properties>
     *     <jsl:checkpoint-algorithm ref="{artifact-name}">
     *         <jsl:properties>
     *             <jsl:property name="{property-name}" value="{property-value}"/>
     *         </jsl:properties>
     *     </jsl:checkpoint-algorithm>
     *     <jsl:skippable-exception-classes>
     *         <include class="{class-name}"/>
     *         <exclude class="{class-name}"/>
     *     </jsl:skippable-exception-classes>
     *     <jsl:retryable-exception-classes>
     *         <include class="{class-name}"/>
     *         <exclude class="{class-name}"/>
     *     </jsl:retryable-exception-classes>
     *     <jsl:no-rollback-exception-classes>
     *         <include class="{class-name}"/>
     *         <exclude class="{class-name}"/>
     *     </jsl:no-rollback-exception-classes>
     * </jsl:chunk>
     *
     */
    private ChunkStepBuilder createChunkBuilder(Element chunkElem, Namespace ns) {
        final ChunkStepBuilder builder = new ChunkStepBuilder();

        ArtifactBuilder readerBuilder = new ArtifactBuilder();
        ArtifactBuilder processorBuilder = new ArtifactBuilder();
        ArtifactBuilder writerBuilder = new ArtifactBuilder();

        String readerRef = chunkElem.getAttributeValue("reader");
        String processorRef = chunkElem.getAttributeValue("processor");
        String writerRef = chunkElem.getAttributeValue("writer");
        readerBuilder.setRef(readerRef);
        processorBuilder.setRef(processorRef);
        writerBuilder.setRef(writerRef);

        Element propertiesElem = chunkElem.getChild("properties", ns);
        if (propertiesElem != null) {
            Properties properties = createProperties(propertiesElem, ns);
            for (String name : properties.stringPropertyNames()) {
                String value = properties.getProperty(name);
                String[] split = name.split(":");
                if (split.length != 2) {
                    throw new JobXmlException("Chunk property syntax error, it should be <artifact-name:property-name>");
                }
                String artifactName = split[0];
                String propertyName = split[1];
                if (artifactName == null || artifactName.isEmpty()) {
                    throw new JobXmlException("Chunk property syntax error, invalid artifact name '"
                            + artifactName + "'");
                }
                if (artifactName.equals(readerRef)) {
                    readerBuilder.addProperty(propertyName, value);
                } else if (artifactName.equals(processorRef)) {
                    processorBuilder.addProperty(propertyName, value);
                } else if (artifactName.equals(writerRef)) {
                    writerBuilder.addProperty(propertyName, value);
                } else {
                    throw new JobXmlException("Artifact '" + artifactName + "' not found");
                }
            }
        }

        builder.setReader(readerBuilder.build());
        builder.setProcessor(processorBuilder.build());
        builder.setWriter(writerBuilder.build());

        String value = chunkElem.getAttributeValue("checkpoint-policy");
        if (value != null) {
            builder.setCheckpointPolicy(CheckpointPolicy.valueOf(value.toUpperCase()));
        }

        value = chunkElem.getAttributeValue("commit-interval");
        if (value != null) {
            builder.setCommitInterval(Integer.valueOf(value));
        }

        value = chunkElem.getAttributeValue("buffer-size");
        if (value != null) {
            builder.setBufferSize(Integer.valueOf(value));
        }

        value = chunkElem.getAttributeValue("retry-limit");
        if (value != null) {
            builder.setRetryLimit(Integer.valueOf(value));
        }

        value = chunkElem.getAttributeValue("skip-limit");
        if (value != null) {
            builder.setSkipLimit(Integer.valueOf(value));
        }

        Element checkpointAlgoElem = chunkElem.getChild("checkpoint-algorithm", ns);
        if (checkpointAlgoElem != null) {
            builder.setCheckpointAlgo(createArtifact(checkpointAlgoElem, ns));
        }

        Element skippableExceptionClassElem = chunkElem.getChild("skippable-exception-classes", ns);
        if (skippableExceptionClassElem != null) {
            processExceptionClassFilter(skippableExceptionClassElem, ns,
                    new ExceptionClassFilterer() {

                @Override
                public void include(Class<?> clazz) {
                    builder.includeSkippableException(clazz);
                }

                @Override
                public void exclude(Class<?> clazz) {
                    builder.excludeSkippableException(clazz);
                }
            });
        }

        Element retryableExceptionClassElem = chunkElem.getChild("retryable-exception-classes", ns);
        if (retryableExceptionClassElem != null) {
            processExceptionClassFilter(retryableExceptionClassElem, ns,
                    new ExceptionClassFilterer() {

                @Override
                public void include(Class<?> clazz) {
                    builder.includeRetryableException(clazz);
                }

                @Override
                public void exclude(Class<?> clazz) {
                    builder.excludeRetryableException(clazz);
                }
            });
        }

        Element noRollbackExceptionClassElem = chunkElem.getChild("no-rollback-exception-classes", ns);
        if (noRollbackExceptionClassElem != null) {
            processExceptionClassFilter(retryableExceptionClassElem, ns,
                    new ExceptionClassFilterer() {

                @Override
                public void include(Class<?> clazz) {
                    builder.includeNoRollbackException(clazz);
                }

                @Override
                public void exclude(Class<?> clazz) {
                    builder.excludeNoRollbackException(clazz);
                }
            });
        }

        return builder;
    }

    /**
     * Create a partition plan from a job xml fragment.
     *
     * <jsl:plan instances="{value}" threads="{value}">
     *     <jsl:properties partition="{partition-number}">
     *         <jsl:property name="{property-name}" value="{property-value}"/>
     *     </jsl:properties>
     *     <jsl:properties partition="{partition-number}">
     *         <jsl:property name="{property-name}" value="{property-value}"/>
     *     </jsl:properties>
     * </jsl:plan>
     *
     */
    private PartitionPlan createPlan(Element planElem, Namespace ns) {
        PartitionPlanBuilder builder = new PartitionPlanBuilder();
        String value = planElem.getAttributeValue("instances");
        if (value != null) {
            builder.setPartitionCount(Integer.valueOf(value));
        }
        value = planElem.getAttributeValue("threads");
        if (value != null) {
            builder.setThreadCount(Integer.valueOf(value));
        }
        for (Element propertiesElem : planElem.getChildren("properties", ns)) {
            value = propertiesElem.getAttributeValue("partition");
            int partition = -1;
            if (value != null) {
                partition = Integer.valueOf(value);
            }
            Properties properties = createProperties(propertiesElem, ns);
            builder.addProperties(partition, properties);
        }
        return builder.build();
    }

    /**
     * Create a step from a job xml fragment.
     *
     * <jsl:step id="{step-id}" start-limit="{value}" allow-start-if-complete="{value}" next="{node-id}" >
     *     <jsl:partition>...
     *     </jsl:partition>
     *     <jsl:properties>...
     *     </jsl:properties>
     *     <jsl:listeners>...
     *     </jsl:listeners>
     *     <jsl:batchlet|chunk>...
     *     </jsl:batchlet|chunk>
     *     <jsl:fail ... />
     *     <jsl:end ... />
     *     <jsl:stop ... />
     *     <jsl:next ... />
     * </jsl:step>
     */
    private Step createStep(Element stepElem, Namespace ns) {
        final StepBuilder builder;

        Element batchletElem = stepElem.getChild("batchlet", ns);
        if (batchletElem != null) {
           builder = createBatchletBuilder(batchletElem, ns);
        } else {
            Element chunkElem = stepElem.getChild("chunk", ns);
            builder = createChunkBuilder(chunkElem, ns);
        }

        String id = stepElem.getAttributeValue("id");
        builder.setId(id);

        String next = stepElem.getAttributeValue("next");
        builder.setNext(next);

        String startLimit = stepElem.getAttributeValue("start-limit");
        if (startLimit != null) {
            builder.setStartLimit(Integer.valueOf(startLimit));
        }

        String allowStartIfComplete = stepElem.getAttributeValue("allow-start-if-complete");
        if (allowStartIfComplete != null) {
            builder.setAllowStartIfComplete(Boolean.valueOf(startLimit));
        }

        Element partitionElem = stepElem.getChild("partition", ns);
        if (partitionElem != null) {
            Element planElem = partitionElem.getChild("plan", ns);
            if (planElem != null) {
                builder.setPartitionPlan(createPlan(planElem, ns));
            }
            Element mapperElem = partitionElem.getChild("mapper", ns);
            if (mapperElem != null) {
                builder.setPartitionMapper(createArtifact(mapperElem, ns));
            }
            Element reducerElem = partitionElem.getChild("reducer", ns);
            if (reducerElem != null) {
                builder.setPartitionReducer(createArtifact(reducerElem, ns));
            }
            Element collectorElem = partitionElem.getChild("collector", ns);
            if (collectorElem != null) {
                builder.setPartitionCollector(createArtifact(collectorElem, ns));
            }
            Element analyzerElem = partitionElem.getChild("analyzer", ns);
            if (analyzerElem != null) {
                builder.setPartitionAnalyzer(createArtifact(analyzerElem, ns));
            }
        }

        Element propertiesElem = stepElem.getChild("properties", ns);
        if (propertiesElem != null) {
            builder.addProperties(createProperties(propertiesElem, ns));
        }

        Element listenersElem = stepElem.getChild("listeners", ns);
        if (listenersElem != null) {
            builder.addListeners(createListeners(listenersElem, ns));
        }

        processControlElements(stepElem, ns, new ControlElementAdder() {
            @Override
            public void addControlElement(ControlElement controlElement) {
                builder.addControlElement(controlElement);
            }
        });

        return builder.build();
    }

    /**
     * Create a flow from a job xml fragment.
     *
     * <jsl:flow id="{flow-id}" next="{node-id}">
     *     <jsl:properties>...
     *     </jsl:properties>
     *     <jsl:listeners>...
     *     </jsl:listeners>
     *     <jsl:step>...
     *     </jsl:step>
     *     <jsl:split>...
     *     </jsl:split>
     *     <jsl:decision>...
     *     </jsl:decision>
     * </jsl:flow>
     */
    private Flow createFlow(Element flowElem, Namespace ns) {
        FlowBuilder builder = new FlowBuilder();

        String id = flowElem.getAttributeValue("id");
        builder.setId(id);

        String next = flowElem.getAttributeValue("next");
        builder.setNext(next);

        Element propertiesElem = flowElem.getChild("properties", ns);
        if (propertiesElem != null) {
            builder.addProperties(createProperties(propertiesElem, ns));
        }

        Element listenersElem = flowElem.getChild("listeners", ns);
        if (listenersElem != null) {
            builder.addListeners(createListeners(listenersElem, ns));
        }

        for (Element stepElem : flowElem.getChildren("step", ns)) {
            builder.addStep(createStep(stepElem, ns));
        }

        for (Element splitElem : flowElem.getChildren("split", ns)) {
            builder.addSplit(createSplit(splitElem, ns));
        }

        for (Element decisionElem : flowElem.getChildren("decision", ns)) {
            builder.addDecision(createDecision(decisionElem, ns));
        }

        return builder.build();
    }

    /**
     * Create a split from a job xml fragment.
     *
     * <jsl:split id="{split-id}" next="{node-id}">
     *     <jsl:properties>...
     *     </jsl:properties>
     *     <jsl:listeners>...
     *     </jsl:listeners>
     *     <jsl:flow>...
     *     </jsl:flow>
     * </jsl:split>
     */
    private Split createSplit(Element splitElem, Namespace ns) {
        SplitBuilder builder = new SplitBuilder();

        String id = splitElem.getAttributeValue("id");
        builder.setId(id);

        String next = splitElem.getAttributeValue("next");
        builder.setNext(next);

        Element propertiesElem = splitElem.getChild("properties", ns);
        if (propertiesElem != null) {
            builder.addProperties(createProperties(propertiesElem, ns));
        }

        Element listenersElem = splitElem.getChild("listeners", ns);
        if (listenersElem != null) {
            builder.addListeners(createListeners(listenersElem, ns));
        }

        for (Element flowElem : splitElem.getChildren("flow", ns)) {
            builder.addFlow(createFlow(flowElem, ns));
        }

        return builder.build();
    }

    /**
     * Create a end element from a job xml fragment.
     *
     * <jsl:end on="{exit-status}" exit-status="{exit-status}" />
     */
    private EndElement createEndElement(Element endElem) {
        String on = endElem.getAttributeValue("on");
        String exitStatus = endElem.getAttributeValue("exit-status");
        return new EndElement(on, exitStatus);
    }

    /**
     * Create a fail element from a job xml fragment.
     *
     * <jsl:fail on="{exit-status}" exit-status="{exit-status}" />
     */
    private FailElement createFailElement(Element failElem) {
        String on = failElem.getAttributeValue("on");
        String exitStatus = failElem.getAttributeValue("exit-status");
        return new FailElement(on, exitStatus);
    }

    /**
     * Create a stop element from a job xml fragment.
     *
     * <jsl:stop on="{exit-status}" exit-status="{exit-status} restart="{step-id|flow-id|split-id}" />
     */
    private StopElement createStopElement(Element stopElem) {
        String on = stopElem.getAttributeValue("on");
        String exitStatus = stopElem.getAttributeValue("exit-status");
        String restart = stopElem.getAttributeValue("restart");
        return new StopElement(on, exitStatus, restart);
    }

    /**
     * Create a next element from a job xml fragment.
     *
     * <jsl:next on="{exit-status}" to="{step-id|flow-id|split-id}" />
     */
    private NextElement createNextElement(Element failElem) {
        String on = failElem.getAttributeValue("on");
        String to = failElem.getAttributeValue("to");
        return new NextElement(on, to);
    }

    /**
     * Create control elements from a job xml fragment.
     *
     * <jsl:fail on="{exit-status}" exit-status="{exit-status}" />
     * <jsl:end on="{exit-status}" exit-status="{exit-status}" />
     * <jsl:stop on="{exit-status}" exit-status="{exit-status} restart="{step-id|flow-id|split-id}" />
     * <jsl:next on="{exit-status}" to="{step-id|flow-id|split-id}" />
     */
    private void processControlElements(Element elem, Namespace ns, ControlElementAdder adder) {
        for (Element childElem : elem.getChildren()) {
            if (childElem.getNamespace().equals(ns)) {
                if ("end".equals(childElem.getName())) {
                    adder.addControlElement(createEndElement(childElem));
                } else if ("fail".equals(childElem.getName())) {
                    adder.addControlElement(createFailElement(childElem));
                } else if ("stop".equals(childElem.getName())) {
                    adder.addControlElement(createStopElement(childElem));
                } else if ("next".equals(childElem.getName())) {
                    adder.addControlElement(createNextElement(childElem));
                }
            }
        }
    }

    /**
     * Create a decision from a job xml fragment.
     *
     * <jsl:decision id="{decision-id}" ref="{artifact-name}">
     *     <jsl:properties>
     *         <jsl:property name="{property-name}" value="{property-value}"/>
     *     </jsl:properties>
     *     <jsl:fail ... />
     *     <jsl:end ... />
     *     <jsl:stop ... />
     *     <jsl:next ... />
     * </jsl:decision>
     *
     */
    private Decision createDecision(Element decisionElem, Namespace ns) {
        final DecisionBuilder builder = new DecisionBuilder();

        String id = decisionElem.getAttributeValue("id");
        builder.setId(id);

        Artifact artifact = createArtifact(decisionElem, ns);
        builder.setArtifact(artifact);

        processControlElements(decisionElem, ns, new ControlElementAdder() {
            @Override
            public void addControlElement(ControlElement controlElement) {
                builder.addControlElement(controlElement);
            }
        });

        return builder.build();
    }

    /**
     * Create a job from a job xml fragment.
     *
     * <jsl:job id="{job-id}" restartable="{value}">
     *     <jsl:properties>...
     *     </jsl:properties>
     *     <jsl:listeners>...
     *     </jsl:listeners>
     *     <jsl:step>...
     *     </jsl:step>
     *     <jsl:split>...
     *     </jsl:split>
     *     <jsl:flow>...
     *     </jsl:flow>
     *     <jsl:decision>...
     *     </jsl:decision>
     * </jsl:job>
     */
    private Job createJob(Element jobElem, Namespace ns) {
        JobBuilder builder = new JobBuilder();

        String id = jobElem.getAttributeValue("id");
        builder.setId(id);

        String value = jobElem.getAttributeValue("restartable");
        if (value != null) {
            builder.setRestartable(Boolean.valueOf(value));
        }

        Element propertiesElem = jobElem.getChild("properties", ns);
        if (propertiesElem != null) {
            builder.addProperties(createProperties(jobElem, ns));
        }

        Element listenersElem = jobElem.getChild("listeners", ns);
        if (listenersElem != null) {
            builder.addListeners(createListeners(listenersElem, ns));
        }

        for (Element stepElem : jobElem.getChildren("step", ns)) {
            builder.addStep(createStep(stepElem, ns));
        }

        for (Element splitElem : jobElem.getChildren("split", ns)) {
            builder.addSplit(createSplit(splitElem, ns));
        }

        for (Element flowElem : jobElem.getChildren("flow", ns)) {
            builder.addFlow(createFlow(flowElem, ns));
        }

        for (Element decisionElem : jobElem.getChildren("decision", ns)) {
            builder.addDecision(createDecision(decisionElem, ns));
        }

        return builder.build();
    }

    private Job load(InputStream is) {
        try {
            SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
            Schema schema = schemaFactory.newSchema(getClass().getResource("/jobXML.xsd"));

            SAXBuilder builder = new SAXBuilder(new XMLReaderSchemaFactory(schema));
            Document document = builder.build(is);

            Element root = document.getRootElement();
            Namespace ns = Namespace.getNamespace(NS_PREFIX, NS_URI);

            Job job = createJob(root, ns);

            // check job consistency
            ConsistencyReport report = new JobConsistencyChecker(job).check();

            LOGGER.debug("Loaded job '{}'", job.getId());

            return job;
        } catch (IOException e) {
            throw new JobXmlException(e);
        } catch (JDOMException e) {
            throw new JobXmlException(e);
        } catch (SAXException e) {
            throw new JobXmlException(e);
        }
    }

    public Job load(String id) throws NoSuchJobException {
        InputStream is = searcher.search(TopLevelNodeType.JOB, id);
        if (is == null) {
            throw new NoSuchJobException("Job '" + id + "' not found");
        }
        try {
            return load(is);
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                LOGGER.error(e.toString(), e);
            }
        }
    }

}
