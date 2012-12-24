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
import fr.jamgotchian.jabat.jobxml.model.Decision;
import fr.jamgotchian.jabat.jobxml.model.DecisionBuilder;
import fr.jamgotchian.jabat.jobxml.model.Flow;
import fr.jamgotchian.jabat.jobxml.model.FlowBuilder;
import fr.jamgotchian.jabat.jobxml.model.Job;
import fr.jamgotchian.jabat.jobxml.model.JobBuilder;
import fr.jamgotchian.jabat.jobxml.model.JobConsistencyChecker;
import fr.jamgotchian.jabat.jobxml.model.JobPath;
import fr.jamgotchian.jabat.jobxml.model.PartitionPlanBuilder;
import fr.jamgotchian.jabat.jobxml.model.Split;
import fr.jamgotchian.jabat.jobxml.model.SplitBuilder;
import fr.jamgotchian.jabat.jobxml.model.Step;
import fr.jamgotchian.jabat.jobxml.model.StepBuilder;
import fr.jamgotchian.jabat.util.JabatException;
import java.io.File;
import java.io.IOException;
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
public class JobXmlLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobXmlLoader.class);

    private final JobPath path = new JobPath();

    private Properties createProperties(Element propertiesElem, Namespace ns) {
        Properties properties = new Properties();
        for (Element propertyElem : propertiesElem.getChildren("property", ns)) {
            String name = propertyElem.getAttributeValue("name");
            String value = propertyElem.getAttributeValue("value");
            properties.setProperty(name, value);
        }
        return properties;
    }

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

    private List<Artifact> createListeners(Element listenersElem, Namespace ns) {
        List<Artifact> listeners = new ArrayList<Artifact>();
        for (Element listenerElem : listenersElem.getChildren("listener", ns)) {
            listeners.add(createArtifact(listenerElem, ns));
        }
        return listeners;
    }

    private BatchletStepBuilder createBatchletBuilder(Element batchletElem, Namespace ns) {
        BatchletStepBuilder builder = new BatchletStepBuilder();
        builder.setArtifact(createArtifact(batchletElem, ns));
        return builder;
    }

    private static interface ExceptionClassFilterer {

        void include(Class<?> clazz);

        void exclude(Class<?> clazz);

    }

    private void processExceptionClassFilter(Element exceptionClassFilterElem, Namespace ns,
                                             ExceptionClassFilterer filterer) {
        try {
            Element includeElem = exceptionClassFilterElem.getChild("include", ns);
            if (includeElem != null) {
                for (Element classElem : includeElem.getChildren("class", ns)) {
                    String className = classElem.getText();
                    filterer.include(Class.forName(className));
                }
            }
            Element excludeElem = exceptionClassFilterElem.getChild("exclude", ns);
            if (excludeElem != null) {
                for (Element classElem : excludeElem.getChildren("class", ns)) {
                    String className = classElem.getText();
                    filterer.exclude(Class.forName(className));
                }
            }
        } catch (ClassNotFoundException e) {
            throw new JabatException(e);
        }
    }

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
                    throw new JabatException("Chunk property syntax error, it should be <artifact-name:property-name>");
                }
                String artifactName = split[0];
                String propertyName = split[1];
                if (artifactName == null || artifactName.isEmpty()) {
                    throw new JabatException("Chunk property syntax error, invalid artifact name '"
                            + artifactName + "'");
                }
                if (artifactName.equals(readerRef)) {
                    readerBuilder.addProperty(propertyName, value);
                } else if (artifactName.equals(processorRef)) {
                    processorBuilder.addProperty(propertyName, value);
                } else if (artifactName.equals(writerRef)) {
                    writerBuilder.addProperty(propertyName, value);
                } else {
                    throw new JabatException("Artifact '" + artifactName + "' not found");
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

    private Step createStep(Element stepElem, Namespace ns) {
        StepBuilder builder;

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

        return builder.build();
    }

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

    private Decision createDecision(Element decisionElem, Namespace ns) {
        DecisionBuilder builder = new DecisionBuilder();

        String id = decisionElem.getAttributeValue("id");
        builder.setId(id);

        Artifact artifact = createArtifact(decisionElem, ns);
        builder.setArtifact(artifact);

        return builder.build();
    }

    private Job createJob(Element jobElem, Namespace ns) {
        JobBuilder builder = new JobBuilder();

        String id = jobElem.getAttributeValue("id");
        builder.setId(id);

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

    private Job loadFile(File file, String jobId) {
        Job job = null;
        try {
            SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
            Schema schema = schemaFactory.newSchema(getClass().getResource("/jobXML.xsd"));

            SAXBuilder builder = new SAXBuilder(new XMLReaderSchemaFactory(schema));
            Document document = builder.build(file);

            Element root = document.getRootElement();
            Namespace ns = Namespace.getNamespace("jsl", "http://batch.jsr352/jsl");
            if ("job".equals(root.getName())) {
                Job job2 = createJob(root, ns);
                if (job2.getId().equals(jobId)) {
                    job = job2;
                }
            }
        } catch (IOException e) {
            throw new JabatException(e);
        } catch (JDOMException e) {
            throw new JabatException(e);
        } catch (SAXException e) {
            throw new JabatException(e);
        }

        // check job consistency
        if (job != null) {
            ConsistencyReport report = new JobConsistencyChecker(job).check();
        }

        LOGGER.debug("Load job xml {} file {}", jobId, file);

        return job;
    }

    public Job load(String id) throws NoSuchJobException {
        for (File file : path.findJobXml()) {
            try {
                Job job = loadFile(file, id);
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
