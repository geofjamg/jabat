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

import fr.jamgotchian.jabat.artifact.BatchletArtifactContext;
import fr.jamgotchian.jabat.artifact.ChunkArtifactContext;
import fr.jamgotchian.jabat.artifact.JobArtifactContext;
import fr.jamgotchian.jabat.artifact.SplitArtifactContext;
import fr.jamgotchian.jabat.checkpoint.ItemCheckpointAlgorithm;
import fr.jamgotchian.jabat.checkpoint.TimeCheckpointAlgorithm;
import fr.jamgotchian.jabat.context.JabatThreadContext;
import fr.jamgotchian.jabat.repository.JabatJobInstance;
import fr.jamgotchian.jabat.repository.JabatStepExecution;
import fr.jamgotchian.jabat.repository.Status;
import fr.jamgotchian.jabat.repository.JabatJobExecution;
import fr.jamgotchian.jabat.job.ChunkStep;
import fr.jamgotchian.jabat.job.Flow;
import fr.jamgotchian.jabat.job.Split;
import fr.jamgotchian.jabat.job.Job;
import fr.jamgotchian.jabat.job.Decision;
import fr.jamgotchian.jabat.job.NodeVisitor;
import fr.jamgotchian.jabat.job.BatchletStep;
import fr.jamgotchian.jabat.job.Node;
import fr.jamgotchian.jabat.job.Artifact;
import fr.jamgotchian.jabat.job.Chainable;
import fr.jamgotchian.jabat.repository.JobRepository;
import fr.jamgotchian.jabat.spi.ArtifactFactory;
import fr.jamgotchian.jabat.task.TaskManager;
import fr.jamgotchian.jabat.util.Externalizables;
import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.batch.api.Batchlet;
import javax.batch.api.CheckpointAlgorithm;
import javax.batch.api.ItemProcessor;
import javax.batch.api.ItemReader;
import javax.batch.api.ItemWriter;
import javax.batch.api.JobListener;
import javax.batch.api.SplitAnalyzer;
import javax.batch.api.SplitCollector;
import javax.batch.api.StepListener;
import javax.batch.spi.TransactionManagerSPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
class JobInstanceExecutor implements NodeVisitor<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobInstanceExecutor.class);

    private final JobManager jobManager;

    private Job job;

    private JabatJobInstance jobInstance;

    private JabatJobExecution jobExecution;

    JobInstanceExecutor(JobManager jobManager, JabatJobInstance jobInstance) {
        this.jobManager = jobManager;
        this.jobInstance = jobInstance;
    }

    private JobRepository getRepository() {
        return jobManager.getRepository();
    }

    private ArtifactFactory getArtifactFactory() {
        return jobManager.getArtifactFactory();
    }

    private TaskManager getTaskManager() {
        return jobManager.getTaskManager();
    }

    @Override
    public void visit(final Job job, Void arg) {
        this.job = job;

        // create a job execution
        jobExecution = getRepository().createJobExecution(jobInstance);

        getTaskManager().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    JabatThreadContext.getInstance().activateJobContext(job, jobInstance, jobExecution);
                    JobArtifactContext artifactContext = new JobArtifactContext(getArtifactFactory());
                    try {
                        // before job listeners
                        for (Artifact a : job.getListeners()) {
                            JobListener l = artifactContext.createJobListener(a.getRef());
                            l.beforeJob();
                        }

                        // run the job
                        jobExecution.setStatus(Status.STARTED);
                        job.getFirstChainableNode().accept(JobInstanceExecutor.this, null);

                        // after job listeners
                        for (JobListener l : artifactContext.getJobListeners()) {
                            l.afterJob();
                        }
                    } finally {
                        artifactContext.release();
                        JabatThreadContext.getInstance().deactivateJobContext();
                    }
                } catch (Throwable t) {
                    LOGGER.error(t.toString(), t);
                }
            }
        });
    }

    private <N extends Node & Chainable> void visitNextNode(N node) {
        assert node.getContainer() != null;
        if (node.getNext() != null) {
            Node next = node.getContainer().getNode(node.getNext());
            next.accept(this, null);
        }
    }

    @Override
    public void visit(BatchletStep step, Void arg) {
        JabatStepExecution stepExecution = getRepository().createStepExecution(step, jobExecution);

        try {
            JabatThreadContext.getInstance().activateStepContext(step, stepExecution);
            BatchletArtifactContext artifactContext = new BatchletArtifactContext(getArtifactFactory());
            try {
                // before step listeners
                for (Artifact a : step.getListeners()) {
                    StepListener l = artifactContext.createStepListener(a.getRef());
                    l.beforeStep();
                }

                Batchlet artifact = artifactContext.createBatchlet(step.getArtifact().getRef());
                stepExecution.setBatchlet(artifact);

                stepExecution.setStatus(Status.STARTED);

                String exitStatus = artifact.process();

                jobExecution.setStatus(Status.COMPLETED);
                stepExecution.setStatus(Status.COMPLETED);

                // after step listeners
                // TODO should be called even if case of error?
                for (StepListener l : artifactContext.getStepListeners()) {
                    l.afterStep();
                }
            } finally {
                artifactContext.release();
                JabatThreadContext.getInstance().deactivateStepContext();
            }

            visitNextNode(step);
        } catch (Throwable t) {
            jobExecution.setStatus(Status.FAILED);
            stepExecution.setStatus(Status.FAILED);
            LOGGER.error(t.toString(), t);
        }
    }

    private static CheckpointAlgorithm getCheckpointAlgorithm(ChunkStep step, ChunkArtifactContext artifactContext) throws Exception {
        switch (step.getCheckpointPolicy()) {
            case ITEM:
                return new ItemCheckpointAlgorithm(step.getCommitInterval());
            case TIME:
                return new TimeCheckpointAlgorithm(step.getCommitInterval());
            case CUSTOM:
                {
                    String ref = step.getCheckpointAlgo().getRef();
                    return artifactContext.createCheckpointAlgorithm(ref);
                }
            default:
                throw new InternalError();
        }
    }

    @Override
    public void visit(ChunkStep step, Void arg) {
        JabatStepExecution stepExecution = getRepository().createStepExecution(step, jobExecution);

        try {
            JabatThreadContext.getInstance().activateStepContext(step, stepExecution);
            ChunkArtifactContext artifactContext = new ChunkArtifactContext(getArtifactFactory());
            try {
                // before step listeners
                for (Artifact a : step.getListeners()) {
                    StepListener l = artifactContext.createStepListener(a.getRef());
                    l.beforeStep();
                }

                ItemReader reader
                        = artifactContext.createItemReader(step.getReader().getRef());
                ItemProcessor processor
                        = artifactContext.createItemProcessor(step.getProcessor().getRef());
                ItemWriter writer
                        = artifactContext.createItemWriter(step.getWriter().getRef());

                stepExecution.setStatus(Status.STARTED);

                // select the checkpoint algorithm
                CheckpointAlgorithm algorithm = getCheckpointAlgorithm(step, artifactContext);

                TransactionManagerSPI transaction = new NoTransactionManager();

                byte[] readerChkptData = null;
                byte[] writerChkptData = null;

                // start the retry loop
                boolean completed = false;
                int retryCount = 0;
                while (!(completed || (step.getRetryLimit() != -1 && retryCount >= step.getRetryLimit()))) {
                    reader.open(Externalizables.deserialize(readerChkptData));
                    try {
                        writer.open(Externalizables.deserialize(writerChkptData));
                        try {
                            try {
                                transaction.begin();
                                algorithm.beginCheckpoint();
                                try {
                                    Object item;
                                    List<Object> buffer = new ArrayList<Object>(step.getBufferSize());
                                    while ((item = reader.readItem()) != null) {
                                        buffer.add(processor.processItem(item));

                                        if (algorithm.isReadyToCheckpoint()) {
                                            readerChkptData = Externalizables.serialize(reader.checkpointInfo());
                                            writerChkptData = Externalizables.serialize(writer.checkpointInfo());

                                            algorithm.endCheckpoint();
                                            transaction.commit();

                                            transaction.begin();
                                            algorithm.beginCheckpoint();
                                        }

                                        // write items if buffer size is zero or the buffer reaches
                                        // the maximum size
                                        if (step.getBufferSize() == 0 || buffer.size() > step.getBufferSize()) {
                                            writer.writeItems(Collections.unmodifiableList(buffer));
                                            buffer.clear();
                                        }
                                    }
                                    // write remaining items
                                    if (buffer.size() > 0) {
                                        writer.writeItems(Collections.unmodifiableList(buffer));
                                    }
                                } finally {
                                    algorithm.endCheckpoint();
                                }
                                transaction.commit();
                                completed = true;
                            } catch (Throwable t) {
                                LOGGER.error(t.toString(), t);
                                transaction.rollback();
                                retryCount++;
                                // retry...
                            }
                        } finally {
                            writer.close();
                        }
                    } finally {
                        reader.close();
                    }
                } // end of retry loop

                // TODO what should be the status if we reach the max number of retry?
                jobExecution.setStatus(Status.COMPLETED);
                stepExecution.setStatus(Status.COMPLETED);

                // after step listeners
                // TODO should be called even if case of error?
                for (StepListener l : artifactContext.getStepListeners()) {
                    l.afterStep();
                }
            } finally {
                artifactContext.release();
                JabatThreadContext.getInstance().deactivateStepContext();
            }

            visitNextNode(step);
        } catch(Throwable t) {
            jobExecution.setStatus(Status.FAILED);
            stepExecution.setStatus(Status.FAILED);
            LOGGER.error(t.toString(), t);
        }
    }

    @Override
    public void visit(Flow flow, Void arg) {
        flow.getFirstChainableNode().accept(this, null);
    }

    @Override
    public void visit(Split split, Void arg) {
        Collection<Node> nodes = split.getNodes();
        if (nodes.size() > 0) {
            try {
                SplitArtifactContext artifactContext = new SplitArtifactContext(getArtifactFactory());
                try {
                    final List<Externalizable> collectedData = new ArrayList<Externalizable>();
                    final SplitCollector collector = split.getCollectorArtifact() != null
                            ? artifactContext.createSplitCollector(split.getCollectorArtifact().getRef())
                            : null;
                    // TODO start split context
                    for (Node node : nodes) {
                        final Flow flow = (Flow) node;
                        getTaskManager().submit(new Runnable() {
                            @Override
                            public void run() {
                                JabatThreadContext.getInstance().activateJobContext(job, jobInstance, jobExecution);
                                // TODO start flow context
                                try {
                                    try {
                                        flow.accept(JobInstanceExecutor.this, null);
                                        if (collector != null) {
                                            collectedData.add(collector.collectSplitData());
                                        }
                                    } finally {
                                        // TODO end flow context
                                        JabatThreadContext.getInstance().deactivateJobContext();
                                    }
                                } catch (Throwable t) {
                                    LOGGER.error(t.toString(), t);
                                }
                            }
                        });
                    }
                    if (split.getAnalyserArtifact() != null) {
                        SplitAnalyzer analyser
                                = artifactContext.createSplitAnalyser(split.getAnalyserArtifact().getRef());
                        for (Externalizable data : collectedData) {
                            analyser.analyzeCollectorData(data);
                            analyser.analyzeStatus(null, null);
                        }
                    }
                    // TODO end split context
                } finally {
                    artifactContext.release();
                }
            } catch (Throwable t) {
                LOGGER.error(t.toString(), t);
            }
        }
    }

    @Override
    public void visit(Decision decision, Void arg) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
