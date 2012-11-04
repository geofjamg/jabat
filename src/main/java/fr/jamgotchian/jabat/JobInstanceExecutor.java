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

import fr.jamgotchian.jabat.checkpoint.TimeCheckpointAlgorithm;
import fr.jamgotchian.jabat.checkpoint.ItemCheckpointAlgorithm;
import fr.jamgotchian.jabat.checkpoint.CustomCheckpointAlgorithm;
import fr.jamgotchian.jabat.checkpoint.CheckpointAlgorithm;
import fr.jamgotchian.jabat.artifact.JobArtifactContext;
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
import fr.jamgotchian.jabat.artifact.BatchletArtifactInstance;
import fr.jamgotchian.jabat.artifact.BatchletArtifactContext;
import fr.jamgotchian.jabat.artifact.CheckpointAlgorithmArtifactInstance;
import fr.jamgotchian.jabat.artifact.JobListenerArtifactInstance;
import fr.jamgotchian.jabat.artifact.ItemProcessorArtifactInstance;
import fr.jamgotchian.jabat.artifact.ItemReaderArtifactInstance;
import fr.jamgotchian.jabat.artifact.ChunkArtifactContext;
import fr.jamgotchian.jabat.artifact.ItemWriterArtifactInstance;
import fr.jamgotchian.jabat.artifact.StepListenerArtifactInstance;
import fr.jamgotchian.jabat.job.Artifact;
import fr.jamgotchian.jabat.job.Chainable;
import fr.jamgotchian.jabat.repository.JobRepository;
import fr.jamgotchian.jabat.task.TaskManager;
import fr.jamgotchian.jabat.util.Externalizables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.batch.spi.ArtifactFactory;
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
                Thread.currentThread().setName("Job " + job.getId());

                try {
                    JabatThreadContext.getInstance().activateJobContext(job, jobInstance, jobExecution);
                    JobArtifactContext artifactContext = new JobArtifactContext(getArtifactFactory());
                    try {
                        // before job listeners
                        for (Artifact a : job.getListenerArtifacts()) {
                            JobListenerArtifactInstance l = artifactContext.createJobListener(a.getRef());
                            l.beforeJob();
                        }

                        // run the job
                        jobExecution.setStatus(Status.STARTED);
                        job.getFirstChainableNode().accept(JobInstanceExecutor.this, null);

                        // after job listeners
                        for (JobListenerArtifactInstance l : artifactContext.getJobListeners()) {
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
        Thread.currentThread().setName("Batchlet " + step.getId());

        JabatStepExecution stepExecution = getRepository().createStepExecution(step, jobExecution);

        try {
            JabatThreadContext.getInstance().activateStepContext(step, stepExecution);
            BatchletArtifactContext artifactContext = new BatchletArtifactContext(getArtifactFactory());
            try {
                // before step listeners
                for (Artifact a : step.getListenerArtifacts()) {
                    StepListenerArtifactInstance l = artifactContext.createStepListener(a.getRef());
                    l.beforeStep();
                }

                BatchletArtifactInstance artifact = artifactContext.createBatchlet(step.getArtifact().getRef());
                stepExecution.setBatchletArtifactInstance(artifact);

                stepExecution.setStatus(Status.STARTED);

                String exitStatus = artifact.process();
            
                jobExecution.setStatus(Status.COMPLETED);
                stepExecution.setStatus(Status.COMPLETED);
                
                // after step listeners
                // TODO should be called even if case of error?
                for (StepListenerArtifactInstance l : artifactContext.getStepListeners()) {
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
                    String ref = step.getCheckpointAlgoArtifact().getRef();
                    CheckpointAlgorithmArtifactInstance instance 
                            = artifactContext.createCheckpointAlgorithm(ref);
                    return new CustomCheckpointAlgorithm(instance);
                }
            default:
                throw new InternalError();
        }
    }

    @Override
    public void visit(ChunkStep step, Void arg) {
        Thread.currentThread().setName("Chunk " + step.getId());

        JabatStepExecution stepExecution = getRepository().createStepExecution(step, jobExecution);

        try {
            JabatThreadContext.getInstance().activateStepContext(step, stepExecution);
            ChunkArtifactContext artifactContext = new ChunkArtifactContext(getArtifactFactory());
            try {
                // before step listeners
                for (Artifact a : step.getListenerArtifacts()) {
                    StepListenerArtifactInstance l = artifactContext.createStepListener(a.getRef());
                    l.beforeStep();
                }

                ItemReaderArtifactInstance reader
                        = artifactContext.createItemReader(step.getReaderArtifact().getRef());
                ItemProcessorArtifactInstance processor
                        = artifactContext.createItemProcessor(step.getProcessorArtifact().getRef(),
                                                              reader.getItemType());
                ItemWriterArtifactInstance writer
                        = artifactContext.createItemWriter(step.getWriterArtifact().getRef(),
                                                           processor.getOutputItemType());

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
                                            readerChkptData = Externalizables.serialize(reader.getCheckpointInfo());
                                            writerChkptData = Externalizables.serialize(writer.getCheckpointInfo());

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
                for (StepListenerArtifactInstance l : artifactContext.getStepListeners()) {
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
        Thread.currentThread().setName("Flow " + flow.getId());
        flow.getFirstChainableNode().accept(this, null);
    }

    @Override
    public void visit(Split split, Void arg) {
        Thread.currentThread().setName("Split " + split.getId());
        Collection<Node> nodes = split.getNodes();
        if (nodes.size() > 0) {
            final Iterator<Node> it = nodes.iterator();
            Node firstNode = it.next();
            while (it.hasNext()) {
                getTaskManager().submit(new Runnable() {
                    @Override
                    public void run() {
                        JabatThreadContext.getInstance().activateJobContext(job, jobInstance, jobExecution);
                        try {
                            it.next().accept(JobInstanceExecutor.this, null);
                        } finally {
                            JabatThreadContext.getInstance().deactivateJobContext();
                        }
                    }
                });
            }
            firstNode.accept(this, null);
        }
    }

    @Override
    public void visit(Decision decision, Void arg) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
