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

import fr.jamgotchian.jabat.context.JabatThreadContext;
import fr.jamgotchian.jabat.repository.JabatJobInstance;
import fr.jamgotchian.jabat.repository.JabatStepExecution;
import fr.jamgotchian.jabat.repository.Status;
import fr.jamgotchian.jabat.repository.JabatJobExecution;
import fr.jamgotchian.jabat.job.ChunkStepNode;
import fr.jamgotchian.jabat.job.FlowNode;
import fr.jamgotchian.jabat.job.SplitNode;
import fr.jamgotchian.jabat.job.Job;
import fr.jamgotchian.jabat.job.DecisionNode;
import fr.jamgotchian.jabat.job.NodeVisitor;
import fr.jamgotchian.jabat.job.BatchletStepNode;
import fr.jamgotchian.jabat.job.Node;
import fr.jamgotchian.jabat.artifact.BatchletArtifact;
import fr.jamgotchian.jabat.artifact.JobListenerArtifact;
import fr.jamgotchian.jabat.artifact.ProcessItemArtifact;
import fr.jamgotchian.jabat.artifact.ReadItemArtifact;
import fr.jamgotchian.jabat.artifact.WriteItemsArtifact;
import fr.jamgotchian.jabat.job.Chainable;
import fr.jamgotchian.jabat.job.Listener;
import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.batch.spi.TransactionManagerSPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
class JobInstanceExecutor implements NodeVisitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobInstanceExecutor.class);

    private final JobManager jobManager;

    private Job job;

    private JabatJobInstance jobInstance;

    private JabatJobExecution jobExecution;

    JobInstanceExecutor(JobManager jobManager, JabatJobInstance jobInstance) {
        this.jobManager = jobManager;
        this.jobInstance = jobInstance;
    }

    @Override
    public void visit(final Job job) {
        this.job = job;

        // create a job execution
        jobExecution = jobManager.getRepository().createJobExecution(jobInstance);

        jobManager.getTaskManager().submit(new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName("Job " + job.getId());

                try {
                    JabatThreadContext.getInstance().activateJobContext(job, jobInstance, jobExecution);
                    List<JobListenerArtifact> artifacts = new ArrayList<JobListenerArtifact>();
                    try {
                        // before job listeners
                        for (Listener l : job.getListeners()) {
                            Object obj = jobManager.getArtifactFactory().create(l.getRef());
                            JobListenerArtifact artifact = new JobListenerArtifact(obj);
                            artifacts.add(artifact);
                            artifact.beforeJob();
                        }

                        // run the job
                        jobExecution.setStatus(Status.STARTED);
                        job.getFirstChainableNode().accept(JobInstanceExecutor.this);

                        // after job listeners
                        for (JobListenerArtifact artifact : artifacts) {
                            artifact.afterJob();
                        }
                    } finally {
                        for (JobListenerArtifact artifact : artifacts) {
                            jobManager.getArtifactFactory().destroy(artifact.getObject());
                        }
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
            next.accept(this);
        }
    }

    @Override
    public void visit(BatchletStepNode step) {
        Thread.currentThread().setName("Batchlet " + step.getId());

        JabatStepExecution stepExecution = jobManager.getRepository().createStepExecution(step, jobExecution);

        try {
            JabatThreadContext.getInstance().activateStepContext(step, stepExecution);
            Object obj = null;
            try {
                obj = jobManager.getArtifactFactory().create(step.getRef());

                BatchletArtifact artifact = new BatchletArtifact(obj);
                stepExecution.setBatchletArtifact(artifact);

                stepExecution.setStatus(Status.STARTED);

                String exitStatus = artifact.process();
            } finally {
                if (obj != null) {
                    jobManager.getArtifactFactory().destroy(obj);
                }
                JabatThreadContext.getInstance().deactivateStepContext();
            }
            jobExecution.setStatus(Status.COMPLETED);
            stepExecution.setStatus(Status.COMPLETED);

            visitNextNode(step);
        } catch (Throwable t) {
            jobExecution.setStatus(Status.FAILED);
            stepExecution.setStatus(Status.FAILED);
            LOGGER.error(t.toString(), t);
        }
    }

    @Override
    public void visit(ChunkStepNode step) {
        Thread.currentThread().setName("Chunk " + step.getId());

        JabatStepExecution stepExecution = jobManager.getRepository().createStepExecution(step, jobExecution);

        try {
            Object readerObj = null;
            Object processorObj = null;
            Object writerObj = null;
            JabatThreadContext.getInstance().activateStepContext(step, stepExecution);
            try {
                readerObj = jobManager.getArtifactFactory().create(step.getReaderRef());
                processorObj = jobManager.getArtifactFactory().create(step.getProcessorRef());
                writerObj = jobManager.getArtifactFactory().create(step.getWriterRef());

                ReadItemArtifact reader = new ReadItemArtifact(readerObj);
                Class<?> itemType = reader.getItemType();
                ProcessItemArtifact processor = new ProcessItemArtifact(processorObj, itemType);
                Class<?> outputItemType = processor.getOutputItemType();
                WriteItemsArtifact writer = new WriteItemsArtifact(writerObj, outputItemType);

                stepExecution.setStatus(Status.STARTED);

                CheckpointAlgorithm algorithm = new ItemCheckpointAlgorithm(step);
                TransactionManagerSPI transaction = new NoTransactionManager();

                Externalizable readerChkptInfo = null;
                Externalizable writerChkptInfo = null;
                boolean completed = false;
                int retryCount = 0;
                while (!(completed || (step.getRetryLimit() != -1 && retryCount >= step.getRetryLimit()))) {
                    try {
                        try {
                            try {
                                reader.open(readerChkptInfo);
                                writer.open(writerChkptInfo);

                                transaction.begin();
                                algorithm.beginCheckpoint();
                                try {
                                    Object item;
                                    List<Object> buffer = new ArrayList<Object>(step.getBufferSize());
                                    while ((item = reader.readItem()) != null) {
                                        buffer.add(processor.processItem(item));

                                        if (algorithm.isReadyToCheckpoint()) {
                                            writer.writeItems(Collections.unmodifiableList(buffer));
                                            buffer.clear();

                                            readerChkptInfo = reader.getCheckpointInfo();
                                            writerChkptInfo = writer.getCheckpointInfo();
                                            algorithm.endCheckpoint();
                                            transaction.commit();

                                            transaction.begin();
                                            algorithm.beginCheckpoint();
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
                            reader.close();
                        }
                    } finally {
                        writer.close();
                    }
                } // end of retry loop
            } finally {
                if (readerObj != null) {
                    jobManager.getArtifactFactory().destroy(readerObj);
                }
                if (processorObj != null) {
                    jobManager.getArtifactFactory().destroy(processorObj);
                }
                if (writerObj != null) {
                    jobManager.getArtifactFactory().destroy(writerObj);
                }
                JabatThreadContext.getInstance().deactivateStepContext();
            }
            jobExecution.setStatus(Status.COMPLETED);
            stepExecution.setStatus(Status.COMPLETED);

            visitNextNode(step);
        } catch(Throwable t) {
            jobExecution.setStatus(Status.FAILED);
            stepExecution.setStatus(Status.FAILED);
            LOGGER.error(t.toString(), t);
        }
    }

    @Override
    public void visit(FlowNode flow) {
        Thread.currentThread().setName("Flow " + flow.getId());
        flow.getFirstChainableNode().accept(this);
    }

    @Override
    public void visit(SplitNode split) {
        Thread.currentThread().setName("Split " + split.getId());
        Collection<Node> nodes = split.getNodes();
        if (nodes.size() > 0) {
            final Iterator<Node> it = nodes.iterator();
            Node firstNode = it.next();
            while (it.hasNext()) {
                jobManager.getTaskManager().submit(new Runnable() {
                    @Override
                    public void run() {
                        JabatThreadContext.getInstance().activateJobContext(job, jobInstance, jobExecution);
                        try {
                            it.next().accept(JobInstanceExecutor.this);
                        } finally {
                            JabatThreadContext.getInstance().deactivateJobContext();
                        }
                    }
                });
            }
            firstNode.accept(this);
        }
    }

    @Override
    public void visit(DecisionNode decision) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
