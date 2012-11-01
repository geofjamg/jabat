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
import fr.jamgotchian.jabat.job.ChunkStepNode;
import fr.jamgotchian.jabat.job.FlowNode;
import fr.jamgotchian.jabat.job.SplitNode;
import fr.jamgotchian.jabat.job.Job;
import fr.jamgotchian.jabat.job.DecisionNode;
import fr.jamgotchian.jabat.job.NodeVisitor;
import fr.jamgotchian.jabat.job.BatchletStepNode;
import fr.jamgotchian.jabat.job.Node;
import fr.jamgotchian.jabat.artifact.BatchletArtifact;
import fr.jamgotchian.jabat.artifact.BatchletArtifactContext;
import fr.jamgotchian.jabat.artifact.JobListenerArtifact;
import fr.jamgotchian.jabat.artifact.ProcessItemArtifact;
import fr.jamgotchian.jabat.artifact.ReadItemArtifact;
import fr.jamgotchian.jabat.artifact.ChunkArtifactContext;
import fr.jamgotchian.jabat.artifact.WriteItemsArtifact;
import fr.jamgotchian.jabat.job.Chainable;
import fr.jamgotchian.jabat.job.Listener;
import fr.jamgotchian.jabat.repository.JobRepository;
import fr.jamgotchian.jabat.task.TaskManager;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
    public void visit(final Job job) {
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
                        for (Listener l : job.getListeners()) {
                            JobListenerArtifact artifact = artifactContext.createJobListener(l.getRef());
                            artifact.beforeJob();
                        }

                        // run the job
                        jobExecution.setStatus(Status.STARTED);
                        job.getFirstChainableNode().accept(JobInstanceExecutor.this);

                        // after job listeners
                        for (JobListenerArtifact artifact : artifactContext.getJobListeners()) {
                            artifact.afterJob();
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
            next.accept(this);
        }
    }

    @Override
    public void visit(BatchletStepNode step) {
        Thread.currentThread().setName("Batchlet " + step.getId());

        JabatStepExecution stepExecution = getRepository().createStepExecution(step, jobExecution);

        try {
            JabatThreadContext.getInstance().activateStepContext(step, stepExecution);
            BatchletArtifactContext artifactContext = new BatchletArtifactContext(getArtifactFactory());
            try {
                BatchletArtifact artifact = artifactContext.createBatchlet(step.getRef());
                stepExecution.setBatchletArtifact(artifact);

                stepExecution.setStatus(Status.STARTED);

                String exitStatus = artifact.process();
            } finally {
                artifactContext.release();
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

    private static CheckpointAlgorithm getCheckpointAlgorithm(ChunkStepNode step) {
        switch (step.getCheckpointPolicy()) {
            case ITEM:
                return new ItemCheckpointAlgorithm(step.getCommitInterval());
            case TIME:
                return new TimeCheckpointAlgorithm(step.getCommitInterval());
            case CUSTOM:
                return new CustomCheckpointAlgorithm(null); // TODO
            default:
                throw new InternalError();
        }
    }

    private static Externalizable deserialize(byte[] data) throws IOException, ClassNotFoundException {
        if (data == null) {
            return null;
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(bis);
        Externalizable externalizable;
        try {
            externalizable = (Externalizable) is.readObject();
        } finally {
            is.close();
        }
        return externalizable;
    }

    private static byte[] serialize(Externalizable externalizable) throws IOException {
        if (externalizable == null) {
            return null;
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(bos);
        try {
            os.writeObject(externalizable);
        } finally {
            os.close();
        }
        return bos.toByteArray();
    }

    @Override
    public void visit(ChunkStepNode step) {
        Thread.currentThread().setName("Chunk " + step.getId());

        JabatStepExecution stepExecution = getRepository().createStepExecution(step, jobExecution);

        try {
            JabatThreadContext.getInstance().activateStepContext(step, stepExecution);
            ChunkArtifactContext artifactContext = new ChunkArtifactContext(getArtifactFactory());
            try {
                ReadItemArtifact reader
                        = artifactContext.createItemReader(step.getReaderRef());
                ProcessItemArtifact processor
                        = artifactContext.createItemProcessor(step.getProcessorRef(),
                                                              reader.getItemType());
                WriteItemsArtifact writer
                        = artifactContext.createItemWriter(step.getWriterRef(),
                                                           processor.getOutputItemType());

                stepExecution.setStatus(Status.STARTED);

                // select the checkpoint algorithm
                CheckpointAlgorithm algorithm = getCheckpointAlgorithm(step);

                TransactionManagerSPI transaction = new NoTransactionManager();

                byte[] readerChkptData = null;
                byte[] writerChkptData = null;

                // start the retry loop
                boolean completed = false;
                int retryCount = 0;
                while (!(completed || (step.getRetryLimit() != -1 && retryCount >= step.getRetryLimit()))) {
                    reader.open(deserialize(readerChkptData));
                    try {
                        writer.open(deserialize(writerChkptData));
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
                                            readerChkptData = serialize(reader.getCheckpointInfo());
                                            writerChkptData = serialize(writer.getCheckpointInfo());

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
            } finally {
                artifactContext.release();
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
                getTaskManager().submit(new Runnable() {
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
