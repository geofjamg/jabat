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

import fr.jamgotchian.jabat.artifact.ArtifactFactory;
import fr.jamgotchian.jabat.artifact.BatchletArtifactContext;
import fr.jamgotchian.jabat.artifact.ChunkArtifactContext;
import fr.jamgotchian.jabat.artifact.JobArtifactContext;
import fr.jamgotchian.jabat.artifact.SplitArtifactContext;
import fr.jamgotchian.jabat.artifact.StepArtifactContext;
import fr.jamgotchian.jabat.checkpoint.ItemCheckpointAlgorithm;
import fr.jamgotchian.jabat.checkpoint.TimeCheckpointAlgorithm;
import fr.jamgotchian.jabat.context.JabatThreadContext;
import fr.jamgotchian.jabat.job.Artifact;
import fr.jamgotchian.jabat.job.BatchletStep;
import fr.jamgotchian.jabat.job.Chainable;
import fr.jamgotchian.jabat.job.ChunkStep;
import fr.jamgotchian.jabat.job.Decision;
import fr.jamgotchian.jabat.job.Flow;
import fr.jamgotchian.jabat.job.Job;
import fr.jamgotchian.jabat.job.JobUtil;
import fr.jamgotchian.jabat.job.Node;
import fr.jamgotchian.jabat.job.NodeVisitor;
import fr.jamgotchian.jabat.job.Split;
import fr.jamgotchian.jabat.job.Step;
import fr.jamgotchian.jabat.repository.JabatJobExecution;
import fr.jamgotchian.jabat.repository.JabatJobInstance;
import fr.jamgotchian.jabat.repository.JabatStepExecution;
import fr.jamgotchian.jabat.repository.JobRepository;
import fr.jamgotchian.jabat.repository.Status;
import fr.jamgotchian.jabat.task.TaskManager;
import fr.jamgotchian.jabat.task.TaskResultListener;
import fr.jamgotchian.jabat.transaction.NoTransactionManager;
import fr.jamgotchian.jabat.util.Externalizables;
import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import javax.batch.api.Batchlet;
import javax.batch.api.CheckpointAlgorithm;
import javax.batch.api.ItemProcessor;
import javax.batch.api.ItemReader;
import javax.batch.api.ItemWriter;
import javax.batch.api.JobListener;
import javax.batch.api.PartitionAnalyzer;
import javax.batch.api.PartitionCollector;
import javax.batch.api.PartitionMapper;
import javax.batch.api.PartitionReducer;
import javax.batch.api.SplitAnalyzer;
import javax.batch.api.SplitCollector;
import javax.batch.api.StepListener;
import javax.batch.api.parameters.PartitionPlan;
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

    private Properties jobParameters;

    JobInstanceExecutor(JobManager jobManager, JabatJobInstance jobInstance,
                        Properties jobParameters) {
        this.jobManager = jobManager;
        this.jobInstance = jobInstance;
        this.jobParameters = jobParameters;
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
                    // create job context
                    JabatThreadContext.getInstance().createJobContext(job, jobInstance, jobExecution);

                    // apply substitutions to job level elements
                    JobUtil.substitute(job, jobParameters);

                    // store job level properties in job context
                    JabatThreadContext.getInstance().getJobContext()
                            .setProperties(job.getSubstitutedProperties());

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

                        // destroy job context
                        JabatThreadContext.getInstance().destroyJobContext();
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

    private static void notifyBeforeStep(Step step, StepArtifactContext artifactContext) throws Exception {
        for (Artifact a : step.getListeners()) {
            StepListener l = artifactContext.createStepListener(a.getRef());
            l.beforeStep();
        }
    }

    private static void notifyAfterStep(Step step, StepArtifactContext artifactContext) throws Exception {
        for (StepListener l : artifactContext.getStepListeners()) {
            l.afterStep();
        }
    }

    private static boolean isPartionned(Step step) {
        return step.getPartitionPlan() != null || step.getPartitionMapper() != null;
    }

    private static PartitionPlan createPartitionPlan(Step step, StepArtifactContext artifactContext) throws Exception {
        if (step.getPartitionMapper() != null) {
            // dynamic defintion of the partition plan though an artifact
            String ref = step.getPartitionMapper().getRef();
            PartitionMapper mapper = artifactContext.createPartitionMapper(ref);
            return mapper.mapPartitions();
        } else {
            // static defintion of the partition plan
            return step.getPartitionPlan();
        }
    }

    private static PartitionReducer createPartitionReducer(Step step, StepArtifactContext artifactContext) throws Exception {
        if (step.getPartitionReducer() != null) {
            String ref = step.getPartitionReducer().getRef();
            return artifactContext.createPartitionReducer(ref);
        }
        return null;
    }

    private static PartitionCollector createPartitionCollector(Step step, StepArtifactContext artifactContext) throws Exception {
        if (step.getPartitionCollector() != null) {
            String ref= step.getPartitionCollector().getRef();
            return artifactContext.createPartitionCollector(ref);
        }
        return null;
    }

    private static PartitionAnalyzer createPartitionAnalyser(Step step, StepArtifactContext artifactContext) throws Exception {
        if (step.getPartitionAnalyser() != null) {
            String ref = step.getPartitionAnalyser().getRef();
            return artifactContext.createPartitionAnalyser(ref);
        }
        return null;
    }

    private static class PartitionContext {

        private Externalizable data;

        private String exitStatus;

        private PartitionContext() {
        }

        private Externalizable getData() {
            return data;
        }

        private void setData(Externalizable data) {
            this.data = data;
        }

        private String getExitStatus() {
            return exitStatus;
        }

        private void setExitStatus(String exitStatus) {
            this.exitStatus = exitStatus;
        }

    }

    @Override
    public void visit(final BatchletStep step, Void arg) {
        final JabatStepExecution stepExecution = getRepository().createStepExecution(step, jobExecution);

        try {
            // create step context
            JabatThreadContext.getInstance().createStepContext(step, stepExecution);

            // apply substitutions to step level elements
            JobUtil.substitute(step, jobParameters);

            // store step level properties in step context
            JabatThreadContext.getInstance().getStepContext().setProperties(step.getProperties());

            final BatchletArtifactContext artifactContext = new BatchletArtifactContext(getArtifactFactory());
            try {
                // before step listeners
                notifyBeforeStep(step, artifactContext);

                stepExecution.setStatus(Status.STARTED);

                if (isPartionned(step)) {

                    // create partition reducer
                    PartitionReducer reducer = createPartitionReducer(step, artifactContext);

                    // begin partitioned step
                    if (reducer != null) {
                        reducer.beginPartitionedStep();
                    }

                    // create partition plan
                    final PartitionPlan plan = createPartitionPlan(step, artifactContext);

                    // prepare a task for each parttion
                    List<Callable<PartitionContext>> tasks = new ArrayList<Callable<PartitionContext>>();

                    for (int i = 0; i < plan.getPartitionCount(); i++) {
                        final int partitionNumber = i;

                        tasks.add(new Callable<PartitionContext>() {

                            @Override
                            public PartitionContext call() throws Exception {
                                PartitionContext partitionContext = new PartitionContext();
                                try {
                                    // each partion has its own job context
                                    // PENDING clone the parent job context?
                                    JabatThreadContext.getInstance().createJobContext(job, jobInstance, jobExecution);
                                    try {
                                        // each partition has its own step context
                                        // PENDING clone the step job context?
                                        JabatThreadContext.getInstance().createStepContext(step, stepExecution);

                                        // store in the step context step level properties overriden
                                        // partition properties
                                        Properties properties = new Properties();
                                        properties.putAll(step.getProperties());
                                        if (plan.getPartitionProperties() != null) {
                                            properties.putAll(JobUtil.substitute(plan.getPartitionProperties()[partitionNumber], jobParameters, step));
                                        }
                                        JabatThreadContext.getInstance().getStepContext().setProperties(properties);

                                        try {
                                            Batchlet artifact = artifactContext.createBatchlet(step.getArtifact().getRef());
                                            jobManager.getRunningBatchlets().put(stepExecution.getId(), artifact);

                                            // processing
                                            String exitStatus = artifact.process();

                                            // TODO batchlet has been stopped...

                                            // store the exit status return by the batchlet artifact
                                            // in the partition context
                                            partitionContext.setExitStatus(exitStatus);

                                            // create partition collector
                                            PartitionCollector collector = createPartitionCollector(step, artifactContext);

                                            // collect data
                                            if (collector != null) {
                                                Externalizable data = collector.collectPartitionData();
                                                partitionContext.setData(data);
                                            }
                                        } finally {
                                            // store the exit status set in the step context in the partition context
                                            // PENDING consequently, it overrides the one returned by the batchlet artifact?
                                            String exitStatus = JabatThreadContext.getInstance().getStepContext().getExitStatus();
                                            if (exitStatus != null) {
                                                partitionContext.setExitStatus(exitStatus);
                                            }

                                            // destroy the step context of the partition
                                            JabatThreadContext.getInstance().destroyStepContext();
                                        }
                                    } finally {
                                        JabatThreadContext.getInstance().destroyJobContext();
                                    }
                                } catch (Throwable t) {
                                    LOGGER.error(t.toString(), t);
                                }
                                return partitionContext;
                            }
                        });
                    }

                    final PartitionAnalyzer analyser = createPartitionAnalyser(step, artifactContext);

                    // run partitions on a thread pool, wait for all partitions to
                    // end and call the analyser each time a partition ends
                    jobManager.getTaskManager().submitAndWait(tasks, plan.getThreadCount(),
                            new TaskResultListener<PartitionContext>() {

                        @Override
                        public void onSuccess(PartitionContext partitionContext) {
                            if (analyser != null) {
                                try {
                                    analyser.analyzeCollectorData(partitionContext.getData());
                                    analyser.analyzeStatus(Status.COMPLETED.name(), partitionContext.getExitStatus());
                                } catch (Throwable t) {
                                    LOGGER.error(t.toString(), t);
                                }
                            }
                        }

                        @Override
                        public void onFailure(Throwable thrown) {
                            // TODO stop all the other partitions
                            try {
                                // PENDING which value for the exit status, null or the same as the batch status?
                                analyser.analyzeStatus(Status.FAILED.name(), null);
                            } catch (Throwable t) {
                                LOGGER.error(t.toString(), t);
                            }
                        }
                    });

                } else {
                    Batchlet artifact = artifactContext.createBatchlet(step.getArtifact().getRef());
                    jobManager.getRunningBatchlets().put(stepExecution.getId(), artifact);

                    // processing
                    String exitStatus = artifact.process();

                    // TODO batchlet has been stopped...

                    // update the exit status of the step execution with the one
                    // returned by the batchlet artifact
                    stepExecution.setExitStatus(exitStatus);
                }

                // update the batch status to COMPLETED
                stepExecution.setStatus(Status.COMPLETED);

                // update the exit status of the step execution with the one stored
                // in the step context
                String exitStatus = JabatThreadContext.getInstance().getStepContext().getExitStatus();
                if (exitStatus != null) {
                    stepExecution.setExitStatus(exitStatus);
                }

                // the batch and exit status of the job are intially the same as the
                // batch and exit status on the last execution element to run
                jobExecution.setStatus(stepExecution.getStatusEnum());
                // PENDING a job has an exit status?

                // batch and exit status can be overridden by a decision element
                // TODO manage decision elements

                // after step listeners
                // TODO should be called even in case of error?
                notifyAfterStep(step, artifactContext);
            } finally {
                jobManager.getRunningBatchlets().removeAll(stepExecution.getId());

                artifactContext.release();

                // store step context persistent area
                // TODO

                // destroy step context
                JabatThreadContext.getInstance().destroyStepContext();
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
            // create step context
            JabatThreadContext.getInstance().createStepContext(step, stepExecution);

            // apply substitutions to step level elements
            JobUtil.substitute(step, jobParameters);

            // store step level properties in step context
            JabatThreadContext.getInstance().getStepContext()
                    .setProperties(step.getProperties());

            ChunkArtifactContext artifactContext = new ChunkArtifactContext(getArtifactFactory());
            try {
                // before step listeners
                notifyBeforeStep(step, artifactContext);

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
                    // open item reader
                    reader.open(Externalizables.deserialize(readerChkptData));
                    try {
                        // open item writer
                        writer.open(Externalizables.deserialize(writerChkptData));
                        try {
                            try {
                                transaction.begin();
                                algorithm.beginCheckpoint();
                                try {
                                    // chunk processing
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
                            // close item writer
                            writer.close();
                        }
                    } finally {
                        // close item reader
                        reader.close();
                    }
                } // end of retry loop

                // TODO what should be the status if we reach the max number of retry?
                jobExecution.setStatus(Status.COMPLETED);
                stepExecution.setStatus(Status.COMPLETED);

                // after step listeners
                // TODO should be called even if case of error?
                notifyAfterStep(step, artifactContext);
            } finally {
                artifactContext.release();

                // store step context persistent area

                // destroy step context
                JabatThreadContext.getInstance().destroyStepContext();
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
        // create flow context
        // TODO

        flow.getFirstChainableNode().accept(this, null);

        // destroy flow context
        // TODO
    }

    @Override
    public void visit(Split split, Void arg) {
        Collection<Node> nodes = split.getNodes();
        if (nodes.size() > 0) {
            try {
                // create split context
                // TODO

                SplitArtifactContext artifactContext = new SplitArtifactContext(getArtifactFactory());
                try {
                    final List<Externalizable> collectedData = new ArrayList<Externalizable>();
                    final SplitCollector collector = split.getCollector() != null
                            ? artifactContext.createSplitCollector(split.getCollector().getRef())
                            : null;
                    // for each flow
                    for (Node node : nodes) {
                        final Flow flow = (Flow) node;

                        // run flow in its own thread
                        getTaskManager().submit(new Runnable() {
                            @Override
                            public void run() {
                                JabatThreadContext.getInstance().createJobContext(job, jobInstance, jobExecution);
                                try {
                                    try {
                                        flow.accept(JobInstanceExecutor.this, null);
                                        // collect data
                                        if (collector != null) {
                                            collectedData.add(collector.collectSplitData());
                                        }
                                    } finally {
                                        JabatThreadContext.getInstance().destroyJobContext();
                                    }
                                } catch (Throwable t) {
                                    LOGGER.error(t.toString(), t);
                                }
                            }
                        });
                    }
                    if (split.getAnalyser() != null) {
                        SplitAnalyzer analyser
                                = artifactContext.createSplitAnalyser(split.getAnalyser().getRef());
                        for (Externalizable data : collectedData) {
                            // analyse data
                            analyser.analyzeCollectorData(data);

                            // analyse status
                            analyser.analyzeStatus(null, null);
                        }
                    }
                } finally {
                    artifactContext.release();

                    // destroy split context
                    // TODO
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
