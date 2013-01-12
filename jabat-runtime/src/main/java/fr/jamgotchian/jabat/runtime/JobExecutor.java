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
package fr.jamgotchian.jabat.runtime;

import fr.jamgotchian.jabat.jobxml.JobUtil;
import fr.jamgotchian.jabat.jobxml.model.Artifact;
import fr.jamgotchian.jabat.jobxml.model.BatchletStep;
import fr.jamgotchian.jabat.jobxml.model.Chainable;
import fr.jamgotchian.jabat.jobxml.model.ChunkStep;
import fr.jamgotchian.jabat.jobxml.model.ControlElement;
import fr.jamgotchian.jabat.jobxml.model.Decision;
import fr.jamgotchian.jabat.jobxml.model.EndElement;
import fr.jamgotchian.jabat.jobxml.model.FailElement;
import fr.jamgotchian.jabat.jobxml.model.Flow;
import fr.jamgotchian.jabat.jobxml.model.Job;
import fr.jamgotchian.jabat.jobxml.model.NextElement;
import fr.jamgotchian.jabat.jobxml.model.Node;
import fr.jamgotchian.jabat.jobxml.model.NodeVisitor;
import fr.jamgotchian.jabat.jobxml.model.Split;
import fr.jamgotchian.jabat.jobxml.model.Step;
import fr.jamgotchian.jabat.jobxml.model.StopElement;
import fr.jamgotchian.jabat.runtime.artifact.ArtifactContainer;
import fr.jamgotchian.jabat.runtime.checkpoint.ItemCheckpointAlgorithm;
import fr.jamgotchian.jabat.runtime.checkpoint.TimeCheckpointAlgorithm;
import fr.jamgotchian.jabat.runtime.context.JabatJobContext;
import fr.jamgotchian.jabat.runtime.context.ThreadContext;
import fr.jamgotchian.jabat.runtime.repository.BatchStatus;
import fr.jamgotchian.jabat.runtime.repository.JabatJobExecution;
import fr.jamgotchian.jabat.runtime.repository.JabatJobInstance;
import fr.jamgotchian.jabat.runtime.repository.JabatStepExecution;
import fr.jamgotchian.jabat.runtime.task.TaskResultListener;
import fr.jamgotchian.jabat.runtime.transaction.NoTransactionManager;
import fr.jamgotchian.jabat.runtime.util.Externalizables;
import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import javax.batch.api.Batchlet;
import javax.batch.api.CheckpointAlgorithm;
import javax.batch.api.Decider;
import javax.batch.api.ItemProcessor;
import javax.batch.api.ItemReader;
import javax.batch.api.ItemWriter;
import javax.batch.api.JobListener;
import javax.batch.api.PartitionAnalyzer;
import javax.batch.api.PartitionCollector;
import javax.batch.api.PartitionMapper;
import javax.batch.api.PartitionReducer;
import javax.batch.api.StepListener;
import javax.batch.api.parameters.PartitionPlan;
import javax.batch.runtime.spi.TransactionManagerSPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
class JobExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutor.class);

    private final Job job;

    private final Properties jobParameters;

    private final JabatJobInstance jobInstance;

    JobExecutor(Job job, Properties jobParameters, JabatJobInstance jobInstance) {
        this.job = job;
        this.jobParameters = jobParameters;
        this.jobInstance = jobInstance;
    }

    void execute(JobExecutionContext executionContext) {
        new NodeVisitorImpl().visit(job, executionContext);
    }

    private static void notifyBeforeStep(Step step, ArtifactContainer container) throws Exception {
        for (Artifact a : step.getListeners()) {
            StepListener l = container.create(a.getRef(), StepListener.class);
            l.beforeStep();
        }
    }

    private static void notifyAfterStep(Step step, ArtifactContainer container) throws Exception {
        for (StepListener l : container.get(StepListener.class)) {
            l.afterStep();
        }
    }

    private static boolean isPartionned(Step step) {
        return step.getPartitionPlan() != null || step.getPartitionMapper() != null;
    }

    private static PartitionPlan createPartitionPlan(Step step, ArtifactContainer container) throws Exception {
        if (step.getPartitionMapper() != null) {
            // dynamic defintion of the partition plan though an artifact
            String ref = step.getPartitionMapper().getRef();
            PartitionMapper mapper = container.create(ref, PartitionMapper.class);
            return mapper.mapPartitions();
        } else {
            // static defintion of the partition plan
            return step.getPartitionPlan();
        }
    }

    private static PartitionReducer createPartitionReducer(Step step, ArtifactContainer container) throws Exception {
        if (step.getPartitionReducer() != null) {
            String ref = step.getPartitionReducer().getRef();
            return container.create(ref, PartitionReducer.class);
        }
        return null;
    }

    private static PartitionCollector createPartitionCollector(Step step, ArtifactContainer container) throws Exception {
        if (step.getPartitionCollector() != null) {
            String ref= step.getPartitionCollector().getRef();
            return container.create(ref, PartitionCollector.class);
        }
        return null;
    }

    private static PartitionAnalyzer createPartitionAnalyser(Step step, ArtifactContainer container) throws Exception {
        if (step.getPartitionAnalyzer() != null) {
            String ref = step.getPartitionAnalyzer().getRef();
            return container.create(ref, PartitionAnalyzer.class);
        }
        return null;
    }

    private static CheckpointAlgorithm getCheckpointAlgorithm(ChunkStep step, ArtifactContainer container) throws Exception {
        switch (step.getCheckpointPolicy()) {
            case ITEM:
                return new ItemCheckpointAlgorithm(step.getCommitInterval());
            case TIME:
                return new TimeCheckpointAlgorithm(step.getCommitInterval());
            case CUSTOM:
                {
                    String ref = step.getCheckpointAlgo().getRef();
                    return container.create(ref, CheckpointAlgorithm.class);
                }
            default:
                throw new InternalError();
        }
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

    private class NodeVisitorImpl implements NodeVisitor<JobExecutionContext> {

        private JabatJobExecution jobExecution;

        @Override
        public void visit(final Job job, final JobExecutionContext executionContext) {

            // create a job execution
            jobExecution = executionContext.getRepository().createJobExecution(jobInstance, jobParameters);

            executionContext.getTaskManager().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        // create job context
                        ThreadContext.getInstance().createJobContext(job, jobInstance, jobExecution);

                        // apply substitutions to job level elements
                        JobUtil.substitute(job, jobParameters);

                        // store job level properties in job context
                        ThreadContext.getInstance().getJobContext()
                                .setProperties(job.getSubstitutedProperties());

                        ArtifactContainer container = executionContext.createArtifactContainer();
                        try {
                            // before job listeners
                            for (Artifact a : job.getListeners()) {
                                JobListener l = container.create(a.getRef(), JobListener.class);
                                l.beforeJob();
                            }

                            // run the job
                            jobExecution.setStatus(BatchStatus.STARTED);
                            job.getFirstChainableNode().accept(NodeVisitorImpl.this, executionContext);

                            // after job listeners
                            for (JobListener l : container.get(JobListener.class)) {
                                l.afterJob();
                            }
                        } finally {
                            container.release();

                            // remove job context
                            ThreadContext.getInstance().removeJobContext();
                        }
                    } catch (Throwable t) {
                        LOGGER.error(t.toString(), t);
                    }
                }
            });
        }

        private <N extends Node & Chainable> void visitNextNode(N node, JobExecutionContext executionContext) {
            assert node.getContainer() != null;
            if (node.getNext() != null) {
                Node next = node.getContainer().getNode(node.getNext());
                next.accept(this, executionContext);
            }
        }

        @Override
        public void visit(final BatchletStep step, final JobExecutionContext executionContext) {
            final JabatStepExecution stepExecution = executionContext.getRepository().createStepExecution(step, jobExecution);

            try {
                // create step context
                ThreadContext.getInstance().createStepContext(step, stepExecution);

                // apply substitutions to step level elements
                JobUtil.substitute(step, jobParameters);

                // store step level properties in step context
                ThreadContext.getInstance().getStepContext().setProperties(step.getProperties());

                final ArtifactContainer container = executionContext.createArtifactContainer();
                try {
                    // before step listeners
                    notifyBeforeStep(step, container);

                    stepExecution.setStatus(BatchStatus.STARTED);

                    if (isPartionned(step)) {

                        // create partition reducer
                        PartitionReducer reducer = createPartitionReducer(step, container);

                        // begin partitioned step
                        if (reducer != null) {
                            reducer.beginPartitionedStep();
                        }

                        // create partition plan
                        final PartitionPlan plan = createPartitionPlan(step, container);

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
                                        ThreadContext.getInstance().createJobContext(job, jobInstance, jobExecution);
                                        try {
                                            // each partition has its own step context
                                            // PENDING clone the step job context?
                                            ThreadContext.getInstance().createStepContext(step, stepExecution);

                                            // store in the step context step level properties overriden
                                            // partition properties
                                            Properties properties = new Properties();
                                            properties.putAll(step.getProperties());
                                            if (plan.getPartitionProperties() != null) {
                                                properties.putAll(JobUtil.substitute(plan.getPartitionProperties()[partitionNumber], jobParameters, step));
                                            }
                                            ThreadContext.getInstance().getStepContext().setProperties(properties);

                                            try {
                                                Batchlet artifact = container.create(step.getArtifact().getRef(), Batchlet.class);
                                                executionContext.getRunningBatchlets().put(stepExecution.getId(), artifact);

                                                // processing
                                                String exitStatus = artifact.process();

                                                // TODO batchlet has been stopped...

                                                // store the exit status return by the batchlet artifact
                                                // in the partition context
                                                partitionContext.setExitStatus(exitStatus);

                                                // create partition collector
                                                PartitionCollector collector = createPartitionCollector(step, container);

                                                // collect data
                                                if (collector != null) {
                                                    Externalizable data = collector.collectPartitionData();
                                                    partitionContext.setData(data);
                                                }
                                            } finally {
                                                // store the exit status set in the step context in the partition context
                                                // PENDING consequently, it overrides the one returned by the batchlet artifact?
                                                String exitStatus = ThreadContext.getInstance().getStepContext().getExitStatus();
                                                if (exitStatus != null) {
                                                    partitionContext.setExitStatus(exitStatus);
                                                }

                                                // remove the step context of the partition
                                                ThreadContext.getInstance().removeStepContext();
                                            }
                                        } finally {
                                            ThreadContext.getInstance().removeJobContext();
                                        }
                                    } catch (Throwable t) {
                                        LOGGER.error(t.toString(), t);
                                    }
                                    return partitionContext;
                                }
                            });
                        }

                        final PartitionAnalyzer analyser = createPartitionAnalyser(step, container);

                        // run partitions on a thread pool, wait for all partitions to
                        // end and call the analyser each time a partition ends
                        executionContext.getTaskManager().submitAndWait(tasks, plan.getThreadCount(),
                                new TaskResultListener<PartitionContext>() {

                            @Override
                            public void onSuccess(PartitionContext partitionContext) {
                                if (analyser != null) {
                                    try {
                                        analyser.analyzeCollectorData(partitionContext.getData());
                                        analyser.analyzeStatus(BatchStatus.COMPLETED.name(), partitionContext.getExitStatus());
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
                                    analyser.analyzeStatus(BatchStatus.FAILED.name(), null);
                                } catch (Throwable t) {
                                    LOGGER.error(t.toString(), t);
                                }
                            }
                        });

                    } else {
                        Batchlet artifact = container.create(step.getArtifact().getRef(), Batchlet.class);
                        executionContext.getRunningBatchlets().put(stepExecution.getId(), artifact);

                        // processing
                        String exitStatus = artifact.process();

                        // TODO batchlet has been stopped...

                        // update the exit status of the step execution with the one
                        // returned by the batchlet artifact
                        stepExecution.setExitStatus(exitStatus);
                    }

                    // update the batch status to COMPLETED
                    stepExecution.setStatus(BatchStatus.COMPLETED);

                    // update the exit status of the step execution with the one stored
                    // in the step context
                    String exitStatus = ThreadContext.getInstance().getStepContext().getExitStatus();
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
                    notifyAfterStep(step, container);
                } finally {
                    executionContext.getRunningBatchlets().removeAll(stepExecution.getId());

                    container.release();

                    // store step context persistent area
                    // TODO

                    // remove step context
                    ThreadContext.getInstance().removeStepContext();
                }

                visitNextNode(step, executionContext);
            } catch (Throwable t) {
                jobExecution.setStatus(BatchStatus.FAILED);
                stepExecution.setStatus(BatchStatus.FAILED);
                LOGGER.error(t.toString(), t);
            }
        }

        @Override
        public void visit(ChunkStep step, JobExecutionContext executionContext) {
            JabatStepExecution stepExecution = executionContext.getRepository().createStepExecution(step, jobExecution);

            try {
                // create step context
                ThreadContext.getInstance().createStepContext(step, stepExecution);

                // apply substitutions to step level elements
                JobUtil.substitute(step, jobParameters);

                // store step level properties in step context
                ThreadContext.getInstance().getStepContext()
                        .setProperties(step.getProperties());

                ArtifactContainer container = executionContext.createArtifactContainer();
                try {
                    // before step listeners
                    notifyBeforeStep(step, container);

                    ItemReader reader
                            = container.create(step.getReader().getRef(), ItemReader.class);
                    ItemProcessor processor
                            = container.create(step.getProcessor().getRef(), ItemProcessor.class);
                    ItemWriter writer
                            = container.create(step.getWriter().getRef(), ItemWriter.class);

                    stepExecution.setStatus(BatchStatus.STARTED);

                    // select the checkpoint algorithm
                    CheckpointAlgorithm algorithm = getCheckpointAlgorithm(step, container);

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
                    jobExecution.setStatus(BatchStatus.COMPLETED);
                    stepExecution.setStatus(BatchStatus.COMPLETED);

                    // after step listeners
                    // TODO should be called even if case of error?
                    notifyAfterStep(step, container);
                } finally {
                    container.release();

                    // store step context persistent area

                    // remove step context
                    ThreadContext.getInstance().removeStepContext();
                }

                visitNextNode(step, executionContext);
            } catch(Throwable t) {
                jobExecution.setStatus(BatchStatus.FAILED);
                stepExecution.setStatus(BatchStatus.FAILED);
                LOGGER.error(t.toString(), t);
            }
        }

        @Override
        public void visit(Flow flow, JobExecutionContext executionContext) {
            // create flow context
            ThreadContext.getInstance().createFlowContext(flow);
            try {
                flow.getFirstChainableNode().accept(this, executionContext);
            } finally {
                // remove flow context
                ThreadContext.getInstance().removeFlowContext();
            }
        }

        @Override
        public void visit(Split split, final JobExecutionContext executionContext) {
            Collection<Node> nodes = split.getNodes();
            if (nodes.size() > 0) {
                try {
                    // create split context
                    ThreadContext.getInstance().createSplitContext(split);

                    try {
                        // get the job context of the parent thread
                        final JabatJobContext jobContext = ThreadContext.getInstance().getJobContext();

                        // for each flow
                        for (Node node : nodes) {
                            final Flow flow = (Flow) node;

                            // run flow in its own thread
                            executionContext.getTaskManager().submit(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        // transfer the job context child threads
                                        ThreadContext.getInstance().setJobContext(jobContext);
                                        try {
                                            // run the flow
                                            flow.accept(NodeVisitorImpl.this, executionContext);
                                        } finally {
                                            ThreadContext.getInstance().removeJobContext();
                                        }
                                    } catch (Throwable t) {
                                        LOGGER.error(t.toString(), t);
                                    }
                                }
                            });
                        }
                    } finally {
                        // remove split context
                        ThreadContext.getInstance().removeSplitContext();
                    }
                } catch (Throwable t) {
                    LOGGER.error(t.toString(), t);
                }
            }
        }

        private ControlElement findControlElement(List<ControlElement> controlElements, String exitStatus) {
            for (ControlElement ctrlElt : controlElements) {
                // TODO : use matching rules (*, ?)
                if (ctrlElt.getOn().equals(exitStatus)) {
                    return ctrlElt;
                }
            }
            return null;
        }

        private void visitControlElements(List<ControlElement> controlElements,
                String exitStatus, Node node, JobExecutionContext executionContext) {
            ControlElement ctrlElt = findControlElement(controlElements, exitStatus);
            if (ctrlElt != null) {
                switch (ctrlElt.getType()) {
                    case FAIL:
                        // stop the job with the fail batch status
                        // TODO stop the job
                        jobExecution.setStatus(BatchStatus.FAILED);
                        jobExecution.setExitStatus(((FailElement) ctrlElt).getExitStatus());
                        break;
                    case END:
                        // stop the job with the completed batch status
                        // TODO stop the job
                        jobExecution.setStatus(BatchStatus.COMPLETED);
                        jobExecution.setExitStatus(((EndElement) ctrlElt).getExitStatus());
                        break;
                    case STOP:
                        {
                            StopElement stopElt = (StopElement) ctrlElt;
                            // stop the job with the stop batch status
                            // TODO stop the job
                            jobExecution.setStatus(BatchStatus.STOPPED);
                            jobExecution.setExitStatus(stopElt.getExitStatus());
                            String restart = stopElt.getRestart();
                            // TODO : step to restart
                        }
                        break;
                    case NEXT:
                        {
                            NextElement nextElt = (NextElement) ctrlElt;
                            Node toNode = node.getContainer().getNode(nextElt.getTo());
                            toNode.accept(this, executionContext);
                        }
                        break;
                }
            }
        }

        @Override
        public void visit(Decision decision, JobExecutionContext executionContext) {
            try {
                ArtifactContainer container = executionContext.createArtifactContainer();
                try {
                    Decider decider = container.create(decision.getArtifact().getRef(), Decider.class);
                    String exitStatus = decider.decide(ThreadContext.getInstance().getDecisionContext());
                    visitControlElements(decision.getControlElements(), exitStatus, decision, executionContext);
                } finally {
                    container.release();
                }
            } catch (Throwable t) {
                LOGGER.error(t.toString(), t);
            }
        }
    }
}
