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
import fr.jamgotchian.jabat.runtime.context.JabatJobContext;
import fr.jamgotchian.jabat.runtime.context.ThreadContext;
import fr.jamgotchian.jabat.runtime.repository.BatchStatus;
import fr.jamgotchian.jabat.runtime.repository.JabatJobExecution;
import fr.jamgotchian.jabat.runtime.repository.JabatStepExecution;
import fr.jamgotchian.jabat.runtime.task.AbstractTaskResultListener;
import fr.jamgotchian.jabat.runtime.task.TaskResultListener;
import fr.jamgotchian.jabat.runtime.transaction.NoTransactionManager;
import fr.jamgotchian.jabat.runtime.util.Externalizables;
import fr.jamgotchian.jabat.runtime.util.RethrowException;
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

    JobExecutor(Job job) {
        this.job = job;
    }

    void execute(JobExecutionContext executionContext, JobExecutionListener listener) {
        new NodeVisitorImpl(listener).visit(job, executionContext);
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

        private JobExecutionListener listener;

        private NodeVisitorImpl(JobExecutionListener listener) {
            this.listener = listener;
        }

        @Override
        public void visit(final Job job, final JobExecutionContext executionContext) {

            executionContext.getTaskManager().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        executionContext.getJobExecution().setStatus(BatchStatus.STARTED);

                        listener.started(executionContext);

                        // create job context
                        ThreadContext.getInstance().createJobContext(job, executionContext.getJobInstance(), executionContext.getJobExecution());

                        // apply substitutions to job level elements
                        JobUtil.substitute(job, executionContext.getJobParameters());

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
                            job.getFirstChainableNode().accept(NodeVisitorImpl.this, executionContext);

                            // after job listeners
                            for (JobListener l : container.get(JobListener.class)) {
                                l.afterJob();
                            }
                        } finally {
                            container.release();

                            // remove job context
                            ThreadContext.getInstance().removeJobContext();

                            listener.finished(executionContext);
                        }
                    } catch (Throwable t) {
                        Throwable t2 = t instanceof RethrowException ? t.getCause() : t;
                        LOGGER.error(t2.toString(), t2);

                        executionContext.stopRunningSteps();

                        executionContext.getJobExecution().setStatus(BatchStatus.FAILED);
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
            final JabatStepExecution stepExecution = executionContext.getRepository()
                    .createStepExecution(step, executionContext.getJobExecution());

            try {
                // create step context
                ThreadContext.getInstance().createStepContext(step, stepExecution);

                // apply substitutions to step level elements
                JobUtil.substitute(step, executionContext.getJobParameters());

                // store step level properties in step context
                ThreadContext.getInstance().getStepContext().setProperties(step.getProperties());

                final ArtifactContainer container = executionContext.createArtifactContainer();
                try {
                    // before step listeners
                    notifyBeforeStep(step, container);

                    stepExecution.setStatus(BatchStatus.STARTED);

                    if (step.isPartionned()) {

                        // create partition reducer
                        PartitionReducer reducer = container.createPartitionReducer(step);

                        // begin partitioned step
                        if (reducer != null) {
                            reducer.beginPartitionedStep();
                        }

                        // create partition plan
                        final PartitionPlan plan = container.createPartitionPlan(step);

                        // prepare a task for each parttion
                        List<Callable<PartitionContext>> tasks = new ArrayList<Callable<PartitionContext>>();

                        final JabatJobContext jobContext = ThreadContext.getInstance().getJobContext();

                        for (int i = 0; i < plan.getPartitionCount(); i++) {
                            final int partitionNumber = i;

                            tasks.add(new Callable<PartitionContext>() {

                                @Override
                                public PartitionContext call() throws Exception {
                                    PartitionContext partitionContext = new PartitionContext();

                                    // each partion has its own job context
                                    // PENDING clone the parent job context?
                                    ThreadContext.getInstance().setJobContext(jobContext);
                                    try {
                                        // each partition has its own step context
                                        // PENDING clone the step job context?
                                        ThreadContext.getInstance().createStepContext(step, stepExecution);

                                        // store in the step context step level properties overriden
                                        // partition properties
                                        Properties properties = new Properties();
                                        properties.putAll(step.getProperties());
                                        if (plan.getPartitionProperties() != null) {
                                            properties.putAll(JobUtil.substitute(plan.getPartitionProperties()[partitionNumber], executionContext.getJobParameters(), step));
                                        }
                                        ThreadContext.getInstance().getStepContext().setProperties(properties);

                                        try {
                                            Batchlet batchlet = container.create(step.getArtifact().getRef(), Batchlet.class);

                                            // processing
                                            String exitStatus = batchlet.process();

                                            // TODO batchlet has been stopped...

                                            // store the exit status return by the batchlet artifact
                                            // in the partition context
                                            partitionContext.setExitStatus(exitStatus);

                                            // create partition collector
                                            PartitionCollector collector = container.createPartitionCollector(step);

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
                                    return partitionContext;
                                }
                            });
                        }

                        final PartitionAnalyzer analyser = container.createPartitionAnalyser(step);

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
                                        // rethrow
                                        throw new RethrowException(t);
                                    }
                                }
                            }

                            @Override
                            public void onFailure(Throwable thrown) {
                                // rethrow
                                throw new RethrowException(thrown);
                            }
                        });

                    } else {
                        Batchlet batchlet = container.create(step.getArtifact().getRef(), Batchlet.class);

                        // processing
                        String exitStatus = batchlet.process();

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
                    executionContext.getJobExecution().setStatus(stepExecution.getStatusEnum());
                    // PENDING a job has an exit status?

                    // batch and exit status can be overridden by a decision element
                    // TODO manage decision elements

                    // after step listeners
                    // TODO should be called even in case of error?
                    notifyAfterStep(step, container);
                } finally {
                    container.release();

                    // store step context persistent area
                    // TODO

                    // remove step context
                    ThreadContext.getInstance().removeStepContext();
                }

                visitNextNode(step, executionContext);
            } catch (Throwable t) {
                stepExecution.setStatus(BatchStatus.FAILED);
                // rethrow
                throw new RethrowException(t);
            }
        }

        @Override
        public void visit(ChunkStep step, JobExecutionContext executionContext) {
            JabatStepExecution stepExecution = executionContext.getRepository()
                    .createStepExecution(step, executionContext.getJobExecution());

            try {
                // create step context
                ThreadContext.getInstance().createStepContext(step, stepExecution);

                // apply substitutions to step level elements
                JobUtil.substitute(step, executionContext.getJobParameters());

                // store step level properties in step context
                ThreadContext.getInstance().getStepContext().setProperties(step.getProperties());

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
                    CheckpointAlgorithm algorithm = container.createCheckpointAlgorithm(step);

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
                    executionContext.getJobExecution().setStatus(BatchStatus.COMPLETED);
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
                stepExecution.setStatus(BatchStatus.FAILED);
                // rethrow
                throw new RethrowException(t);
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

                        // one subtask for each flow
                        List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();

                        for (Node node : nodes) {
                            final Flow flow = (Flow) node;

                            tasks.add(new Callable<Void>() {

                                @Override
                                public Void call() throws Exception {
                                    // transfer the job context child threads
                                    ThreadContext.getInstance().setJobContext(jobContext);
                                    try {
                                        // run the flow
                                        flow.accept(NodeVisitorImpl.this, executionContext);
                                    } finally {
                                        ThreadContext.getInstance().removeJobContext();
                                    }
                                    return null;
                                }
                            });
                        }

                        executionContext.getTaskManager().submitAndWait(tasks, tasks.size(), new AbstractTaskResultListener<Void>() {

                            @Override
                            public void onFailure(Throwable thrown) {
                                // rethrow
                                throw new RethrowException(thrown);
                            }
                        });

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
                JabatJobExecution jobExecution = executionContext.getJobExecution();
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
                // rethrow
                throw new RethrowException(t);
            }
        }
    }
}
