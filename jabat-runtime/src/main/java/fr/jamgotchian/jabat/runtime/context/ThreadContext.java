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
package fr.jamgotchian.jabat.runtime.context;

import fr.jamgotchian.jabat.jobxml.model.Artifact;
import fr.jamgotchian.jabat.jobxml.model.Flow;
import fr.jamgotchian.jabat.jobxml.model.Job;
import fr.jamgotchian.jabat.jobxml.model.Split;
import fr.jamgotchian.jabat.jobxml.model.Step;
import fr.jamgotchian.jabat.runtime.repository.JabatJobExecution;
import fr.jamgotchian.jabat.runtime.repository.JabatJobInstance;
import fr.jamgotchian.jabat.runtime.repository.JabatStepExecution;
import fr.jamgotchian.jabat.runtime.util.JabatRuntimeException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import javax.batch.annotation.BatchContext;
import javax.batch.annotation.BatchProperty;
import javax.batch.runtime.context.FlowContext;
import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.SplitContext;
import javax.batch.runtime.context.StepContext;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ThreadContext {

    private static final ThreadContext INSTANCE = new ThreadContext();

    public static ThreadContext getInstance() {
        return INSTANCE;
    }

    private final ThreadLocal<JabatJobContext> jobContext = new ThreadLocal<JabatJobContext>();

    private final ThreadLocal<JabatStepContext> stepContext = new ThreadLocal<JabatStepContext>();

    private final ThreadLocal<JabatFlowContext> flowContext = new ThreadLocal<JabatFlowContext>();

    private final ThreadLocal<JabatSplitContext> splitContext = new ThreadLocal<JabatSplitContext>();

    /* decision context is the context of last step|flow|split that finished */
    private final ThreadLocal<javax.batch.runtime.context.BatchContext> decisionContext
            = new ThreadLocal<javax.batch.runtime.context.BatchContext>();

    public JabatJobContext getJobContext() {
        return jobContext.get();
    }

    public JabatJobContext createJobContext(Job job, JabatJobInstance jobInstance, JabatJobExecution jobExecution) {
        JabatJobContext context = new JabatJobContext(job, jobInstance, jobExecution);
        jobContext.set(context);
        return context;
    }

    public void setJobContext(JabatJobContext jobContext) {
        this.jobContext.set(jobContext);
    }

    public void removeJobContext() {
        jobContext.remove();
    }

    public JabatStepContext getStepContext() {
        return stepContext.get();
    }

    public JabatStepContext createStepContext(Step step, JabatStepExecution stepExecution) {
        JabatStepContext context = new JabatStepContext(step, stepExecution);
        stepContext.set(context);
        return context;
    }

    public void removeStepContext() {
        decisionContext.set(stepContext.get());
        stepContext.remove();
    }

    public JabatFlowContext getFlowContext() {
        return flowContext.get();
    }

    public JabatFlowContext createFlowContext(Flow flow) {
        JabatFlowContext context = new JabatFlowContext(flow);
        flowContext.set(context);
        return context;
    }

    public void removeFlowContext() {
        decisionContext.set(flowContext.get());
        flowContext.remove();
    }

    public JabatSplitContext getSplitContext() {
        return splitContext.get();
    }

    public JabatSplitContext createSplitContext(Split split) {
        JabatSplitContext context = new JabatSplitContext(split);
        splitContext.set(context);
        return context;
    }

    public void removeSplitContext() {
        decisionContext.set(splitContext.get());
        splitContext.remove();
    }

    public javax.batch.runtime.context.BatchContext getDecisionContext() {
        return decisionContext.get();
    }

    public void inject(Object instance, String name) throws IllegalAccessException {
        Class<?> clazz = instance.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            // context injection
            if (field.isAnnotationPresent(BatchContext.class)) {
                Class<?> fieldType = field.getType();
                Object fieldValue;
                if (fieldType == JobContext.class) {
                    fieldValue = jobContext.get();
                } else if (fieldType == StepContext.class) {
                    fieldValue = stepContext.get();
                } else if (fieldType == SplitContext.class) {
                    fieldValue = splitContext.get();
                } else if (fieldType == FlowContext.class) {
                    fieldValue = flowContext.get();
                } else {
                    throw new JabatRuntimeException("Field annotated with "
                            + BatchContext.class.getName()
                            + " should have one of the following type: "
                            + JobContext.class.getName() + ", " + StepContext.class.getName() + ", "
                            + SplitContext.class.getName() + "or " + FlowContext.class.getName());
                }
                if (!field.isAccessible()) {
                    field.setAccessible(true);
                }
                field.set(instance, fieldValue);
            }
            // property injection
            BatchProperty batchProperty = field.getAnnotation(BatchProperty.class);
            if (batchProperty != null) {
                if (Modifier.isFinal(field.getModifiers())) {
                    throw new JabatRuntimeException("Field annotated with "
                            + BatchProperty.class.getName() + " should not be final");
                }
                if (field.getType() == String.class) {
                    // get the current artifact
                    JabatStepContext context = stepContext.get();
                    if (context == null) {
                        throw new JabatRuntimeException("Step context is not set");
                    }
                    Artifact artifact = context.getNode().getArtifact(name);
                    String propertyName = batchProperty.name().isEmpty()
                            ? field.getName() : batchProperty.name();
                    String value = artifact.getSubstitutedProperties().getProperty(propertyName);
                    if (value != null) {
                        if (!field.isAccessible()) {
                            field.setAccessible(true);
                        }
                        field.set(instance, value);
                    }
                } else {
                    throw new JabatRuntimeException("Field annotated with "
                            + BatchProperty.class.getName() + " should be of type "
                            + String.class.getName());
                }
            }
        }
    }
}
