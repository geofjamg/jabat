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
package fr.jamgotchian.jabat.context;

import fr.jamgotchian.jabat.job.Flow;
import fr.jamgotchian.jabat.job.Job;
import fr.jamgotchian.jabat.job.Split;
import fr.jamgotchian.jabat.job.Step;
import fr.jamgotchian.jabat.repository.JabatJobExecution;
import fr.jamgotchian.jabat.repository.JabatJobInstance;
import fr.jamgotchian.jabat.repository.JabatStepExecution;
import java.io.Externalizable;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatThreadContext {

    private static final JabatThreadContext INSTANCE = new JabatThreadContext();

    public static JabatThreadContext getInstance() {
        return INSTANCE;
    }

    private final ThreadLocal<JabatJobContext<Object>> jobContext 
            = new ThreadLocal<JabatJobContext<Object>>();

    private final ThreadLocal<JabatStepContext<Object, Externalizable>> stepContext
            = new ThreadLocal<JabatStepContext<Object, Externalizable>>();

    private final ThreadLocal<JabatFlowContext<Object>> flowContext 
            = new ThreadLocal<JabatFlowContext<Object>>();

    private final ThreadLocal<JabatSplitContext<Object>> splitContext 
            = new ThreadLocal<JabatSplitContext<Object>>();

    public JabatJobContext<Object> getActiveJobContext() {
        return jobContext.get();
    }

    public void activateJobContext(Job job, JabatJobInstance jobInstance, JabatJobExecution jobExecution) {
        jobContext.set(new JabatJobContext<Object>(job, jobInstance, jobExecution));
    }

    public void deactivateJobContext() {
        jobContext.remove();
    }

    public JabatStepContext<Object, Externalizable> getActiveStepContext() {
        return stepContext.get();
    }

    public void activateStepContext(Step step, JabatStepExecution stepExecution) {
        stepContext.set(new JabatStepContext<Object, Externalizable>(step, stepExecution));
    }

    public void deactivateStepContext() {
        stepContext.remove();
    }

    public void activateFlowContext(Flow flow) {
        flowContext.set(new JabatFlowContext<Object>(flow));
    }

    public void deactivateFlowContext() {
        flowContext.remove();
    }

    public void activateSplitContext(Split split) {
        splitContext.set(new JabatSplitContext<Object>(split));
    }

    public void deactivateSplitContext() {
        splitContext.remove();
    }

}
