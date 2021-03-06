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
package fr.jamgotchian.jabat.runtime.repository;

import fr.jamgotchian.jabat.jobxml.model.Job;
import fr.jamgotchian.jabat.jobxml.model.Step;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * A job repository contains meta data about currently running processes.
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public interface JobRepository {

    JabatJobInstance createJobInstance(Job job);

    JabatJobExecution createJobExecution(JabatJobInstance jobInstance, Properties jobParameters);

    JabatStepExecution createStepExecution(Step step, JabatJobExecution jobExecution);

    Set<String> getJobIds();

    List<Long> getJobInstanceIds(String id);

    JabatJobInstance getJobInstance(long id);

    JabatJobExecution getJobExecution(long id);

    JabatStepExecution getStepExecution(long id);

}
