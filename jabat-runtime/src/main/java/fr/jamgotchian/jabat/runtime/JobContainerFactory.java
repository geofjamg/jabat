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

import fr.jamgotchian.jabat.runtime.artifact.ArtifactFactory;
import fr.jamgotchian.jabat.runtime.artifact.BatchXml;
import fr.jamgotchian.jabat.runtime.artifact.BatchXmlParser;
import fr.jamgotchian.jabat.runtime.repository.JobRepository;
import fr.jamgotchian.jabat.runtime.task.TaskManager;
import fr.jamgotchian.jabat.runtime.util.JabatRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobContainerFactory {

    private static final Class<? extends TaskManager> DEFAULT_TASK_MANAGER_CLASS
            = fr.jamgotchian.jabat.runtime.task.impl.ExecutorServiceTaskManager.class;

    private static final Class<? extends JobRepository> DEFAULT_JOB_REPOSITORY_CLASS
            = fr.jamgotchian.jabat.runtime.repository.impl.JobRepositoryImpl.class;

    private BatchXml batchXml;

    private Class<? extends ArtifactFactory> artifactFactoryClass;

    private Class<? extends TaskManager> taskManagerClass;

    private Class<? extends JobRepository> jobRepositoryClass;

    public JobContainerFactory() {
        InputStream is = getClass().getResourceAsStream("/jabat.properties");
        if (is != null) {
            Properties props = new Properties();
            try {
                props.load(is);
                String artifactFactoryClassName = props.getProperty("jabat.artifactFactory");
                if (artifactFactoryClassName !=  null) {
                    artifactFactoryClass = Class.forName(artifactFactoryClassName).asSubclass(ArtifactFactory.class);
                }
                String taskManagerClassName = props.getProperty("jabat.taskManager");
                if (taskManagerClassName != null) {
                    taskManagerClass = Class.forName(taskManagerClassName).asSubclass(TaskManager.class);
                }
                String jobRepositoryClassName = props.getProperty("jabat.jobRepository");
                if (jobRepositoryClassName != null) {
                    jobRepositoryClass = Class.forName(jobRepositoryClassName).asSubclass(JobRepository.class);
                }
            } catch (IOException e) {
                throw new JabatRuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new JabatRuntimeException(e);
            }
        }
    }

    public void setBatchXml(BatchXml batchXml) {
        this.batchXml = batchXml;
    }

    private BatchXml getBatchXml() {
        if (batchXml == null) {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("META-INF/batch.xml");
            if (is == null) {
                throw new JabatRuntimeException("batch.xml not found");
            }
            return new BatchXmlParser().parse(is);
        } else {
            return batchXml;
        }
    }

    public ArtifactFactory getArtifactFactory() throws ReflectiveOperationException {
        if (artifactFactoryClass == null) {
            return null;
        } else {
            return artifactFactoryClass.newInstance();
        }
    }

    public void setArtifactFactoryClass(Class<? extends ArtifactFactory> artifactFactoryClass) {
        this.artifactFactoryClass = artifactFactoryClass;
    }

    private TaskManager getTaskManager() throws ReflectiveOperationException {
        if (taskManagerClass == null) {
            return DEFAULT_TASK_MANAGER_CLASS.newInstance();
        } else {
            return taskManagerClass.newInstance();
        }
    }

    public void setTaskManagerClass(Class<? extends TaskManager> taskManagerClass) {
        this.taskManagerClass = taskManagerClass;
    }

    private JobRepository getJobRepository() throws ReflectiveOperationException {
        if (jobRepositoryClass == null) {
            return DEFAULT_JOB_REPOSITORY_CLASS.newInstance();
        } else {
            return jobRepositoryClass.newInstance();
        }
    }

    public void setJobRepositoryClass(Class<? extends JobRepository> jobRepositoryClass) {
        this.jobRepositoryClass = jobRepositoryClass;
    }

    public JobContainer newInstance() {
        try {
            ArtifactFactory artifactFactory = getArtifactFactory();
            TaskManager taskManager = getTaskManager();
            JobRepository repository = getJobRepository();
            return new JobContainer(getBatchXml(), artifactFactory, taskManager, repository);
        } catch(ReflectiveOperationException e) {
            throw new JabatRuntimeException(e);
        }
    }
}
