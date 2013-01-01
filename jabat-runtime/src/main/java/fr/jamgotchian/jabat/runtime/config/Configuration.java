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
package fr.jamgotchian.jabat.runtime.config;

import fr.jamgotchian.jabat.runtime.ArtifactFactory;
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
public class Configuration {

    private static final Class<? extends ArtifactFactory> DEFAULT_ARTIFACT_FACTORY_CLASS
            = fr.jamgotchian.jabat.runtime.artifact.impl.BatchXmlArtifactFactory.class;

    private static final Class<? extends TaskManager> DEFAULT_TASK_MANAGER_CLASS
            = fr.jamgotchian.jabat.runtime.task.impl.ExecutorServiceTaskManager.class;

    private static final Class<? extends JobRepository> DEFAULT_JOB_REPOSITORY_CLASS
            = fr.jamgotchian.jabat.runtime.repository.impl.JobRepositoryImpl.class;

    private Class<? extends ArtifactFactory> artifactFactoryClass;

    private Class<? extends TaskManager> taskManagerClass;

    private Class<? extends JobRepository> jobRepositoryClass;

    public Configuration() {
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

    public Class<? extends ArtifactFactory> getArtifactFactoryClass() {
        if (artifactFactoryClass == null) {
            return DEFAULT_ARTIFACT_FACTORY_CLASS;
        } else {
            return artifactFactoryClass;
        }
    }

    public void setArtifactFactoryClass(Class<? extends ArtifactFactory> artifactFactoryClass) {
        this.artifactFactoryClass = artifactFactoryClass;
    }

    public Class<? extends TaskManager> getTaskManagerClass() {
        if (taskManagerClass == null) {
            return DEFAULT_TASK_MANAGER_CLASS;
        } else {
            return taskManagerClass;
        }
    }

    public void setTaskManagerClass(Class<? extends TaskManager> taskManagerClass) {
        this.taskManagerClass = taskManagerClass;
    }

    public Class<? extends JobRepository> getJobRepositoryClass() {
        if (jobRepositoryClass == null) {
            return DEFAULT_JOB_REPOSITORY_CLASS;
        } else {
            return jobRepositoryClass;
        }
    }

    public void setJobRepositoryClass(Class<? extends JobRepository> jobRepositoryClass) {
        this.jobRepositoryClass = jobRepositoryClass;
    }

}
