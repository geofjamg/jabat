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
package fr.jamgotchian.jabat.jbossas.extension;

import fr.jamgotchian.jabat.JobManager;
import fr.jamgotchian.jabat.config.Configuration;
import org.jboss.logging.Logger;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatService implements Service<JabatService> {

    private final Logger LOGGER = Logger.getLogger(JabatService.class);

    public static final ServiceName NAME = ServiceName.JBOSS.append("jabat");

    private final Configuration config = new Configuration();

    private final JobManager jobManager = new JobManager(config);

    public Configuration getConfig() {
        return config;
    }

    public JobManager getJobManager() {
        return jobManager;
    }

    @Override
    public void start(StartContext context) throws StartException {
        LOGGER.info("Start Jabat service");
        try {
            jobManager.initialize();
        } catch (Throwable t) {
            throw new StartException(t);
        }
    }

    @Override
    public void stop(StopContext context) {
        LOGGER.info("Stop Jabat service");
        try {
            jobManager.shutdown();
        } catch (Throwable t) {
            LOGGER.error(t.toString(), t);
        }
    }

    @Override
    public JabatService getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }

}
