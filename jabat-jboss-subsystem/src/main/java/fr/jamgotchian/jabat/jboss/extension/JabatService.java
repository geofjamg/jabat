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
package fr.jamgotchian.jabat.jboss.extension;

import org.jboss.logging.Logger;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatService implements Service<JabatService> {

    private final Logger LOGGER = Logger.getLogger(JabatService.class);

    @Override
    public void start(StartContext context) throws StartException {
        LOGGER.info("Start Jabat service");
    }

    @Override
    public void stop(StopContext context) {
        LOGGER.info("Stop Jabat service");
    }

    @Override
    public JabatService getValue() throws IllegalStateException, IllegalArgumentException {
        return this;
    }

}
