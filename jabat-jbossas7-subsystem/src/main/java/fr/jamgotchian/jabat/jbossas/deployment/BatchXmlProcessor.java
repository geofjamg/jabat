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
package fr.jamgotchian.jabat.jbossas.deployment;

import fr.jamgotchian.jabat.jbossas.extension.JabatService;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.as.server.deployment.module.ResourceRoot;
import org.jboss.logging.Logger;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceRegistry;
import org.jboss.vfs.VirtualFile;

/**
 * Deployment processor that detects batch.xml file.
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchXmlProcessor implements DeploymentUnitProcessor {

    private final Logger LOGGER = Logger.getLogger(BatchXmlProcessor.class);
    private Map<String, URL> batchXmlUrls = new HashMap<String, URL>();

    @Override
    public void deploy(DeploymentPhaseContext phaseContext) throws DeploymentUnitProcessingException {
        DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
        ResourceRoot root = phaseContext.getDeploymentUnit().getAttachment(Attachments.DEPLOYMENT_ROOT);
        // batch.xml is a marker for deployment containing xml jobs
        VirtualFile batchXml = root.getRoot().getChild("META-INF/batch.xml");
        if (batchXml.exists() && batchXml.isFile()) {
            JabatService service = getJabatService(phaseContext.getServiceRegistry());
            if (service != null) {
                try {
                    batchXmlUrls.put(deploymentUnit.getName(), batchXml.asFileURL());
                } catch (MalformedURLException e) {
                    LOGGER.error(e.toString(), e);
                }
            }
            JabatDeploymentMarker.mark(deploymentUnit);
        }
    }

    @Override
    public void undeploy(DeploymentUnit context) {
        String name = context.getName();
        JabatService service = getJabatService(context.getServiceRegistry());
        if (service != null) {
            batchXmlUrls.remove(name);
        }
    }

    private JabatService getJabatService(ServiceRegistry registry) {
        ServiceController<?> container = registry.getService(JabatService.NAME);
        if (container != null) {
            JabatService service = (JabatService) container.getValue();
            return service;
        }
        return null;
    }
}
