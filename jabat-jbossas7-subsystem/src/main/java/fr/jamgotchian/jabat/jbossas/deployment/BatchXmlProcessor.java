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

import fr.jamgotchian.jabat.runtime.artifact.BatchXml;
import fr.jamgotchian.jabat.runtime.artifact.BatchXmlParser;
import java.io.IOException;
import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.as.server.deployment.module.ResourceRoot;
import org.jboss.logging.Logger;
import org.jboss.vfs.VirtualFile;

/**
 * Deployment processor that detects batch.xml file.
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchXmlProcessor implements DeploymentUnitProcessor {

    private static final Logger LOGGER = Logger.getLogger(BatchXmlProcessor.class);

    private static final String META_INF_BATCH_XML = "META-INF/batch.xml";

    private static final String WEB_INF_BATCH_XML = "WEB-INF/batch.xml";

    @Override
    public void deploy(DeploymentPhaseContext phaseContext) throws DeploymentUnitProcessingException {
        DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
        ResourceRoot root = phaseContext.getDeploymentUnit().getAttachment(Attachments.DEPLOYMENT_ROOT);
        // batch.xml is a marker for deployment containing xml jobs
        BatchXml batchXml = getBatchXml(deploymentUnit, root, META_INF_BATCH_XML);
        if (batchXml == null) {
            batchXml = getBatchXml(deploymentUnit, root, WEB_INF_BATCH_XML);
        }
        if (batchXml != null) {
            // mark the deployment
            JabatDeploymentMarker.mark(deploymentUnit, batchXml);
        }
    }

    private static BatchXml getBatchXml(DeploymentUnit deploymentUnit, ResourceRoot root, String batchXmlPath) {
        VirtualFile file = root.getRoot().getChild(batchXmlPath);
        if (file.exists() && file.isFile()) {
            BatchXml batchXml;
            try {
                batchXml = new BatchXmlParser().parse(file.openStream());
            } catch (IOException e) {
                LOGGER.warn(e.toString(), e);
                batchXml = new BatchXml();
            }
            return batchXml;
        }
        return null;
    }

    @Override
    public void undeploy(DeploymentUnit context) {
    }

}
