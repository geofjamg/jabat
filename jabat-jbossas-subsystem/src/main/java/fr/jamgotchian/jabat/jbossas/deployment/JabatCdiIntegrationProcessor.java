/*
 * Copyright 2013 Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>.
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

import fr.jamgotchian.jabat.cdi.JabatCdiExtension;
import java.util.List;
import javax.enterprise.inject.spi.Extension;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.as.server.deployment.Phase;
import org.jboss.as.weld.WeldDeploymentMarker;
import org.jboss.as.weld.deployment.WeldAttachments;
import org.jboss.logging.Logger;
import org.jboss.weld.bootstrap.spi.Metadata;

/**
 * A deployment processor that register the Jabat CDI extension.
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatCdiIntegrationProcessor implements DeploymentUnitProcessor {

    private final Logger LOGGER = Logger.getLogger(JabatCdiIntegrationProcessor.class);

    public static final Phase PHASE = Phase.POST_MODULE;

    public static final int PRIORITY = 0x4000;

    @Override
    public void deploy(DeploymentPhaseContext phaseContext) throws DeploymentUnitProcessingException {
        DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
        DeploymentUnit parent = deploymentUnit.getParent() == null ? deploymentUnit : deploymentUnit.getParent();
        if (WeldDeploymentMarker.isPartOfWeldDeployment(deploymentUnit)) {
            final List<Metadata<Extension>> extensions = parent.getAttachmentList(WeldAttachments.PORTABLE_EXTENSIONS);
            boolean found = false;
            for (Metadata<Extension> extension : extensions) {
                if (extension.getValue() instanceof JabatCdiExtension) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                final JabatCdiExtension ext = new JabatCdiExtension();
                Metadata<Extension> metadata = new Metadata<Extension>() {
                    @Override
                    public Extension getValue() {
                        return ext;
                    }

                    @Override
                    public String getLocation() {
                        return JabatCdiExtension.class.getName();
                    }
                };
                parent.addToAttachmentList(WeldAttachments.PORTABLE_EXTENSIONS, metadata);
            }
        }
    }

    @Override
    public void undeploy(DeploymentUnit context) {
    }

}
