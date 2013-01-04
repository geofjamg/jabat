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

import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.as.server.deployment.module.ModuleDependency;
import org.jboss.as.server.deployment.module.ModuleSpecification;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoader;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatDependencyProcessor implements DeploymentUnitProcessor {

    public static ModuleIdentifier BATCH_API = ModuleIdentifier.create("javax.batch.api");
    public static ModuleIdentifier JABAT = ModuleIdentifier.create("fr.jamgotchian.jabat");

    @Override
    public void deploy(DeploymentPhaseContext phaseContext) throws DeploymentUnitProcessingException {
        final DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
        if (!JabatDeploymentMarker.isJabatDeployment(deploymentUnit)) {
            return;
        }
        final ModuleSpecification moduleSpecification = deploymentUnit.getAttachment(Attachments.MODULE_SPECIFICATION);
        final ModuleLoader moduleLoader = Module.getBootModuleLoader();
        addDependency(moduleSpecification, moduleLoader, BATCH_API);
        addDependency(moduleSpecification, moduleLoader, JABAT);
    }

    private void addDependency(ModuleSpecification moduleSpecification, ModuleLoader moduleLoader,
                               ModuleIdentifier moduleIdentifier) {
        moduleSpecification.addSystemDependency(new ModuleDependency(moduleLoader, moduleIdentifier, false, false, true, false));
    }

    @Override
    public void undeploy(DeploymentUnit context) {
    }

}
