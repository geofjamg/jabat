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

import fr.jamgotchian.jabat.jbossas.deployment.JabatCdiIntegrationProcessor;
import fr.jamgotchian.jabat.jbossas.deployment.JabatScanningProcessor;
import java.util.List;
import org.jboss.as.controller.AbstractBoottimeAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.server.AbstractDeploymentChainStep;
import org.jboss.as.server.DeploymentProcessorTarget;
import org.jboss.dmr.ModelNode;
import org.jboss.logging.Logger;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceController.Mode;

/**
 * Handler responsible for adding the subsystem resource to the model.
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
class JabatSubsystemAdd extends AbstractBoottimeAddStepHandler {

    private final Logger LOGGER = Logger.getLogger(JabatSubsystemAdd.class);

    static final JabatSubsystemAdd INSTANCE = new JabatSubsystemAdd();

    private JabatSubsystemAdd() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void populateModel(ModelNode operation, ModelNode model) throws OperationFailedException {
        model.setEmptyObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void performBoottime(OperationContext context, ModelNode operation, ModelNode model,
            ServiceVerificationHandler verificationHandler, List<ServiceController<?>> newControllers)
            throws OperationFailedException {

        JabatService service = new JabatService();
        ServiceController<JabatService> controller = context.getServiceTarget()
                .addService(JabatService.NAME, service)
                .addListener(verificationHandler)
                .setInitialMode(Mode.ACTIVE)
                .install();
        newControllers.add(controller);

        context.addStep(new AbstractDeploymentChainStep() {
            @Override
            public void execute(DeploymentProcessorTarget processorTarget) {
                processorTarget.addDeploymentProcessor(JabatScanningProcessor.PHASE,
                        JabatScanningProcessor.PRIORITY, new JabatScanningProcessor());
                processorTarget.addDeploymentProcessor(JabatCdiIntegrationProcessor.PHASE,
                        JabatCdiIntegrationProcessor.PRIORITY, new JabatCdiIntegrationProcessor());
            }
        }, OperationContext.Stage.RUNTIME);
    }

}
