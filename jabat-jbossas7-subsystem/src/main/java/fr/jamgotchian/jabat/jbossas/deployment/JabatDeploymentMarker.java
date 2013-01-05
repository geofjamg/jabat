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

import org.jboss.as.server.deployment.AttachmentKey;
import org.jboss.as.server.deployment.DeploymentUnit;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatDeploymentMarker {

    private static final AttachmentKey<Boolean> ATTACHMENT_KEY = AttachmentKey.create(Boolean.class);

    public static void mark(DeploymentUnit deployment) {
        if (deployment.getParent() != null) {
            deployment.getParent().putAttachment(ATTACHMENT_KEY, true);
        } else {
            deployment.putAttachment(ATTACHMENT_KEY, true);
        }
    }

    public static boolean isJabatDeployment(DeploymentUnit deploymentUnit) {
        DeploymentUnit deployment = deploymentUnit.getParent() == null ? deploymentUnit : deploymentUnit.getParent();
        Boolean val = deployment.getAttachment(ATTACHMENT_KEY);
        return val != null && val;
    }

}
