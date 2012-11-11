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
package fr.jamgotchian.jabat.artifact;

import fr.jamgotchian.jabat.artifact.annotated.JobListenerAnnotatedClass;
import java.lang.reflect.InvocationTargetException;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobListenerArtifactInstance extends ArtifactInstance {

    private JobListenerAnnotatedClass annotatedClass;

    public JobListenerArtifactInstance(Object object, String ref) {
        super(object, ref);
    }

    @Override
    public void initialize() {
        annotatedClass = new JobListenerAnnotatedClass(object.getClass());
    }

    public void beforeJob() throws Exception {
        try {
            if (annotatedClass.getBeforeJobMethod() != null) {
                annotatedClass.getBeforeJobMethod().invoke(object);
            }
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

    public void afterJob() throws Exception {
        try {
            if (annotatedClass.getAfterJobMethod() != null) {
                annotatedClass.getAfterJobMethod().invoke(object);
            }
        } catch(InvocationTargetException e) {
            if (e.getCause() instanceof Exception) {
                throw (Exception) e.getCause();
            } else {
                throw e;
            }
        }
    }

}
