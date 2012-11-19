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
package fr.jamgotchian.jabat.job;

import fr.jamgotchian.jabat.util.JabatException;
import java.io.IOException;
import java.util.Properties;
import org.antlr.runtime.RecognitionException;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class PropertyValueSubstitutor {

    private final Job job;

    private final Properties jobParameters;

    public PropertyValueSubstitutor(Job job, Properties parameters) {
        if (job == null) {
            throw new IllegalArgumentException("job is null");
        }
        if (parameters == null) {
            throw new IllegalArgumentException("parameters is null");
        }
        this.job = job;
        this.jobParameters = parameters;
    }

    public void substitute() {
        visitor.visit(job, new Properties());
    }

    private NodeVisitor<Properties> visitor = new NodeVisitor<Properties>() {

        private void substitute(Properties properties, Properties jobProperties) {
            try {
                for (String name : properties.stringPropertyNames()) {
                    String value = properties.getProperty(name);
                    properties.setProperty(name, JobUtil.substitute(value, jobParameters, jobProperties));
                }
                jobProperties.putAll(job.getProperties());
            } catch (IOException e) {
                throw new JabatException(e);
            } catch (RecognitionException e) {
                throw new JabatException(e);
            }
        }

        @Override
        public void visit(Job job, Properties jobProperties) {
            substitute(job.getProperties(), jobProperties);
            for (Node node : job.getNodes()) {
                node.accept(this, new Properties(jobProperties));
            }
        }

        @Override
        public void visit(BatchletStep step, Properties jobProperties) {
            substitute(step.getProperties(), jobProperties);
            substitute(step.getArtifact().getProperties(), jobProperties);
        }

        @Override
        public void visit(ChunkStep step, Properties jobProperties) {
            substitute(step.getProperties(), jobProperties);
            substitute(step.getReader().getProperties(), new Properties(jobProperties));
            substitute(step.getProcessor().getProperties(), new Properties(jobProperties));
            substitute(step.getWriter().getProperties(), new Properties(jobProperties));
        }

        @Override
        public void visit(Flow flow, Properties jobProperties) {
            substitute(flow.getProperties(), jobProperties);
            for (Node node : flow.getNodes()) {
                node.accept(this, new Properties(jobProperties));
            }
        }

        @Override
        public void visit(Split split, Properties jobProperties) {
            substitute(split.getProperties(), jobProperties);
            for (Node node : split.getNodes()) {
                node.accept(this, new Properties(jobProperties));
            }
        }

        @Override
        public void visit(Decision decision, Properties jobProperties) {
            substitute(decision.getProperties(), jobProperties);
        }
    };

}
