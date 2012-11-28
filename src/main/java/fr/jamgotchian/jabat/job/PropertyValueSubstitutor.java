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

    private final Properties jobParameters;

    public PropertyValueSubstitutor(Properties parameters) {
        if (parameters == null) {
            throw new IllegalArgumentException("parameters is null");
        }
        this.jobParameters = parameters;
    }

    public void substitute(Node node) {
        Properties jobProperties = getJobPropertiesAtLevel(node);
        substitute(node, jobProperties);
        for (Artifact artifact : node.getArtifacts()) {
            substitute(artifact, jobProperties);
        }
    }

    private void substitute(Propertiable propertiable, Properties jobProperties) {
        Properties result = substitute(propertiable.getProperties(), jobProperties);
        propertiable.getSubstitutedProperties().clear();
        propertiable.getSubstitutedProperties().putAll(result);
    }

    private Properties getJobPropertiesAtLevel(Node node) {
        Properties jobProperties = new Properties();
        if (node.getContainer() != null) {
            getJobPropertiesAtLevel(node.getContainer(), jobProperties);
        }
        return jobProperties;
    }

    private void getJobPropertiesAtLevel(Node node, Properties jobProperties) {
        if (node.getContainer() != null) {
            getJobPropertiesAtLevel(node.getContainer(), jobProperties);
        }
        Properties result = substitute(node.getProperties(), jobProperties);
        // merge result of substitution with job properties
        jobProperties.putAll(result);
    }

    private Properties substitute(Properties properties, Properties jobProperties) {
        Properties result = new Properties();
        try {
            for (String name : properties.stringPropertyNames()) {
                String value = properties.getProperty(name);
                String substitutedValue = JobUtil.substitute(value, jobParameters, jobProperties);
                result.setProperty(name, substitutedValue);
            }
        } catch (IOException e) {
            throw new JabatException(e);
        } catch (RecognitionException e) {
            throw new JabatException(e);
        }
        return result;
    }

}
