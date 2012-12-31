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
package fr.jamgotchian.jabat.jobxml;

import fr.jamgotchian.jabat.jobxml.util.JobXmlException;
import java.io.File;
import java.net.URISyntaxException;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobPathImpl implements JobPath {

    @Override
    public File[] getDirectories() {
        try {
            return new File[] { new File(getClass().getResource("/META-INF").toURI()) };
        } catch (URISyntaxException e) {
            throw new JobXmlException(e);
        }
    }

}