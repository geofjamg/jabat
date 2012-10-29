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

import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobUtilTest {

    private Properties parameters;

    private Propertiable propertiable;

    public JobUtilTest() {
    }

    @Before
    public void setUp() {
        parameters = new Properties();
        parameters.setProperty("infile.name", "in.txt");
        propertiable = new PropertiableImpl();
    }

    @After
    public void tearDown() {
        parameters = null;
        propertiable = null;
    }

    @Test
    public void testSubstitute() throws Exception {
        assertEquals("in.txt", JobUtil.substitute("in.txt", parameters, propertiable));
        assertEquals("in.txt", JobUtil.substitute("#{jobParameters['infile.name']}", parameters, propertiable));
        assertEquals(null, JobUtil.substitute("#{jobParameters['foo']}", parameters, propertiable));
        assertEquals("in.txt", JobUtil.substitute("#{jobParameters['foo']}?:in.txt", parameters, propertiable));
    }
}
