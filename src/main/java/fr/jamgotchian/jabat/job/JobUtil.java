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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobUtil {

    private JobUtil() {
    }

    public static String substitute(String value, Properties parameters, Properties properties)
            throws IOException, RecognitionException {
        String result = null;
        InputStream is = new ByteArrayInputStream(value.getBytes("UTF-8"));
        try {
            JobXmlSubstitutionLexer lexer = new JobXmlSubstitutionLexer(new ANTLRInputStream(is, "UTF-8"));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            JobXmlSubstitutionParser parser = new JobXmlSubstitutionParser(tokens);
            parser.parameters = parameters;
            parser.properties = properties;
            result = parser.attributeValue();
        } finally {
            is.close();
        }
        return result;
    }
}
