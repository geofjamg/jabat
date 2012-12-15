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

grammar JobXmlSubstitution;

@header {
package fr.jamgotchian.jabat.jobxml;
}

@lexer::header {
package fr.jamgotchian.jabat.jobxml;
}

@members {
String propertyName;
java.util.Properties jobParameters;
fr.jamgotchian.jabat.jobxml.JobUtil.JobProperties jobProperties;

@Override
public void emitErrorMessage(String message) {
    throw new fr.jamgotchian.jabat.util.JabatException(message);
}

public String getErrorHeader(RecognitionException e) {
    StringBuilder builder = new StringBuilder();
    if (propertyName != null) {
        builder.append("property '").append(propertyName).append("' ");
    }
    builder.append("character ").append(e.charPositionInLine);
    return builder.toString();
}
}

attributeValue returns [String value]
    : n = valueExpression { $value = $n.value; } (QUESTION_MARK_COLON n = valueExpression { $value = ($value == null ? $n.value :$value); })? ;

valueExpression returns [String value]
    : (
        (SHARP_OPEN_CURLY_BRACKET n = operatorExpression { $value = $n.value; } CLOSE_CURLY_BRACKET)
        | STRING_LITERAL { $value = $STRING_LITERAL.text; }
      )
      (n = valueExpression { $value += $n.value; })? ;

operatorExpression returns [String value]
    : n = operator1 { $value = $n.value; }
    | n = operator2 { $value = $n.value; }
    | n = operator3 { $value = $n.value; }
    ;

operator1 returns [String value] :
    JOB_PARAMETERS OPEN_SQUARE_BRACKET n = singleQuotedStringLiteral { $value = (jobParameters == null ? null : jobParameters.getProperty($n.value)); } CLOSE_SQUARE_BRACKET ;

operator2 returns [String value] :
    JOB_PROPERTIES OPEN_SQUARE_BRACKET n = singleQuotedStringLiteral { $value = (jobProperties == null ? null : jobProperties.getProperty($n.value)); } CLOSE_SQUARE_BRACKET ;

operator3 returns [String value] :
    SYSTEM_PROPERTIES OPEN_SQUARE_BRACKET n = singleQuotedStringLiteral { $value = System.getProperty($n.value); } CLOSE_SQUARE_BRACKET ;

singleQuotedStringLiteral returns [String value] :
    QUOTE STRING_LITERAL { $value = $STRING_LITERAL.text; } QUOTE ;

JOB_PARAMETERS : 'jobParameters' ;

JOB_PROPERTIES : 'jobProperties' ;

SYSTEM_PROPERTIES : 'systemProperties' ;

QUESTION_MARK_COLON : '?:' ;

SHARP_OPEN_CURLY_BRACKET : '#{' ;

CLOSE_CURLY_BRACKET : '}' ;

OPEN_SQUARE_BRACKET : '[' ;

CLOSE_SQUARE_BRACKET : ']' ;

QUOTE : '\'' ;

STRING_LITERAL : ('a'..'z'|'A'..'Z'|'0'..'9'|'.'|'/'|'_'|'-')+ ;

WS : (' '|'\t'|'\n'|'\r')+ {skip();} ;
