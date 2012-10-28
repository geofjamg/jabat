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

attributeValue : valueExpression (QUESTION_MARK_COLON valueExpression)? ;

valueExpression : (SHARP_OPEN_CURLY_BRACKET operatorExpression CLOSE_CURLY_BRACKET)
                | (STRING_LITERAL OPEN_SQUARE_BRACKET valueExpression CLOSE_SQUARE_BRACKET) ;

operatorExpression : operator1 | operator2 | operator3 ;

operator1 : JOB_PARAMETERS OPEN_SQUARE_BRACKET singleQuotedStringLiteral CLOSE_SQUARE_BRACKET ;

operator2 : JOB_PROPERTIES OPEN_SQUARE_BRACKET singleQuotedStringLiteral CLOSE_SQUARE_BRACKET ;

operator3 : SYSTEM_PROPERTIES OPEN_SQUARE_BRACKET singleQuotedStringLiteral CLOSE_SQUARE_BRACKET ;

singleQuotedStringLiteral : QUOTE STRING_LITERAL QUOTE ;

JOB_PARAMETERS : 'jobParameters' ;

JOB_PROPERTIES : 'jobProperties' ;

SYSTEM_PROPERTIES : 'systemProperties' ;

QUESTION_MARK_COLON : '?:' ;

SHARP_OPEN_CURLY_BRACKET : '#{' ;

CLOSE_CURLY_BRACKET : '}' ;

OPEN_SQUARE_BRACKET : '[' ;

CLOSE_SQUARE_BRACKET : ']' ;

QUOTE : '\'' ;

STRING_LITERAL : ('a'..'z'|'A'..'Z'|'0'..'9'|'.')+ ;

WS : (' '|'\t'|'\n'|'\r')+ {skip();} ;
