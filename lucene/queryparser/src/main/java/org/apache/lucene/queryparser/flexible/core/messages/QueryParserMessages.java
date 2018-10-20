/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.queryparser.flexible.core.messages;

import org.apache.lucene.queryparser.flexible.messages.NLS;

/**
 * Flexible Query Parser message bundle class
 */
public class QueryParserMessages extends NLS {

  private static final String BUNDLE_NAME = QueryParserMessages.class.getName();

  private QueryParserMessages() {
    // Do not instantiate
  }

  static {
    // register all string ids with NLS class and initialize static string
    // values
    NLS.initializeMessages(BUNDLE_NAME, QueryParserMessages.class);
  }

  // static string must match the strings in the property files.
  public static String INVALID_SYNTAX;
  public static String INVALID_SYNTAX_CANNOT_PARSE;
  public static String INVALID_SYNTAX_FUZZY_LIMITS;
  public static String INVALID_SYNTAX_FUZZY_EDITS;
  public static String INVALID_SYNTAX_ESCAPE_UNICODE_TRUNCATION;
  public static String INVALID_SYNTAX_ESCAPE_CHARACTER;
  public static String INVALID_SYNTAX_ESCAPE_NONE_HEX_UNICODE;
  public static String NODE_ACTION_NOT_SUPPORTED;
  public static String PARAMETER_VALUE_NOT_SUPPORTED;
  public static String LUCENE_QUERY_CONVERSION_ERROR;
  public static String EMPTY_MESSAGE;
  public static String WILDCARD_NOT_SUPPORTED;
  public static String TOO_MANY_BOOLEAN_CLAUSES;
  public static String LEADING_WILDCARD_NOT_ALLOWED;
  public static String COULD_NOT_PARSE_NUMBER;
  public static String NUMBER_CLASS_NOT_SUPPORTED_BY_NUMERIC_RANGE_QUERY;
  public static String UNSUPPORTED_NUMERIC_DATA_TYPE;
  public static String NUMERIC_CANNOT_BE_EMPTY;

}
