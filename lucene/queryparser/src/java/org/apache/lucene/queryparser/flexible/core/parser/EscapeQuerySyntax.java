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
package org.apache.lucene.queryparser.flexible.core.parser;

import java.util.Locale;

/**
 * A parser needs to implement {@link EscapeQuerySyntax} to allow the QueryNode to escape the
 * queries, when the toQueryString method is called.
 */
public interface EscapeQuerySyntax {
  /**
   * Type of escaping: String for escaping syntax, NORMAL for escaping reserved words (like AND) in
   * terms
   */
  public enum Type {
    STRING,
    NORMAL;
  }

  /**
   * @param text - text to be escaped
   * @param locale - locale for the current query
   * @param type - select the type of escape operation to use
   * @return escaped text
   */
  CharSequence escape(CharSequence text, Locale locale, Type type);
}
