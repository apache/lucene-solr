package org.apache.lucene.queryparser.ext;

import org.apache.lucene.queryparser.classic.QueryParser;

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

/**
 * {@link ExtensionQuery} holds all query components extracted from the original
 * query string like the query field and the extension query string.
 * 
 * @see Extensions
 * @see ExtendableQueryParser
 * @see ParserExtension
 */
public class ExtensionQuery {

  private final String field;
  private final String rawQueryString;
  private final QueryParser topLevelParser;

  /**
   * Creates a new {@link ExtensionQuery}
   * 
   * @param field
   *          the query field
   * @param rawQueryString
   *          the raw extension query string
   */
  public ExtensionQuery(QueryParser topLevelParser, String field, String rawQueryString) {
    this.field = field;
    this.rawQueryString = rawQueryString;
    this.topLevelParser = topLevelParser;
  }

  /**
   * Returns the query field
   * 
   * @return the query field
   */
  public String getField() {
    return field;
  }

  /**
   * Returns the raw extension query string
   * 
   * @return the raw extension query string
   */
  public String getRawQueryString() {
    return rawQueryString;
  }
  
  /**
   * Returns the top level parser which created this {@link ExtensionQuery} 
   * @return the top level parser which created this {@link ExtensionQuery}
   */
  public QueryParser getTopLevelParser() {
    return topLevelParser;
  }
}
