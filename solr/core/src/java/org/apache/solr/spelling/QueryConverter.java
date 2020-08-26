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
package org.apache.solr.spelling;
import java.util.Collection;

import org.apache.lucene.analysis.Analyzer;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * <p>
 * The QueryConverter is an abstract base class defining a method for converting
 * input "raw" queries into a set of tokens for spell checking. It is used to
 * "parse" the CommonParams.Q (the input query) and converts it to tokens.
 * </p>
 * 
 * <p>
 * It is only invoked for the CommonParams.Q parameter, and <b>not</b> the
 * "spellcheck.q" parameter. Systems that use their own query parser or those
 * that find issue with the basic implementation should implement their
 * own QueryConverter instead of using the provided implementation
 * (SpellingQueryConverter) by overriding the appropriate methods on the
 * SpellingQueryConverter and registering it in the solrconfig.xml
 * </p>
 * 
 * <p>
 * Refer to <a href="http://wiki.apache.org/solr/SpellCheckComponent">SpellCheckComponent</a>
 * for more details
 * </p>
 * 
 * @since solr 1.3
 */
public abstract class QueryConverter implements NamedListInitializedPlugin {
  @SuppressWarnings({"rawtypes"})
  private NamedList args;

  protected Analyzer analyzer;
  
  /**
   * <p>This term is marked prohibited in the query with the minus sign.</p>
   * 
   */
  public static final int PROHIBITED_TERM_FLAG = 16384;
  /**
   * <p>This term is marked required in the query with the plus sign.</p>
   */
  public static final int REQUIRED_TERM_FLAG = 32768;
  /**
   * <p>
   * This term is directly followed by a boolean operator (AND/OR/NOT)
   * and this operator differs from the prior boolean operator
   * in the query (this signifies this term is likely part of a different
   * query clause than the next term in the query)
   * </p>
   */
  public static final int TERM_PRECEDES_NEW_BOOLEAN_OPERATOR_FLAG = 65536;
  /**
   * <p>
   * This term exists in a query that contains boolean operators
   * (AND/OR/NOT)
   * </p>
   */
  public static final int TERM_IN_BOOLEAN_QUERY_FLAG = 131072;
  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    this.args = args;
  }

  /**
   * Returns the Collection of {@link Token}s for
   *         the query. Offsets on the Token should correspond to the correct
   *         offset in the origQuery
   */
  public abstract Collection<Token> convert(String original);

  /**
   * Set the analyzer to use. Must be set before any calls to convert.
   */
  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  public Analyzer getAnalyzer() {
    return analyzer;
  }
}
