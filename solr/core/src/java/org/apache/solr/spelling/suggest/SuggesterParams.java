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
package org.apache.solr.spelling.suggest;

public interface SuggesterParams {
  public static final String SUGGEST_PREFIX = "suggest.";

  /**
   * The name of the dictionary to be used for giving the suggestion for a
   * request. The value for this parameter is configured in solrconfig.xml
   */
  public static final String SUGGEST_DICT = SUGGEST_PREFIX + "dictionary";

  /**
   * The count of suggestions to return for each query term not in the index and/or dictionary.
   * <p>
   * If this parameter is absent in the request then only one suggestion is
   * returned. If it is more than one then a maximum of given suggestions are
   * returned for each token in the query.
   */
  public static final String SUGGEST_COUNT = SUGGEST_PREFIX + "count";
  
  /**
   * Use the value for this parameter as the query to spell check.
   * <p>
   * This parameter is <b>optional</b>. If absent, then the q parameter is
   * used.
   */
  public static final String SUGGEST_Q = SUGGEST_PREFIX + "q";

  /**
   * Whether to build the index or not. Optional and false by default.
   */
  public static final String SUGGEST_BUILD = SUGGEST_PREFIX + "build";
  
  /**
   * Whether to build the index or not for all suggesters in the component.
   * Optional and false by default.
   * This parameter does not need any suggest dictionary names to be specified
   */
  public static final String SUGGEST_BUILD_ALL = SUGGEST_PREFIX + "buildAll";

  /**
   * Whether to reload the index. Optional and false by default.
   */
  public static final String SUGGEST_RELOAD = SUGGEST_PREFIX + "reload";

  /**
   * Whether to reload the index or not for all suggesters in the component.
   * Optional and false by default.
   * This parameter does not need any suggest dictionary names to be specified
   */
  public static final String SUGGEST_RELOAD_ALL = SUGGEST_PREFIX + "reloadAll";

  /**
   * contextFilterQuery to use for filtering the result of the suggestion
   */
  public static final String SUGGEST_CONTEXT_FILTER_QUERY = SUGGEST_PREFIX + "cfq";

  /**
   * Whether keyword should be highlighted in result or not
   */
  public static final String SUGGEST_HIGHLIGHT = SUGGEST_PREFIX + "highlight";


  /**
   * Whether all terms are required or not
   */
  public static final String SUGGEST_ALL_TERMS_REQUIRED = SUGGEST_PREFIX + "allTermsRequired";

}
