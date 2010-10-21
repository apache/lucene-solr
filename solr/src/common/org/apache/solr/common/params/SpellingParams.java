/**
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

package org.apache.solr.common.params;

/**
 * Parameters used for spellchecking
 * 
 * @since solr 1.3
 */
public interface SpellingParams {

  public static final String SPELLCHECK_PREFIX = "spellcheck.";

  /**
   * The name of the dictionary to be used for giving the suggestion for a
   * request. The value for this parameter is configured in solrconfig.xml
   */
  public static final String SPELLCHECK_DICT = SPELLCHECK_PREFIX + "dictionary";

  /**
   * The count of suggestions needed for a given query.
   * <p/>
   * If this parameter is absent in the request then only one suggestion is
   * returned. If it is more than one then a maximum of given suggestions are
   * returned for each token in the query.
   */
  public static final String SPELLCHECK_COUNT = SPELLCHECK_PREFIX + "count";

  /**
   * When this parameter is set to true and the misspelled word exists in the
   * user field, only words that occur more frequently in the Solr field than
   * the one given will be returned. The default value is false.
   * <p/>
   * <b>This is applicable only for dictionaries built from Solr fields.</b>
   */
  public static final String SPELLCHECK_ONLY_MORE_POPULAR = SPELLCHECK_PREFIX + "onlyMorePopular";

  /**
   * Whether to use the extended response format, which is more complicated but
   * richer. Returns the document frequency for each suggestion and returns one
   * suggestion block for each term in the query string. Default is false.
   * <p/>
   * <b>This is applicable only for dictionaries built from Solr fields.</b>
   */
  public static final String SPELLCHECK_EXTENDED_RESULTS = SPELLCHECK_PREFIX + "extendedResults";

  /**
   * Use the value for this parameter as the query to spell check.
   * <p/>
   * This parameter is <b>optional</b>. If absent, then the q parameter is
   * used.
   */
  public static final String SPELLCHECK_Q = SPELLCHECK_PREFIX + "q";

  /**
   * Whether to build the index or not. Optional and false by default.
   */
  public static final String SPELLCHECK_BUILD = SPELLCHECK_PREFIX + "build";

  /**
   * Whether to reload the index. Optional and false by default.
   */
  public static final String SPELLCHECK_RELOAD = SPELLCHECK_PREFIX + "reload";

  /**
   * Take the top suggestion for each token and create a new query from it
   */
  public static final String SPELLCHECK_COLLATE = SPELLCHECK_PREFIX + "collate";
  /**
   * <p>
   * The maximum number of collations to return.  Default=1.  Ignored if "spellcheck.collate" is false.
   * </p>
   */
  public static final String SPELLCHECK_MAX_COLLATIONS = SPELLCHECK_PREFIX + "maxCollations"; 
  /**
   * <p>
   * The maximum number of collations to test by querying against the index.   
   * When testing, the collation is substituted for the original query's "q" param.  Any "qf"s are retained.
   * If this is set to zero, does not test for hits before returning collations (returned collations may result in zero hits).
   * Default=0. Ignored of "spellcheck.collate" is false. 
   * </p>
   */
  public static final String SPELLCHECK_MAX_COLLATION_TRIES = SPELLCHECK_PREFIX + "maxCollationTries";
  
  /**
   * <p>
   * Whether to use the Extended Results Format for collations. 
   * Includes "before>after" pairs to easily allow clients to generate messages like "no results for PORK.  did you mean POLK?"
   * Also indicates the # of hits each collation will return on re-query.  Default=false, which retains 1.4-compatible output.
   * </p>
   */
  public static final String SPELLCHECK_COLLATE_EXTENDED_RESULTS = SPELLCHECK_PREFIX + "collateExtendedResults";
  
  /**
   * Certain spelling implementations may allow for an accuracy setting.
   */
  public static final String SPELLCHECK_ACCURACY = SPELLCHECK_PREFIX + "accuracy";
  
}
