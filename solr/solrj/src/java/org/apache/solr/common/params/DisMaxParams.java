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
package org.apache.solr.common.params;

    

/**
 * A collection of params used in DisMaxRequestHandler,
 * both for Plugin initialization and for Requests.
 */
public interface DisMaxParams {
  
  /** query and init param for tiebreaker value */
  public static String TIE = "tie";
  
  /** query and init param for query fields */
  public static String QF = "qf";
  
  /** query and init param for phrase boost fields */
  public static String PF = "pf";
  
  /** query and init param for bigram phrase boost fields */
  public static String PF2 = "pf2";
  
  /** query and init param for trigram phrase boost fields */
  public static String PF3 = "pf3";
  
  /** query and init param for MinShouldMatch specification */
  public static String MM = "mm";

  /**
   * If set to true, will try to reduce MM if tokens are removed from some clauses but not all
   */
  public static String MM_AUTORELAX = "mm.autoRelax";

  /**
   * query and init param for Phrase Slop value in phrase
   * boost query (in pf fields)
   */
  public static String PS = "ps";
  
  /** default phrase slop for bigram phrases (pf2)  */
  public static String PS2 = "ps2";
  
  /** default phrase slop for bigram phrases (pf3)  */
  public static String PS3 = "ps3";
    
  /**
   * query and init param for phrase Slop value in phrases
   * explicitly included in the user's query string ( in qf fields)
   */
  public static String QS = "qs";
  
  /** query and init param for boosting query */
  public static String BQ = "bq";
  
  /** query and init param for boosting functions */
  public static String BF = "bf";
  
  /**
   * Alternate query (expressed in Solr QuerySyntax)
   * to use if main query (q) is empty
   */
  public static String ALTQ = "q.alt";
  
  /** query and init param for field list */
  public static String GEN = "gen";

  /**
   * User fields. The fields that can be used by the end user to create field-specific queries.
   */
  public static String UF = "uf";
    
  /**
   * Lowercase Operators. If set to true, 'or' and 'and' will be considered OR and AND, otherwise
   * lowercase operators will be considered terms to search for.
   */
  public static String LOWERCASE_OPS = "lowercaseOperators";

  /**
   * Multiplicative boost. Boost functions which scores are going to be multiplied to the score
   * of the main query (instead of just added, like with bf)
   */
  public static String MULT_BOOST = "boost";

  /**
   * If set to true, stopwords are removed from the query.
   */
  public static String STOPWORDS = "stopwords";
}
