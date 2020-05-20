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
 * A collection of params used in ExtendedDismaxQueryParser,
 * both for Plugin initialization and for Requests.
 */
public interface ExtendedDisMaxParams extends DisMaxParams {
  
  /**
   * Query and init param for bigram phrase boost fields.
   */
  public static String PF2 = "pf2";
  
  /**
   * Query and init param for trigram phrase boost fields.
   */
  public static String PF3 = "pf3";

  /**
   * Default phrase slop for bigram phrases (pf2).
   */
  public static String PS2 = "ps2";
  
  /**
   * Default phrase slop for trigram phrases (pf3).
   */
  public static String PS3 = "ps3";

  /**
   * User fields. The fields that can be used by the end user to create field-specific queries.
   */
  public static final String UF = "uf";
    
  /**
   * Lowercase Operators. If set to true, 'or' and 'and' will be considered OR and AND, otherwise
   * lowercase operators will be considered terms to search for.
   */
  public static final String LOWERCASE_OPS = "lowercaseOperators";

  /**
   * Multiplicative boost. Boost functions which scores are going to be multiplied to the score
   * of the main query (instead of just added, like with bf)
   */
  public static final String MULT_BOOST = "boost";

  /**
   * If set to true, stopwords are removed from the query.
   */
  public static final String STOPWORDS = "stopwords";
}
