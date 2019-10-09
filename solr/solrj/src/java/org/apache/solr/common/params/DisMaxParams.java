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
  
  /** Query and init param for tiebreaker value. */
  public static String TIE = "tie";
  
  /** Query and init param for query fields. */
  public static String QF = "qf";
  
  /** Query and init param for phrase boost fields. */
  public static String PF = "pf";
  
  /** 
   * Query and init param for bigram phrase boost fields.
   * @deprecated
   * This parameter is specific to the Extended DisMax Query Parser and should
   * not be retrieved from here.
   * <p> Use {@link ExtendedDisMaxParams#PF2} instead.
   */
  @Deprecated
  public static String PF2 = "pf2";
  
  /** 
   * Query and init param for trigram phrase boost fields.
   * @deprecated
   * This parameter is specific to the Extended DisMax Query Parser and should
   * not be retrieved from here.
   * <p> Use {@link ExtendedDisMaxParams#PF3} instead.
   */
  @Deprecated
  public static String PF3 = "pf3";
  
  /** Query and init param for MinShouldMatch specification. */
  public static String MM = "mm";

  /**
   * Query and init param for MinShouldMatch specification.
   * <p>
   * If set to true, will try to reduce MM if tokens are removed from some clauses but not all.
   * </p>
   */
  public static String MM_AUTORELAX = "mm.autoRelax";

  /**
   * Query and init param for Phrase Slop value in phrase boost query (in pf fields).
   */
  public static String PS = "ps";
  
  /**
   * Default phrase slop for bigram phrases (pf2).
   * @deprecated
   * This parameter is specific to the Extended DisMax Query Parser and should
   * not be retrieved from here.
   * <p> Use {@link ExtendedDisMaxParams#PS2} instead.
   */
  @Deprecated
  public static String PS2 = "ps2";
  
  /**
   * Default phrase slop for trigram phrases (pf3).
   * @deprecated
   * This parameter is specific to the Extended DisMax Query Parser and should
   * not be retrieved from here.
   * <p> Use {@link ExtendedDisMaxParams#PS3} instead.
   */
  @Deprecated
  public static String PS3 = "ps3";
    
  /**
   * Query and init param for phrase Slop value in phrases
   * explicitly included in the user's query string (in qf fields).
   */
  public static String QS = "qs";
  
  /** Query and init param for boosting query. */
  public static String BQ = "bq";
  
  /** Query and init param for boosting functions. */
  public static String BF = "bf";
  
  /**
   * Alternate query (expressed in Solr QuerySyntax)
   * to use if main query (q) is empty.
   */
  public static String ALTQ = "q.alt";
  
  /** Query and init param for field list. */
  public static String GEN = "gen";
}
