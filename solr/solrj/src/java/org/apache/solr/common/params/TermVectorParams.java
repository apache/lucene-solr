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
 *
 *
 **/
public interface TermVectorParams {

  public static final String TV_PREFIX = "tv.";

    /**
  * Return Term Frequency info
  * */
  public static final String TF =  TV_PREFIX + "tf";
  /**
  * Return Term Vector position information
  *
  * */
  public static final String POSITIONS = TV_PREFIX + "positions";
  /**
  * Return Term Vector payloads information
  *
  * */
  public static final String PAYLOADS = TV_PREFIX + "payloads";
  /**
  * Return offset information, if available
  * */
  public static final String OFFSETS = TV_PREFIX + "offsets";
  /**
  * Return IDF information.  May be expensive
  * */
  public static final String DF = TV_PREFIX + "df";

  /**
   * Return TF-IDF calculation, i.e. (tf / idf).  May be expensive.
   */
  public static final String TF_IDF = TV_PREFIX + "tf_idf";


  /**
   * Return all the options: TF, positions, offsets, idf
   */
  public static final String ALL = TV_PREFIX + "all";

  /**
   * The fields to get term vectors for
   */
  public static final String FIELDS = TV_PREFIX + "fl";

  /**
   * The Doc Ids (Lucene internal ids) of the docs to get the term vectors for
   */
  public static final String DOC_IDS = TV_PREFIX + "docIds";
}
