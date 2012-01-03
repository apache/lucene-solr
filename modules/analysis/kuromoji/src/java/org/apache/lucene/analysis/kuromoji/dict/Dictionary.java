package org.apache.lucene.analysis.kuromoji.dict;

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

public interface Dictionary {
  
  public static final String INTERNAL_SEPARATOR = "\u0000";
  
  /**
   * Get left id of specified word
   * @param wordId
   * @return	left id
   */
  public int getLeftId(int wordId);
  
  /**
   * Get right id of specified word
   * @param wordId
   * @return	left id
   */
  public int getRightId(int wordId);
  
  /**
   * Get word cost of specified word
   * @param wordId
   * @return	left id
   */
  public int getWordCost(int wordId);
  
  /**
   * Get all features of tokens
   * @param wordId word ID of token
   * @return All features of the token
   */
  public String getAllFeatures(int wordId);
  
  /**
   * Get all features as array
   * @param wordId word ID of token
   * @return Array containing all features of the token
   */
  public String[] getAllFeaturesArray(int wordId);
  
  /**
   * Get Part-Of-Speech of tokens
   * @param wordId word ID of token
   * @return Part-Of-Speech of the token
   */
  public String getPartOfSpeech(int wordId);
  
  /**
   * Get reading of tokens
   * @param wordId word ID of token
   * @return Reading of the token
   */
  public String getReading(int wordId);
  
  /**
   * Get feature(s) of tokens
   * @param wordId word ID token
   * @param fields array of index. If this is empty, return all features.
   * @return Features of the token
   */
  public String getFeature(int wordId, int... fields);
}
