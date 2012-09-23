package org.apache.lucene.analysis.ja.dict;

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
 * Dictionary interface for retrieving morphological data
 * by id.
 */
public interface Dictionary {
  
  public static final String INTERNAL_SEPARATOR = "\u0000";
  
  /**
   * Get left id of specified word
   * @return left id
   */
  public int getLeftId(int wordId);
  
  /**
   * Get right id of specified word
   * @return right id
   */
  public int getRightId(int wordId);
  
  /**
   * Get word cost of specified word
   * @return word's cost
   */
  public int getWordCost(int wordId);
  
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
  public String getReading(int wordId, char surface[], int off, int len);
  
  /**
   * Get base form of word
   * @param wordId word ID of token
   * @return Base form (only different for inflected words, otherwise null)
   */
  public String getBaseForm(int wordId, char surface[], int off, int len);
  
  /**
   * Get pronunciation of tokens
   * @param wordId word ID of token
   * @return Pronunciation of the token
   */
  public String getPronunciation(int wordId, char surface[], int off, int len);
  
  /**
   * Get inflection type of tokens
   * @param wordId word ID of token
   * @return inflection type, or null
   */
  public String getInflectionType(int wordId);
  
  /**
   * Get inflection form of tokens
   * @param wordId word ID of token
   * @return inflection form, or null
   */
  public String getInflectionForm(int wordId);
  // TODO: maybe we should have a optimal method, a non-typesafe
  // 'getAdditionalData' if other dictionaries like unidic have additional data
}
