package org.apache.lucene.search.spell;


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
 *  SuggestWord, used in suggestSimilar method in SpellChecker class.
 * <p/>
 * Default sort is first by score, then by frequency.
 * 
 *
 */
public final class SuggestWord{
  
  /**
   * Creates a new empty suggestion with null text.
   */
  public SuggestWord() {}
  
  /**
   * the score of the word
   */
  public float score;

  /**
   * The freq of the word
   */
  public int freq;

  /**
   * the suggested word
   */
  public String string;

}