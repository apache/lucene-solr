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
package org.apache.solr.search;
/**
 * A class to hold "phrase slop" and "boost" parameters for pf, pf2, pf3 parameters
 **/
public class FieldParams {
  private final int wordGrams;  // make bigrams if 2, trigrams if 3, or all if 0
  private final int slop;
  private final float boost;
  private final String field;
  public FieldParams(String field, int wordGrams, int slop, float boost) {
    this.wordGrams = wordGrams;
    this.slop      = slop;
    this.boost     = boost;
    this.field     = field;
  }
  public int getWordGrams() {
    return wordGrams;
  }
  public int getSlop() {
    return slop;
  }
  public float getBoost() {
    return boost;
  }
  public String getField() {
    return field;
  }
  
}
