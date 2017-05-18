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

package org.apache.lucene.queries.mlt.terms;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class has the responsibility of storing the term frequencies count for all the terms in each document field.
 * It is an auxiliary data structure used for the Lucene More Like This
 */
public class DocumentTermFrequencies {
  private Map<String,FieldTermFrequencies > perFieldTermFrequencies;

  public DocumentTermFrequencies() {
    perFieldTermFrequencies = new HashMap<>();
  }

  public FieldTermFrequencies get(String fieldName){
    FieldTermFrequencies requestedTermFrequencies = perFieldTermFrequencies.get(fieldName);
    if(requestedTermFrequencies == null){
      requestedTermFrequencies = new FieldTermFrequencies(fieldName);
      perFieldTermFrequencies.put(fieldName,requestedTermFrequencies);
    }
    return requestedTermFrequencies;
  }

  public Collection<FieldTermFrequencies> getAll(){
    return perFieldTermFrequencies.values();
  }

  public void increment(String fieldName, String term, int frequency) {
    FieldTermFrequencies fieldTermFrequencies = this.get(fieldName);
    fieldTermFrequencies.incrementFrequency(term,frequency);
  }

  public class FieldTermFrequencies{
    private String fieldName;
    private Map<String, Int> perTermFrequency;

    public FieldTermFrequencies(String fieldName) {
      this.fieldName = fieldName;
      this.perTermFrequency = new HashMap<>();
    }

    public Int get(String term){
      return perTermFrequency.get(term);
    }

    private void incrementFrequency(String term, int frequency){
      Int freqWrapper = perTermFrequency.get(term);
      if (freqWrapper == null) {
        freqWrapper = new Int();
        perTermFrequency.put(term, freqWrapper);
        freqWrapper.frequency = frequency;
      } else {
        freqWrapper.frequency+=frequency;
      }
    }

    public Set<Map.Entry<String, Int>> getAll(){
      return perTermFrequency.entrySet();
    }

    public Collection<Int> getAllFrequencies(){
      return perTermFrequency.values();
    }

    public int size(){
      return perTermFrequency.size();
    }

    public String getFieldName() {
      return fieldName;
    }

    public void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }

  }

  /**
   * Use for frequencies and to avoid renewing Integers.
   */
  static class Int {
    int frequency;
    Int() {
      frequency = 1;
    }
  }

}
