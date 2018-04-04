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
package org.apache.solr.spelling;

public class SpellCheckCorrection {
  private Token original;
  private String originalAsString = null;
  private String correction;
  private int numberOfOccurences;

  public Token getOriginal() {
    return original;
  }
  
  public String getOriginalAsString() {
    if (originalAsString == null && original != null) {
      originalAsString = original.toString();
    }
    return originalAsString;
  }

  public void setOriginal(Token original) {
    this.original = original;
    this.originalAsString = null;
  }

  public String getCorrection() {
    return correction;
  }

  public void setCorrection(String correction) {
    this.correction = correction;
  }

  public int getNumberOfOccurences() {
    return numberOfOccurences;
  }

  public void setNumberOfOccurences(int numberOfOccurences) {
    this.numberOfOccurences = numberOfOccurences;
  }

}
