package org.apache.lucene.analysis.ko;

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
 * Index word extracted from a phrase.
 */
public class IndexWord {

  // the word to be indexed
  private String word;
  
  // the offset from the start of input text
  private int offset = 0;

  // when the input text is a chinese text, the korean sound text of it is extracted as a index word. 
  private int increment = 1;
  
  // the derived korean sound text has the <KOREAN> type
  private String type;

  public IndexWord() {
    
  }
  
  public IndexWord(String word, int pos) {
    this.word = word;
    this.offset = pos;
  }
  
  public IndexWord(String word, int pos, int inc) {
    this(word, pos);
    this.increment = inc;
  }
  
  public IndexWord(String word, int pos, int inc, String t) {
    this(word, pos, inc);
    this.type = t;
  }
  
  public String getWord() {
    return word;
  }

  public void setWord(String word) {
    this.word = word;
  }
  
  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }
  
  public int getIncrement() {
    return increment;
  }

  public void setIncrement(int increment) {
    this.increment = increment;
  }
    
  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  } 
}
