package org.apache.lucene.analysis.ko.morph;

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
 * 복합명사의 개별단어에 대한 정보를 담고있는 클래스 
 */
public class CompoundEntry {
  
  private String word;
  
  private int offset = -1;
  
  private boolean exist = true;
  
  private char pos = PatternConstants.POS_NOUN;
  
  public CompoundEntry() {
    
  }
  
  public CompoundEntry(String w) {
    this.word = w;
  }
  
  public CompoundEntry(String w,int o) {
    this(w);
    this.offset = o;    
  }
  
  public CompoundEntry(String w,int o, boolean is) {
    this(w,o);
    this.exist = is;    
  }
  
  public CompoundEntry(String w,int o, boolean is, char p) {
    this(w,o,is);
    this.pos = p;
  }
  
  public void setWord(String w) {
    this.word = w;
  }
  
  public void setOffset(int o) {
    this.offset = o;
  }
  
  public String getWord() {
    return this.word;
  }
  
  public int getOffset() {
    return this.offset;
  }
  
  public boolean isExist() {
    return exist;
  }
  
  public void setExist(boolean is) {
    this.exist = is;
  }
  
  public char getPos() {
    return pos;
  }

  public void setPos(char pos) {
    this.pos = pos;
  }  
}
