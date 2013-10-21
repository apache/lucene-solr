package org.apache.lucene.analysis.ko.dic;

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
  private final String word;
  private final boolean exist;

  public CompoundEntry(String word, boolean exist) {
    this.word = word;
    this.exist = exist;    
  }
  
  public String getWord() {
    return word;
  }
  
  public boolean isExist() {
    return exist;
  }
}
