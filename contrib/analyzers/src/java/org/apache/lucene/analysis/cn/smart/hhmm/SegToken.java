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

package org.apache.lucene.analysis.cn.smart.hhmm;

/**
 * SmartChineseAnalyzer internal token
 */
public class SegToken {
  public char[] charArray;

  public int startOffset;

  public int endOffset;

  public int wordType;

  public int weight;

  public int index;

  public SegToken(String word, int start, int end, int wordType, int weight) {
    this.charArray = word.toCharArray();
    this.startOffset = start;
    this.endOffset = end;
    this.wordType = wordType;
    this.weight = weight;
  }

  public SegToken(char[] idArray, int start, int end, int wordType, int weight) {
    this.charArray = idArray;
    this.startOffset = start;
    this.endOffset = end;
    this.wordType = wordType;
    this.weight = weight;
  }

  // public String toString() {
  // return String.valueOf(charArray) + "/s(" + startOffset + ")e("
  // + endOffset + ")/w(" + weight + ")t(" + wordType + ")";
  // }

  // public boolean equals(RawToken t) {
  // return this.startOffset == t.startOffset
  // && this.endOffset == t.endOffset;
  // }
}
