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
package org.apache.lucene.analysis.cn.smart.hhmm;

import java.util.Arrays;

import org.apache.lucene.analysis.cn.smart.WordType; // for javadocs

/**
 * SmartChineseAnalyzer internal token
 * @lucene.experimental
 */
public class SegToken {
  /**
   * Character array containing token text
   */
  public char[] charArray;

  /**
   * start offset into original sentence
   */
  public int startOffset;

  /**
   * end offset into original sentence
   */
  public int endOffset;

  /**
   * {@link WordType} of the text 
   */
  public int wordType;

  /**
   * word frequency
   */
  public int weight;

  /**
   * during segmentation, this is used to store the index of the token in the token list table
   */
  public int index;

  /**
   * Create a new SegToken from a character array.
   * 
   * @param idArray character array containing text
   * @param start start offset of SegToken in original sentence
   * @param end end offset of SegToken in original sentence
   * @param wordType {@link WordType} of the text
   * @param weight word frequency
   */
  public SegToken(char[] idArray, int start, int end, int wordType, int weight) {
    this.charArray = idArray;
    this.startOffset = start;
    this.endOffset = end;
    this.wordType = wordType;
    this.weight = weight;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    for(int i=0;i<charArray.length;i++) {
      result = prime * result + charArray[i];
    }
    result = prime * result + endOffset;
    result = prime * result + index;
    result = prime * result + startOffset;
    result = prime * result + weight;
    result = prime * result + wordType;
    return result;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SegToken other = (SegToken) obj;
    if (!Arrays.equals(charArray, other.charArray))
      return false;
    if (endOffset != other.endOffset)
      return false;
    if (index != other.index)
      return false;
    if (startOffset != other.startOffset)
      return false;
    if (weight != other.weight)
      return false;
    if (wordType != other.wordType)
      return false;
    return true;
  }

}
