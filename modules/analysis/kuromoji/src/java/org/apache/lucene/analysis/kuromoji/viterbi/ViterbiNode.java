package org.apache.lucene.analysis.kuromoji.viterbi;

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

public final class ViterbiNode {
  public enum Type {
    KNOWN,
    UNKNOWN,
    USER
  }
  
  private final int wordId;
  
  private final char[] surfaceForm;
  private final int offset;
  private final int length;
  
  private final int leftId;
  
  private final int rightId;
  
  /** word cost for this node */
  private final int wordCost;
  
  /** minimum path cost found thus far */
  private int pathCost;
  
  private ViterbiNode leftNode;
  
  private final Type type;
  
  private final int startIndex;
  
  public ViterbiNode(int wordId, char[] surfaceForm, int offset, int length, int leftId, int rightId, int wordCost, int startIndex, Type type) {
    this.wordId = wordId;
    this.surfaceForm = surfaceForm;
    this.offset = offset;
    this.length = length;
    this.leftId = leftId;
    this.rightId = rightId;
    this.wordCost = wordCost;
    this.startIndex = startIndex;
    this.type = type;
  }
  
  
  /**
   * @return the wordId
   */
  public int getWordId() {
    return wordId;
  }
  
  /**
   * @return the surfaceForm
   */
  public char[] getSurfaceForm() {
    return surfaceForm;
  }
  
  /**
   * @return start offset into surfaceForm
   */
  public int getOffset() {
    return offset;
  }
  
  /**
   * @return length of surfaceForm
   */
  public int getLength() {
    return length;
  }
  
  /**
   * @return the surfaceForm as a String
   */
  public String getSurfaceFormString() {
    return new String(surfaceForm, offset, length);
  }
  
  /**
   * @return the leftId
   */
  public int getLeftId() {
    return leftId;
  }
  
  /**
   * @return the rightId
   */
  public int getRightId() {
    return rightId;
  }
  
  /**
   * @return the cost
   */
  public int getWordCost() {
    return wordCost;
  }
  
  /**
   * @return the cost
   */
  public int getPathCost() {
    return pathCost;
  }
  
  /**
   * param cost minimum path cost found this far
   */
  public void setPathCost(int pathCost) {
    this.pathCost = pathCost;
  }
  
  public void setLeftNode(ViterbiNode node) {
    leftNode = node;
  }
  
  public ViterbiNode getLeftNode() {
    return leftNode;
  }
  
  public int getStartIndex() {
    return startIndex;
  }
  
  public Type getType() {
    return type;
  }
}
