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
 * A pair of tokens in {@link SegGraph}
 */
public class SegTokenPair {

  public char[] charArray;

  /**
   * index of the first token in {@link SegGraph}
   */
  public int from;

  /**
   * index of the second token in {@link SegGraph}
   */
  public int to;

  public double weight;

  public SegTokenPair(char[] idArray, int from, int to, double weight) {
    this.charArray = idArray;
    this.from = from;
    this.to = to;
    this.weight = weight;
  }

  // public String toString() {
  // return String.valueOf(charArray) + ":f(" + from + ")t(" + to + "):"
  // + weight;
  // }

  // public boolean equals(SegTokenPair tp) {
  // return this.from == tp.from && this.to == tp.to;
  // }

}
