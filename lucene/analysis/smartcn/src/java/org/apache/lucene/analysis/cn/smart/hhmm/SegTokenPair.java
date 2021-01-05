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

/**
 * A pair of tokens in {@link SegGraph}
 *
 * @lucene.experimental
 */
class SegTokenPair {

  public char[] charArray;

  /** index of the first token in {@link SegGraph} */
  public int from;

  /** index of the second token in {@link SegGraph} */
  public int to;

  public double weight;

  public SegTokenPair(char[] idArray, int from, int to, double weight) {
    this.charArray = idArray;
    this.from = from;
    this.to = to;
    this.weight = weight;
  }

  /** @see java.lang.Object#hashCode() */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    for (int i = 0; i < charArray.length; i++) {
      result = prime * result + charArray[i];
    }
    result = prime * result + from;
    result = prime * result + to;
    long temp;
    temp = Double.doubleToLongBits(weight);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  /** @see java.lang.Object#equals(java.lang.Object) */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    SegTokenPair other = (SegTokenPair) obj;
    if (!Arrays.equals(charArray, other.charArray)) return false;
    if (from != other.from) return false;
    if (to != other.to) return false;
    if (Double.doubleToLongBits(weight) != Double.doubleToLongBits(other.weight)) return false;
    return true;
  }
}
