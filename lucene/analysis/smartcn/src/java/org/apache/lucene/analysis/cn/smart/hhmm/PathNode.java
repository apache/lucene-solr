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

/**
 * SmartChineseAnalyzer internal node representation
 *
 * <p>Used by {@link BiSegGraph} to maximize the segmentation with the Viterbi algorithm.
 *
 * @lucene.experimental
 */
class PathNode implements Comparable<PathNode> {
  public double weight;

  public int preNode;

  @Override
  public int compareTo(PathNode pn) {
    if (weight < pn.weight) return -1;
    else if (weight == pn.weight) return 0;
    else return 1;
  }

  /** @see java.lang.Object#hashCode() */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + preNode;
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
    PathNode other = (PathNode) obj;
    if (preNode != other.preNode) return false;
    if (Double.doubleToLongBits(weight) != Double.doubleToLongBits(other.weight)) return false;
    return true;
  }
}
