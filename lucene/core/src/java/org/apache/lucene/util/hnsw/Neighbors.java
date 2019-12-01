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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.util.PriorityQueue;

/** Abstract neighbors queue; subclasses should implement the priority function
 * {@link PriorityQueue#lessThan(Object, Object)} */
public abstract class Neighbors extends PriorityQueue<Neighbor> {

  protected Neighbors(int maxSize) {
    this(maxSize, null);
  }

  protected Neighbors(int maxSize, Neighbor seed) {
    super(maxSize);
    if (seed != null) {
      add(seed);
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Neighbors=[");
    this.iterator().forEachRemaining(sb::append);
    sb.append("]");
    return sb.toString();
  }

}
