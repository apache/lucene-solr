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

/** Neighbors queue. */
public abstract class Neighbors extends PriorityQueue<Neighbor> {

  public static Neighbors create(int maxSize, boolean reversed) {
    if (reversed) {
      return new ReverseNeighbors(maxSize);
    } else {
      return new ForwardNeighbors(maxSize);
    }
  }

  public abstract boolean reversed();

  private Neighbors(int maxSize) {
    super(maxSize);
  }

  private static class ForwardNeighbors extends Neighbors {
    ForwardNeighbors(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean lessThan(Neighbor a, Neighbor b) {
      if (a.score == b.score) {
        return a.node > b.node;
      }
      return a.score < b.score;
    }

    @Override
    public boolean reversed() { return false; }
  }

  private static class ReverseNeighbors extends Neighbors {
    ReverseNeighbors(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean lessThan(Neighbor a, Neighbor b) {
      if (a.score == b.score) {
        return a.node > b.node;
      }
      return b.score < a.score;
    }

    @Override
    public boolean reversed() { return true; }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Neighbors=[");
    this.iterator().forEachRemaining(sb::append);
    sb.append("]");
    return sb.toString();
  }

}
