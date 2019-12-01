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

final class ImmutableNeighbor extends Neighbor {
  final int docId;
  final float distance;

  ImmutableNeighbor(int docId, float distance) {
    this.docId = docId;
    this.distance = distance;
  }

  @Override
  public int docId() {
    return docId;
  }

  @Override
  public float distance() {
    return distance;
  }

  @Override
  public boolean isDeferred() {
    return false;
  }

  @Override
  public int compareTo(Neighbor o) {
    if (this.distance() == o.distance() && this.docId() == o.docId()) {
      return 0;
    }
    if (this.distance() == o.distance()) {
      return this.docId() < o.docId() ? -1 : 1;
    }
    return this.distance() < o.distance() ? -1 : 1;
  }

}
