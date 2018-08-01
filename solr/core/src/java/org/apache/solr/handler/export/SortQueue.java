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

package org.apache.solr.handler.export;

class SortQueue extends PriorityQueue<SortDoc> {

  private SortDoc proto;
  private Object[] cache;

  public SortQueue(int len, SortDoc proto) {
    super(len);
    this.proto = proto;
  }

  protected boolean lessThan(SortDoc t1, SortDoc t2) {
    return t1.lessThan(t2);
  }

  protected void populate() {
    Object[] heap = getHeapArray();
    cache = new SortDoc[heap.length];
    for (int i = 1; i < heap.length; i++) {
      cache[i] = heap[i] = proto.copy();
    }
    size = maxSize;
  }

  protected void reset() {
    Object[] heap = getHeapArray();
    if(cache != null) {
      System.arraycopy(cache, 1, heap, 1, heap.length-1);
      size = maxSize;
    } else {
      populate();
    }
  }
}