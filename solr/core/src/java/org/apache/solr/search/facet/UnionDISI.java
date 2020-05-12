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
package org.apache.solr.search.facet;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.search.facet.FacetFieldProcessorByArrayDV.SegCountGlobal;
import org.apache.solr.search.facet.FacetFieldProcessorByArrayDV.SegCountPerSeg;

final class UnionDISI extends SweepDISI {

  final int maxIdx;
  private final SubIterStruct baseSub;
  private boolean collectBase;
  private final PriorityQueue<SubIterStruct> queue;
  private SubIterStruct top;
  private int docId = -1;

  private static final class SubIterStruct {
    private final DocIdSetIterator sub;
    private final int index;
    private int docId;
    public SubIterStruct(DocIdSetIterator sub, int index) throws IOException {
      this.sub = sub;
      this.index = index;
      nextDoc();
    }
    public void nextDoc() throws IOException {
      docId = sub.nextDoc();
    }
  }
  UnionDISI(DocIdSetIterator[] subIterators, CountSlotAcc[] countAccs, int size, int baseIdx) throws IOException {
    super(size, countAccs);
    this.maxIdx = size - 1;
    queue = new PriorityQueue<SubIterStruct>(size) {
      @Override
      protected boolean lessThan(SubIterStruct a, SubIterStruct b) {
        return a.docId < b.docId;
      }
    };
    int i = maxIdx;
    SubIterStruct tmpBaseSub = null;
    do {
      final SubIterStruct subIterStruct = new SubIterStruct(subIterators[i], i);
      queue.add(subIterStruct);
      if (i == baseIdx) {
        tmpBaseSub = subIterStruct;
      }
    } while (i-- > 0);
    baseSub = tmpBaseSub;
    top = queue.top();
  }

  @Override
  public int nextDoc() throws IOException {
    if (top.docId == docId) {
      do {
        top.nextDoc();
      } while ((top = queue.updateTop()).docId == docId);
    }
    if (baseSub != null) {
      collectBase = false;
    }
    return docId = top.docId;
  }

  @Override
  public boolean collectBase() {
    assert top.docId != docId : "must call registerCounts() before collectBase()";
    return collectBase;
  }

  // nocommit: even if there are reasons why SweepCountAware should define 2 distinct registerCounts methods
  // nocommit: since the impls here are idenitcal, and only depend on the common SegCounter.map method, we
  // nocommit: might as well refactor these identical impls to just be stubs that call a new
  // nocommit: 'private int _registerCounts(SegCounter)'
  
  
  @Override
  public int registerCounts(SegCountGlobal segCounter) throws IOException {
    int i = -1;
    do {
      if (!collectBase && top == baseSub) {
        collectBase = true;
      }
      segCounter.map(top.index, ++i);
      top.nextDoc();
    } while ((top = queue.updateTop()).docId == docId);
    return i;
  }

  @Override
  public int registerCounts(SegCountPerSeg segCounter) throws IOException {
    int i = -1;
    do {
      if (!collectBase && top == baseSub) {
        collectBase = true;
      }
      segCounter.map(top.index, ++i);
      top.nextDoc();
    } while ((top = queue.updateTop()).docId == docId);
    return i;
  }

}
