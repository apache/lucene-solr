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
import org.apache.solr.search.DocIterator;
import org.apache.lucene.util.PriorityQueue;

final class UnionDocIterator extends SweepDocIterator {

  private final int maxIdx;
  private final SubIterStruct baseSub;
  private boolean collectBase;
  private final PriorityQueue<SubIterStruct> queue;
  private SubIterStruct top;
  private int docId = -1;

  private static final class SubIterStruct {
    private final DocIterator sub;
    private final int index;
    private int docId;
    public SubIterStruct(DocIterator sub, int index) throws IOException {
      this.sub = sub;
      this.index = index;
      nextDoc();
    }
    public void nextDoc() {
      docId = sub.hasNext() ? sub.nextDoc() : DocIdSetIterator.NO_MORE_DOCS;
    }
  }
  UnionDocIterator(DocIterator[] subIterators, int baseIdx) throws IOException {
    super(subIterators.length);
    this.maxIdx = size - 1;
    queue = new PriorityQueue<SubIterStruct>(size) {
      @Override
      protected boolean lessThan(SubIterStruct a, SubIterStruct b) {
        return a.docId < b.docId;
      }
    };
    SubIterStruct tmpBase = null;
    int i = maxIdx;
    do {
      SubIterStruct subIterStruct = new SubIterStruct(subIterators[i], i);
      queue.add(subIterStruct);
      if (i == baseIdx) {
        tmpBase = subIterStruct;
      }
    } while (i-- > 0);
    this.baseSub = tmpBase;
    top = queue.top();
  }

  @Override
  public int nextDoc() {
    if (top.docId == docId) {
      do {
        top.nextDoc();
      } while ((top = queue.updateTop()).docId == docId);
    }
    collectBase = false;
    return docId = top.docId;
  }

  @Override
  public boolean hasNext() {
    if (top.docId == docId) {
      do {
        top.nextDoc();
      } while ((top = queue.updateTop()).docId == docId);
    }
    return top.docId != DocIdSetIterator.NO_MORE_DOCS;
  }

  @Override
  public boolean collectBase() {
    assert top.docId != docId : "must call registerCounts() before collectBase()";
    return collectBase;
  }

  @Override
  public int registerCounts(SegCounter segCounts) {
    int i = -1;
    do {
      if (!collectBase && top == baseSub) {
        collectBase = true;
      }
      segCounts.map(top.index, ++i);
      top.nextDoc();
    } while ((top = queue.updateTop()).docId == docId);
    return i;
  }
}
