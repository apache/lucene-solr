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

package org.apache.solr.search;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

/**
 * A {@link org.apache.lucene.search.DocIdSetIterator} which allows single-segment access to a wrapped top-level (multi-segment) DISI.
 *
 * Useful as a shim between a top-level data structure, and logic that attempts to operate on it using segment-level docIds.
 * The same wrapped DISI can be used serially by multiple PerSegmentViewDocIdSetIterators
 */
public class PerSegmentViewDocIdSetIterator extends DocIdSetIterator {

  private final DocIdSetIterator disi;
  private final int docBase;
  private int nextDocBase;
  //TODO Should I have some notion of where the segment ends, so that other DISI's being lined up here aren't comparing docs across segments (which will never match)

  public PerSegmentViewDocIdSetIterator(DocIdSetIterator topLevelIterator, int docBase, int nextDocBase) {
    this.disi = topLevelIterator;
    this.docBase = docBase;
    this.nextDocBase = nextDocBase;
  }

  @Override
  public int docID() {
    return translateTopLevelDocIdToPerSegment(disi.docID());
  }

  @Override
  public int nextDoc() throws IOException {
    return translateTopLevelDocIdToPerSegment(disi.nextDoc());
  }

  @Override
  public int advance(int perSegmentTarget) throws IOException {
    if (perSegmentTarget == NO_MORE_DOCS) {
      return NO_MORE_DOCS;
    }

    final int topLevelTarget = perSegmentTarget + docBase;
    if (topLevelTarget >= nextDocBase) {
      return NO_MORE_DOCS; // There may be other docs in subsequent segments, but since this view only covers one segment...
    }
    return translateTopLevelDocIdToPerSegment(disi.advance(topLevelTarget));
  }

  private int translateTopLevelDocIdToPerSegment(int topLevelDocId) {
    if (topLevelDocId == NO_MORE_DOCS) {
      return NO_MORE_DOCS;
    }
    if (topLevelDocId >= NO_MORE_DOCS) {
      return NO_MORE_DOCS;
    }
    return topLevelDocId - docBase;
  }

  @Override
  public long cost() {
    return disi.cost();
  }
}
