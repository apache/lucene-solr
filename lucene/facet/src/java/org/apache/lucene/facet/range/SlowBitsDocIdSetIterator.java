package org.apache.lucene.facet.range;

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

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;

/** Wraps a {@link Bits} to make a {@link DocIdSetIterator};
 *  this is typically a slow thing to do and should only be
 *  used as a last resort! */
class SlowBitsDocIdSetIterator extends DocIdSetIterator {
  private final Bits bits;
  private final long cost;
  private final int maxDoc;
  private final Bits acceptDocs;
  private int doc = -1;

  public SlowBitsDocIdSetIterator(Bits bits, long cost, Bits acceptDocs) {
    this.bits = bits;
    this.cost = cost;
    this.maxDoc = bits.length();
    this.acceptDocs = acceptDocs;
  }

  @Override
  public int advance(int target) throws IOException {
    if (target == NO_MORE_DOCS) {
      doc = target;
      return doc;
    }
    doc = target-1;
    return nextDoc();
  }

  @Override
  public int nextDoc() throws IOException {
    assert doc != NO_MORE_DOCS;
    while (true) {
      doc++;
      if (doc == maxDoc) {
        return doc = NO_MORE_DOCS;
      }
      if ((acceptDocs == null || acceptDocs.get(doc)) && bits.get(doc)) {
        return doc;
      }
    }
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public long cost() {
    return cost;
  }
}