package org.apache.lucene.util.packed;

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
import java.util.BitSet;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BaseDocIdSetTestCase;

public class TestEliasFanoDocIdSet extends BaseDocIdSetTestCase<EliasFanoDocIdSet> {

  @Override
  public EliasFanoDocIdSet copyOf(final BitSet bs, final int numBits) throws IOException {
    final EliasFanoDocIdSet set = new EliasFanoDocIdSet(bs.cardinality(), numBits - 1);
    set.encodeFromDisi(new DocIdSetIterator() {
      int doc = -1;

      @Override
      public int nextDoc() throws IOException {
        doc = bs.nextSetBit(doc + 1);
        if (doc == -1) {
          doc = NO_MORE_DOCS;
        }
        assert doc < numBits;
        return doc;
      }
      
      @Override
      public int docID() {
        return doc;
      }
      
      @Override
      public long cost() {
        return bs.cardinality();
      }
      
      @Override
      public int advance(int target) throws IOException {
        return slowAdvance(target);
      }
    });
    return set;
  }

}