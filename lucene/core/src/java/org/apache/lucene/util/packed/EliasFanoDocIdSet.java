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

package org.apache.lucene.util.packed;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;


/** A DocIdSet in Elias-Fano encoding.
 * @lucene.internal
 */
public class EliasFanoDocIdSet extends DocIdSet {
  final EliasFanoEncoder efEncoder;
  /*
   * Construct an EliasFanoDocIdSet.
   * @param numValues The number of values that can be encoded.
   * @param upperBound  At least the highest value that will be encoded.
   */
  public EliasFanoDocIdSet(int numValues, int upperBound) {
    efEncoder = new EliasFanoEncoder(numValues, upperBound);
  }

  public void encodeFromDisi(DocIdSetIterator disi) throws IOException {
    while (efEncoder.numEncoded < efEncoder.numValues) {
      int x = disi.nextDoc();
      if (x == DocIdSetIterator.NO_MORE_DOCS) {
        throw new IllegalArgumentException("disi: " + disi.toString()
            + "\nhas " + efEncoder.numEncoded
            + " docs, but at least " + efEncoder.numValues + " are required.");
      }
      efEncoder.encodeNext(x);
    }
  }

  /**
   * Provides a {@link DocIdSetIterator} to access encoded document ids.
   */
  @Override
  public DocIdSetIterator iterator() {
    if (efEncoder.lastEncoded >= DocIdSetIterator.NO_MORE_DOCS) {
      throw new UnsupportedOperationException(
          "Highest encoded value too high for DocIdSetIterator.NO_MORE_DOCS: " + efEncoder.lastEncoded);
    }
    return new DocIdSetIterator() {
      private int curDocId = -1;
      private final EliasFanoDecoder efDecoder = efEncoder.getDecoder();

      @Override
      public int docID() {
        return curDocId;
      }

      private int setCurDocID(long nextValue) {
        curDocId = (nextValue == EliasFanoDecoder.NO_MORE_VALUES)
            ?  NO_MORE_DOCS
                : (int) nextValue;
        return curDocId;
      }

      @Override
      public int nextDoc() {
        return setCurDocID(efDecoder.nextValue());
      }

      @Override
      public int advance(int target) {
        return setCurDocID(efDecoder.advanceToValue(target));
      }

      @Override
      public long cost() {
        return efDecoder.numEncoded;
      }
    };
  }

  /** This DocIdSet implementation is cacheable. @return <code>true</code> */
  @Override
  public boolean isCacheable() {
    return true;
  }

  @Override
  public boolean equals(Object other) {
    return ((other instanceof EliasFanoDocIdSet))
        && efEncoder.equals(((EliasFanoDocIdSet) other).efEncoder);
  }

  @Override
  public int hashCode() {
    return efEncoder.hashCode() ^ getClass().hashCode();
  }
}

