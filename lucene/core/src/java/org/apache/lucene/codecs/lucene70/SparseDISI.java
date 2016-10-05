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
package org.apache.lucene.codecs.lucene70;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;

final class SparseDISI extends DocIdSetIterator {

  static void writeBitSet(DocIdSetIterator it, int maxDoc, IndexOutput out) throws IOException {
    int currentIndex = 0;
    long currentBits = 0;
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      final int index = doc >>> 6;
      if (index > currentIndex) {
        out.writeLong(currentBits);
        for (int i = currentIndex + 1; i < index; ++i) {
          out.writeLong(0L);
        }
        currentIndex = index;
        currentBits = 0L;
      }
      currentBits |= 1L << doc;
    }

    out.writeLong(currentBits);
    final int maxIndex = (maxDoc - 1) >>> 6;
    for (int i = currentIndex + 1; i <= maxIndex; ++i) {
      out.writeLong(0L);
    }
  }

  final int maxDoc;
  final int numWords;
  final long cost;
  final RandomAccessInput slice;
  int doc = -1;
  int wordIndex = -1;
  long word;
  int index = -1;

  SparseDISI(int maxDoc, IndexInput in, long offset, long cost) throws IOException {
    this.maxDoc = maxDoc;
    this.numWords = (int) ((maxDoc + 63L) >>> 6);
    this.slice = in.randomAccessSlice(offset, numWords * 8L);
    this.cost = cost;
  }

  @Override
  public int advance(int target) throws IOException {
    if (target >= maxDoc) {
      return doc = NO_MORE_DOCS;
    }

    final int targetWordIndex = target >>> 6;
    for (int i = wordIndex + 1; i <= targetWordIndex; ++i) {
      word = slice.readLong(i << 3);
      index += Long.bitCount(word);
    }
    wordIndex = targetWordIndex;

    long leftBits = word >>> target;
    if (leftBits != 0L) {
      return doc = target + Long.numberOfTrailingZeros(leftBits);
    }

    while (++wordIndex < numWords) {
      word = slice.readLong(wordIndex << 3);
      if (word != 0) {
        index += Long.bitCount(word);
        return doc = (wordIndex << 6) + Long.numberOfTrailingZeros(word);
      }
    }

    return doc = NO_MORE_DOCS;
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(doc + 1);
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public long cost() {
    return cost;
  }

  public int index() {
    return index - Long.bitCount(word >>> doc) + 1;
  }

}
