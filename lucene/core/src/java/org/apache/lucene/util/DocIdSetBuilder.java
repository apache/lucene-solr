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
package org.apache.lucene.util;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.packed.PackedInts;

/**
 * A builder of {@link DocIdSet}s.  At first it uses a sparse structure to gather
 * documents, and then upgrades to a non-sparse bit set once enough hits match.
 *
 * To add documents, you first need to call {@link #grow} in order to reserve
 * space, and then call {@link BulkAdder#add(int)} on the returned
 * {@link BulkAdder}.
 *
 * @lucene.internal
 */
public final class DocIdSetBuilder {

  /** Utility class to efficiently add many docs in one go.
   *  @see DocIdSetBuilder#grow */
  public static abstract class BulkAdder {
    public abstract void add(int doc);
  }

  private static class FixedBitSetAdder extends BulkAdder {
    final FixedBitSet bitSet;

    FixedBitSetAdder(FixedBitSet bitSet) {
      this.bitSet = bitSet;
    }

    @Override
    public void add(int doc) {
      bitSet.set(doc);
    }
  }

  private class BufferAdder extends BulkAdder {

    @Override
    public void add(int doc) {
      buffer[bufferSize++] = doc;
    }

  }

  private final int maxDoc;
  private final int threshold;
  // pkg-private for testing
  final boolean multivalued;
  final double numValuesPerDoc;

  private int[] buffer;
  private int bufferSize;

  private FixedBitSet bitSet;

  private long counter = -1;
  private BulkAdder adder = new BufferAdder();

  /**
   * Create a builder that can contain doc IDs between {@code 0} and {@code maxDoc}.
   */
  public DocIdSetBuilder(int maxDoc) {
    this(maxDoc, -1, -1);
  }

  /** Create a {@link DocIdSetBuilder} instance that is optimized for
   *  accumulating docs that match the given {@link Terms}. */
  public DocIdSetBuilder(int maxDoc, Terms terms) throws IOException {
    this(maxDoc, terms.getDocCount(), terms.getSumDocFreq());
  }

  /** Create a {@link DocIdSetBuilder} instance that is optimized for
   *  accumulating docs that match the given {@link PointValues}. */
  public DocIdSetBuilder(int maxDoc, PointValues values, String field) throws IOException {
    this(maxDoc, values.getDocCount(field), values.size(field));
  }

  DocIdSetBuilder(int maxDoc, int docCount, long valueCount) {
    this.maxDoc = maxDoc;
    this.multivalued = docCount < 0 || docCount != valueCount;
    this.numValuesPerDoc = (docCount < 0 || valueCount < 0)
        // assume one value per doc, this means the cost will be overestimated
        // if the docs are actually multi-valued
        ? 1
        // otherwise compute from index stats
        : (double) valueCount / docCount;
    assert numValuesPerDoc >= 1;

    // For ridiculously small sets, we'll just use a sorted int[]
    // maxDoc >>> 7 is a good value if you want to save memory, lower values
    // such as maxDoc >>> 11 should provide faster building but at the expense
    // of using a full bitset even for quite sparse data
    this.threshold = maxDoc >>> 7;

    this.buffer = new int[0];
    this.bufferSize = 0;
    this.bitSet = null;
  }

  private void upgradeToBitSet() {
    assert bitSet == null;
    bitSet = new FixedBitSet(maxDoc);
    for (int i = 0; i < bufferSize; ++i) {
      bitSet.set(buffer[i]);
    }
    counter = this.bufferSize;
    this.buffer = null;
    this.bufferSize = 0;
    this.adder = new FixedBitSetAdder(bitSet);
  }

  /** Grows the buffer to at least minSize, but never larger than threshold. */
  private void growBuffer(int minSize) {
    assert minSize < threshold;
    if (buffer.length < minSize) {
      int nextSize = Math.min(threshold, ArrayUtil.oversize(minSize, Integer.BYTES));
      buffer = Arrays.copyOf(buffer, nextSize);
    }
  }

  /**
   * Add the content of the provided {@link DocIdSetIterator} to this builder.
   * NOTE: if you need to build a {@link DocIdSet} out of a single
   * {@link DocIdSetIterator}, you should rather use {@link RoaringDocIdSet.Builder}.
   */
  public void add(DocIdSetIterator iter) throws IOException {
    grow((int) Math.min(Integer.MAX_VALUE, iter.cost()));

    if (bitSet != null) {
      bitSet.or(iter);
    } else {
      while (true) {
        assert buffer.length <= threshold;
        final int end = buffer.length;
        for (int i = bufferSize; i < end; ++i) {
          final int doc = iter.nextDoc();
          if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            bufferSize = i;
            return;
          }
          buffer[bufferSize++] = doc;
        }
        bufferSize = end;

        if (bufferSize + 1 >= threshold) {
          break;
        }

        growBuffer(bufferSize+1);
      }

      upgradeToBitSet();
      for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
        bitSet.set(doc);
      }
    }
  }

  /**
   * Reserve space and return a {@link BulkAdder} object that can be used to
   * add up to {@code numDocs} documents.
   */
  public BulkAdder grow(int numDocs) {
    if (bitSet == null) {
      final long newLength = (long) bufferSize + numDocs;
      if (newLength < threshold) {
        growBuffer((int) newLength);
      } else {
        upgradeToBitSet();
        counter += numDocs;
      }
    } else {
      counter += numDocs;
    }
    return adder;
  }

  private static int dedup(int[] arr, int length) {
    if (length == 0) {
      return 0;
    }
    int l = 1;
    int previous = arr[0];
    for (int i = 1; i < length; ++i) {
      final int value = arr[i];
      assert value >= previous;
      if (value != previous) {
        arr[l++] = value;
        previous = value;
      }
    }
    return l;
  }

  private static boolean noDups(int[] a, int len) {
    for (int i = 1; i < len; ++i) {
      assert a[i-1] < a[i];
    }
    return true;
  }

  /**
   * Build a {@link DocIdSet} from the accumulated doc IDs.
   */
  public DocIdSet build() {
    try {
      if (bitSet != null) {
        assert counter >= 0;
        final long cost = Math.round(counter / numValuesPerDoc);
        return new BitDocIdSet(bitSet, cost);
      } else {
        LSBRadixSorter sorter = new LSBRadixSorter();
        sorter.sort(PackedInts.bitsRequired(maxDoc - 1), buffer, bufferSize);
        final int l;
        if (multivalued) {
          l = dedup(buffer, bufferSize);
        } else {
          assert noDups(buffer, bufferSize);
          l = bufferSize;
        }
        assert l <= bufferSize;
        buffer = ArrayUtil.grow(buffer, l + 1);
        buffer[l] = DocIdSetIterator.NO_MORE_DOCS;
        return new IntArrayDocIdSet(buffer, l);
      }
    } finally {
      this.buffer = null;
      this.bufferSize = 0;
      this.bitSet = null;
    }
  }

}
