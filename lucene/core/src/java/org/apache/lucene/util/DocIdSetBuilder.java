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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

  private static class Buffer {
    int[] array;
    int length;

    Buffer(int length) {
      this.array = new int[length];
      this.length = 0;
    }

    Buffer(int[] array, int length) {
      this.array = array;
      this.length = length;
    }
  }

  private static class BufferAdder extends BulkAdder {
    final Buffer buffer;

    BufferAdder(Buffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public void add(int doc) {
      buffer.array[buffer.length++] = doc;
    }
  }

  private final int maxDoc;
  private final int threshold;
  // pkg-private for testing
  final boolean multivalued;
  final double numValuesPerDoc;

  private List<Buffer> buffers = new ArrayList<>();
  private int totalAllocated; // accumulated size of the allocated buffers

  private FixedBitSet bitSet;

  private long counter = -1;
  private BulkAdder adder;

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
    if (docCount <= 0 || valueCount < 0) {
      // assume one value per doc, this means the cost will be overestimated
      // if the docs are actually multi-valued
      this.numValuesPerDoc = 1;
    } else {
      // otherwise compute from index stats
      this.numValuesPerDoc = (double) valueCount / docCount;
    }

    assert numValuesPerDoc >= 1: "valueCount=" + valueCount + " docCount=" + docCount;

    // For ridiculously small sets, we'll just use a sorted int[]
    // maxDoc >>> 7 is a good value if you want to save memory, lower values
    // such as maxDoc >>> 11 should provide faster building but at the expense
    // of using a full bitset even for quite sparse data
    this.threshold = maxDoc >>> 7;

    this.bitSet = null;
  }

  /**
   * Add the content of the provided {@link DocIdSetIterator} to this builder.
   * NOTE: if you need to build a {@link DocIdSet} out of a single
   * {@link DocIdSetIterator}, you should rather use {@link RoaringDocIdSet.Builder}.
   */
  public void add(DocIdSetIterator iter) throws IOException {
    if (bitSet != null) {
      bitSet.or(iter);
      return;
    }
    int cost = (int) Math.min(Integer.MAX_VALUE, iter.cost());
    BulkAdder adder = grow(cost);
    for (int i = 0; i < cost; ++i) {
      int doc = iter.nextDoc();
      if (doc == DocIdSetIterator.NO_MORE_DOCS) {
        return;
      }
      adder.add(doc);
    }
    for (int doc = iter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.nextDoc()) {
      grow(1).add(doc);
    }
  }

  /**
   * Reserve space and return a {@link BulkAdder} object that can be used to
   * add up to {@code numDocs} documents.
   */
  public BulkAdder grow(int numDocs) {
    if (bitSet == null) {
      if ((long) totalAllocated + numDocs <= threshold) {
        ensureBufferCapacity(numDocs);
      } else {
        upgradeToBitSet();
        counter += numDocs;
      }
    } else {
      counter += numDocs;
    }
    return adder;
  }

  private void ensureBufferCapacity(int numDocs) {
    if (buffers.isEmpty()) {
      addBuffer(additionalCapacity(numDocs));
      return;
    }

    Buffer current = buffers.get(buffers.size() - 1);
    if (current.array.length - current.length >= numDocs) {
      // current buffer is large enough
      return;
    }
    if (current.length < current.array.length - (current.array.length >>> 3)) {
      // current buffer is less than 7/8 full, resize rather than waste space
      growBuffer(current, additionalCapacity(numDocs));
    } else {
      addBuffer(additionalCapacity(numDocs));
    }
  }

  private int additionalCapacity(int numDocs) {
    // exponential growth: the new array has a size equal to the sum of what
    // has been allocated so far
    int c = totalAllocated;
    // but is also >= numDocs + 1 so that we can store the next batch of docs
    // (plus an empty slot so that we are more likely to reuse the array in build())
    c = Math.max(numDocs + 1, c);
    // avoid cold starts
    c = Math.max(32, c);
    // do not go beyond the threshold
    c = Math.min(threshold - totalAllocated, c);
    return c;
  }

  private Buffer addBuffer(int len) {
    Buffer buffer = new Buffer(len);
    buffers.add(buffer);
    adder = new BufferAdder(buffer);
    totalAllocated += buffer.array.length;
    return buffer;
  }

  private void growBuffer(Buffer buffer, int additionalCapacity) {
    buffer.array = Arrays.copyOf(buffer.array, buffer.array.length + additionalCapacity);
    totalAllocated += additionalCapacity;
  }

  private void upgradeToBitSet() {
    assert bitSet == null;
    FixedBitSet bitSet = new FixedBitSet(maxDoc);
    long counter = 0;
    for (Buffer buffer : buffers) {
      int[] array = buffer.array;
      int length = buffer.length;
      counter += length;
      for (int i = 0; i < length; ++i) {
        bitSet.set(array[i]);
      }
    }
    this.bitSet = bitSet;
    this.counter = counter;
    this.buffers = null;
    this.adder = new FixedBitSetAdder(bitSet);
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
        Buffer concatenated = concat(buffers);
        LSBRadixSorter sorter = new LSBRadixSorter();
        sorter.sort(PackedInts.bitsRequired(maxDoc - 1), concatenated.array, concatenated.length);
        final int l;
        if (multivalued) {
          l = dedup(concatenated.array, concatenated.length);
        } else {
          assert noDups(concatenated.array, concatenated.length);
          l = concatenated.length;
        }
        assert l <= concatenated.length;
        concatenated.array[l] = DocIdSetIterator.NO_MORE_DOCS;
        return new IntArrayDocIdSet(concatenated.array, l);
      }
    } finally {
      this.buffers = null;
      this.bitSet = null;
    }
  }

  /**
   * Concatenate the buffers in any order, leaving at least one empty slot in
   * the end
   * NOTE: this method might reuse one of the arrays
   */
  private static Buffer concat(List<Buffer> buffers) {
    int totalLength = 0;
    Buffer largestBuffer = null;
    for (Buffer buffer : buffers) {
      totalLength += buffer.length;
      if (largestBuffer == null || buffer.array.length > largestBuffer.array.length) {
        largestBuffer = buffer;
      }
    }
    if (largestBuffer == null) {
      return new Buffer(1);
    }
    int[] docs = largestBuffer.array;
    if (docs.length < totalLength + 1) {
      docs = Arrays.copyOf(docs, totalLength + 1);
    }
    totalLength = largestBuffer.length;
    for (Buffer buffer : buffers) {
      if (buffer != largestBuffer) {
        System.arraycopy(buffer.array, 0, docs, totalLength, buffer.length);
        totalLength += buffer.length;
      }
    }
    return new Buffer(docs, totalLength);
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

}
