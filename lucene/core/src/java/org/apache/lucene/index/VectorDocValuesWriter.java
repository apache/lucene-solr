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
package org.apache.lucene.index;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Writes float arrays using the same methods as BinaryDocValuesWriter while exposing its internal
 * buffer for random access reads. */
public class VectorDocValuesWriter extends DocValuesWriter {

  /** Maximum length for a vector field. */
  private static final int MAX_LENGTH = ArrayUtil.MAX_ARRAY_LENGTH;

  // Number of float vector values per block:
  private final static int BLOCK_SIZE = 8192;

  private final List<ByteBuffer> buffers;
  private final Counter iwBytesUsed;
  private final PackedLongValues.Builder lengths;
  private final FieldInfo fieldInfo;
  private final int dimension;
  private final int bufferCapacity; // how many documents in a buffer
  private final DocsWithFieldSet docsWithField;

  // We record all docids having vectors so we can get their index with a binary search
  // nocommit Can we find a more efficient data structure for this map. We need a random access data
  // structure in order to find the vector for a given docid, so we can perform KNN graph searches
  // while adding docs. We *also* need to be able to iterate over docids with values.
  private int[] docBufferMap;
  private FloatBuffer currentBuffer;
  
  private long bytesUsed;
  private int lastDocID = -1;

  public VectorDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    dimension = getDimensionFromAttribute(fieldInfo);
    bufferCapacity = BLOCK_SIZE / dimension / 4;
    //System.out.println("dimension=" + dimension + " buffer capacity=" + bufferCapacity);
    buffers = new ArrayList<>();
    docBufferMap = new int[bufferCapacity];
    allocateNewBuffer();
    lengths = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    docsWithField = new DocsWithFieldSet();
    updateBytesUsed();
  }

  public static int getDimensionFromAttribute(FieldInfo fieldInfo) {
    String dimStr = fieldInfo.getAttribute(VectorDocValues.DIMENSION_ATTR);
    if (dimStr == null) {
      // TODO: make this impossible?
      throw new IllegalStateException("DocValues type for vector field '" + fieldInfo.name + "' was indexed without a dimension.");
    }
    int dim = Integer.valueOf(dimStr);
    if (dim > MAX_LENGTH) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" has dimension " + dim + ", which exceeds the maximum: " + MAX_LENGTH);
    }
    return dim;
  }

  public void addValue(int docID, float[] value) {
    if (docID <= lastDocID) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (value == null) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length != dimension) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" has the wrong dimension: " + value.length + ". It must match its type whose dimension is " + dimension);
    }
    ensureCapacity();
    // TODO: all the lengths are the same: can we just write out some degenerate PackedLongValues?
    lengths.add(value.length);
    currentBuffer.put(value);
    updateBytesUsed();
    docBufferMap[docsWithField.cost()] = docID;
    docsWithField.add(docID);
    lastDocID = docID;
  }

  private void ensureCapacity() {
    if (currentBuffer.hasRemaining() == false) {
      allocateNewBuffer();
    }
  }

  private void allocateNewBuffer() {
    buffers.add(ByteBuffer.allocate(4 * dimension * (BLOCK_SIZE / 4 / dimension)));
    currentBuffer = buffers.get(buffers.size() - 1).asFloatBuffer();
    docBufferMap = ArrayUtil.grow(docBufferMap, buffers.size() * bufferCapacity);
  }

  private void updateBytesUsed() {
    final long newBytesUsed =
      lengths.ramBytesUsed()
      + (dimension * 4 * BLOCK_SIZE * buffers.size())
      + docsWithField.ramBytesUsed()
      + RamUsageEstimator.sizeOf(docBufferMap);
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  public void finish(int maxDoc) {
  }

  @Override
  Sorter.DocComparator getDocComparator(int numDoc, SortField sortField) throws IOException {
    throw new IllegalArgumentException("It is forbidden to sort on a binary field");
  }

  private SortingLeafReader.CachedBinaryDVs sortDocValues(int maxDoc, Sorter.DocMap sortMap, BinaryDocValues oldValues) throws IOException {
    FixedBitSet docsWithField = new FixedBitSet(maxDoc);
    BytesRef[] values = new BytesRef[maxDoc];
    while (true) {
      int docID = oldValues.nextDoc();
      if (docID == NO_MORE_DOCS) {
        break;
      }
      int newDocID = sortMap.oldToNew(docID);
      docsWithField.set(newDocID);
      values[newDocID] = BytesRef.deepCopyOf(oldValues.binaryValue());
    }
    return new SortingLeafReader.CachedBinaryDVs(values, docsWithField);
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    final PackedLongValues lengths = this.lengths.build();
    final SortingLeafReader.CachedBinaryDVs sorted;
    if (sortMap != null) {
      sorted = sortDocValues(state.segmentInfo.maxDoc(), sortMap, new BufferedBinaryDocValues());
    } else {
      sorted = null;
    }
    dvConsumer.addBinaryField(fieldInfo,
                              new EmptyDocValuesProducer() {
                                @Override
                                public BinaryDocValues getBinary(FieldInfo fieldInfoIn) {
                                  if (fieldInfoIn != fieldInfo) {
                                    throw new IllegalArgumentException("wrong fieldInfo");
                                  }
                                  if (sorted == null) {
                                    return new BufferedBinaryDocValues();
                                  } else {
                                    return new SortingLeafReader.SortingBinaryDocValues(sorted);
                                  }
                                }
                              });
  }

  public VectorDocValues getBufferedValues() {
    // nocommit avoid the extra bytes copy by writing a BufferedVectorDocValues with a FloatBuffer wrapping the byte buffers
    return VectorDocValues.get(new BufferedBinaryDocValues(), dimension);
  }

  // Iterates over the values as bytes in forward order. Also supports random access positioning via advanceExact.
  private class BufferedBinaryDocValues extends BinaryDocValues {
    final private int stride;
    final private BytesRefBuilder value;

    private int currentDocIndex;
    private int docId;
    
    BufferedBinaryDocValues() {
      value = new BytesRefBuilder();
      stride = dimension * 4;
      value.grow(stride);
      value.setLength(stride);
      currentDocIndex = -1;
      docId = -1;
    }

    @Override
    public int docID() {
      return docId;
    }

    @Override
    public int nextDoc() throws IOException {
      if (++currentDocIndex < docsWithField.cost()) {
        copyVector();
        docId = docBufferMap[currentDocIndex];
      } else {
        docId = NO_MORE_DOCS;
      }
      return docId;
    }

    private void copyVector() {
      ByteBuffer buffer = buffers.get(currentDocIndex / bufferCapacity);
      buffer.position((currentDocIndex % bufferCapacity) * stride);
      buffer.get(value.bytes(), 0, stride);
    }

    @Override
    public int advance(int target) {
      int idx = Arrays.binarySearch(docBufferMap, 0, docsWithField.cost(), target);
      if (idx < 0) {
        idx = -1 - idx;
        if (idx >= docBufferMap.length) {
          currentDocIndex = NO_MORE_DOCS;
          docId = NO_MORE_DOCS;
          return NO_MORE_DOCS;
        }
      }
      currentDocIndex = idx;
      copyVector();
      docId = docBufferMap[currentDocIndex];
      return docId;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      int idx = Arrays.binarySearch(docBufferMap, 0, docsWithField.cost(), target);
      if (idx < 0) {
        return false;
      } else {
        currentDocIndex = idx;
        copyVector();
        docId = target;
        return true;
      }
    }

    @Override
    public long cost() {
      return docsWithField.cost();
    }

    @Override
    public BytesRef binaryValue() {
      return value.get();
    }
  }

  @Override
  DocIdSetIterator getDocIdSet() {
    return docsWithField.iterator();
  }
}
