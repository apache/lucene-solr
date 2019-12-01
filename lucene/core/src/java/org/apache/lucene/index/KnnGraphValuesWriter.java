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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.codecs.KnnGraphReader;
import org.apache.lucene.codecs.KnnGraphWriter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HNSWGraph;
import org.apache.lucene.util.hnsw.HNSWGraphBuilder;

/** Buffers up pending vector value(s) and knn graph per doc, then flushes when segment flushes. */
public class KnnGraphValuesWriter implements Accountable {

  private final FieldInfo fieldInfo;
  private final Counter iwBytesUsed;
  private final DocsWithFieldSet docsWithFieldVec;
  private final DocsWithFieldSet docsWithFieldGrp;
  private final IntsRefBuilder docsRef;
  private final List<float[]> buffer;
  private final ByteBuffersDataOutput bufferOut;
  private final int numDimensions;
  private final VectorValues.DistanceFunction distFunc;
  private final HNSWGraphBuilder hnswBuilder;

  private int addedDocs = 0;
  private int lastDocID = -1;

  private long bytesUsed = 0L;

  public KnnGraphValuesWriter(DocumentsWriterPerThread docWriter, FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = docWriter.bytesUsed;
    this.docsWithFieldVec = new DocsWithFieldSet();
    this.docsWithFieldGrp = new DocsWithFieldSet();
    this.docsRef = new IntsRefBuilder();
    this.buffer = new ArrayList<>();
    this.bufferOut = new ByteBuffersDataOutput();
    this.numDimensions = fieldInfo.getVectorNumDimensions();
    this.distFunc = fieldInfo.getVectorDistFunc();
    this.hnswBuilder = new HNSWGraphBuilder(distFunc);
    this.bytesUsed = docsWithFieldGrp.ramBytesUsed() + docsWithFieldGrp.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(hnswBuilder);
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, BytesRef binaryValue) throws IOException {
    if (docID <= lastDocID) {
      throw new IllegalArgumentException("VectorValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (binaryValue == null) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\": null value not allowed");
    }

    // write the vector
    bufferOut.writeBytes(binaryValue.bytes);
    docsWithFieldVec.add(docID);
    docsRef.grow(docID + 1);
    docsRef.setIntAt(docID, addedDocs++);
    docsRef.setLength(docID + 1);
    if (docID > lastDocID + 1) {
      Arrays.fill(docsRef.ints(), lastDocID + 1, docID, -1);
    }

    // write the graph
    buffer.add(VectorValues.decode(binaryValue.bytes, numDimensions));
    if (distFunc != VectorValues.DistanceFunction.NONE) {
      float[] query = VectorValues.decode(binaryValue.bytes, numDimensions);
      hnswBuilder.insert(docID, query, getVectorValues());
      docsWithFieldGrp.add(docID);
    }

    updateBytesUsed();

    lastDocID = docID;
  }

  private void updateBytesUsed() {
    final long newBytesUsed = docsWithFieldVec.ramBytesUsed() + docsWithFieldGrp.ramBytesUsed() +
        bufferOut.ramBytesUsed() +
        RamUsageEstimator.sizeOf(docsRef.ints()) +
        RamUsageEstimator.shallowSizeOf(buffer) +
        RamUsageEstimator.shallowSizeOf(hnswBuilder);
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, KnnGraphWriter graphWriter) throws IOException {
    VectorValues vectors = new BufferedVectorValues(numDimensions, bufferOut.toDataInput(), docsWithFieldVec.iterator());
    KnnGraphValues graph = new BufferedKnnGraphValues(hnswBuilder.build(), docsWithFieldGrp.iterator());

    final VectorValues vectorValues;
    final KnnGraphValues graphValues;
    if (sortMap == null) {
      vectorValues = vectors;
      graphValues = graph;
    } else {
      // TODO
      vectorValues = vectors;
      graphValues = graph;
    }

    graphWriter.writeField(fieldInfo,
        new KnnGraphReader() {
          @Override
          public void checkIntegrity() throws IOException {
          }

          @Override
          public VectorValues getVectorValues(String field) throws IOException {
            return vectorValues;
          }

          @Override
          public KnnGraphValues getGraphValues(String field) throws IOException {
            return graphValues;
          }

          @Override
          public void close() throws IOException {
          }

          @Override
          public long ramBytesUsed() {
            return 0L;
          }
        });
  }

  private VectorValues getVectorValues() {
    return new VectorValues() {
      int docID = -1;
      float[] value = new float[0];

      @Override
      public float[] vectorValue() throws IOException {
        return value;
      }

      @Override
      public boolean seek(int target) throws IOException {
        if (target < 0) {
          throw new IllegalArgumentException("target must be a positive integer: " + target);
        }
        if (target >= KnnGraphValuesWriter.this.docsRef.length()) {
          docID = NO_MORE_DOCS;
          return false;
        }
        docID = target;

        int position = docsRef.ints()[target];
        if (position < 0) {
          throw new IllegalArgumentException("no vector value for doc: " + target);
        }
        value = KnnGraphValuesWriter.this.buffer.get(position);
        return true;
      }

      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int nextDoc() throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long cost() {
        return KnnGraphValuesWriter.this.docsRef.length();
      }
    };
  }

  @Override
  public long ramBytesUsed() {
    return bytesUsed;
  }

  private static class BufferedVectorValues extends VectorValues {

    final DocIdSetIterator docsWithField;
    final DataInput bytesIterator;
    final byte[] value;
    final int numDims;

    BufferedVectorValues(int numDims, DataInput bytesIterator, DocIdSetIterator docsWithField) {
      this.numDims = numDims;
      this.value = new byte[Float.BYTES * numDims];
      this.bytesIterator = bytesIterator;
      this.docsWithField = docsWithField;
    }

    @Override
    public float[] vectorValue() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] binaryValue() throws IOException {
      return value;
    }

    @Override
    public boolean seek(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docID() {
      return docsWithField.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithField.nextDoc();
      if (docID != NO_MORE_DOCS) {
        bytesIterator.readBytes(value, 0, value.length);
      }
      return docID;
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return docsWithField.cost();
    }
  }

  private static class BufferedKnnGraphValues extends KnnGraphValues {

    final HNSWGraph hnswGraph;
    final DocIdSetIterator docsWithField;

    private int maxLevel = -1;

    BufferedKnnGraphValues(HNSWGraph hnsw, DocIdSetIterator docsWithField) {
      this.hnswGraph = hnsw;
      this.docsWithField = docsWithField;
    }

    @Override
    public int getTopLevel() {
      return hnswGraph.topLevel();
    }

    @Override
    public int getMaxLevel() {
      return maxLevel;
    }

    @Override
    public IntsRef getFriends(int level) {
      return hnswGraph.getFriends(level, docID());
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docID() {
      return docsWithField.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docId = docsWithField.nextDoc();
      if (docId != NO_MORE_DOCS) {
        boolean levelFound = false;
        for (int l = hnswGraph.topLevel(); l >= 0; l--) {
          if (hnswGraph.hasFriends(l, docId)) {
            maxLevel = l;
            levelFound = true;
            break;
          }
        }
        assert levelFound;
      }
      return docId;
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return docsWithField.cost();
    }
  }


}
