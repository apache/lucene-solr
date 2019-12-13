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

import org.apache.lucene.codecs.KnnGraphReader;
import org.apache.lucene.codecs.KnnGraphWriter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.HNSWGraph;
import org.apache.lucene.util.hnsw.HNSWGraphWriter;

/** Buffers up pending vector value(s) and knn graph per doc, then flushes when segment flushes. */
public class KnnGraphValuesWriter implements Accountable {

  private final FieldInfo fieldInfo;
  private final Counter iwBytesUsed;
  private final DocsWithFieldSet docsWithFieldVec;
  private final DocsWithFieldSet docsWithFieldGrp;
  private final HNSWGraphWriter hnswGraphWriter;

  private int lastDocID = -1;

  private long bytesUsed = 0L;

  public KnnGraphValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    this.docsWithFieldVec = new DocsWithFieldSet();
    this.docsWithFieldGrp = new DocsWithFieldSet();
    this.hnswGraphWriter = new HNSWGraphWriter(fieldInfo.getVectorNumDimensions(), fieldInfo.getVectorDistFunc());
    this.bytesUsed = docsWithFieldGrp.ramBytesUsed() + docsWithFieldGrp.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(hnswGraphWriter);
    if (iwBytesUsed != null) {
      iwBytesUsed.addAndGet(bytesUsed);
    }
  }

  public void addValue(int docID, BytesRef binaryValue) throws IOException {
    if (docID <= lastDocID) {
      throw new IllegalArgumentException("VectorValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (binaryValue == null) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\": null value not allowed");
    }

    hnswGraphWriter.insert(docID, binaryValue);
    docsWithFieldVec.add(docID);
    docsWithFieldGrp.add(docID);

    updateBytesUsed();

    lastDocID = docID;
  }

  private void updateBytesUsed() {
    final long newBytesUsed = docsWithFieldVec.ramBytesUsed() + docsWithFieldGrp.ramBytesUsed() + hnswGraphWriter.ramBytesUsed();
    if (iwBytesUsed != null) {
      iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    }
    bytesUsed = newBytesUsed;
  }

  public void flush(Sorter.DocMap sortMap, KnnGraphWriter graphWriter) throws IOException {
    hnswGraphWriter.finish();
    VectorValues vectors = new BufferedVectorValues(docsWithFieldVec.iterator(), hnswGraphWriter.rawVectorsArray());
    KnnGraphValues graph = new BufferedKnnGraphValues(docsWithFieldGrp.iterator(), hnswGraphWriter.hnswGraph());

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

  @Override
  public long ramBytesUsed() {
    return bytesUsed;
  }

  private static class BufferedVectorValues extends VectorValues {

    final DocIdSetIterator docsWithField;
    final float[][] vectorsArray;

    int bufferPos = 0;
    float[] value;

    BufferedVectorValues(DocIdSetIterator docsWithField, float[][] vectorsArray) {
      this.docsWithField = docsWithField;
      this.vectorsArray = vectorsArray;
    }

    @Override
    public float[] vectorValue() throws IOException {
      return value;
    }

    @Override
    public int docID() {
      return docsWithField.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithField.nextDoc();
      if (docID != NO_MORE_DOCS) {
        value = vectorsArray[bufferPos++];
      }
      return docID;
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean seek(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return docsWithField.cost();
    }
  }

  private static class BufferedKnnGraphValues extends KnnGraphValues {

    final DocIdSetIterator docsWithField;
    final HNSWGraph hnswGraph;

    private int maxLevel = -1;

    BufferedKnnGraphValues(DocIdSetIterator docsWithField, HNSWGraph hnsw) {
      this.docsWithField = docsWithField;
      this.hnswGraph = hnsw;
    }

    @Override
    public int getTopLevel() {
      return hnswGraph.topLevel();
    }

    @Override
    public int[] getEnterPoints() {
      return hnswGraph.getEnterPoints().stream().mapToInt(Integer::intValue).toArray();
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
        for (int l = hnswGraph.topLevel(); l > 0; l--) {
          if (hnswGraph.hasFriends(l, docId)) {
            maxLevel = l;
            return docId;
          }
        }
        // A node in a singleton graph has no friends
        maxLevel = 0;
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
