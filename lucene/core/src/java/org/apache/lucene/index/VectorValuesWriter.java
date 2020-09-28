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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.codecs.VectorWriter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

/** Buffers up pending vector value(s) per doc, then flushes when segment flushes. */
public class VectorValuesWriter {

  private final FieldInfo fieldInfo;
  private final Counter iwBytesUsed;
  private final List<float[]> vectors = new ArrayList<>();
  private final DocsWithFieldSet docsWithField;

  private int lastDocID = -1;

  private long bytesUsed;

  VectorValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    this.docsWithField = new DocsWithFieldSet();
    this.bytesUsed = docsWithField.ramBytesUsed();
    if (iwBytesUsed != null) {
      iwBytesUsed.addAndGet(bytesUsed);
    }
  }

  public void addValue(int docID, float[] vectorValue) {
    if (docID == lastDocID) {
      throw new IllegalArgumentException("VectorValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    assert docID > lastDocID;
    docsWithField.add(docID);
    vectors.add(Arrays.copyOf(vectorValue, vectorValue.length));
    updateBytesUsed();
    lastDocID = docID;
  }

  private void updateBytesUsed() {
    final long newBytesUsed = docsWithField.ramBytesUsed()
            + vectors.size() * 5 // pointer plus array overhead for each array??
            + vectors.size() * vectors.get(0).length * Float.BYTES;
    if (iwBytesUsed != null) {
      iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    }
    bytesUsed = newBytesUsed;
  }

  public void flush(Sorter.DocMap sortMap, VectorWriter graphWriter) throws IOException {
    VectorValues vectorValues = new BufferedVectorValues(docsWithField, vectors, fieldInfo.getVectorDimension(), fieldInfo.getVectorScoreFunction());
    if (sortMap != null) {
      // TODO sorted index
      throw new UnsupportedOperationException();
    }
    graphWriter.writeField(fieldInfo, vectorValues);
  }

  private static class BufferedVectorValues extends VectorValues {

    final DocsWithFieldSet docsWithField;

    // These are always the vectors of a VectorValuesWriter, which are copied when added to it
    final List<float[]> vectors;
    final VectorValues.ScoreFunction scoreFunction;
    final int dimension;

    final ByteBuffer buffer;

    DocIdSetIterator docsWithFieldIter;
    int bufferPos = 0;
    float[] value;

    BufferedVectorValues(DocsWithFieldSet docsWithField, List<float[]> vectorsArray, int dimension, VectorValues.ScoreFunction scoreFunction) {
      this.docsWithField = docsWithField;
      this.vectors = vectorsArray;
      this.dimension = dimension;
      this.scoreFunction = scoreFunction;
      buffer = ByteBuffer.allocate(dimension * Float.BYTES);
      docsWithFieldIter = docsWithField.iterator();
    }

    @Override
    public BufferedVectorValues copy() {
      return new BufferedVectorValues(docsWithField, vectors, dimension, scoreFunction);
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public int size() {
      return vectors.size();
    }

    @Override
    public VectorValues.ScoreFunction scoreFunction() {
      return scoreFunction;
    }

    @Override
    public BytesRef binaryValue() {
      buffer.asFloatBuffer().put(value);
      return new BytesRef(buffer.array());
    }

    @Override
    public float[] vectorValue() {
      return value;
    }

    @Override
    public float[] vectorValue(int targetOrd) {
      return vectors.get(targetOrd);
    }

    @Override
    public int docID() {
      return docsWithFieldIter.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithFieldIter.nextDoc();
      if (docID != NO_MORE_DOCS) {
        value = vectors.get(bufferPos++);
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return docsWithFieldIter.cost();
    }

    @Override
    public TopDocs search(float[] target, int k, int fanout) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
