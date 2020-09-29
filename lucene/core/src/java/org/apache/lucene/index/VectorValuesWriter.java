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
import java.util.List;

import org.apache.lucene.codecs.VectorWriter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.ArrayUtil;
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

  /**
   * Adds a value for the given document. Only a single value may be added.
   * @param docID the value is added to this document
   * @param vectorValue the value to add
   * @throws IllegalArgumentException if a value has already been added to the given document
   */
  public void addValue(int docID, float[] vectorValue) {
    if (docID == lastDocID) {
      throw new IllegalArgumentException("VectorValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed per field)");
    }
    assert docID > lastDocID;
    docsWithField.add(docID);
    vectors.add(ArrayUtil.copyOfSubArray(vectorValue, 0, vectorValue.length));
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

  /**
   * Flush this field's values to storage, sorting the values in accordance with sortMap
   * @param sortMap specifies the order of documents being flushed, or null if they are to be flushed in docid order
   * @param vectorWriter the Codec's vector writer that handles the actual encoding and I/O
   * @throws IOException if there is an error writing the field and its values
   */
  public void flush(Sorter.DocMap sortMap, VectorWriter vectorWriter) throws IOException {
    VectorValues vectorValues = new BufferedVectorValues(docsWithField, vectors, fieldInfo.getVectorDimension(), fieldInfo.getVectorScoreFunction());
    if (sortMap != null) {
      vectorWriter.writeField(fieldInfo, new SortingVectorValues(vectorValues, sortMap));
    } else {
      vectorWriter.writeField(fieldInfo, vectorValues);
    }
  }

  private static class SortingVectorValues extends VectorValues {

    private final VectorValues delegate;
    private final VectorValues.RandomAccess randomAccess;
    private final int[] offsets;
    private int docId = -1;

    SortingVectorValues(VectorValues delegate, Sorter.DocMap sortMap) throws IOException {
      this.delegate = delegate;
      randomAccess = delegate.randomAccess();
      offsets = new int[sortMap.size()];
      int offset = 1; // 0 means no values for this document
      int docID;
      while ((docID = delegate.nextDoc()) != NO_MORE_DOCS) {
        int newDocID = sortMap.oldToNew(docID);
        offsets[newDocID] = offset++;
      }
    }

    @Override
    public int docID() {
      return docId;
    }

    @Override
    public int nextDoc() throws IOException {
      while (docId < offsets.length - 1) {
        ++docId;
        if (offsets[docId] != 0) {
          return docId;
        }
      }
      docId = NO_MORE_DOCS;
      return docId;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
      int oldOffset = offsets[docId] - 1;
      return randomAccess.binaryValue(oldOffset);
    }

    @Override
    public float[] vectorValue() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int dimension() {
      return delegate.dimension();
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public ScoreFunction scoreFunction() {
      return delegate.scoreFunction();
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return size();
    }

    @Override
    public RandomAccess randomAccess() {
      throw new UnsupportedOperationException();
    }
  }

  private static class BufferedVectorValues extends VectorValues implements VectorValues.RandomAccess {

    final DocsWithFieldSet docsWithField;

    // These are always the vectors of a VectorValuesWriter, which are copied when added to it
    final List<float[]> vectors;
    final VectorValues.ScoreFunction scoreFunction;
    final int dimension;

    final ByteBuffer buffer;
    final BytesRef binaryValue;

    DocIdSetIterator docsWithFieldIter;
    int bufferPos = 0;
    float[] value;

    BufferedVectorValues(DocsWithFieldSet docsWithField, List<float[]> vectorsArray, int dimension, VectorValues.ScoreFunction scoreFunction) {
      this.docsWithField = docsWithField;
      this.vectors = vectorsArray;
      this.dimension = dimension;
      this.scoreFunction = scoreFunction;
      buffer = ByteBuffer.allocate(dimension * Float.BYTES);
      binaryValue = new BytesRef(buffer.array());
      docsWithFieldIter = docsWithField.iterator();
    }

    @Override
    public RandomAccess randomAccess() {
      return this;
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
      return binaryValue;
    }

    @Override
    public BytesRef binaryValue(int targetOrd) {
      // Note: this will overwrite any value returned by binaryValue(), but in our private usage these never conflict
      buffer.asFloatBuffer().put(vectors.get(targetOrd));
      return binaryValue;
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
