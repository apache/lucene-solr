package org.apache.lucene.index.values;

/**
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
import java.util.Arrays;

import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ReaderUtil;

/**
 * @lucene.experimental
 */
public class MultiDocValues extends DocValues {

  public static class DocValuesIndex {
    public final static DocValuesIndex[] EMPTY_ARRAY = new DocValuesIndex[0];
    final int start;
    final int length;
    final DocValues docValues;

    public DocValuesIndex(DocValues docValues, int start, int length) {
      this.docValues = docValues;
      this.start = start;
      this.length = length;
    }
  }

  private DocValuesIndex[] docValuesIdx;
  private int[] starts;

  public MultiDocValues() {
    starts = new int[0];
    docValuesIdx = new DocValuesIndex[0];
  }

  public MultiDocValues(DocValuesIndex[] docValuesIdx) {
    reset(docValuesIdx);
  }

  @Override
  public DocValuesEnum getEnum(AttributeSource source) throws IOException {
    return new MultiValuesEnum(docValuesIdx, starts);
  }

  @Override
  public Source load() throws IOException {
    return new MultiSource(docValuesIdx, starts);
  }

  public void close() throws IOException {
    super.close();
  }

  public DocValues reset(DocValuesIndex[] docValuesIdx) {
    int[] start = new int[docValuesIdx.length];
    for (int i = 0; i < docValuesIdx.length; i++) {
      start[i] = docValuesIdx[i].start;
    }
    this.starts = start;
    this.docValuesIdx = docValuesIdx;
    return this;
  }

  public static class DummyDocValues extends DocValues {
    final int maxDoc;
    final Source emptySoruce;

    public DummyDocValues(int maxDoc, ValueType type) {
      this.maxDoc = maxDoc;
      this.emptySoruce = new EmptySource(type);
    }

    @Override
    public DocValuesEnum getEnum(AttributeSource attrSource) throws IOException {
      return emptySoruce.getEnum(attrSource);
    }

    @Override
    public Source load() throws IOException {
      return emptySoruce;
    }

    @Override
    public ValueType type() {
      return emptySoruce.type();
    }

    public void close() throws IOException {
      super.close();
    }

  }

  private static class MultiValuesEnum extends DocValuesEnum {
    private DocValuesIndex[] docValuesIdx;
    private final int maxDoc;
    private int currentStart;
    private int currentMax;
    private int currentDoc = -1;
    private DocValuesEnum currentEnum;
    private final int[] starts;

    public MultiValuesEnum(DocValuesIndex[] docValuesIdx, int[] starts)
        throws IOException {
      super(docValuesIdx[0].docValues.type());
      this.docValuesIdx = docValuesIdx;
      final DocValuesIndex last = docValuesIdx[docValuesIdx.length - 1];
      maxDoc = last.start + last.length;
      final DocValuesIndex idx = docValuesIdx[0];
      currentEnum = idx.docValues.getEnum(this.attributes());
      currentEnum.copyFrom(this);
      intsRef = currentEnum.intsRef;
      currentMax = idx.length;
      currentStart = 0;
      this.starts = starts;
    }

    @Override
    public void close() throws IOException {
      currentEnum.close();
    }

    @Override
    public int advance(int target) throws IOException {
      assert target > currentDoc : "target " + target
          + " must be > than the current doc " + currentDoc;
      int relativeDoc = target - currentStart;
      do {
        if (target >= maxDoc) {// we are beyond max doc
          return currentDoc = NO_MORE_DOCS;
        }
        if (target >= currentMax) {
          final int idx = ReaderUtil.subIndex(target, starts);
          currentEnum.close();
          currentEnum = docValuesIdx[idx].docValues.getEnum();
          currentEnum.copyFrom(this);
          currentStart = docValuesIdx[idx].start;
          currentMax = currentStart + docValuesIdx[idx].length;
          relativeDoc = target - currentStart;
        }
        target = currentMax; // make sure that we advance to the next enum if the current is exhausted

      } while ((relativeDoc = currentEnum.advance(relativeDoc)) == NO_MORE_DOCS);
      return currentDoc = currentStart + relativeDoc;
    }

    @Override
    public int docID() {
      return currentDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(currentDoc + 1);
    }
  }

  private static class MultiSource extends Source {
    private int numDocs = 0;
    private int start = 0;
    private Source current;
    private final int[] starts;
    private final DocValuesIndex[] docValuesIdx;

    public MultiSource(DocValuesIndex[] docValuesIdx, int[] starts) {
      this.docValuesIdx = docValuesIdx;
      this.starts = starts;
      assert docValuesIdx.length != 0;

    }

    public long getInt(int docID) {
      final int doc = ensureSource(docID);
      return current.getInt(doc);
    }

    private final int ensureSource(int docID) {
      if (docID >= start && docID < start+numDocs) {
        return docID - start;
      } else {
        final int idx = ReaderUtil.subIndex(docID, starts);
        assert idx >= 0 && idx < docValuesIdx.length : "idx was " + idx
            + " for doc id: " + docID + " slices : " + Arrays.toString(starts);
        assert docValuesIdx[idx] != null;
        try {
          current = docValuesIdx[idx].docValues.getSource();
          missingValue.copy(current.getMissing());
        } catch (IOException e) {
          throw new RuntimeException("load failed", e); // TODO how should we
          // handle this
        }

        start = docValuesIdx[idx].start;
        numDocs = docValuesIdx[idx].length;
        return docID - start;
      }
    }

    public double getFloat(int docID) {
      final int doc = ensureSource(docID);
      return current.getFloat(doc);
    }

    public BytesRef getBytes(int docID, BytesRef bytesRef) {
      final int doc = ensureSource(docID);
      return current.getBytes(doc, bytesRef);
    }

    @Override
    public DocValuesEnum getEnum(AttributeSource attrSource) throws IOException {
      throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public ValueType type() {
      return docValuesIdx[0].docValues.type();
    }

  }

  private static class EmptySource extends Source {
    private final ValueType type;

    public EmptySource(ValueType type) {
      this.type = type;
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      return this.missingValue.bytesValue;

    }

    @Override
    public double getFloat(int docID) {
      return missingValue.doubleValue;
    }

    @Override
    public long getInt(int docID) {
      return missingValue.longValue;
    }

    @Override
    public DocValuesEnum getEnum(AttributeSource attrSource) throws IOException {
      return DocValuesEnum.emptyEnum(type);
    }

    @Override
    public ValueType type() {
      return type;
    }
  }

  @Override
  public ValueType type() {
    return this.docValuesIdx[0].docValues.type();
  }
}
