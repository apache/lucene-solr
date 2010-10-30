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
import org.apache.lucene.util.FloatsRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.ReaderUtil;

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
  public ValuesEnum getEnum(AttributeSource source) throws IOException {
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
    final Values type;
    static final Source DUMMY = new DummySource();

    public DummyDocValues(int maxDoc, Values type) {
      this.type = type;
      this.maxDoc = maxDoc;
    }

    @Override
    public ValuesEnum getEnum(AttributeSource attrSource) throws IOException {
      return new DummyEnum(attrSource, maxDoc, type);
    }

    @Override
    public Source load() throws IOException {
      return DUMMY;
    }
   
    @Override
    public Values type() {
      return type;
    }

    public void close() throws IOException {
      super.close();
    }

  }

  private static class MultiValuesEnum extends ValuesEnum {
    private DocValuesIndex[] docValuesIdx;
    private final int maxDoc;
    private int currentStart;
    private int currentMax;
    private int currentDoc = -1;
    private ValuesEnum currentEnum;
    private final int[] starts;

    public MultiValuesEnum(DocValuesIndex[] docValuesIdx, int[] starts)
        throws IOException {
      super(docValuesIdx[0].docValues.type());
      this.docValuesIdx = docValuesIdx;
      final DocValuesIndex last = docValuesIdx[docValuesIdx.length - 1];
      maxDoc = last.start + last.length;
      final DocValuesIndex idx = docValuesIdx[0];
      currentEnum = idx.docValues.getEnum(this.attributes());
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
        if (target >= maxDoc) // we are beyond max doc
          return currentDoc = NO_MORE_DOCS;
        if (target >= currentMax) {
          final int idx = ReaderUtil.subIndex(target, starts);
          currentEnum.close();
          currentEnum = docValuesIdx[idx].docValues.getEnum(this.attributes());
          currentStart = docValuesIdx[idx].start;
          currentMax = currentStart + docValuesIdx[idx].length;
          relativeDoc = target - currentStart;
        } else {
          return currentDoc = currentStart + currentEnum.advance(relativeDoc);
        }
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

    }

    public long getInt(int docID) {
      final int doc = ensureSource(docID);
      return current.getInt(doc);
    }

    private final int ensureSource(int docID) {
      int n = docID - start;
      if (n >= numDocs) {
        final int idx = ReaderUtil.subIndex(docID, starts);
        assert idx >= 0 && idx < docValuesIdx.length : "idx was " + idx
            + " for doc id: " + docID + " slices : " + Arrays.toString(starts);
        assert docValuesIdx[idx] != null;
        try {
          current = docValuesIdx[idx].docValues.load();
        } catch (IOException e) {
          throw new RuntimeException("load failed", e); // TODO how should we
          // handle this
        }

        start = docValuesIdx[idx].start;
        numDocs = docValuesIdx[idx].length;
        n = docID - start;
      }
      return n;
    }

    public double getFloat(int docID) {
      final int doc = ensureSource(docID);
      return current.getFloat(doc);
    }

    public BytesRef getBytes(int docID) {
      final int doc = ensureSource(docID);
      return current.getBytes(doc);
    }

    public long ramBytesUsed() {
      return current.ramBytesUsed();
    }

  }

  private static class DummySource extends Source {
    private final BytesRef ref = new BytesRef();

    @Override
    public BytesRef getBytes(int docID) {
      return ref;
    }

    @Override
    public double getFloat(int docID) {
      return 0.0d;
    }

    @Override
    public long getInt(int docID) {
      return 0;
    }

    public long ramBytesUsed() {
      return 0;
    }
  }

  private static class DummyEnum extends ValuesEnum {
    private int pos = -1;
    private final int maxDoc;

    public DummyEnum(AttributeSource source, int maxDoc, Values type) {
      super(source, type);
      this.maxDoc = maxDoc;
      switch (type) {
      case BYTES_VAR_STRAIGHT:
      case BYTES_FIXED_STRAIGHT:
      case BYTES_FIXED_DEREF:
      case BYTES_FIXED_SORTED:
      case BYTES_VAR_DEREF:
      case BYTES_VAR_SORTED:
        // nocommit - this is not correct for Fixed_straight
        BytesRef bytes = attr.bytes();
        bytes.length = 0;
        bytes.offset = 0;
        break;
      case PACKED_INTS:
      case PACKED_INTS_FIXED:
        LongsRef ints = attr.ints();
        ints.set(0);
        break;

      case SIMPLE_FLOAT_4BYTE:
      case SIMPLE_FLOAT_8BYTE:
        FloatsRef floats = attr.floats();
        floats.set(0d);
        break;
      default:
        throw new IllegalArgumentException("unknown Values type: " + type);
      }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public int advance(int target) throws IOException {
      return pos = (pos < maxDoc ? target : NO_MORE_DOCS);
    }

    @Override
    public int docID() {
      return pos;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(pos + 1);
    }
  }

  @Override
  public Values type() {
    return this.docValuesIdx[0].docValues.type();
  }
}
