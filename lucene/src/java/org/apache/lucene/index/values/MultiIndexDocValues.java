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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ReaderUtil;

/**
 * A wrapper for compound IndexReader providing access to per segment
 * {@link IndexDocValues}
 * 
 * @lucene.experimental
 * @lucene.internal
 */
public class MultiIndexDocValues extends IndexDocValues {

  public static class DocValuesIndex {
    public final static DocValuesIndex[] EMPTY_ARRAY = new DocValuesIndex[0];
    final int start;
    final int length;
    final IndexDocValues docValues;

    public DocValuesIndex(IndexDocValues docValues, int start, int length) {
      this.docValues = docValues;
      this.start = start;
      this.length = length;
    }
  }

  private DocValuesIndex[] docValuesIdx;
  private int[] starts;
  private ValueType type;
  private int valueSize;

  public MultiIndexDocValues() {
    starts = new int[0];
    docValuesIdx = new DocValuesIndex[0];
  }

  public MultiIndexDocValues(DocValuesIndex[] docValuesIdx) {
    reset(docValuesIdx);
  }

  @Override
  public Source load() throws IOException {
    return new MultiSource(docValuesIdx, starts, false);
  }

  public IndexDocValues reset(DocValuesIndex[] docValuesIdx) {
    final int[] start = new int[docValuesIdx.length];
    TypePromoter promoter = TypePromoter.getIdentityPromoter();
    for (int i = 0; i < docValuesIdx.length; i++) {
      start[i] = docValuesIdx[i].start;
      if (!(docValuesIdx[i].docValues instanceof EmptyDocValues)) {
        // only promote if not a dummy
        final TypePromoter incomingPromoter = TypePromoter.create(
            docValuesIdx[i].docValues.type(),
            docValuesIdx[i].docValues.getValueSize());
        promoter = promoter.promote(incomingPromoter);
        if (promoter == null) {
          throw new IllegalStateException("Can not promote " + incomingPromoter);
        }
      }
    }
    this.type = promoter.type();
    this.valueSize = promoter.getValueSize();
    this.starts = start;
    this.docValuesIdx = docValuesIdx;
    return this;
  }

  public static class EmptyDocValues extends IndexDocValues {
    final int maxDoc;
    final Source emptySource;

    public EmptyDocValues(int maxDoc, ValueType type) {
      this.maxDoc = maxDoc;
      this.emptySource = new EmptySource(type);
    }

    @Override
    public Source load() throws IOException {
      return emptySource;
    }

    @Override
    public ValueType type() {
      return emptySource.type();
    }


    @Override
    public Source getDirectSource() throws IOException {
      return emptySource;
    }
  }

  private static class MultiSource extends Source {
    private int numDocs = 0;
    private int start = 0;
    private Source current;
    private final int[] starts;
    private final DocValuesIndex[] docValuesIdx;
    private boolean direct;

    public MultiSource(DocValuesIndex[] docValuesIdx, int[] starts, boolean direct) {
      super(docValuesIdx[0].docValues.type());
      this.docValuesIdx = docValuesIdx;
      this.starts = starts;
      assert docValuesIdx.length != 0;
      this.direct = direct;
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
          if (direct) {
            current = docValuesIdx[idx].docValues.getDirectSource();
          } else {
            current = docValuesIdx[idx].docValues.getSource();
          }
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
  }

  // TODO: this is dup of IndexDocValues.getDefaultSource()?
  private static class EmptySource extends Source {

    public EmptySource(ValueType type) {
      super(type);
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      ref.length = 0;
      return ref;

    }

    @Override
    public double getFloat(int docID) {
      return 0d;
    }

    @Override
    public long getInt(int docID) {
      return 0;
    }
  }

  @Override
  public ValueType type() {
    return type;
  }

  @Override
  public int getValueSize() {
    return valueSize;
  }

  @Override
  public Source getDirectSource() throws IOException {
    return new MultiSource(docValuesIdx, starts, true);
  }
}
