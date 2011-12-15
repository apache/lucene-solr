package org.apache.lucene.index;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.ReaderUtil.Gather;

/**
 * A wrapper for compound IndexReader providing access to per segment
 * {@link DocValues}
 * 
 * @lucene.experimental
 * @lucene.internal
 */
public class MultiDocValues extends DocValues {

  public static class DocValuesSlice {
    public final static DocValuesSlice[] EMPTY_ARRAY = new DocValuesSlice[0];
    final int start;
    final int length;
    DocValues docValues;

    public DocValuesSlice(DocValues docValues, int start, int length) {
      this.docValues = docValues;
      this.start = start;
      this.length = length;
    }
  }

  private DocValuesSlice[] slices;
  private int[] starts;
  private Type type;
  private int valueSize;

  private MultiDocValues(DocValuesSlice[] slices, int[] starts, TypePromoter promotedType) {
    this.starts = starts;
    this.slices = slices;
    this.type = promotedType.type();
    this.valueSize = promotedType.getValueSize();
  }
  
  /**
   * Returns a single {@link DocValues} instance for this field, merging
   * their values on the fly.
   * 
   * <p>
   * <b>NOTE</b>: this is a slow way to access DocValues. It's better to get the
   * sub-readers (using {@link Gather}) and iterate through them yourself.
   */
  public static DocValues getDocValues(IndexReader r, final String field) throws IOException {
    final IndexReader[] subs = r.getSequentialSubReaders();
    if (subs == null) {
      // already an atomic reader
      return r.docValues(field);
    } else if (subs.length == 0) {
      // no fields
      return null;
    } else if (subs.length == 1) {
      return getDocValues(subs[0], field);
    } else {      
      final List<DocValuesSlice> slices = new ArrayList<DocValuesSlice>();
      
      final TypePromoter promotedType[] = new TypePromoter[1];
      promotedType[0] = TypePromoter.getIdentityPromoter();
      
      // gather all docvalues fields, accumulating a promoted type across 
      // potentially incompatible types
      
      new ReaderUtil.Gather(r) {
        @Override
        protected void add(int base, IndexReader r) throws IOException {
          final DocValues d = r.docValues(field);
          if (d != null) {
            TypePromoter incoming = TypePromoter.create(d.type(), d.getValueSize());
            promotedType[0] = promotedType[0].promote(incoming);
          }
          slices.add(new DocValuesSlice(d, base, r.maxDoc()));
        }
      }.run();
      
      // return null if no docvalues encountered anywhere
      if (promotedType[0] == TypePromoter.getIdentityPromoter()) {
        return null;
      }
           
      // populate starts and fill gaps with empty docvalues 
      int starts[] = new int[slices.size()];
      for (int i = 0; i < slices.size(); i++) {
        DocValuesSlice slice = slices.get(i);
        starts[i] = slice.start;
        if (slice.docValues == null) {
          Type promoted = promotedType[0].type();
          switch(promoted) {
            case BYTES_FIXED_DEREF:
            case BYTES_FIXED_STRAIGHT:
              slice.docValues = new EmptyFixedDocValues(slice.length, promoted, promotedType[0].getValueSize());
              break;
            default:
              slice.docValues = new EmptyDocValues(slice.length, promoted);
          }
        }
      }
      
      return new MultiDocValues(slices.toArray(new DocValuesSlice[slices.size()]), starts, promotedType[0]);
    }
  }

  @Override
  public Source load() throws IOException {
    return new MultiSource(slices, starts, false, type);
  }

  public static class EmptyDocValues extends DocValues {
    final int maxDoc;
    final Source emptySource;

    public EmptyDocValues(int maxDoc, Type type) {
      this.maxDoc = maxDoc;
      this.emptySource = new EmptySource(type);
    }

    @Override
    public Source load() throws IOException {
      return emptySource;
    }

    @Override
    public Type type() {
      return emptySource.type();
    }


    @Override
    public Source getDirectSource() throws IOException {
      return emptySource;
    }
  }
  
  public static class EmptyFixedDocValues extends DocValues {
    final int maxDoc;
    final Source emptyFixedSource;
    final int valueSize;

    public EmptyFixedDocValues(int maxDoc, Type type, int valueSize) {
      this.maxDoc = maxDoc;
      this.emptyFixedSource = new EmptyFixedSource(type, valueSize);
      this.valueSize = valueSize;
    }

    @Override
    public Source load() throws IOException {
      return emptyFixedSource;
    }

    @Override
    public Type type() {
      return emptyFixedSource.type();
    }

    @Override
    public int getValueSize() {
      return valueSize;
    }

    @Override
    public Source getDirectSource() throws IOException {
      return emptyFixedSource;
    }
  }

  private static class MultiSource extends Source {
    private int numDocs = 0;
    private int start = 0;
    private Source current;
    private final int[] starts;
    private final DocValuesSlice[] slices;
    private boolean direct;

    public MultiSource(DocValuesSlice[] slices, int[] starts, boolean direct, Type type) {
      super(type);
      this.slices = slices;
      this.starts = starts;
      assert slices.length != 0;
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
        assert idx >= 0 && idx < slices.length : "idx was " + idx
            + " for doc id: " + docID + " slices : " + Arrays.toString(starts);
        assert slices[idx] != null;
        try {
          if (direct) {
            current = slices[idx].docValues.getDirectSource();
          } else {
            current = slices[idx].docValues.getSource();
          }
        } catch (IOException e) {
          throw new RuntimeException("load failed", e); // TODO how should we
          // handle this
        }

        start = slices[idx].start;
        numDocs = slices[idx].length;
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

  // TODO: this is dup of DocValues.getDefaultSource()?
  private static class EmptySource extends Source {

    public EmptySource(Type type) {
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
  
  private static class EmptyFixedSource extends Source {
    private final int valueSize;
    
    public EmptyFixedSource(Type type, int valueSize) {
      super(type);
      this.valueSize = valueSize;
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      ref.grow(valueSize);
      ref.length = valueSize;
      Arrays.fill(ref.bytes, ref.offset, ref.offset+valueSize, (byte)0);
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
  public Type type() {
    return type;
  }

  @Override
  public int getValueSize() {
    return valueSize;
  }

  @Override
  public Source getDirectSource() throws IOException {
    return new MultiSource(slices, starts, true, type);
  }
}
