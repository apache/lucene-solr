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
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.SortedBytesMergeUtils.MergeContext;
import org.apache.lucene.index.SortedBytesMergeUtils.SortedSourceSlice;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.ReaderUtil.Gather;
import org.apache.lucene.util.packed.PackedInts.Reader;

/**
 * A wrapper for CompositeIndexReader providing access to per segment
 * {@link DocValues}
 * 
 * @lucene.experimental
 * @lucene.internal
 */
public class MultiDocValues extends DocValues {
  
  private static DocValuesPuller DEFAULT_PULLER = new DocValuesPuller();
  private static final DocValuesPuller NORMS_PULLER = new DocValuesPuller() {
    public DocValues pull(AtomicReader reader, String field) throws IOException {
      return reader.normValues(field);
    }
    
    public boolean stopLoadingOnNull(AtomicReader reader, String field) throws IOException {
      // for norms we drop all norms if one leaf reader has no norms and the field is present
      FieldInfos fieldInfos = reader.getFieldInfos();
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      return fieldInfo != null && fieldInfo.omitNorms;
    }
  };

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
  
  private static class DocValuesPuller {
    public DocValues pull(AtomicReader reader, String field) throws IOException {
      return reader.docValues(field);
    }
    
    public boolean stopLoadingOnNull(AtomicReader reader, String field) throws IOException {
      return false;
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
    return getDocValues(r, field, DEFAULT_PULLER);
  }
  
  /**
   * Returns a single {@link DocValues} instance for this norms field, merging
   * their values on the fly.
   * 
   * <p>
   * <b>NOTE</b>: this is a slow way to access DocValues. It's better to get the
   * sub-readers (using {@link Gather}) and iterate through them yourself.
   */
  public static DocValues getNormDocValues(IndexReader r, final String field) throws IOException {
    return getDocValues(r, field, NORMS_PULLER);
  }
  
 
  private static DocValues getDocValues(IndexReader r, final String field, final DocValuesPuller puller) throws IOException {
    if (r instanceof AtomicReader) {
      // already an atomic reader
      return puller.pull((AtomicReader) r, field);
    }
    assert r instanceof CompositeReader;
    final IndexReader[] subs = ((CompositeReader) r).getSequentialSubReaders();
    if (subs.length == 0) {
      // no fields
      return null;
    } else if (subs.length == 1) {
      return getDocValues(subs[0], field, puller);
    } else {      
      final List<DocValuesSlice> slices = new ArrayList<DocValuesSlice>();
      
      final TypePromoter promotedType[] = new TypePromoter[1];
      promotedType[0] = TypePromoter.getIdentityPromoter();
      
      // gather all docvalues fields, accumulating a promoted type across 
      // potentially incompatible types
      
      new ReaderUtil.Gather(r) {
        boolean stop = false;
        @Override
        protected void add(int base, AtomicReader r) throws IOException {
          if (stop) {
            return;
          }
          final DocValues d = puller.pull(r, field);
          if (d != null) {
            TypePromoter incoming = TypePromoter.create(d.getType(), d.getValueSize());
            promotedType[0] = promotedType[0].promote(incoming);
          } else if (puller.stopLoadingOnNull(r, field)){
            promotedType[0] = TypePromoter.getIdentityPromoter(); // set to identity to return null
            stop = true;
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
            case BYTES_FIXED_SORTED:
              assert promotedType[0].getValueSize() >= 0;
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
    public Type getType() {
      return emptySource.getType();
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
    public Type getType() {
      return emptyFixedSource.getType();
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
    private Object cachedArray; // cached array if supported

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

    @Override
    public SortedSource asSortedSource() {
      try {
        if (type == Type.BYTES_FIXED_SORTED || type == Type.BYTES_VAR_SORTED) {
          DocValues[] values = new DocValues[slices.length];
          Comparator<BytesRef> comp = null;
          for (int i = 0; i < values.length; i++) {
            values[i] = slices[i].docValues;
            if (!(values[i] instanceof EmptyDocValues)) {
              Comparator<BytesRef> comparator = values[i].getDirectSource()
                  .asSortedSource().getComparator();
              assert comp == null || comp == comparator;
              comp = comparator;
            }
          }
          assert comp != null;
          final int globalNumDocs = globalNumDocs();
          final MergeContext ctx = SortedBytesMergeUtils.init(type, values,
              comp, globalNumDocs);
          List<SortedSourceSlice> slices = SortedBytesMergeUtils.buildSlices(
              docBases(), new int[values.length][], values, ctx);
          RecordingBytesRefConsumer consumer = new RecordingBytesRefConsumer(
              type);
          final int maxOrd = SortedBytesMergeUtils.mergeRecords(ctx, consumer,
              slices);
          final int[] docToOrd = new int[globalNumDocs];
          for (SortedSourceSlice slice : slices) {
            slice.toAbsolutOrds(docToOrd);
          }
          return new MultiSortedSource(type, comp, consumer.pagedBytes,
              ctx.sizePerValues, maxOrd, docToOrd, consumer.ordToOffset);
        }
      } catch (IOException e) {
        throw new RuntimeException("load failed", e);
      }
      return super.asSortedSource();
    }
    
    private int globalNumDocs() {
      int docs = 0;
      for (int i = 0; i < slices.length; i++) {
        docs += slices[i].length;
      }
      return docs;
    }
    
    private int[] docBases() {
      int[] docBases = new int[slices.length];
      for (int i = 0; i < slices.length; i++) {
        docBases[i] = slices[i].start;
      }
      return docBases;
    }
    
    public boolean hasArray() {
      boolean oneRealSource = false;
      for (DocValuesSlice slice : slices) {
        try {
          Source source = slice.docValues.getSource();
          if (source instanceof EmptySource) {
            /*
             * empty source marks a gap in the array skip if we encounter one
             */
            continue;
          }
          oneRealSource = true;
          if (!source.hasArray()) {
            return false;
          }
        } catch (IOException e) {
          throw new RuntimeException("load failed", e);
        }
      }
      return oneRealSource;
    }

    @Override
    public Object getArray() {
      if (!hasArray()) {
        return null;
      }
      try {
        Class<?> componentType = null;
        Object[] arrays = new Object[slices.length];
        int numDocs = 0;
        for (int i = 0; i < slices.length; i++) {
          DocValuesSlice slice = slices[i];
          Source source = slice.docValues.getSource();
          Object array = null;
          if (!(source instanceof EmptySource)) {
            // EmptySource is skipped - marks a gap in the array
            array = source.getArray();
          }
          numDocs += slice.length;
          if (array != null) {
            if (componentType == null) {
              componentType = array.getClass().getComponentType();
            }
            assert componentType == array.getClass().getComponentType();
          }
          arrays[i] = array;
        }
        assert componentType != null;
        synchronized (this) {
          if (cachedArray != null) {
            return cachedArray;
          }
          final Object globalArray = Array.newInstance(componentType, numDocs);

          for (int i = 0; i < slices.length; i++) {
            DocValuesSlice slice = slices[i];
            if (arrays[i] != null) {
              assert slice.length == Array.getLength(arrays[i]);
              System.arraycopy(arrays[i], 0, globalArray, slice.start,
                  slice.length);
            }
          }
          return cachedArray = globalArray;
        }
      } catch (IOException e) {
        throw new RuntimeException("load failed", e);
      }
    }
  }
  
  private static final class RecordingBytesRefConsumer implements SortedBytesMergeUtils.BytesRefConsumer {
    private final static int PAGED_BYTES_BITS = 15;
    final PagedBytes pagedBytes = new PagedBytes(PAGED_BYTES_BITS);
    long[] ordToOffset;
    
    public RecordingBytesRefConsumer(Type type) {
      ordToOffset = type == Type.BYTES_VAR_SORTED ? new long[2] : null;
    }
    @Override
    public void consume(BytesRef ref, int ord, long offset) throws IOException {
      pagedBytes.copy(ref);
      if (ordToOffset != null) {
        if (ord+1 >= ordToOffset.length) {
          ordToOffset = ArrayUtil.grow(ordToOffset, ord + 2);
        }
        ordToOffset[ord+1] = offset;
      }
    }
    
  }
  
  private static final class MultiSortedSource extends SortedSource {
    private final PagedBytes.Reader data;
    private final int[] docToOrd;
    private final long[] ordToOffset;
    private int size;
    private int valueCount;
    public MultiSortedSource(Type type, Comparator<BytesRef> comparator, PagedBytes pagedBytes, int size, int numValues, int[] docToOrd, long[] ordToOffset) {
      super(type, comparator);
      data = pagedBytes.freeze(true);
      this.size = size;
      this.valueCount = numValues;
      this.docToOrd = docToOrd;
      this.ordToOffset = ordToOffset;
    }

    @Override
    public int ord(int docID) {
      return docToOrd[docID];
    }

    @Override
    public BytesRef getByOrd(int ord, BytesRef bytesRef) {
      int size = this.size;
      long offset = (ord*size);
      if (ordToOffset != null) {
        offset =  ordToOffset[ord];
        size = (int) (ordToOffset[1 + ord] - offset);
      }
      assert size >=0;
      return data.fillSlice(bytesRef, offset, size);
     }

    @Override
    public Reader getDocToOrd() {
      return null;
    }

    @Override
    public int getValueCount() {
      return valueCount;
    }
  }

  // TODO: this is dup of DocValues.getDefaultSource()?
  private static class EmptySource extends SortedSource {

    public EmptySource(Type type) {
      super(type, BytesRef.getUTF8SortedAsUnicodeComparator());
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

    @Override
    public SortedSource asSortedSource() {
      if (getType() == Type.BYTES_FIXED_SORTED || getType() == Type.BYTES_VAR_SORTED) {
        
      }
      return super.asSortedSource();
    }

    @Override
    public int ord(int docID) {
      return 0;
    }

    @Override
    public BytesRef getByOrd(int ord, BytesRef bytesRef) {
      bytesRef.length = 0;
      bytesRef.offset = 0;
      return bytesRef;
    }

    @Override
    public Reader getDocToOrd() {
      return null;
    }

    @Override
    public int getValueCount() {
      return 1;
    }
    
  }
  
  private static class EmptyFixedSource extends EmptySource {
    private final int valueSize;
    private final byte[] valueArray;
    public EmptyFixedSource(Type type, int valueSize) {
      super(type);
      this.valueSize = valueSize;
      valueArray = new byte[valueSize];
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
    
    @Override
    public BytesRef getByOrd(int ord, BytesRef bytesRef) {
      bytesRef.bytes = valueArray;
      bytesRef.length = valueSize;
      bytesRef.offset = 0;
      return bytesRef;
    }
  }

  @Override
  public Type getType() {
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
