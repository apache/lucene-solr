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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * An {@link org.apache.lucene.index.CodecReader} which supports sorting documents by a given
 * {@link Sort}. This can be used to re-sort and index after it's been created by wrapping all
 * readers of the index with this reader and adding it to a fresh IndexWriter via
 * {@link IndexWriter#addIndexes(CodecReader...)}.
 * NOTE: This reader should only be used for merging. Pulling fields from this reader might be very costly and memory
 * intensive.
 *
 * @lucene.experimental
 */
public final class SortingCodecReader extends FilterCodecReader {

  private static class SortingBits implements Bits {

    private final Bits in;
    private final Sorter.DocMap docMap;

    SortingBits(final Bits in, Sorter.DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public boolean get(int index) {
      return in.get(docMap.newToOld(index));
    }

    @Override
    public int length() {
      return in.length();
    }
  }

  private static class SortingPointValues extends PointValues {

    private final PointValues in;
    private final Sorter.DocMap docMap;

    SortingPointValues(final PointValues in, Sorter.DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public void intersect(IntersectVisitor visitor) throws IOException {
      in.intersect(new IntersectVisitor() {
                     @Override
                     public void visit(int docID) throws IOException {
                       visitor.visit(docMap.oldToNew(docID));
                     }

                     @Override
                     public void visit(int docID, byte[] packedValue) throws IOException {
                       visitor.visit(docMap.oldToNew(docID), packedValue);
                     }

                     @Override
                     public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                       return visitor.compare(minPackedValue, maxPackedValue);
                     }
                   });
    }

    @Override
    public long estimatePointCount(IntersectVisitor visitor) {
      return in.estimatePointCount(visitor);
    }

    @Override
    public byte[] getMinPackedValue() throws IOException {
      return in.getMinPackedValue();
    }

    @Override
    public byte[] getMaxPackedValue() throws IOException {
      return in.getMaxPackedValue();
    }

    @Override
    public int getNumDimensions() throws IOException {
      return in.getNumDimensions();
    }

    @Override
    public int getNumIndexDimensions() throws IOException {
      return in.getNumIndexDimensions();
    }

    @Override
    public int getBytesPerDimension() throws IOException {
      return in.getBytesPerDimension();
    }

    @Override
    public long size() {
      return in.size();
    }

    @Override
    public int getDocCount() {
      return in.getDocCount();
    }
  }

  /** Return a sorted view of <code>reader</code> according to the order
   *  defined by <code>sort</code>. If the reader is already sorted, this
   *  method might return the reader as-is. */
  public static CodecReader wrap(CodecReader reader, Sort sort) throws IOException {
    return wrap(reader, new Sorter(sort).sort(reader), sort);
  }

  /** Expert: same as {@link #wrap(org.apache.lucene.index.CodecReader, Sort)} but operates directly on a {@link Sorter.DocMap}. */
  static CodecReader wrap(CodecReader reader, Sorter.DocMap docMap, Sort sort) {
    LeafMetaData metaData = reader.getMetaData();
    LeafMetaData newMetaData = new LeafMetaData(metaData.getCreatedVersionMajor(), metaData.getMinVersion(), sort);
    if (docMap == null) {
      // the reader is already sorted
      return new FilterCodecReader(reader) {
        @Override
        public CacheHelper getCoreCacheHelper() {
          return null;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
          return null;
        }

        @Override
        public LeafMetaData getMetaData() {
          return newMetaData;
        }

        @Override
        public String toString() {
          return "SortingCodecReader(" + in + ")";
        }
      };
    }
    if (reader.maxDoc() != docMap.size()) {
      throw new IllegalArgumentException("reader.maxDoc() should be equal to docMap.size(), got" + reader.maxDoc() + " != " + docMap.size());
    }
    assert Sorter.isConsistent(docMap);
    return new SortingCodecReader(reader, docMap, newMetaData);
  }

  final Sorter.DocMap docMap; // pkg-protected to avoid synthetic accessor methods
  final LeafMetaData metaData;

  private SortingCodecReader(final CodecReader in, final Sorter.DocMap docMap, LeafMetaData metaData) {
    super(in);
    this.docMap = docMap;
    this.metaData = metaData;
  }


  @Override
  public FieldsProducer getPostingsReader() {
    FieldsProducer postingsReader = in.getPostingsReader();
    return new FieldsProducer() {
      @Override
      public void close() throws IOException {
        postingsReader.close();
      }

      @Override
      public void checkIntegrity() throws IOException {
        postingsReader.checkIntegrity();
      }

      @Override
      public Iterator<String> iterator() {
        return postingsReader.iterator();
      }

      @Override
      public Terms terms(String field) throws IOException {
        Terms terms = postingsReader.terms(field);
        return terms == null ? null : new FreqProxTermsWriter.SortingTerms(terms,
            in.getFieldInfos().fieldInfo(field).getIndexOptions(), docMap);
      }

      @Override
      public int size() {
        return postingsReader.size();
      }

      @Override
      public long ramBytesUsed() {
        return postingsReader.ramBytesUsed();
      }
    };
  }

  @Override
  public StoredFieldsReader getFieldsReader() {
    StoredFieldsReader delegate = in.getFieldsReader();
    return newStoredFieldsReader(delegate);
  }

  private StoredFieldsReader newStoredFieldsReader(StoredFieldsReader delegate) {
    return new StoredFieldsReader() {
      @Override
      public void visitDocument(int docID, StoredFieldVisitor visitor) throws IOException {
        delegate.visitDocument(docMap.newToOld(docID), visitor);
      }

      @Override
      public StoredFieldsReader clone() {
        return newStoredFieldsReader(delegate.clone());
      }

      @Override
      public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }

      @Override
      public long ramBytesUsed() {
        return delegate.ramBytesUsed();
      }
    };
  }

  @Override
  public Bits getLiveDocs() {
    final Bits inLiveDocs = in.getLiveDocs();
    if (inLiveDocs == null) {
      return null;
    } else {
      return new SortingBits(inLiveDocs, docMap);
    }
  }

  @Override
  public PointsReader getPointsReader() {
    final PointsReader delegate = in.getPointsReader();
    return new PointsReader() {
      @Override
      public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
      }

      @Override
      public PointValues getValues(String field) throws IOException {
        return new SortingPointValues(delegate.getValues(field), docMap);
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }

      @Override
      public long ramBytesUsed() {
        return delegate.ramBytesUsed();
      }
    };
  }

  @Override
  public NormsProducer getNormsReader() {
    final NormsProducer delegate = in.getNormsReader();
    return new NormsProducer() {
      @Override
      public NumericDocValues getNorms(FieldInfo field) throws IOException {
        return new NumericDocValuesWriter.SortingNumericDocValues(getOrCreateNorms(field.name, () -> getNumericDocValues(delegate.getNorms(field))));
      }

      @Override
      public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }

      @Override
      public long ramBytesUsed() {
        return delegate.ramBytesUsed();
      }
    };
  }

  @Override
  public DocValuesProducer getDocValuesReader() {
    final DocValuesProducer delegate = in.getDocValuesReader();
    return new DocValuesProducer() {
      @Override
      public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return new NumericDocValuesWriter.SortingNumericDocValues(getOrCreateDV(field.name, () -> getNumericDocValues(delegate.getNumeric(field))));
      }

      @Override
      public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return new BinaryDocValuesWriter.SortingBinaryDocValues(getOrCreateDV(field.name,
            () -> new BinaryDocValuesWriter.BinaryDVs(maxDoc(), docMap, delegate.getBinary(field))));
      }

      @Override
      public SortedDocValues getSorted(FieldInfo field) throws IOException {
        SortedDocValues oldDocValues = delegate.getSorted(field);
        return new SortedDocValuesWriter.SortingSortedDocValues(oldDocValues, getOrCreateDV(field.name, () -> {
          int[] ords = new int[maxDoc()];
          Arrays.fill(ords, -1);
          int docID;
          while ((docID = oldDocValues.nextDoc()) != NO_MORE_DOCS) {
            int newDocID = docMap.oldToNew(docID);
            ords[newDocID] = oldDocValues.ordValue();
          }
          return ords;
        }));
      }

      @Override
      public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        final SortedNumericDocValues oldDocValues = delegate.getSortedNumeric(field);
        return new SortedNumericDocValuesWriter.SortingSortedNumericDocValues(oldDocValues, getOrCreateDV(field.name, () ->
            new SortedNumericDocValuesWriter.LongValues(maxDoc(), docMap, oldDocValues, PackedInts.FAST)));
      }

      @Override
      public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        SortedSetDocValues oldDocValues = delegate.getSortedSet(field);
        return new SortedSetDocValuesWriter.SortingSortedSetDocValues(oldDocValues, getOrCreateDV(field.name, () ->
            new SortedSetDocValuesWriter.DocOrds(maxDoc(), docMap, oldDocValues, PackedInts.FAST)));
      }

      @Override
      public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }

      @Override
      public long ramBytesUsed() {
        return delegate.ramBytesUsed();
      }
    };
  }

  private NumericDocValuesWriter.NumericDVs getNumericDocValues(NumericDocValues oldNumerics) throws IOException {
    FixedBitSet docsWithField = new FixedBitSet(maxDoc());
    long[] values = new long[maxDoc()];
    int docID;
    while ((docID = oldNumerics.nextDoc()) != NO_MORE_DOCS) {
      int newDocID = docMap.oldToNew(docID);
      docsWithField.set(newDocID);
      values[newDocID] = oldNumerics.longValue();
    }
    return new NumericDocValuesWriter.NumericDVs(values, docsWithField);
  }

  @Override
  public TermVectorsReader getTermVectorsReader() {
    return newTermVectorsReader(in.getTermVectorsReader());
  }

  private TermVectorsReader newTermVectorsReader(TermVectorsReader delegate) {
    return new TermVectorsReader() {
      @Override
      public Fields get(int doc) throws IOException {
        return delegate.get(docMap.newToOld(doc));
      }

      @Override
      public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
      }

      @Override
      public TermVectorsReader clone() {
        return newTermVectorsReader(delegate.clone());
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }

      @Override
      public long ramBytesUsed() {
        return delegate.ramBytesUsed();
      }
    };
  }

  @Override
  public String toString() {
    return "SortingCodecReader(" + in + ")";
  }

  // no caching on sorted views
  @Override
  public CacheHelper getCoreCacheHelper() {
    return null;
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return null;
  }

  @Override
  public LeafMetaData getMetaData() {
    return metaData;
  }

  // we try to cache the last used DV or Norms instance since during merge
  // this instance is used more than once. We could in addition to this single instance
  // also cache the fields that are used for sorting since we do the work twice for these fields
  private String cachedField;
  private Object cachedObject;
  private boolean cacheIsNorms;

  private <T> T getOrCreateNorms(String field, IOSupplier<T> supplier) throws IOException {
    return getOrCreate(field, true, supplier);
  }

  @SuppressWarnings("unchecked")
  private synchronized  <T> T getOrCreate(String field, boolean norms, IOSupplier<T> supplier) throws IOException {
    if ((field.equals(cachedField) && cacheIsNorms == norms) == false) {
      assert assertCreatedOnlyOnce(field, norms);
      cachedObject = supplier.get();
      cachedField = field;
      cacheIsNorms = norms;
    }
    assert cachedObject != null;
    return (T) cachedObject;
  }

  private final Map<String, Integer> cacheStats = new HashMap<>(); // only with assertions enabled
  private boolean assertCreatedOnlyOnce(String field, boolean norms) {
    assert Thread.holdsLock(this);
    // this is mainly there to make sure we change anything in the way we merge we realize it early
    Integer timesCached = cacheStats.compute(field + "N:" + norms, (s, i) -> i == null ? 1 : i.intValue() + 1);
    if (timesCached > 1) {
      assert norms == false :"[" + field + "] norms must not be cached twice";
      boolean isSortField = false;
      for (SortField sf : metaData.getSort().getSort()) {
        if (field.equals(sf.getField())) {
          isSortField = true;
          break;
        }
      }
      assert timesCached == 2 : "[" + field + "] must not be cached more than twice but was cached: "
          + timesCached + " times isSortField: " + isSortField;
      assert isSortField : "only sort fields should be cached twice but [" + field + "] is not a sort field";
    }
    return true;
  }

  private <T> T getOrCreateDV(String field, IOSupplier<T> supplier) throws IOException {
    return getOrCreate(field, false, supplier);
  }
}
