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
package org.apache.solr.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.*;
import org.apache.lucene.index.MultiDocValues.MultiSortedDocValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;

/**
 * This class forces a composite reader (eg a {@link
 * MultiReader} or {@link DirectoryReader}) to emulate a
 * {@link LeafReader}.  This requires implementing the postings
 * APIs on-the-fly, using the static methods in {@link
 * MultiTerms}, {@link MultiDocValues}, by stepping through
 * the sub-readers to merge fields/terms, appending docs, etc.
 *
 * <p><b>NOTE</b>: this class almost always results in a
 * performance hit.  If this is important to your use case,
 * you'll get better performance by gathering the sub readers using
 * {@link IndexReader#getContext()} to get the
 * leaves and then operate per-LeafReader,
 * instead of using this class.
 */

public final class SlowCompositeReaderWrapper extends LeafReader {

  private final CompositeReader in;
  private final LeafMetaData metaData;

  // Cached copy of FieldInfos to prevent it from being re-created on each
  // getFieldInfos call.  Most (if not all) other LeafReader implementations
  // also have a cached FieldInfos instance so this is consistent. SOLR-12878
  private final FieldInfos fieldInfos;

  final Map<String,Terms> cachedTerms = new ConcurrentHashMap<>();

  // TODO: consider ConcurrentHashMap ?
  // TODO: this could really be a weak map somewhere else on the coreCacheKey,
  // but do we really need to optimize slow-wrapper any more?
  final Map<String,OrdinalMap> cachedOrdMaps = new HashMap<>();

  /** This method is sugar for getting an {@link LeafReader} from
   * an {@link IndexReader} of any kind. If the reader is already atomic,
   * it is returned unchanged, otherwise wrapped by this class.
   */
  public static LeafReader wrap(IndexReader reader) throws IOException {
    if (reader instanceof CompositeReader) {
      return new SlowCompositeReaderWrapper((CompositeReader) reader);
    } else {
      assert reader instanceof LeafReader;
      return (LeafReader) reader;
    }
  }

  SlowCompositeReaderWrapper(CompositeReader reader) throws IOException {
    in = reader;
    in.registerParentReader(this);
    if (reader.leaves().isEmpty()) {
      metaData = new LeafMetaData(Version.LATEST.major, Version.LATEST, null);
    } else {
      Version minVersion = Version.LATEST;
      for (LeafReaderContext leafReaderContext : reader.leaves()) {
        Version leafVersion = leafReaderContext.reader().getMetaData().getMinVersion();
        if (leafVersion == null) {
          minVersion = null;
          break;
        } else if (minVersion.onOrAfter(leafVersion)) {
          minVersion = leafVersion;
        }
      }
      metaData = new LeafMetaData(reader.leaves().get(0).reader().getMetaData().getCreatedVersionMajor(), minVersion, null);
    }
    fieldInfos = FieldInfos.getMergedFieldInfos(in);
  }

  @Override
  public String toString() {
    return "SlowCompositeReaderWrapper(" + in + ")";
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return in.getReaderCacheHelper();
  }

  @Override
  public CacheHelper getCoreCacheHelper() {
    // TODO: this is trappy as the expectation is that core keys live for a long
    // time, but here we need to bound it to the lifetime of the wrapped
    // composite reader? Unfortunately some features seem to rely on this...
    return in.getReaderCacheHelper();
  }

  @Override
  public Terms terms(String field) throws IOException {
    ensureOpen();
    try {
      return cachedTerms.computeIfAbsent(field, f -> {
        try {
          return MultiTerms.getTerms(in, f);
        } catch (IOException e) { // yuck!  ...sigh... checked exceptions with built-in lambdas are a pain
          throw new RuntimeException("unwrapMe", e);
        }
      });
    } catch (RuntimeException e) {
      if (e.getMessage().equals("unwrapMe") && e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw e;
    }
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getNumericValues(in, field); // TODO cache?
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getBinaryValues(in, field); // TODO cache?
  }
  
  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getSortedNumericValues(in, field); // TODO cache?
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    ensureOpen();
    OrdinalMap map = null;
    synchronized (cachedOrdMaps) {
      map = cachedOrdMaps.get(field);
      if (map == null) {
        // uncached, or not a multi dv
        SortedDocValues dv = MultiDocValues.getSortedValues(in, field);
        if (dv instanceof MultiSortedDocValues) {
          map = ((MultiSortedDocValues)dv).mapping;
          IndexReader.CacheHelper cacheHelper = getReaderCacheHelper();
          if (cacheHelper != null && map.owner == cacheHelper.getKey()) {
            cachedOrdMaps.put(field, map);
          }
        }
        return dv;
      }
    }
    int size = in.leaves().size();
    final SortedDocValues[] values = new SortedDocValues[size];
    final int[] starts = new int[size+1];
    long totalCost = 0;
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = in.leaves().get(i);
      final LeafReader reader = context.reader();
      final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.SORTED) {
        return null;
      }
      SortedDocValues v = reader.getSortedDocValues(field);
      if (v == null) {
        v = DocValues.emptySorted();
      }
      totalCost += v.cost();
      values[i] = v;
      starts[i] = context.docBase;
    }
    starts[size] = maxDoc();
    return new MultiSortedDocValues(values, starts, map, totalCost);
  }
  
  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    ensureOpen();
    OrdinalMap map = null;
    synchronized (cachedOrdMaps) {
      map = cachedOrdMaps.get(field);
      if (map == null) {
        // uncached, or not a multi dv
        SortedSetDocValues dv = MultiDocValues.getSortedSetValues(in, field);
        if (dv instanceof MultiDocValues.MultiSortedSetDocValues) {
          map = ((MultiDocValues.MultiSortedSetDocValues)dv).mapping;
          IndexReader.CacheHelper cacheHelper = getReaderCacheHelper();
          if (cacheHelper != null && map.owner == cacheHelper.getKey()) {
            cachedOrdMaps.put(field, map);
          }
        }
        return dv;
      }
    }
   
    assert map != null;
    int size = in.leaves().size();
    final SortedSetDocValues[] values = new SortedSetDocValues[size];
    final int[] starts = new int[size+1];
    long cost = 0;
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = in.leaves().get(i);
      final LeafReader reader = context.reader();
      final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      if(fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.SORTED_SET){
        return null;
      }
      SortedSetDocValues v = reader.getSortedSetDocValues(field);
      if (v == null) {
        v = DocValues.emptySortedSet();
      }
      values[i] = v;
      starts[i] = context.docBase;
      cost += v.cost();
    }
    starts[size] = maxDoc();
    return new MultiDocValues.MultiSortedSetDocValues(values, starts, map, cost);
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    ensureOpen();
    return MultiDocValues.getNormValues(in, field); // TODO cache?
  }
  
  @Override
  public Fields getTermVectors(int docID) throws IOException {
    ensureOpen();
    return in.getTermVectors(docID);
  }

  @Override
  public int numDocs() {
    // Don't call ensureOpen() here (it could affect performance)
    return in.numDocs();
  }

  @Override
  public int maxDoc() {
    // Don't call ensureOpen() here (it could affect performance)
    return in.maxDoc();
  }

  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    ensureOpen();
    in.document(docID, visitor);
  }

  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return MultiBits.getLiveDocs(in); // TODO cache?
  }

  @Override
  public PointValues getPointValues(String field) {
    ensureOpen();
    return null; // because not supported.  Throw UOE?
  }

  @Override
  public FieldInfos getFieldInfos() {
    return fieldInfos;
  }

  @Override
  protected void doClose() throws IOException {
    // TODO: as this is a wrapper, should we really close the delegate?
    in.close();
  }

  @Override
  public void checkIntegrity() throws IOException {
    ensureOpen();
    for (LeafReaderContext ctx : in.leaves()) {
      ctx.reader().checkIntegrity();
    }
  }

  @Override
  public LeafMetaData getMetaData() {
    return metaData;
  }
}
