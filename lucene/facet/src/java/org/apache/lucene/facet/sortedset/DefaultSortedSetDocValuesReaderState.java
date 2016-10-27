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
package org.apache.lucene.facet.sortedset;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState.OrdRange;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

/**
 * Default implementation of {@link SortedSetDocValuesFacetCounts}. You must ensure the original
 * {@link IndexReader} passed to the constructor is not closed whenever you use this class!
 */
public class DefaultSortedSetDocValuesReaderState extends SortedSetDocValuesReaderState {

  private final String field;
  private final int valueCount;

  /** {@link IndexReader} passed to the constructor. */
  public final IndexReader origReader;

  private final Map<String,OrdinalMap> cachedOrdMaps = new HashMap<>();

  private final Map<String,OrdRange> prefixToOrdRange = new HashMap<>();

  /** Creates this, pulling doc values from the default {@link
   *  FacetsConfig#DEFAULT_INDEX_FIELD_NAME}. */ 
  public DefaultSortedSetDocValuesReaderState(IndexReader reader) throws IOException {
    this(reader, FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
  }

  /** Creates this, pulling doc values from the specified
   *  field. */
  public DefaultSortedSetDocValuesReaderState(IndexReader reader, String field) throws IOException {
    this.field = field;
    this.origReader = reader;

    // We need this to create thread-safe MultiSortedSetDV
    // per collector:
    SortedSetDocValues dv = getDocValues();
    if (dv == null) {
      throw new IllegalArgumentException("field \"" + field + "\" was not indexed with SortedSetDocValues");
    }
    if (dv.getValueCount() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("can only handle valueCount < Integer.MAX_VALUE; got " + dv.getValueCount());
    }
    valueCount = (int) dv.getValueCount();

    // TODO: we can make this more efficient if eg we can be
    // "involved" when OrdinalMap is being created?  Ie see
    // each term/ord it's assigning as it goes...
    String lastDim = null;
    int startOrd = -1;

    // TODO: this approach can work for full hierarchy?;
    // TaxoReader can't do this since ords are not in
    // "sorted order" ... but we should generalize this to
    // support arbitrary hierarchy:
    for(int ord=0;ord<valueCount;ord++) {
      final BytesRef term = dv.lookupOrd(ord);
      String[] components = FacetsConfig.stringToPath(term.utf8ToString());
      if (components.length != 2) {
        throw new IllegalArgumentException("this class can only handle 2 level hierarchy (dim/value); got: " + Arrays.toString(components) + " " + term.utf8ToString());
      }
      if (!components[0].equals(lastDim)) {
        if (lastDim != null) {
          prefixToOrdRange.put(lastDim, new OrdRange(startOrd, ord-1));
        }
        startOrd = ord;
        lastDim = components[0];
      }
    }

    if (lastDim != null) {
      prefixToOrdRange.put(lastDim, new OrdRange(startOrd, valueCount-1));
    }
  }

  /** Return top-level doc values. */
  @Override
  public SortedSetDocValues getDocValues() throws IOException {
    // TODO: this is dup'd from slow composite reader wrapper ... can we factor it out to share?
    OrdinalMap map = null;
    synchronized (cachedOrdMaps) {
      map = cachedOrdMaps.get(field);
      if (map == null) {
        // uncached, or not a multi dv
        SortedSetDocValues dv = MultiDocValues.getSortedSetValues(origReader, field);
        if (dv instanceof MultiDocValues.MultiSortedSetDocValues) {
          map = ((MultiDocValues.MultiSortedSetDocValues)dv).mapping;
          if (map.owner == origReader.getCoreCacheKey()) {
            cachedOrdMaps.put(field, map);
          }
        }
        return dv;
      }
    }
   
    assert map != null;
    int size = origReader.leaves().size();
    final SortedSetDocValues[] values = new SortedSetDocValues[size];
    final int[] starts = new int[size+1];
    long cost = 0;
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = origReader.leaves().get(i);
      final LeafReader reader = context.reader();
      final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.SORTED_SET) {
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
    starts[size] = origReader.maxDoc();
    return new MultiSortedSetDocValues(values, starts, map, cost);
  }

  /** Returns mapping from prefix to {@link OrdRange}. */
  @Override
  public Map<String,OrdRange> getPrefixToOrdRange() {
    return prefixToOrdRange;
  }

  /** Returns the {@link OrdRange} for this dimension. */
  @Override
  public OrdRange getOrdRange(String dim) {
    return prefixToOrdRange.get(dim);
  }

  /** Indexed field we are reading. */
  @Override
  public String getField() {
    return field;
  }
  
  @Override
  public IndexReader getOrigReader() {
    return origReader;
  }

  /** Number of unique labels. */
  @Override
  public int getSize() {
    return valueCount;
  }

}
