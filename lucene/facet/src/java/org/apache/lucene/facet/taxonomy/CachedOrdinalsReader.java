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
package org.apache.lucene.facet.taxonomy;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A per-segment cache of documents' facet ordinals. Every
 * {@link CachedOrds} holds the ordinals in a raw {@code
 * int[]}, and therefore consumes as much RAM as the total
 * number of ordinals found in the segment, but saves the
 * CPU cost of decoding ordinals during facet counting.
 * 
 * <p>
 * <b>NOTE:</b> every {@link CachedOrds} is limited to 2.1B
 * total ordinals. If that is a limitation for you then
 * consider limiting the segment size to fewer documents, or
 * use an alternative cache which pages through the category
 * ordinals.
 * 
 * <p>
 * <b>NOTE:</b> when using this cache, it is advised to use
 * a {@link DocValuesFormat} that does not cache the data in
 * memory, at least for the category lists fields, or
 * otherwise you'll be doing double-caching.
 *
 * <p>
 * <b>NOTE:</b> create one instance of this and re-use it
 * for all facet implementations (the cache is per-instance,
 * not static).
 */
public class CachedOrdinalsReader extends OrdinalsReader implements Accountable {

  private final OrdinalsReader source;

  private final Map<Object,CachedOrds> ordsCache = new WeakHashMap<>();

  /** Sole constructor. */
  public CachedOrdinalsReader(OrdinalsReader source) {
    this.source = source;
  }

  private synchronized CachedOrds getCachedOrds(LeafReaderContext context) throws IOException {
    Object cacheKey = context.reader().getCoreCacheKey();
    CachedOrds ords = ordsCache.get(cacheKey);
    if (ords == null) {
      ords = new CachedOrds(source.getReader(context), context.reader().maxDoc());
      ordsCache.put(cacheKey, ords);
    }

    return ords;
  }

  @Override
  public String getIndexFieldName() {
    return source.getIndexFieldName();
  }

  @Override
  public OrdinalsSegmentReader getReader(LeafReaderContext context) throws IOException {
    final CachedOrds cachedOrds = getCachedOrds(context);
    return new OrdinalsSegmentReader() {
      @Override
      public void get(int docID, IntsRef ordinals) {
        ordinals.ints = cachedOrds.ordinals;
        ordinals.offset = cachedOrds.offsets[docID];
        ordinals.length = cachedOrds.offsets[docID+1] - ordinals.offset;
      }
    };
  }

  /** Holds the cached ordinals in two parallel {@code int[]} arrays. */
  public static final class CachedOrds implements Accountable {

    /** Index into {@link #ordinals} for each document. */
    public final int[] offsets;

    /** Holds ords for all docs. */
    public final int[] ordinals;

    /**
     * Creates a new {@link CachedOrds} from the {@link BinaryDocValues}.
     * Assumes that the {@link BinaryDocValues} is not {@code null}.
     */
    public CachedOrds(OrdinalsSegmentReader source, int maxDoc) throws IOException {
      offsets = new int[maxDoc + 1];
      int[] ords = new int[maxDoc]; // let's assume one ordinal per-document as an initial size

      // this aggregator is limited to Integer.MAX_VALUE total ordinals.
      long totOrds = 0;
      final IntsRef values = new IntsRef(32);
      for (int docID = 0; docID < maxDoc; docID++) {
        offsets[docID] = (int) totOrds;
        source.get(docID, values);
        long nextLength = totOrds + values.length;
        if (nextLength > ords.length) {
          if (nextLength > ArrayUtil.MAX_ARRAY_LENGTH) {
            throw new IllegalStateException("too many ordinals (>= " + nextLength + ") to cache");
          }
          ords = ArrayUtil.grow(ords, (int) nextLength);
        }
        System.arraycopy(values.ints, 0, ords, (int) totOrds, values.length);
        totOrds = nextLength;
      }
      offsets[maxDoc] = (int) totOrds;
      
      // if ords array is bigger by more than 10% of what we really need, shrink it
      if ((double) totOrds / ords.length < 0.9) { 
        this.ordinals = new int[(int) totOrds];
        System.arraycopy(ords, 0, this.ordinals, 0, (int) totOrds);
      } else {
        this.ordinals = ords;
      }
    }

    @Override
    public long ramBytesUsed() {
      long mem = RamUsageEstimator.shallowSizeOf(this) + RamUsageEstimator.sizeOf(offsets);
      if (offsets != ordinals) {
        mem += RamUsageEstimator.sizeOf(ordinals);
      }
      return mem;
    }
  }

  @Override
  public synchronized long ramBytesUsed() {
    long bytes = 0;
    for(CachedOrds ords : ordsCache.values()) {
      bytes += ords.ramBytesUsed();
    }

    return bytes;
  }
  
  @Override
  public synchronized Collection<Accountable> getChildResources() {
    return Accountables.namedAccountables("segment", ordsCache);
  }
}
