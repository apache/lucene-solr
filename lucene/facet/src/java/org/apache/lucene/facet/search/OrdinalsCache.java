package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.facet.encoding.IntDecoder;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;

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

/**
 * A per-segment cache of documents' category ordinals. Every {@link CachedOrds}
 * holds the ordinals in a raw {@code int[]}, and therefore consumes as much RAM
 * as the total number of ordinals found in the segment.
 * 
 * <p>
 * <b>NOTE:</b> every {@link CachedOrds} is limited to 2.1B total ordinals. If
 * that is a limitation for you then consider limiting the segment size to less
 * documents, or use an alternative cache which pages through the category
 * ordinals.
 * 
 * <p>
 * <b>NOTE:</b> when using this cache, it is advised to use a
 * {@link DocValuesFormat} that does not cache the data in memory, at least for
 * the category lists fields, or otherwise you'll be doing double-caching.
 */
public class OrdinalsCache {
  
  /** Holds the cached ordinals in two paralel {@code int[]} arrays. */
  public static final class CachedOrds {
    
    public final int[] offsets;
    public final int[] ordinals;

    /**
     * Creates a new {@link CachedOrds} from the {@link BinaryDocValues}.
     * Assumes that the {@link BinaryDocValues} is not {@code null}.
     */
    public CachedOrds(BinaryDocValues dv, int maxDoc, CategoryListParams clp) {
      final BytesRef buf = new BytesRef();

      offsets = new int[maxDoc + 1];
      int[] ords = new int[maxDoc]; // let's assume one ordinal per-document as an initial size

      // this aggregator is limited to Integer.MAX_VALUE total ordinals.
      int totOrds = 0;
      final IntDecoder decoder = clp.createEncoder().createMatchingDecoder();
      final IntsRef values = new IntsRef(32);
      for (int docID = 0; docID < maxDoc; docID++) {
        offsets[docID] = totOrds;
        dv.get(docID, buf);
        if (buf.length > 0) {
          // this document has facets
          decoder.decode(buf, values);
          if (totOrds + values.length >= ords.length) {
            ords = ArrayUtil.grow(ords, totOrds + values.length + 1);
          }
          for (int i = 0; i < values.length; i++) {
            ords[totOrds++] = values.ints[i];
          }
        }
      }
      offsets[maxDoc] = totOrds;
      
      // if ords array is bigger by more than 10% of what we really need, shrink it
      if ((double) totOrds / ords.length < 0.9) { 
        this.ordinals = new int[totOrds];
        System.arraycopy(ords, 0, this.ordinals, 0, totOrds);
      } else {
        this.ordinals = ords;
      }
    }
  }

  private static final Map<BinaryDocValues,CachedOrds> intsCache = new WeakHashMap<BinaryDocValues,CachedOrds>();
  
  /**
   * Returns the {@link CachedOrds} relevant to the given
   * {@link AtomicReaderContext}, or {@code null} if there is no
   * {@link BinaryDocValues} in this reader for the requested
   * {@link CategoryListParams#field}.
   */
  public static synchronized CachedOrds getCachedOrds(AtomicReaderContext context, CategoryListParams clp) throws IOException {
    BinaryDocValues dv = context.reader().getBinaryDocValues(clp.field);
    if (dv == null) {
      return null;
    }
    CachedOrds ci = intsCache.get(dv);
    if (ci == null) {
      ci = new CachedOrds(dv, context.reader().maxDoc(), clp);
      intsCache.put(dv, ci);
    }
    return ci;
  }

}
