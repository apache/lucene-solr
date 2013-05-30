package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.facet.encoding.DGapVInt8IntDecoder;
import org.apache.lucene.facet.encoding.DGapVInt8IntEncoder;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;

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
 * A {@link FacetsAggregator} which counts the number of times each category
 * appears in the given set of documents. This aggregator reads the categories
 * from the {@link BinaryDocValues} field defined by
 * {@link CategoryListParams#field}, and assumes that the category ordinals were
 * encoded with {@link DGapVInt8IntEncoder}.
 * 
 * @lucene.experimental
 */
public final class FastCountingFacetsAggregator extends IntRollupFacetsAggregator {
  
  private final BytesRef buf = new BytesRef(32);
  
  /**
   * Asserts that this {@link FacetsCollector} can handle the given
   * {@link FacetSearchParams}. Returns {@code null} if true, otherwise an error
   * message.
   */
  final static boolean verifySearchParams(FacetSearchParams fsp) {
    // verify that all category lists were encoded with DGapVInt
    for (FacetRequest fr : fsp.facetRequests) {
      CategoryListParams clp = fsp.indexingParams.getCategoryListParams(fr.categoryPath);
      if (clp.createEncoder().createMatchingDecoder().getClass() != DGapVInt8IntDecoder.class) {
        return false;
      }
    }
    
    return true;
  }

  @Override
  public final void aggregate(MatchingDocs matchingDocs, CategoryListParams clp, FacetArrays facetArrays) 
      throws IOException {
    assert clp.createEncoder().createMatchingDecoder().getClass() == DGapVInt8IntDecoder.class 
        : "this aggregator assumes ordinals were encoded as dgap+vint";
    
    final BinaryDocValues dv = matchingDocs.context.reader().getBinaryDocValues(clp.field);
    if (dv == null) { // this reader does not have DocValues for the requested category list
      return;
    }
    
    final int length = matchingDocs.bits.length();
    final int[] counts = facetArrays.getIntArray();
    int doc = 0;
    while (doc < length && (doc = matchingDocs.bits.nextSetBit(doc)) != -1) {
      dv.get(doc, buf);
      if (buf.length > 0) {
        // this document has facets
        final int upto = buf.offset + buf.length;
        int ord = 0;
        int offset = buf.offset;
        int prev = 0;
        while (offset < upto) {
          byte b = buf.bytes[offset++];
          if (b >= 0) {
            prev = ord = ((ord << 7) | b) + prev;
            assert ord < counts.length: "ord=" + ord + " vs maxOrd=" + counts.length;
            ++counts[ord];
            ord = 0;
          } else {
            ord = (ord << 7) | (b & 0x7F);
          }
        }
      }
      ++doc;
    }
  }
  
}
