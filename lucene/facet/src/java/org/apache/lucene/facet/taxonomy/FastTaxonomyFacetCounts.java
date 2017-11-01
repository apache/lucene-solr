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
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Computes facets counts, assuming the default encoding
 *  into DocValues was used.
 *
 * @lucene.experimental */
public class FastTaxonomyFacetCounts extends IntTaxonomyFacets {

  /** Create {@code FastTaxonomyFacetCounts}, which also
   *  counts all facet labels. */
  public FastTaxonomyFacetCounts(TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc) throws IOException {
    this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc);
  }

  /** Create {@code FastTaxonomyFacetCounts}, using the
   *  specified {@code indexFieldName} for ordinals.  Use
   *  this if you had set {@link
   *  FacetsConfig#setIndexFieldName} to change the index
   *  field name for certain dimensions. */
  public FastTaxonomyFacetCounts(String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc) throws IOException {
    super(indexFieldName, taxoReader, config, fc);
    count(fc.getMatchingDocs());
  }

  /** Create {@code FastTaxonomyFacetCounts}, using the
   *  specified {@code indexFieldName} for ordinals, and
   *  counting all non-deleted documents in the index.  This is 
   *  the same result as searching on {@link MatchAllDocsQuery},
   *  but faster */
  public FastTaxonomyFacetCounts(String indexFieldName, IndexReader reader, TaxonomyReader taxoReader, FacetsConfig config) throws IOException {
    super(indexFieldName, taxoReader, config, null);
    countAll(reader);
  }

  private final void count(List<MatchingDocs> matchingDocs) throws IOException {
    for(MatchingDocs hits : matchingDocs) {
      BinaryDocValues dv = hits.context.reader().getBinaryDocValues(indexFieldName);
      if (dv == null) { // this reader does not have DocValues for the requested category list
        continue;
      }

      DocIdSetIterator it = ConjunctionDISI.intersectIterators(Arrays.asList(
          hits.bits.iterator(), dv));
      
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        final BytesRef bytesRef = dv.binaryValue();
        byte[] bytes = bytesRef.bytes;
        int end = bytesRef.offset + bytesRef.length;
        int ord = 0;
        int offset = bytesRef.offset;
        int prev = 0;
        while (offset < end) {
          byte b = bytes[offset++];
          if (b >= 0) {
            prev = ord = ((ord << 7) | b) + prev;
            increment(ord);
            ord = 0;
          } else {
            ord = (ord << 7) | (b & 0x7F);
          }
        }
      }
    }

    rollup();
  }

  private final void countAll(IndexReader reader) throws IOException {
    for(LeafReaderContext context : reader.leaves()) {
      BinaryDocValues dv = context.reader().getBinaryDocValues(indexFieldName);
      if (dv == null) { // this reader does not have DocValues for the requested category list
        continue;
      }

      Bits liveDocs = context.reader().getLiveDocs();

      for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
        if (liveDocs != null && liveDocs.get(doc) == false) {
          continue;
        }
        final BytesRef bytesRef = dv.binaryValue();
        byte[] bytes = bytesRef.bytes;
        int end = bytesRef.offset + bytesRef.length;
        int ord = 0;
        int offset = bytesRef.offset;
        int prev = 0;
        while (offset < end) {
          byte b = bytes[offset++];
          if (b >= 0) {
            prev = ord = ((ord << 7) | b) + prev;
            increment(ord);
            ord = 0;
          } else {
            ord = (ord << 7) | (b & 0x7F);
          }
        }
      }
    }

    rollup();
  }
}
