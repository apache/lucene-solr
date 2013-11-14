package org.apache.lucene.facet.simple;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.facet.simple.SimpleFacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

public class TaxonomyFacetCounts extends Facets {
  private final FacetsConfig facetsConfig;
  private final TaxonomyReader taxoReader;
  private final int[] counts;
  private final String facetsFieldName;
  private final int[] children;
  private final int[] parents;
  private final int[] siblings;

  public TaxonomyFacetCounts(TaxonomyReader taxoReader, FacetsConfig facetsConfig, SimpleFacetsCollector fc) throws IOException {
    this(FacetsConfig.DEFAULT_INDEXED_FIELD_NAME, taxoReader, facetsConfig, fc);
  }

  public TaxonomyFacetCounts(String facetsFieldName, TaxonomyReader taxoReader, FacetsConfig facetsConfig, SimpleFacetsCollector fc) throws IOException {
    this.taxoReader = taxoReader;
    this.facetsFieldName = facetsFieldName;
    this.facetsConfig = facetsConfig;
    ParallelTaxonomyArrays pta = taxoReader.getParallelTaxonomyArrays();
    children = pta.children();
    parents = pta.parents();
    siblings = pta.siblings();
    counts = new int[taxoReader.getSize()];
    count(fc.getMatchingDocs());
  }

  private final void count(List<MatchingDocs> matchingDocs) throws IOException {
    //System.out.println("count matchingDocs=" + matchingDocs + " facetsField=" + facetsFieldName);
    for(MatchingDocs hits : matchingDocs) {
      BinaryDocValues dv = hits.context.reader().getBinaryDocValues(facetsFieldName);
      if (dv == null) { // this reader does not have DocValues for the requested category list
        continue;
      }
      FixedBitSet bits = hits.bits;
    
      final int length = hits.bits.length();
      int doc = 0;
      BytesRef scratch = new BytesRef();
      //System.out.println("count seg=" + hits.context.reader());
      while (doc < length && (doc = bits.nextSetBit(doc)) != -1) {
        //System.out.println("  doc=" + doc);
        dv.get(doc, scratch);
        byte[] bytes = scratch.bytes;
        int end = scratch.offset + scratch.length;
        int ord = 0;
        int offset = scratch.offset;
        int prev = 0;
        while (offset < end) {
          byte b = bytes[offset++];
          if (b >= 0) {
            prev = ord = ((ord << 7) | b) + prev;
            assert ord < counts.length: "ord=" + ord + " vs maxOrd=" + counts.length;
            ++counts[ord];
            ord = 0;
          } else {
            ord = (ord << 7) | (b & 0x7F);
          }
        }
        ++doc;
      }
    }

    // nocommit we could do this lazily instead:

    // Rollup any necessary dims:
    for(Map.Entry<String,FacetsConfig.DimConfig> ent : facetsConfig.getDimConfigs().entrySet()) {
      String dim = ent.getKey();
      FacetsConfig.DimConfig ft = ent.getValue();
      if (ft.hierarchical && ft.multiValued == false) {
        int dimRootOrd = taxoReader.getOrdinal(new FacetLabel(dim));
        // It can be -1 if this field was declared in the
        // facetsConfig but never indexed:
        if (dimRootOrd > 0) {
          counts[dimRootOrd] += rollup(children[dimRootOrd]);
        }
      }
    }
  }

  private int rollup(int ord) {
    int sum = 0;
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      int childValue = counts[ord] + rollup(children[ord]);
      counts[ord] = childValue;
      sum += childValue;
      ord = siblings[ord];
    }
    return sum;
  }

  /** Return the count for a specific path.  Returns -1 if
   *  this path doesn't exist, else the count. */
  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    int ord = taxoReader.getOrdinal(FacetLabel.create(dim, path));
    if (ord < 0) {
      return -1;
    }
    return counts[ord];
  }

  @Override
  public SimpleFacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    FacetLabel cp = FacetLabel.create(dim, path);
    int ord = taxoReader.getOrdinal(cp);
    if (ord == -1) {
      //System.out.println("no ord for path=" + path);
      return null;
    }
    return getTopChildren(cp, ord, topN);
  }

  private SimpleFacetResult getTopChildren(FacetLabel path, int dimOrd, int topN) throws IOException {

    TopOrdCountQueue q = new TopOrdCountQueue(topN);
    
    int bottomCount = 0;

    int ord = children[dimOrd];
    int totCount = 0;

    TopOrdCountQueue.OrdAndCount reuse = null;
    while(ord != TaxonomyReader.INVALID_ORDINAL) {
      if (counts[ord] > 0) {
        totCount += counts[ord];
        if (counts[ord] > bottomCount) {
          if (reuse == null) {
            reuse = new TopOrdCountQueue.OrdAndCount();
          }
          reuse.ord = ord;
          reuse.count = counts[ord];
          reuse = q.insertWithOverflow(reuse);
          if (q.size() == topN) {
            bottomCount = q.top().count;
          }
        }
      }

      ord = siblings[ord];
    }

    if (totCount == 0) {
      //System.out.println("totCount=0 for path=" + path);
      return null;
    }

    FacetsConfig.DimConfig ft = facetsConfig.getDimConfig(path.components[0]);
    if (ft.hierarchical && ft.multiValued) {
      totCount = counts[dimOrd];
    }

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    for(int i=labelValues.length-1;i>=0;i--) {
      TopOrdCountQueue.OrdAndCount ordAndCount = q.pop();
      FacetLabel child = taxoReader.getPath(ordAndCount.ord);
      labelValues[i] = new LabelAndValue(child.components[path.length], ordAndCount.count);
    }

    return new SimpleFacetResult(path, totCount, labelValues);
  }

  @Override
  public List<SimpleFacetResult> getAllDims(int topN) throws IOException {
    int ord = children[TaxonomyReader.ROOT_ORDINAL];
    List<SimpleFacetResult> results = new ArrayList<SimpleFacetResult>();
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      SimpleFacetResult result = getTopChildren(taxoReader.getPath(ord), ord, topN);
      if (result != null) {
        results.add(result);
      }
      ord = siblings[ord];
    }

    // Sort by highest count:
    Collections.sort(results,
                     new Comparator<SimpleFacetResult>() {
                       @Override
                       public int compare(SimpleFacetResult a, SimpleFacetResult b) {
                         if (a.value.intValue() > b.value.intValue()) {
                           return -1;
                         } else if (b.value.intValue() > a.value.intValue()) {
                           return 1;
                         } else {
                           // Tie break by dimension
                           return a.path.components[0].compareTo(b.path.components[0]);
                         }
                       }
                     });

    return results;
  }
}
