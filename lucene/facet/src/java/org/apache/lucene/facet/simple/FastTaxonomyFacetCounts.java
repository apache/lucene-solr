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
import java.util.List;
import java.util.Map;

import org.apache.lucene.facet.simple.SimpleFacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

// nocommit jdoc that this assumes/requires the default encoding
public class FastTaxonomyFacetCounts extends TaxonomyFacets {
  private final int[] counts;

  public FastTaxonomyFacetCounts(TaxonomyReader taxoReader, FacetsConfig config, SimpleFacetsCollector fc) throws IOException {
    this(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, taxoReader, config, fc);
  }

  public FastTaxonomyFacetCounts(String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config, SimpleFacetsCollector fc) throws IOException {
    super(indexFieldName, taxoReader, config);
    counts = new int[taxoReader.getSize()];
    count(fc.getMatchingDocs());
  }

  private final void count(List<MatchingDocs> matchingDocs) throws IOException {
    for(MatchingDocs hits : matchingDocs) {
      BinaryDocValues dv = hits.context.reader().getBinaryDocValues(indexFieldName);
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
            //System.out.println("    ord=" + ord);
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
    for(Map.Entry<String,FacetsConfig.DimConfig> ent : config.getDimConfigs().entrySet()) {
      String dim = ent.getKey();
      FacetsConfig.DimConfig ft = ent.getValue();
      if (ft.hierarchical && ft.multiValued == false) {
        int dimRootOrd = taxoReader.getOrdinal(new FacetLabel(dim));
        // It can be -1 if this field was declared in the
        // config but never indexed:
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
    verifyDim(dim);
    int ord = taxoReader.getOrdinal(FacetLabel.create(dim, path));
    if (ord < 0) {
      return -1;
    }
    return counts[ord];
  }

  @Override
  public SimpleFacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    if (topN <= 0) {
      throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
    }
    FacetsConfig.DimConfig dimConfig = verifyDim(dim);
    //System.out.println("ftfc.getTopChildren topN=" + topN);
    FacetLabel cp = FacetLabel.create(dim, path);
    int dimOrd = taxoReader.getOrdinal(cp);
    if (dimOrd == -1) {
      //System.out.println("no ord for dim=" + dim + " path=" + path);
      return null;
    }

    TopOrdAndIntQueue q = new TopOrdAndIntQueue(Math.min(taxoReader.getSize(), topN));
    
    int bottomCount = 0;

    int ord = children[dimOrd];
    int totCount = 0;
    int childCount = 0;

    TopOrdAndIntQueue.OrdAndValue reuse = null;
    while(ord != TaxonomyReader.INVALID_ORDINAL) {
      //System.out.println("  check ord=" + ord + " label=" + taxoReader.getPath(ord) + " topN=" + topN);
      if (counts[ord] > 0) {
        totCount += counts[ord];
        childCount++;
        if (counts[ord] > bottomCount) {
          if (reuse == null) {
            reuse = new TopOrdAndIntQueue.OrdAndValue();
          }
          reuse.ord = ord;
          reuse.value = counts[ord];
          reuse = q.insertWithOverflow(reuse);
          if (q.size() == topN) {
            bottomCount = q.top().value;
          }
        }
      }

      ord = siblings[ord];
    }

    if (totCount == 0) {
      //System.out.println("  no matches");
      return null;
    }

    if (dimConfig.multiValued) {
      if (dimConfig.requireDimCount) {
        totCount = counts[dimOrd];
      } else {
        // Our sum'd count is not correct, in general:
        totCount = -1;
      }
    } else {
      // Our sum'd dim count is accurate, so we keep it
    }

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    for(int i=labelValues.length-1;i>=0;i--) {
      TopOrdAndIntQueue.OrdAndValue ordAndValue = q.pop();
      FacetLabel child = taxoReader.getPath(ordAndValue.ord);
      labelValues[i] = new LabelAndValue(child.components[cp.length], ordAndValue.value);
    }

    return new SimpleFacetResult(totCount, labelValues, childCount);
  }
}
