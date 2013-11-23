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
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;

/** Reads from any {@link OrdinalsReader}; use {@link
 *  FastTaxonomyFacetCounts} if you are just using the
 *  default encoding from {@link BinaryDocValues}. */

// nocommit remove & add specialized Cached variation only?
public class TaxonomyFacetCounts extends TaxonomyFacets {
  private final OrdinalsReader ordinalsReader;
  private final int[] counts;

  public TaxonomyFacetCounts(OrdinalsReader ordinalsReader, TaxonomyReader taxoReader, FacetsConfig config, SimpleFacetsCollector fc) throws IOException {
    super(ordinalsReader.getIndexFieldName(), taxoReader, config);
    this.ordinalsReader = ordinalsReader;
    counts = new int[taxoReader.getSize()];
    count(fc.getMatchingDocs());
  }

  private final void count(List<MatchingDocs> matchingDocs) throws IOException {
    IntsRef scratch  = new IntsRef();
    for(MatchingDocs hits : matchingDocs) {
      OrdinalsReader.OrdinalsSegmentReader ords = ordinalsReader.getReader(hits.context);
      FixedBitSet bits = hits.bits;
    
      final int length = hits.bits.length();
      int doc = 0;
      while (doc < length && (doc = bits.nextSetBit(doc)) != -1) {
        ords.get(doc, scratch);
        for(int i=0;i<scratch.length;i++) {
          counts[scratch.ints[scratch.offset+i]]++;
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
    FacetLabel cp = FacetLabel.create(dim, path);
    int dimOrd = taxoReader.getOrdinal(cp);
    if (dimOrd == -1) {
      //System.out.println("no ord for path=" + path);
      return null;
    }

    TopOrdAndIntQueue q = new TopOrdAndIntQueue(Math.min(taxoReader.getSize(), topN));
    
    int bottomCount = 0;

    int ord = children[dimOrd];
    int totCount = 0;
    int childCount = 0;

    TopOrdAndIntQueue.OrdAndValue reuse = null;
    while(ord != TaxonomyReader.INVALID_ORDINAL) {
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

    return new SimpleFacetResult(cp, totCount, labelValues, childCount);
  }
}
