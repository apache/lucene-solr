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
import java.util.Map;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.TopOrdAndIntQueue;

/** Base class for all taxonomy-based facets that aggregate
 *  to a per-ords int[]. */

public abstract class IntTaxonomyFacets extends TaxonomyFacets {

  /** Per-ordinal value. */
  protected final int[] values;

  /** Sole constructor. */
  protected IntTaxonomyFacets(String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config) throws IOException {
    super(indexFieldName, taxoReader, config);
    values = new int[taxoReader.getSize()];
  }
  
  /** Rolls up any single-valued hierarchical dimensions. */
  protected void rollup() throws IOException {
    // Rollup any necessary dims:
    for(Map.Entry<String,DimConfig> ent : config.getDimConfigs().entrySet()) {
      String dim = ent.getKey();
      DimConfig ft = ent.getValue();
      if (ft.hierarchical && ft.multiValued == false) {
        int dimRootOrd = taxoReader.getOrdinal(new FacetLabel(dim));
        // It can be -1 if this field was declared in the
        // config but never indexed:
        if (dimRootOrd > 0) {
          values[dimRootOrd] += rollup(children[dimRootOrd]);
        }
      }
    }
  }

  private int rollup(int ord) {
    int sum = 0;
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      int childValue = values[ord] + rollup(children[ord]);
      values[ord] = childValue;
      sum += childValue;
      ord = siblings[ord];
    }
    return sum;
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    DimConfig dimConfig = verifyDim(dim);
    if (path.length == 0) {
      if (dimConfig.hierarchical && dimConfig.multiValued == false) {
        // ok: rolled up at search time
      } else if (dimConfig.requireDimCount && dimConfig.multiValued) {
        // ok: we indexed all ords at index time
      } else {
        throw new IllegalArgumentException("cannot return dimension-level value alone; use getTopChildren instead");
      }
    }
    int ord = taxoReader.getOrdinal(new FacetLabel(dim, path));
    if (ord < 0) {
      return -1;
    }
    return values[ord];
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    if (topN <= 0) {
      throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
    }
    DimConfig dimConfig = verifyDim(dim);
    FacetLabel cp = new FacetLabel(dim, path);
    int dimOrd = taxoReader.getOrdinal(cp);
    if (dimOrd == -1) {
      return null;
    }

    TopOrdAndIntQueue q = new TopOrdAndIntQueue(Math.min(taxoReader.getSize(), topN));
    
    int bottomValue = 0;

    int ord = children[dimOrd];
    int totValue = 0;
    int childCount = 0;

    TopOrdAndIntQueue.OrdAndValue reuse = null;
    while(ord != TaxonomyReader.INVALID_ORDINAL) {
      if (values[ord] > 0) {
        totValue += values[ord];
        childCount++;
        if (values[ord] > bottomValue) {
          if (reuse == null) {
            reuse = new TopOrdAndIntQueue.OrdAndValue();
          }
          reuse.ord = ord;
          reuse.value = values[ord];
          reuse = q.insertWithOverflow(reuse);
          if (q.size() == topN) {
            bottomValue = q.top().value;
          }
        }
      }

      ord = siblings[ord];
    }

    if (totValue == 0) {
      return null;
    }

    if (dimConfig.multiValued) {
      if (dimConfig.requireDimCount) {
        totValue = values[dimOrd];
      } else {
        // Our sum'd value is not correct, in general:
        totValue = -1;
      }
    } else {
      // Our sum'd dim value is accurate, so we keep it
    }

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    for(int i=labelValues.length-1;i>=0;i--) {
      TopOrdAndIntQueue.OrdAndValue ordAndValue = q.pop();
      FacetLabel child = taxoReader.getPath(ordAndValue.ord);
      labelValues[i] = new LabelAndValue(child.components[cp.length], ordAndValue.value);
    }

    return new FacetResult(dim, path, totValue, labelValues, childCount);
  }
}