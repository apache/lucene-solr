package org.apache.lucene.facet;

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
import java.util.Map;

import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

/** Base class for all taxonomy-based facets that aggregate
 *  to a per-ords float[]. */

public abstract class FloatTaxonomyFacets extends TaxonomyFacets {

  protected final float[] values;

  protected FloatTaxonomyFacets(String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config) throws IOException {
    super(indexFieldName, taxoReader, config);
    values = new float[taxoReader.getSize()];
  }
  
  protected void rollup() throws IOException {
    // Rollup any necessary dims:
    for(Map.Entry<String,FacetsConfig.DimConfig> ent : config.getDimConfigs().entrySet()) {
      String dim = ent.getKey();
      FacetsConfig.DimConfig ft = ent.getValue();
      if (ft.hierarchical && ft.multiValued == false) {
        int dimRootOrd = taxoReader.getOrdinal(new FacetLabel(dim));
        assert dimRootOrd > 0;
        values[dimRootOrd] += rollup(children[dimRootOrd]);
      }
    }
  }

  private float rollup(int ord) {
    float sum = 0;
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      float childValue = values[ord] + rollup(children[ord]);
      values[ord] = childValue;
      sum += childValue;
      ord = siblings[ord];
    }
    return sum;
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    verifyDim(dim);
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
    FacetsConfig.DimConfig dimConfig = verifyDim(dim);
    FacetLabel cp = new FacetLabel(dim, path);
    int dimOrd = taxoReader.getOrdinal(cp);
    if (dimOrd == -1) {
      return null;
    }

    TopOrdAndFloatQueue q = new TopOrdAndFloatQueue(Math.min(taxoReader.getSize(), topN));
    float bottomValue = 0;

    int ord = children[dimOrd];
    float sumValues = 0;
    int childCount = 0;

    TopOrdAndFloatQueue.OrdAndValue reuse = null;
    while(ord != TaxonomyReader.INVALID_ORDINAL) {
      if (values[ord] > 0) {
        sumValues += values[ord];
        childCount++;
        if (values[ord] > bottomValue) {
          if (reuse == null) {
            reuse = new TopOrdAndFloatQueue.OrdAndValue();
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

    if (sumValues == 0) {
      return null;
    }

    if (dimConfig.multiValued) {
      if (dimConfig.requireDimCount) {
        sumValues = values[dimOrd];
      } else {
        // Our sum'd count is not correct, in general:
        sumValues = -1;
      }
    } else {
      // Our sum'd dim count is accurate, so we keep it
    }

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    for(int i=labelValues.length-1;i>=0;i--) {
      TopOrdAndFloatQueue.OrdAndValue ordAndValue = q.pop();
      FacetLabel child = taxoReader.getPath(ordAndValue.ord);
      labelValues[i] = new LabelAndValue(child.components[cp.length], ordAndValue.value);
    }

    return new FacetResult(dim, path, sumValues, labelValues, childCount);
  }
}