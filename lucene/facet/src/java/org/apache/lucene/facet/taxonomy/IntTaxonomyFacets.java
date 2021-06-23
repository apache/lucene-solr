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
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig.DimConfig;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.TopOrdAndIntQueue;

import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;

/** Base class for all taxonomy-based facets that aggregate
 *  to a per-ords int[]. */

public abstract class IntTaxonomyFacets extends TaxonomyFacets {

  /** Per-ordinal value. */
  private final int[] values;
  private final IntIntScatterMap sparseValues;

  /** Sole constructor. */
  protected IntTaxonomyFacets(String indexFieldName, TaxonomyReader taxoReader, FacetsConfig config, FacetsCollector fc) throws IOException {
    super(indexFieldName, taxoReader, config);

    if (useHashTable(fc, taxoReader)) {
      sparseValues = new IntIntScatterMap();
      values = null;
    } else {
      sparseValues = null;
      values = new int[taxoReader.getSize()];
    }
  }

  /** Return true if a sparse hash table should be used for counting, instead of a dense int[]. */
  protected boolean useHashTable(FacetsCollector fc, TaxonomyReader taxoReader) {
    if (taxoReader.getSize() < 1024) {
      // small number of unique values: use an array
      return false;
    }

    if (fc == null) {
      // counting all docs: use an array
      return false;
    }
    
    int maxDoc = 0;
    int sumTotalHits = 0;
    for (MatchingDocs docs : fc.getMatchingDocs()) {
      sumTotalHits += docs.totalHits;
      maxDoc += docs.context.reader().maxDoc();
    }

    // if our result set is < 10% of the index, we collect sparsely (use hash map):
    return sumTotalHits < maxDoc/10;
  }

  /** Increment the count for this ordinal by 1. */
  protected void increment(int ordinal) {
    increment(ordinal, 1);
  }

  /** Increment the count for this ordinal by {@code amount}.. */
  protected void increment(int ordinal, int amount) {
    if (sparseValues != null) {
      sparseValues.addTo(ordinal, amount);
    } else {
      values[ordinal] += amount;
    }
  }

  /** Get the count for this ordinal. */
  protected int getValue(int ordinal) {
    if (sparseValues != null) {
      return sparseValues.get(ordinal);
    } else {
      return values[ordinal];
    }
  }

  /** Rolls up any single-valued hierarchical dimensions. */
  protected void rollup() throws IOException {
    // Rollup any necessary dims:
    int[] children = null;
    for(Map.Entry<String,DimConfig> ent : config.getDimConfigs().entrySet()) {
      String dim = ent.getKey();
      DimConfig ft = ent.getValue();
      if (ft.hierarchical && ft.multiValued == false) {
        int dimRootOrd = taxoReader.getOrdinal(new FacetLabel(dim));
        // It can be -1 if this field was declared in the
        // config but never indexed:
        if (dimRootOrd > 0) {
          if (children == null) {
            // lazy init
            children = getChildren();
          }
          increment(dimRootOrd, rollup(children[dimRootOrd]));
        }
      }
    }
  }

  private int rollup(int ord) throws IOException {
    int[] children = getChildren();
    int[] siblings = getSiblings();
    int sum = 0;
    while (ord != TaxonomyReader.INVALID_ORDINAL) {
      increment(ord, rollup(children[ord]));
      sum += getValue(ord);
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
    return getValue(ord);
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

    int totValue = 0;
    int childCount = 0;

    TopOrdAndIntQueue.OrdAndValue reuse = null;

    // TODO: would be faster if we had a "get the following children" API?  then we
    // can make a single pass over the hashmap

    if (sparseValues != null) {
      for (IntIntCursor c : sparseValues) {
        int count = c.value;
        int ord = c.key;
        if (parents[ord] == dimOrd && count > 0) {
          totValue += count;
          childCount++;
          if (count > bottomValue) {
            if (reuse == null) {
              reuse = new TopOrdAndIntQueue.OrdAndValue();
            }
            reuse.ord = ord;
            reuse.value = count;
            reuse = q.insertWithOverflow(reuse);
            if (q.size() == topN) {
              bottomValue = q.top().value;
            }
          }
        }
      }
    } else {
      int[] children = getChildren();
      int[] siblings = getSiblings();
      int ord = children[dimOrd];
      while(ord != TaxonomyReader.INVALID_ORDINAL) {
        int value = values[ord];
        if (value > 0) {
          totValue += value;
          childCount++;
          if (value > bottomValue) {
            if (reuse == null) {
              reuse = new TopOrdAndIntQueue.OrdAndValue();
            }
            reuse.ord = ord;
            reuse.value = value;
            reuse = q.insertWithOverflow(reuse);
            if (q.size() == topN) {
              bottomValue = q.top().value;
            }
          }
        }

        ord = siblings[ord];
      }
    }

    if (totValue == 0) {
      return null;
    }

    if (dimConfig.multiValued) {
      if (dimConfig.requireDimCount) {
        totValue = getValue(dimOrd);
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
