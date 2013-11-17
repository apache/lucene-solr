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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.facet.simple.SimpleFacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/** Aggregates sum of values from a {@link ValueSource}, for
 *  each facet label. */

// nocommit jdoc that this assumes/requires the default encoding

public class TaxonomyFacetSumValueSource extends Facets {
  private final FacetsConfig facetsConfig;
  private final TaxonomyReader taxoReader;
  private final float[] values;
  private final String facetsFieldName;
  private final int[] children;
  private final int[] parents;
  private final int[] siblings;

  public TaxonomyFacetSumValueSource(TaxonomyReader taxoReader, FacetsConfig facetsConfig, SimpleFacetsCollector fc, ValueSource valueSource) throws IOException {
    this(FacetsConfig.DEFAULT_INDEXED_FIELD_NAME, taxoReader, facetsConfig, fc, valueSource);
  }

  public TaxonomyFacetSumValueSource(String facetsFieldName, TaxonomyReader taxoReader, FacetsConfig facetsConfig, SimpleFacetsCollector fc, ValueSource valueSource) throws IOException {
    this.taxoReader = taxoReader;
    this.facetsFieldName = facetsFieldName;
    this.facetsConfig = facetsConfig;
    ParallelTaxonomyArrays pta = taxoReader.getParallelTaxonomyArrays();
    children = pta.children();
    parents = pta.parents();
    siblings = pta.siblings();
    values = new float[taxoReader.getSize()];
    sumValues(fc.getMatchingDocs(), fc.getKeepScores(), valueSource);
  }

  private static final class FakeScorer extends Scorer {
    float score;
    int docID;
    FakeScorer() { super(null); }
    @Override public float score() throws IOException { return score; }
    @Override public int freq() throws IOException { throw new UnsupportedOperationException(); }
    @Override public int docID() { return docID; }
    @Override public int nextDoc() throws IOException { throw new UnsupportedOperationException(); }
    @Override public int advance(int target) throws IOException { throw new UnsupportedOperationException(); }
    @Override public long cost() { return 0; }
  }

  private final void sumValues(List<MatchingDocs> matchingDocs, boolean keepScores, ValueSource valueSource) throws IOException {
    final FakeScorer scorer = new FakeScorer();
    Map<String, Scorer> context = new HashMap<String, Scorer>();
    context.put("scorer", scorer);
    for(MatchingDocs hits : matchingDocs) {
      BinaryDocValues dv = hits.context.reader().getBinaryDocValues(facetsFieldName);
      if (dv == null) { // this reader does not have DocValues for the requested category list
        continue;
      }
      FixedBitSet bits = hits.bits;
    
      final int length = hits.bits.length();
      int doc = 0;
      int scoresIdx = 0;
      BytesRef scratch = new BytesRef();
      float[] scores = hits.scores;

      FunctionValues functionValues = valueSource.getValues(context, hits.context);
      while (doc < length && (doc = bits.nextSetBit(doc)) != -1) {
        dv.get(doc, scratch);
        if (keepScores) {
          scorer.docID = doc;
          scorer.score = scores[scoresIdx++];
        }
        byte[] bytes = scratch.bytes;
        int end = scratch.offset + scratch.length;
        int ord = 0;
        int offset = scratch.offset;
        int prev = 0;

        float value = (float) functionValues.doubleVal(doc);

        while (offset < end) {
          byte b = bytes[offset++];
          if (b >= 0) {
            prev = ord = ((ord << 7) | b) + prev;
            values[ord] += value;
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
    int ord = taxoReader.getOrdinal(FacetLabel.create(dim, path));
    if (ord < 0) {
      return -1;
    }
    return values[ord];
  }

  @Override
  public SimpleFacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    FacetLabel cp = FacetLabel.create(dim, path);
    int ord = taxoReader.getOrdinal(cp);
    if (ord == -1) {
      return null;
    }
    return getTopChildren(cp, ord, topN);
  }

  private SimpleFacetResult getTopChildren(FacetLabel path, int dimOrd, int topN) throws IOException {

    TopOrdValueQueue q = new TopOrdValueQueue(topN);
    
    float bottomValue = 0;

    int ord = children[dimOrd];
    float sumValues = 0;

    TopOrdValueQueue.OrdAndValue reuse = null;
    while(ord != TaxonomyReader.INVALID_ORDINAL) {
      if (values[ord] > 0) {
        sumValues += values[ord];
        if (values[ord] > bottomValue) {
          if (reuse == null) {
            reuse = new TopOrdValueQueue.OrdAndValue();
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

    FacetsConfig.DimConfig ft = facetsConfig.getDimConfig(path.components[0]);
    if (ft.hierarchical && ft.multiValued) {
      sumValues = values[dimOrd];
    }

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    for(int i=labelValues.length-1;i>=0;i--) {
      TopOrdValueQueue.OrdAndValue ordAndValue = q.pop();
      FacetLabel child = taxoReader.getPath(ordAndValue.ord);
      labelValues[i] = new LabelAndValue(child.components[path.length], ordAndValue.value);
    }

    return new SimpleFacetResult(path, sumValues, labelValues);
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
                         if (a.value.floatValue() > b.value.floatValue()) {
                           return -1;
                         } else if (b.value.floatValue() > a.value.floatValue()) {
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
