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
import org.apache.lucene.util.IntsRef;

/** Aggregates sum of values from a {@link ValueSource}, for
 *  each facet label. */

public class TaxonomyFacetSumValueSource extends TaxonomyFacets {
  private final float[] values;
  private final OrdinalsReader ordinalsReader;

  /** Aggreggates float facet values from the provided
   *  {@link ValueSource}, pulling ordinals using {@link
   *  DocValuesOrdinalsReader} against the default indexed
   *  facet field {@link
   *  FacetsConfig#DEFAULT_INDEX_FIELD_NAME}. */
  public TaxonomyFacetSumValueSource(TaxonomyReader taxoReader, FacetsConfig config,
                                     SimpleFacetsCollector fc, ValueSource valueSource) throws IOException {
    this(new DocValuesOrdinalsReader(FacetsConfig.DEFAULT_INDEX_FIELD_NAME), taxoReader, config, fc, valueSource);
  }

  /** Aggreggates float facet values from the provided
   *  {@link ValueSource}, and pulls ordinals from the
   *  provided {@link OrdinalsReader}. */
  public TaxonomyFacetSumValueSource(OrdinalsReader ordinalsReader, TaxonomyReader taxoReader,
                                     FacetsConfig config, SimpleFacetsCollector fc, ValueSource valueSource) throws IOException {
    super(ordinalsReader.getIndexFieldName(), taxoReader, config);
    this.ordinalsReader = ordinalsReader;
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
    IntsRef scratch = new IntsRef();
    for(MatchingDocs hits : matchingDocs) {
      OrdinalsReader.OrdinalsSegmentReader ords = ordinalsReader.getReader(hits.context);
      FixedBitSet bits = hits.bits;
    
      final int length = hits.bits.length();
      int doc = 0;
      int scoresIdx = 0;
      float[] scores = hits.scores;

      FunctionValues functionValues = valueSource.getValues(context, hits.context);
      while (doc < length && (doc = bits.nextSetBit(doc)) != -1) {
        ords.get(doc, scratch);
        if (keepScores) {
          scorer.docID = doc;
          scorer.score = scores[scoresIdx++];
        }
        float value = (float) functionValues.doubleVal(doc);
        for(int i=0;i<scratch.length;i++) {
          values[scratch.ints[i]] += value;
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
    int ord = taxoReader.getOrdinal(FacetLabel.create(dim, path));
    if (ord < 0) {
      return -1;
    }
    return values[ord];
  }

  @Override
  public SimpleFacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    FacetsConfig.DimConfig dimConfig = verifyDim(dim);
    FacetLabel cp = FacetLabel.create(dim, path);
    int dimOrd = taxoReader.getOrdinal(cp);
    if (dimOrd == -1) {
      return null;
    }

    TopOrdAndFloatQueue q = new TopOrdAndFloatQueue(topN);
    
    float bottomValue = 0;

    int ord = children[dimOrd];
    float sumValues = 0;

    TopOrdAndFloatQueue.OrdAndValue reuse = null;
    while(ord != TaxonomyReader.INVALID_ORDINAL) {
      if (values[ord] > 0) {
        sumValues += values[ord];
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

    if (dimConfig.hierarchical && dimConfig.multiValued) {
      sumValues = values[dimOrd];
    }

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    for(int i=labelValues.length-1;i>=0;i--) {
      TopOrdAndFloatQueue.OrdAndValue ordAndValue = q.pop();
      FacetLabel child = taxoReader.getPath(ordAndValue.ord);
      labelValues[i] = new LabelAndValue(child.components[cp.length], ordAndValue.value);
    }

    return new SimpleFacetResult(cp, sumValues, labelValues);
  }
}
