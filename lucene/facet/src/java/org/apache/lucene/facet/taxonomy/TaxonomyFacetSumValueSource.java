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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.IntsRef;

/** Aggregates sum of values from {@link
 *  FunctionValues#doubleVal}, for each facet label.
 *
 *  @lucene.experimental */
public class TaxonomyFacetSumValueSource extends FloatTaxonomyFacets {
  private final OrdinalsReader ordinalsReader;

  /** Aggreggates float facet values from the provided
   *  {@link ValueSource}, pulling ordinals using {@link
   *  DocValuesOrdinalsReader} against the default indexed
   *  facet field {@link
   *  FacetsConfig#DEFAULT_INDEX_FIELD_NAME}. */
  public TaxonomyFacetSumValueSource(TaxonomyReader taxoReader, FacetsConfig config,
                                     FacetsCollector fc, ValueSource valueSource) throws IOException {
    this(new DocValuesOrdinalsReader(FacetsConfig.DEFAULT_INDEX_FIELD_NAME), taxoReader, config, fc, valueSource);
  }

  /** Aggreggates float facet values from the provided
   *  {@link ValueSource}, and pulls ordinals from the
   *  provided {@link OrdinalsReader}. */
  public TaxonomyFacetSumValueSource(OrdinalsReader ordinalsReader, TaxonomyReader taxoReader,
                                     FacetsConfig config, FacetsCollector fc, ValueSource valueSource) throws IOException {
    super(ordinalsReader.getIndexFieldName(), taxoReader, config);
    this.ordinalsReader = ordinalsReader;
    sumValues(fc.getMatchingDocs(), fc.getKeepScores(), valueSource);
  }

  private final void sumValues(List<MatchingDocs> matchingDocs, boolean keepScores, ValueSource valueSource) throws IOException {
    final FakeScorer scorer = new FakeScorer();
    Map<String, Scorer> context = new HashMap<>();
    if (keepScores) {
      context.put("scorer", scorer);
    }
    IntsRef scratch = new IntsRef();
    for(MatchingDocs hits : matchingDocs) {
      OrdinalsReader.OrdinalsSegmentReader ords = ordinalsReader.getReader(hits.context);
      
      int scoresIdx = 0;
      float[] scores = hits.scores;

      FunctionValues functionValues = valueSource.getValues(context, hits.context);
      DocIdSetIterator docs = hits.bits.iterator();
      
      int doc;
      while ((doc = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        ords.get(doc, scratch);
        if (keepScores) {
          scorer.doc = doc;
          scorer.score = scores[scoresIdx++];
        }
        float value = (float) functionValues.doubleVal(doc);
        for(int i=0;i<scratch.length;i++) {
          values[scratch.ints[i]] += value;
        }
      }
    }

    rollup();
  }

  /** {@link ValueSource} that returns the score for each
   *  hit; use this to aggregate the sum of all hit scores
   *  for each facet label.  */
  public static class ScoreValueSource extends ValueSource {

    /** Sole constructor. */
    public ScoreValueSource() {
    }

    @Override
    public FunctionValues getValues(@SuppressWarnings("rawtypes") Map context, LeafReaderContext readerContext) throws IOException {
      final Scorer scorer = (Scorer) context.get("scorer");
      if (scorer == null) {
        throw new IllegalStateException("scores are missing; be sure to pass keepScores=true to FacetsCollector");
      }
      return new DoubleDocValues(this) {
        @Override
        public double doubleVal(int document) {
          try {
            return scorer.score();
          } catch (IOException exception) {
            throw new RuntimeException(exception);
          }
        }
      };
    }
    
    @Override public boolean equals(Object o) { return o == this; }
    @Override public int hashCode() { return System.identityHashCode(this); }
    @Override public String description() { return "score()"; }
  }
  
}
