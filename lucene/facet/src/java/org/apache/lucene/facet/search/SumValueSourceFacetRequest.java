package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.search.OrdinalValueResolver.FloatValueResolver;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.IntsRef;

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

/**
 * A {@link FacetRequest} which aggregates categories by the sum of the values,
 * returned by a {@link ValueSource}, in the documents they are associated with.
 * This allows aggregating the value of a category by e.g. summing the value of
 * a {@link NumericDocValuesField} indexed for the document, or a more complex
 * expression (from multiple fields) using the expressions module.
 * 
 * @lucene.experimental
 */
public class SumValueSourceFacetRequest extends FacetRequest {

  private static abstract class SumValueSourceFacetsAggregator implements FacetsAggregator {
    
    protected final ValueSource valueSource;
    protected final IntsRef ordinals = new IntsRef(32);
    
    protected SumValueSourceFacetsAggregator(ValueSource valueSource) {
      this.valueSource = valueSource;
    }

    private float doRollup(int ordinal, int[] children, int[] siblings, float[] values) {
      float value = 0f;
      while (ordinal != TaxonomyReader.INVALID_ORDINAL) {
        float childValue = values[ordinal];
        childValue += doRollup(children[ordinal], children, siblings, values);
        values[ordinal] = childValue;
        value += childValue;
        ordinal = siblings[ordinal];
      }
      return value;
    }

    @Override
    public void rollupValues(FacetRequest fr, int ordinal, int[] children, int[] siblings, FacetArrays facetArrays) {
      float[] values = facetArrays.getFloatArray();
      values[ordinal] += doRollup(children[ordinal], children, siblings, values);
    }

    @Override
    public OrdinalValueResolver createOrdinalValueResolver(FacetRequest facetRequest, FacetArrays arrays) {
      return new FloatValueResolver(arrays);
    }
    
  }
  
  private static class ScoreValueSourceFacetsAggregator extends SumValueSourceFacetsAggregator {

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

    ScoreValueSourceFacetsAggregator(ValueSource valueSource) {
      super(valueSource);
    }

    @Override
    public void aggregate(MatchingDocs matchingDocs, CategoryListParams clp, FacetArrays facetArrays) throws IOException {
      final CategoryListIterator cli = clp.createCategoryListIterator(0);
      if (!cli.setNextReader(matchingDocs.context)) {
        return;
      }

      assert matchingDocs.scores != null;

      final FakeScorer scorer = new FakeScorer();
      Map<String, Scorer> context = new HashMap<String, Scorer>();
      context.put("scorer", scorer);

      final FunctionValues fvalues = valueSource.getValues(context, matchingDocs.context);
      final int length = matchingDocs.bits.length();
      final float[] aggValues = facetArrays.getFloatArray();
      int doc = 0;
      int scoresIdx = 0;
      while (doc < length && (doc = matchingDocs.bits.nextSetBit(doc)) != -1) {
        scorer.docID = doc;
        scorer.score = matchingDocs.scores[scoresIdx++];
        cli.getOrdinals(doc, ordinals);
        final int upto = ordinals.offset + ordinals.length;
        float val = (float) fvalues.doubleVal(doc);
        for (int i = ordinals.offset; i < upto; i++) {
          aggValues[ordinals.ints[i]] += val;
        }
        ++doc;
      }
    }

    @Override
    public boolean requiresDocScores() {
      return true;
    }
  }

  private static class NoScoreValueSourceFacetsAggregator extends SumValueSourceFacetsAggregator {

    NoScoreValueSourceFacetsAggregator(ValueSource valueSource) {
      super(valueSource);
    }

    @Override
    public void aggregate(MatchingDocs matchingDocs, CategoryListParams clp, FacetArrays facetArrays) throws IOException {
      final CategoryListIterator cli = clp.createCategoryListIterator(0);
      if (!cli.setNextReader(matchingDocs.context)) {
        return;
      }

      final FunctionValues fvalues = valueSource.getValues(Collections.emptyMap(), matchingDocs.context);
      final int length = matchingDocs.bits.length();
      final float[] aggValues = facetArrays.getFloatArray();
      int doc = 0;
      while (doc < length && (doc = matchingDocs.bits.nextSetBit(doc)) != -1) {
        cli.getOrdinals(doc, ordinals);
        final int upto = ordinals.offset + ordinals.length;
        float val = (float) fvalues.doubleVal(doc);
        for (int i = ordinals.offset; i < upto; i++) {
          aggValues[ordinals.ints[i]] += val;
        }
        ++doc;
      }
    }

    @Override
    public boolean requiresDocScores() {
      return false;
    }
  }

  private final ValueSource valueSource;
  private final boolean requiresDocScores;

  /**
   * Constructor which takes the {@link ValueSource} from which to read the
   * documents' values. You can also specify if the value source requires
   * document scores or not.
   */
  public SumValueSourceFacetRequest(CategoryPath path, int num, ValueSource valueSource, boolean requiresDocScores) {
    super(path, num);
    this.valueSource = valueSource;
    this.requiresDocScores = requiresDocScores;
  }

  @Override
  public FacetsAggregator createFacetsAggregator(FacetIndexingParams fip) {
    if (requiresDocScores) {
      return new ScoreValueSourceFacetsAggregator(valueSource);
    } else {
      return new NoScoreValueSourceFacetsAggregator(valueSource);
    }
  }
  
}
