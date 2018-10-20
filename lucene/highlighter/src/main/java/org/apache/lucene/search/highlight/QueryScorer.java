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
package org.apache.lucene.search.highlight;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanQuery;

/**
 * {@link Scorer} implementation which scores text fragments by the number of
 * unique query terms found. This class converts appropriate {@link Query}s to
 * {@link SpanQuery}s and attempts to score only those terms that participated in
 * generating the 'hit' on the document.
 */
public class QueryScorer implements Scorer {
  private float totalScore;
  private Set<String> foundTerms;
  private Map<String,WeightedSpanTerm> fieldWeightedSpanTerms;
  private float maxTermWeight;
  private int position = -1;
  private String defaultField;
  private CharTermAttribute termAtt;
  private PositionIncrementAttribute posIncAtt;
  private boolean expandMultiTermQuery = true;
  private Query query;
  private String field;
  private IndexReader reader;
  private boolean skipInitExtractor;
  private boolean wrapToCaching = true;
  private int maxCharsToAnalyze;
  private boolean usePayloads = false;

  /**
   * @param query Query to use for highlighting
   */
  public QueryScorer(Query query) {
    init(query, null, null, true);
  }

  /**
   * @param query Query to use for highlighting
   * @param field Field to highlight - pass null to ignore fields
   */
  public QueryScorer(Query query, String field) {
    init(query, field, null, true);
  }

  /**
   * @param query Query to use for highlighting
   * @param field Field to highlight - pass null to ignore fields
   * @param reader {@link IndexReader} to use for quasi tf/idf scoring
   */
  public QueryScorer(Query query, IndexReader reader, String field) {
    init(query, field, reader, true);
  }


  /**
   * @param query to use for highlighting
   * @param reader {@link IndexReader} to use for quasi tf/idf scoring
   * @param field to highlight - pass null to ignore fields
   */
  public QueryScorer(Query query, IndexReader reader, String field, String defaultField) {
    this.defaultField = defaultField;
    init(query, field, reader, true);
  }

  /**
   * @param defaultField - The default field for queries with the field name unspecified
   */
  public QueryScorer(Query query, String field, String defaultField) {
    this.defaultField = defaultField;
    init(query, field, null, true);
  }

  /**
   * @param weightedTerms an array of pre-created {@link WeightedSpanTerm}s
   */
  public QueryScorer(WeightedSpanTerm[] weightedTerms) {
    this.fieldWeightedSpanTerms = new HashMap<>(weightedTerms.length);

    for (int i = 0; i < weightedTerms.length; i++) {
      WeightedSpanTerm existingTerm = fieldWeightedSpanTerms.get(weightedTerms[i].term);

      if ((existingTerm == null) ||
            (existingTerm.weight < weightedTerms[i].weight)) {
        // if a term is defined more than once, always use the highest
        // scoring weight
        fieldWeightedSpanTerms.put(weightedTerms[i].term, weightedTerms[i]);
        maxTermWeight = Math.max(maxTermWeight, weightedTerms[i].getWeight());
      }
    }
    skipInitExtractor = true;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lucene.search.highlight.Scorer#getFragmentScore()
   */
  @Override
  public float getFragmentScore() {
    return totalScore;
  }

  /**
   *
   * @return The highest weighted term (useful for passing to
   *         GradientFormatter to set top end of coloring scale).
   */
  public float getMaxTermWeight() {
    return maxTermWeight;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lucene.search.highlight.Scorer#getTokenScore(org.apache.lucene.analysis.Token,
   *      int)
   */
  @Override
  public float getTokenScore() {
    position += posIncAtt.getPositionIncrement();
    String termText = termAtt.toString();

    WeightedSpanTerm weightedSpanTerm;

    if ((weightedSpanTerm = fieldWeightedSpanTerms.get(
              termText)) == null) {
      return 0;
    }

    if (weightedSpanTerm.positionSensitive &&
          !weightedSpanTerm.checkPosition(position)) {
      return 0;
    }

    float score = weightedSpanTerm.getWeight();

    // found a query term - is it unique in this doc?
    if (!foundTerms.contains(termText)) {
      totalScore += score;
      foundTerms.add(termText);
    }

    return score;
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.Scorer#init(org.apache.lucene.analysis.TokenStream)
   */
  @Override
  public TokenStream init(TokenStream tokenStream) throws IOException {
    position = -1;
    termAtt = tokenStream.addAttribute(CharTermAttribute.class);
    posIncAtt = tokenStream.addAttribute(PositionIncrementAttribute.class);
    if(!skipInitExtractor) {
      if(fieldWeightedSpanTerms != null) {
        fieldWeightedSpanTerms.clear();
      }
      return initExtractor(tokenStream);
    }
    return null;
  }
  
  /**
   * Retrieve the {@link WeightedSpanTerm} for the specified token. Useful for passing
   * Span information to a {@link Fragmenter}.
   *
   * @param token to get {@link WeightedSpanTerm} for
   * @return WeightedSpanTerm for token
   */
  public WeightedSpanTerm getWeightedSpanTerm(String token) {
    return fieldWeightedSpanTerms.get(token);
  }

  /**
   */
  private void init(Query query, String field, IndexReader reader, boolean expandMultiTermQuery) {
    this.reader = reader;
    this.expandMultiTermQuery = expandMultiTermQuery;
    this.query = query;
    this.field = field;
  }
  
  private TokenStream initExtractor(TokenStream tokenStream) throws IOException {
    WeightedSpanTermExtractor qse = newTermExtractor(defaultField);
    qse.setMaxDocCharsToAnalyze(maxCharsToAnalyze);
    qse.setExpandMultiTermQuery(expandMultiTermQuery);
    qse.setWrapIfNotCachingTokenFilter(wrapToCaching);
    qse.setUsePayloads(usePayloads);
    if (reader == null) {
      this.fieldWeightedSpanTerms = qse.getWeightedSpanTerms(query, 1f,
          tokenStream, field);
    } else {
      this.fieldWeightedSpanTerms = qse.getWeightedSpanTermsWithScores(query, 1f,
          tokenStream, field, reader);
    }
    if(qse.isCachedTokenStream()) {
      return qse.getTokenStream();
    }
    
    return null;
  }
  
  protected WeightedSpanTermExtractor newTermExtractor(String defaultField) {
    return new WeightedSpanTermExtractor(defaultField);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.lucene.search.highlight.Scorer#startFragment(org.apache.lucene.search.highlight.TextFragment)
   */
  @Override
  public void startFragment(TextFragment newFragment) {
    foundTerms = new HashSet<>();
    totalScore = 0;
  }
  
  /**
   * @return true if multi-term queries should be expanded
   */
  public boolean isExpandMultiTermQuery() {
    return expandMultiTermQuery;
  }

  /**
   * Controls whether or not multi-term queries are expanded
   * against a {@link MemoryIndex} {@link IndexReader}.
   * 
   * @param expandMultiTermQuery true if multi-term queries should be expanded
   */
  public void setExpandMultiTermQuery(boolean expandMultiTermQuery) {
    this.expandMultiTermQuery = expandMultiTermQuery;
  }

  /**
   * Whether or not we should capture payloads in {@link MemoryIndex} at each position so that queries can access them.
   * This does not apply to term vector based TokenStreams, which support payloads only when the term vector has them.
   */
  public boolean isUsePayloads() {
    return usePayloads;
  }

  public void setUsePayloads(boolean usePayloads) {
    this.usePayloads = usePayloads;
  }

  /**
   * By default, {@link TokenStream}s that are not of the type
   * {@link CachingTokenFilter} are wrapped in a {@link CachingTokenFilter} to
   * ensure an efficient reset - if you are already using a different caching
   * {@link TokenStream} impl and you don't want it to be wrapped, set this to
   * false. Note that term-vector based tokenstreams are detected and won't be
   * wrapped either.
   */
  public void setWrapIfNotCachingTokenFilter(boolean wrap) {
    this.wrapToCaching = wrap;
  }

  public void setMaxDocCharsToAnalyze(int maxDocCharsToAnalyze) {
    this.maxCharsToAnalyze = maxDocCharsToAnalyze;
  }
}
