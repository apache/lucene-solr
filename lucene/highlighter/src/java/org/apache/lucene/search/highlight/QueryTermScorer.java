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

import java.util.HashMap;
import java.util.HashSet;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;

/**
 * {@link Scorer} implementation which scores text fragments by the number of
 * unique query terms found. This class uses the {@link QueryTermExtractor}
 * class to process determine the query terms and their boosts to be used.
 */
// TODO: provide option to boost score of fragments near beginning of document
// based on fragment.getFragNum()
public class QueryTermScorer implements Scorer {
  
  TextFragment currentTextFragment = null;
  HashSet<String> uniqueTermsInFragment;

  float totalScore = 0;
  float maxTermWeight = 0;
  private HashMap<String,WeightedTerm> termsToFind;

  private CharTermAttribute termAtt;

  /**
   * 
   * @param query a Lucene query (ideally rewritten using query.rewrite before
   *        being passed to this class and the searcher)
   */
  public QueryTermScorer(Query query) {
    this(QueryTermExtractor.getTerms(query));
  }

  /**
   * 
   * @param query a Lucene query (ideally rewritten using query.rewrite before
   *        being passed to this class and the searcher)
   * @param fieldName the Field name which is used to match Query terms
   */
  public QueryTermScorer(Query query, String fieldName) {
    this(QueryTermExtractor.getTerms(query, false, fieldName));
  }

  /**
   * 
   * @param query a Lucene query (ideally rewritten using query.rewrite before
   *        being passed to this class and the searcher)
   * @param reader used to compute IDF which can be used to a) score selected
   *        fragments better b) use graded highlights eg set font color
   *        intensity
   * @param fieldName the field on which Inverse Document Frequency (IDF)
   *        calculations are based
   */
  public QueryTermScorer(Query query, IndexReader reader, String fieldName) {
    this(QueryTermExtractor.getIdfWeightedTerms(query, reader, fieldName));
  }

  public QueryTermScorer(WeightedTerm[] weightedTerms) {
    termsToFind = new HashMap<>();
    for (int i = 0; i < weightedTerms.length; i++) {
      WeightedTerm existingTerm = termsToFind
          .get(weightedTerms[i].term);
      if ((existingTerm == null)
          || (existingTerm.weight < weightedTerms[i].weight)) {
        // if a term is defined more than once, always use the highest scoring
        // weight
        termsToFind.put(weightedTerms[i].term, weightedTerms[i]);
        maxTermWeight = Math.max(maxTermWeight, weightedTerms[i].getWeight());
      }
    }
  }

  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.Scorer#init(org.apache.lucene.analysis.TokenStream)
   */
  @Override
  public TokenStream init(TokenStream tokenStream) {
    termAtt = tokenStream.addAttribute(CharTermAttribute.class);
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.highlight.FragmentScorer#startFragment(org.apache
   * .lucene.search.highlight.TextFragment)
   */
  @Override
  public void startFragment(TextFragment newFragment) {
    uniqueTermsInFragment = new HashSet<>();
    currentTextFragment = newFragment;
    totalScore = 0;

  }


  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.Scorer#getTokenScore()
   */
  @Override
  public float getTokenScore() {
    String termText = termAtt.toString();

    WeightedTerm queryTerm = termsToFind.get(termText);
    if (queryTerm == null) {
      // not a query term - return
      return 0;
    }
    // found a query term - is it unique in this doc?
    if (!uniqueTermsInFragment.contains(termText)) {
      totalScore += queryTerm.getWeight();
      uniqueTermsInFragment.add(termText);
    }
    return queryTerm.getWeight();
  }


  /* (non-Javadoc)
   * @see org.apache.lucene.search.highlight.Scorer#getFragmentScore()
   */
  @Override
  public float getFragmentScore() {
    return totalScore;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.lucene.search.highlight.FragmentScorer#allFragmentsProcessed()
   */
  public void allFragmentsProcessed() {
    // this class has no special operations to perform at end of processing
  }

  /**
   * 
   * @return The highest weighted term (useful for passing to GradientFormatter
   *         to set top end of coloring scale.
   */
  public float getMaxTermWeight() {
    return maxTermWeight;
  }
}
