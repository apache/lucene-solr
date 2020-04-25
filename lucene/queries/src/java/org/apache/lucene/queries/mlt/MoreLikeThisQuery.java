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
package org.apache.lucene.queries.mlt;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

/**
 * A simple wrapper for MoreLikeThis for use in scenarios where a Query object is required eg
 * in custom QueryParser extensions. At query.rewrite() time the reader is used to construct the
 * actual MoreLikeThis object and obtain the real Query object.
 */
public class MoreLikeThisQuery extends Query {

  private String likeText;
  private String[] moreLikeFields;
  private Analyzer analyzer;
  private final String fieldName;
  private float percentTermsToMatch = 0.3f;
  private int minTermFrequency = 1;
  private int maxQueryTerms = 5;
  private Set<?> stopWords = null;
  private int minDocFreq = -1;

  /**
   * @param moreLikeFields fields used for similarity measure
   */
  public MoreLikeThisQuery(String likeText, String[] moreLikeFields, Analyzer analyzer, String fieldName) {
    this.likeText = Objects.requireNonNull(likeText);
    this.moreLikeFields = Objects.requireNonNull(moreLikeFields);
    this.analyzer = Objects.requireNonNull(analyzer);
    this.fieldName = Objects.requireNonNull(fieldName);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    MoreLikeThis mlt = new MoreLikeThis(reader);

    mlt.setFieldNames(moreLikeFields);
    mlt.setAnalyzer(analyzer);
    mlt.setMinTermFreq(minTermFrequency);
    if (minDocFreq >= 0) {
      mlt.setMinDocFreq(minDocFreq);
    }
    mlt.setMaxQueryTerms(maxQueryTerms);
    mlt.setStopWords(stopWords);
    BooleanQuery bq = (BooleanQuery) mlt.like(fieldName, new StringReader(likeText));
    BooleanQuery.Builder newBq = new BooleanQuery.Builder();
    for (BooleanClause clause : bq) {
      newBq.add(clause);
    }
    //make at least half the terms match
    newBq.setMinimumNumberShouldMatch((int) (bq.clauses().size() * percentTermsToMatch));
    return newBq.build();
  }

  /* (non-Javadoc)
  * @see org.apache.lucene.search.Query#toString(java.lang.String)
  */
  @Override
  public String toString(String field) {
    return "like:" + likeText;
  }

  public float getPercentTermsToMatch() {
    return percentTermsToMatch;
  }

  public void setPercentTermsToMatch(float percentTermsToMatch) {
    this.percentTermsToMatch = percentTermsToMatch;
  }

  public Analyzer getAnalyzer() {
    return analyzer;
  }

  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  public String getLikeText() {
    return likeText;
  }

  public void setLikeText(String likeText) {
    this.likeText = likeText;
  }

  public int getMaxQueryTerms() {
    return maxQueryTerms;
  }

  public void setMaxQueryTerms(int maxQueryTerms) {
    this.maxQueryTerms = maxQueryTerms;
  }

  public int getMinTermFrequency() {
    return minTermFrequency;
  }

  public void setMinTermFrequency(int minTermFrequency) {
    this.minTermFrequency = minTermFrequency;
  }

  public String[] getMoreLikeFields() {
    return moreLikeFields;
  }

  public void setMoreLikeFields(String[] moreLikeFields) {
    this.moreLikeFields = moreLikeFields;
  }

  public Set<?> getStopWords() {
    return stopWords;
  }

  public void setStopWords(Set<?> stopWords) {
    this.stopWords = stopWords;
  }

  public int getMinDocFreq() {
    return minDocFreq;
  }

  public void setMinDocFreq(int minDocFreq) {
    this.minDocFreq = minDocFreq;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = classHash();
    result = prime * result + Objects.hash(analyzer, fieldName, likeText, stopWords);
    result = prime * result + maxQueryTerms;
    result = prime * result + minDocFreq;
    result = prime * result + minTermFrequency;
    result = prime * result + Arrays.hashCode(moreLikeFields);
    result = prime * result + Float.floatToIntBits(percentTermsToMatch);
    return result;
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(MoreLikeThisQuery other) {
    return maxQueryTerms == other.maxQueryTerms &&
           minDocFreq == other.minDocFreq &&
           minTermFrequency == other.minTermFrequency &&
           Float.floatToIntBits(percentTermsToMatch) == Float.floatToIntBits(other.percentTermsToMatch) &&
           analyzer.equals(other.analyzer) &&
           fieldName.equals(other.fieldName) &&
           likeText.equals(other.likeText) &&
           Arrays.equals(moreLikeFields, other.moreLikeFields) &&
           Objects.equals(stopWords, other.stopWords);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }
}
