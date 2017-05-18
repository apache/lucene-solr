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

import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanQuery;

public final class MoreLikeThisParameters {

  /**
   * Default maximum number of tokens to parse in each example doc field that is not stored with TermVector support.
   *
   * @see #getMaxNumTokensParsed
   */
  public static final int DEFAULT_MAX_NUM_TOKENS_PARSED = 5000;

  /**
   * Ignore terms with less than this frequency in the source doc.
   *
   * @see #getMinTermFreq
   * @see #setMinTermFreq
   */
  public static final int DEFAULT_MIN_TERM_FREQ = 2;

  /**
   * Ignore words which do not occur in at least this many docs.
   *
   * @see #getMinDocFreq
   * @see #setMinDocFreq
   */
  public static final int DEFAULT_MIN_DOC_FREQ = 5;

  /**
   * Ignore words which occur in more than this many docs.
   *
   * @see #getMaxDocFreq
   * @see #setMaxDocFreq
   * @see #setMaxDocFreqPct
   */
  public static final int DEFAULT_MAX_DOC_FREQ = Integer.MAX_VALUE;

  /**
   * Default field names. Null is used to specify that the field names should be looked
   * up at runtime from the provided reader.
   */
  public static final String[] DEFAULT_FIELD_NAMES = new String[]{"contents"};

  /**
   * Ignore words less than this length or if 0 then this has no effect.
   *
   * @see #getMinWordLen
   * @see #setMinWordLen
   */
  public static final int DEFAULT_MIN_WORD_LENGTH = 0;

  /**
   * Ignore words greater than this length or if 0 then this has no effect.
   *
   * @see #getMaxWordLen
   * @see #setMaxWordLen
   */
  public static final int DEFAULT_MAX_WORD_LENGTH = 0;

  /**
   * Default set of stopwords.
   * If null means to allow stop words.
   *
   * @see #setStopWords
   * @see #getStopWords
   */
  public static final Set<?> DEFAULT_STOP_WORDS = null;

  /**
   * Current set of stop words.
   */
  private Set<?> stopWords = DEFAULT_STOP_WORDS;

  /**
   * Return a Query with no more than this many terms.
   *
   * @see BooleanQuery#getMaxClauseCount
   * @see #getMaxQueryTerms
   * @see #setMaxQueryTerms
   */
  public static final int DEFAULT_MAX_QUERY_TERMS = 25;

  /**
   * Analyzer that will be used to parse the doc.
   * This analyzer will be used for all the fields in the document.
   */
  private Analyzer analyzer = null;

  /**
   * Advanced :
   * Pass a specific Analyzer per field
   */
  private Map<String,Analyzer> fieldToAnalyzer = null;

  /**
   * Ignore words less frequent that this.
   */
  private int minTermFreq = DEFAULT_MIN_TERM_FREQ;

  /**
   * Ignore words which do not occur in at least this many docs.
   */
  private int minDocFreq = DEFAULT_MIN_DOC_FREQ;

  /**
   * Ignore words which occur in more than this many docs.
   */
  private int maxDocFreq = DEFAULT_MAX_DOC_FREQ;

  /**
   * If enabled a queryTimeBoostFactor will applied to each query term.
   * This queryTimeBoostFactor is the term score.
   * More the term is considered interesting, stronger the queryTimeBoost
   */
  private boolean boostEnabled = false;

  /**
   * Generic queryTimeBoostFactor that will affect all the fields.
   * This can be override specifying a boost factor per field.
   */
  private float queryTimeBoostFactor = 1.0f;

  /**
   * Boost factor per field, it overrides the generic queryTimeBoostFactor.
   */
  private Map<String,Float> fieldToQueryTimeBoostFactor = null;

  /**
   * Field name we'll analyze.
   */
  private String[] fieldNames = DEFAULT_FIELD_NAMES;

  /**
   * The maximum number of tokens to parse in each example doc field that is not stored with TermVector support
   */
  private int maxNumTokensParsed = DEFAULT_MAX_NUM_TOKENS_PARSED;

  /**
   * Ignore words if less than this len.
   */
  private int minWordLen = DEFAULT_MIN_WORD_LENGTH;

  /**
   * Ignore words if greater than this len.
   */
  private int maxWordLen = DEFAULT_MAX_WORD_LENGTH;

  /**
   * Don't return a query longer than this.
   */
  private int maxQueryTerms = DEFAULT_MAX_QUERY_TERMS;


  public float getQueryTimeBoostFactor() {
    return queryTimeBoostFactor;
  }

  public void setQueryTimeBoostFactor(float queryTimeBoostFactor) {
    this.queryTimeBoostFactor = queryTimeBoostFactor;
  }

   public Map<String, Float> getFieldToQueryTimeBoostFactor() {
     return fieldToQueryTimeBoostFactor;
   }

   public void setFieldToQueryTimeBoostFactor(Map<String, Float> fieldToQueryTimeBoostFactor) {
     this.fieldToQueryTimeBoostFactor = fieldToQueryTimeBoostFactor;
   }

   /**
   * Returns an analyzer that will be used to parse source doc with. The default analyzer
   * is not set.
   *
   * @return the analyzer that will be used to parse source doc with.
   */
  public Analyzer getAnalyzer() {
    return analyzer;
  }

  /**
   * Sets the analyzer to use. An analyzer is not required for generating a query
   * when using {@link MoreLikeThis} like(int docId) and term Vector is available
   * for the fields we are interested in using for similarity.
   * method, all other 'like' methods require an analyzer.
   *
   * @param analyzer the analyzer to use to tokenize text.
   */
  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  public Map<String, Analyzer> getFieldToAnalyzer() {
    return fieldToAnalyzer;
  }

  public void setFieldToAnalyzer(Map<String, Analyzer> fieldToAnalyzer) {
    this.fieldToAnalyzer = fieldToAnalyzer;
  }

  /**
   * Returns the frequency below which terms will be ignored in the source doc. The default
   * frequency is the {@link #DEFAULT_MIN_TERM_FREQ}.
   *
   * @return the frequency below which terms will be ignored in the source doc.
   */
  public int getMinTermFreq() {
    return minTermFreq;
  }

  /**
   * Sets the frequency below which terms will be ignored in the source doc.
   *
   * @param minTermFreq the frequency below which terms will be ignored in the source doc.
   */
  public void setMinTermFreq(int minTermFreq) {
    this.minTermFreq = minTermFreq;
  }

  /**
   * Returns the frequency at which words will be ignored which do not occur in at least this
   * many docs. The default frequency is {@link #DEFAULT_MIN_DOC_FREQ}.
   *
   * @return the frequency at which words will be ignored which do not occur in at least this
   *         many docs.
   */
  public int getMinDocFreq() {
    return minDocFreq;
  }

  /**
   * Sets the frequency at which words will be ignored which do not occur in at least this
   * many docs.
   *
   * @param minDocFreq the frequency at which words will be ignored which do not occur in at
   * least this many docs.
   */
  public void setMinDocFreq(int minDocFreq) {
    this.minDocFreq = minDocFreq;
  }

  /**
   * Returns the maximum frequency in which words may still appear.
   * Words that appear in more than this many docs will be ignored. The default frequency is
   * {@link #DEFAULT_MAX_DOC_FREQ}.
   *
   * @return get the maximum frequency at which words are still allowed,
   *         words which occur in more docs than this are ignored.
   */
  public int getMaxDocFreq() {
    return maxDocFreq;
  }

  /**
   * Set the maximum frequency in which words may still appear. Words that appear
   * in more than this many docs will be ignored.
   *
   * @param maxFreq the maximum count of documents that a term may appear
   * in to be still considered relevant
   */
  public void setMaxDocFreq(int maxFreq) {
    this.maxDocFreq = maxFreq;
  }

  /**
   * Set the maximum percentage in which words may still appear. Words that appear
   * in more than this many percent of all docs will be ignored.
   *
   * @param maxPercentage the maximum percentage of documents (0-100) that a term may appear
   * in to be still considered relevant
   */
  public void setMaxDocFreqPct(IndexReader ir, int maxPercentage) {
    this.maxDocFreq = maxPercentage * ir.numDocs() / 100;
  }


  /**
   * Returns whether to boostEnabled terms in query based on "score" or not. The default is
   * false.
   *
   * @return whether to boostEnabled terms in query based on "score" or not.
   * @see #enableBoost
   */
  public boolean isBoostEnabled() {
    return boostEnabled;
  }

  /**
   * Sets whether to boostEnabled terms in query based on "score" or not.
   *
   * @param boostEnabled true to boostEnabled terms in query based on "score", false otherwise.
   * @see #isBoostEnabled
   */
  public void enableBoost(boolean boostEnabled) {
    this.boostEnabled = boostEnabled;
  }

  /**
   * Returns the field names that will be used when generating the 'More Like This' query.
   * The default field names that will be used is {@link #DEFAULT_FIELD_NAMES}.
   *
   * @return the field names that will be used when generating the 'More Like This' query.
   */
  public String[] getFieldNames() {
    return fieldNames;
  }

  /**
   * Sets the field names that will be used when generating the 'More Like This' query.
   * Set this to null for the field names to be determined at runtime from the IndexReader
   * provided in the constructor.
   *
   * @param fieldNames the field names that will be used when generating the 'More Like This'
   * query.
   */
  public void setFieldNames(String[] fieldNames) {
    this.fieldNames = fieldNames;
  }

  /**
   * Returns the minimum term length below which words will be ignored. Set this to 0 for no
   * minimum term length. The default is {@link #DEFAULT_MIN_WORD_LENGTH}.
   *
   * @return the minimum term length below which words will be ignored.
   */
  public int getMinWordLen() {
    return minWordLen;
  }

  /**
   * Sets the minimum term length below which words will be ignored.
   *
   * @param minWordLen the minimum term length below which words will be ignored.
   */
  public void setMinWordLen(int minWordLen) {
    this.minWordLen = minWordLen;
  }

  /**
   * Returns the maximum term length above which words will be ignored. Set this to 0 for no
   * maximum term length. The default is {@link #DEFAULT_MAX_WORD_LENGTH}.
   *
   * @return the maximum term length above which words will be ignored.
   */
  public int getMaxWordLen() {
    return maxWordLen;
  }

  /**
   * Sets the maximum term length above which words will be ignored.
   *
   * @param maxWordLen the maximum term length above which words will be ignored.
   */
  public void setMaxWordLen(int maxWordLen) {
    this.maxWordLen = maxWordLen;
  }

  /**
   * Set the set of stopwords.
   * Any term in this set is considered "uninteresting" and ignored.
   * Even if your Analyzer allows stopwords, you might want to tell the MoreLikeThis code to ignore them, as
   * for the purposes of document similarity it seems reasonable to assume that "a stop term is never interesting".
   *
   * @param stopWords set of stopwords, if null it means to allow stop words
   * @see #getStopWords
   */
  public void setStopWords(Set<?> stopWords) {
    this.stopWords = stopWords;
  }

  /**
   * Get the current stop words being used.
   *
   * @see #setStopWords
   */
  public Set<?> getStopWords() {
    return stopWords;
  }


  /**
   * Returns the maximum number of query terms that will be included in any generated query.
   * The default is {@link #DEFAULT_MAX_QUERY_TERMS}.
   *
   * @return the maximum number of query terms that will be included in any generated query.
   */
  public int getMaxQueryTerms() {
    return maxQueryTerms;
  }

  /**
   * Sets the maximum number of query terms that will be included in any generated query.
   *
   * @param maxQueryTerms the maximum number of query terms that will be included in any
   * generated query.
   */
  public void setMaxQueryTerms(int maxQueryTerms) {
    this.maxQueryTerms = maxQueryTerms;
  }

  /**
   * @return The maximum number of tokens to parse in each example doc field that is not stored with TermVector support
   * @see #DEFAULT_MAX_NUM_TOKENS_PARSED
   */
  public int getMaxNumTokensParsed() {
    return maxNumTokensParsed;
  }

  /**
   * @param i The maximum number of tokens to parse in each example doc field that is not stored with TermVector support
   */
  public void setMaxNumTokensParsed(int i) {
    maxNumTokensParsed = i;
  }

}
