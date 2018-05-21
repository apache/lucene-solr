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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.BooleanQuery;

/**
 * This class models all the parameters that affect how to generate a More Like This Query:
 * <p>
 * <br>
 * <h3>More Advanced Usage</h3>
 * <p>
 * You may want to use {@link #setFieldNames setFieldNames(...)} so you can examine
 * multiple fields (e.g. body and title) for similarity.
 * <p>
 * Depending on the size of your index and the size and makeup of your documents you
 * may want to call the other set methods to control how the similarity queries are
 * generated:
 * <ul>
 * <li> {@link #setMinTermFreq setMinTermFreq(...)}
 * <li> {@link #setMinDocFreq setMinDocFreq(...)}
 * <li> {@link #setMaxDocFreq setMaxDocFreq(...)}
 * <li> {@link #setMaxDocFreqPct setMaxDocFreqPct(...)}
 * <li> {@link #setMinWordLen setMinWordLen(...)}
 * <li> {@link #setMaxWordLen setMaxWordLen(...)}
 * <li> {@link #setMaxQueryTerms setMaxQueryTerms(...)}
 * <li> {@link #setMaxNumTokensParsed setMaxNumTokensParsed(...)}
 * <li> {@link #setStopWords setStopWord(...)}
 * </ul>
 * <br>
 * <hr>
 */
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
  public static final String[] DEFAULT_FIELD_NAMES = null;

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

  /**
   * Current set of stop words.
   */
  private Set<?> stopWords = DEFAULT_STOP_WORDS;

  /**
   * The boost configuration will be used to manage how :
   * - terms are boosted in the MLT query
   * - fields are boosted in the MLT query
   */
  private BoostProperties boostConfiguration = new BoostProperties();

  /**
   * Returns the boost configurations that regulate how terms and fields
   * are boosted in a More Like This query
   *
   * @return boost properties that will be used when building the query
   */
  public BoostProperties getBoostConfiguration() {
    return boostConfiguration;
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
   * many docs.
   */
  public int getMinDocFreq() {
    return minDocFreq;
  }

  /**
   * Sets the frequency at which words will be ignored which do not occur in at least this
   * many docs.
   *
   * @param minDocFreq the frequency at which words will be ignored which do not occur in at
   *                   least this many docs.
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
   * words which occur in more docs than this are ignored.
   */
  public int getMaxDocFreq() {
    return maxDocFreq;
  }

  /**
   * Set the maximum frequency in which words may still appear. Words that appear
   * in more than this many docs will be ignored.
   *
   * @param maxFreq the maximum count of documents that a term may appear
   *                in to be still considered relevant
   */
  public void setMaxDocFreq(int maxFreq) {
    this.maxDocFreq = maxFreq;
  }

  /**
   * Set the maximum percentage in which words may still appear. Words that appear
   * in more than this many percent of all docs will be ignored.
   *
   * @param maxPercentage the maximum percentage of documents (0-100) that a term may appear
   *                      in to be still considered relevant
   */
  public void setMaxDocFreqPct(IndexReader ir, int maxPercentage) {
    this.maxDocFreq = maxPercentage * ir.numDocs() / 100;
  }

  /**
   * Returns the field names that will be used when generating the 'More Like This' query.
   * If current field names are null, fetch them from the index reader.
   * The default field names that will be used is {@link #DEFAULT_FIELD_NAMES}.
   *
   * @return the field names that will be used when generating the 'More Like This' query.
   */
  public String[] getFieldNamesOrInit(IndexReader ir) {
    if (fieldNames == null) {
      // gather list of valid fields from lucene
      Collection<String> fields = MultiFields.getIndexedFields(ir);
      fieldNames = fields.toArray(new String[fields.size()]);
    }
    return fieldNames;
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
   *                   query.
   */
  public void setFieldNames(String[] fieldNames) {
    this.fieldNames = Arrays.stream(fieldNames).map(fieldName -> fieldName.split("\\^")[0]).toArray(String[]::new);
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
   *                      generated query.
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

  /**
   * Describe the parameters that control how the "more like this" query is formed.
   */
  public String describeParams() {
    StringBuilder sb = new StringBuilder();
    sb.append("\t").append("maxQueryTerms  : ").append(this.getMaxQueryTerms()).append("\n");
    sb.append("\t").append("minWordLen     : ").append(this.getMinWordLen()).append("\n");
    sb.append("\t").append("maxWordLen     : ").append(this.getMaxWordLen()).append("\n");
    sb.append("\t").append("fieldNames     : ");
    String delim = "";
    for (String fieldName : fieldNames) {
      sb.append(delim).append(fieldName);
      delim = ", ";
    }
    sb.append("\n");
    sb.append("\t").append("boost          : ").append(this.getBoostConfiguration().isBoostByTermScore()).append("\n");
    sb.append("\t").append("minTermFreq    : ").append(this.getMinTermFreq()).append("\n");
    sb.append("\t").append("minDocFreq     : ").append(this.getMinDocFreq()).append("\n");
    return sb.toString();
  }

  public class BoostProperties {
    /**
     * By default the query time boost factor is not applied
     */
    public static final boolean DEFAULT_BOOST = false;

    /**
     * By default the query time boost factor is equal to 1.0
     */
    private static final float DEFAULT_BOOST_FACTOR = 1.0f;
    
    /**
     * If enabled a query time boost factor will applied to each query term.
     * This boost factor is the term score ( calculated by the similarity function).
     * More the term is considered interesting, stronger the query time boost
     */
    private boolean boostByTermScore = DEFAULT_BOOST;

    /**
     * This is an additional multiplicative factor that may affect how strongly
     * the query terms will be boosted.
     * If a query time boost factor > 1 is specified, each query term boost
     * is equal the term score multiplied by this factor.
     */
    private float boostFactor = DEFAULT_BOOST_FACTOR;

    /**
     * This is an alternative to the generic boost factor.
     * This map allows to boost each field differently. 
     */
    private Map<String, Float> fieldToBoostFactor = new HashMap<>();

    public BoostProperties() {
    }

    public void addFieldWithBoost(String boostedField) {
      String fieldName;
      String boost;
      if (boostedField.contains("^")) {
        String[] field2boost = boostedField.split("\\^");
        fieldName = field2boost[0];
        boost = field2boost[1];
        if (boost != null) {
          fieldToBoostFactor.put(fieldName, Float.parseFloat(boost));
        }
      } else {
        fieldToBoostFactor.put(boostedField, boostFactor);
      }
    }

    public void setBoostFactor(float boostFactor) {
      this.boostFactor = boostFactor;
    }

    public float getFieldBoost(String fieldName) {
      float queryTimeBoost = boostFactor;
      if (fieldToBoostFactor != null) {
        Float currentFieldQueryTimeBoost = fieldToBoostFactor.get(fieldName);
        if (currentFieldQueryTimeBoost != null) {
          queryTimeBoost = currentFieldQueryTimeBoost;
        }
      }
      return queryTimeBoost;
    }

    public void setFieldToBoostFactor(Map<String, Float> fieldToBoostFactor) {
      this.fieldToBoostFactor = fieldToBoostFactor;
    }

    /**
     * Returns whether a query time boost for each term based on its score is enabled or not. 
     * The default is false.
     *
     * @return whether to boostByTermScore terms in query based on "score" or not.
     * @see #setBoost
     */
    public boolean isBoostByTermScore() {
      return boostByTermScore;
    }

    /**
     * Sets whether to set a query time boost for each term based on its score or not.
     *
     * @param boostEnabled true to boost each term based on its score, false otherwise.
     * @see #isBoostByTermScore
     */
    public void setBoost(boolean boostEnabled) {
      this.boostByTermScore = boostEnabled;
    }
  }

}
