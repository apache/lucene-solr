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
 */
public final class MoreLikeThisParameters {
  /**
   * Default maximum number of tokens to parse in each example doc field that is not stored with TermVector support.
   */
  public static final int DEFAULT_MAX_NUM_TOKENS_PARSED = 5000;

  /**
   * Ignore terms with less than this frequency in the source doc.
   *
   */
  public static final int DEFAULT_MIN_TERM_FREQ = 2;

  /**
   * Ignore words which do not occur in at least this many docs.
   *
   */
  public static final int DEFAULT_MIN_DOC_FREQ = 5;

  /**
   * Ignore words which occur in more than this many docs.
   *
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
   */
  public static final int DEFAULT_MIN_WORD_LENGTH = 0;

  /**
   * Ignore words greater than this length or if 0 then this has no effect.
   *
   */
  public static final int DEFAULT_MAX_WORD_LENGTH = 0;

  /**
   * Default set of stopwords.
   * If null means to allow stop words.
   *
   */
  public static final Set<?> DEFAULT_STOP_WORDS = null;

  /**
   * Return a Query with no more than this many terms.
   *
   * @see BooleanQuery#getMaxClauseCount
   */
  public static final int DEFAULT_MAX_QUERY_TERMS = 25;

  /**
   * Analyzer that will be used to parse the doc.
   * This analyzer will be used for all the fields in the document.
   */
  Analyzer analyzer = null;

  /**
   * Ignore words less frequent that this.
   */
  int minTermFreq = DEFAULT_MIN_TERM_FREQ;

  /**
   * Ignore words which do not occur in at least this many docs.
   */
  int minDocFreq = DEFAULT_MIN_DOC_FREQ;

  /**
   * Ignore words which occur in more than this many docs.
   */
  int maxDocFreq = DEFAULT_MAX_DOC_FREQ;

  /**
   * Field name we'll analyze.
   */
  String[] fieldNames = DEFAULT_FIELD_NAMES;

  /**
   * The maximum number of tokens to parse in each example doc field that is not stored with TermVector support
   */
  int maxNumTokensParsed = DEFAULT_MAX_NUM_TOKENS_PARSED;

  /**
   * Ignore words if less than this len.
   */
  int minWordLen = DEFAULT_MIN_WORD_LENGTH;

  /**
   * Ignore words if greater than this len.
   */
  int maxWordLen = DEFAULT_MAX_WORD_LENGTH;

  /**
   * Don't return a query longer than this.
   */
  int maxQueryTerms = DEFAULT_MAX_QUERY_TERMS;

  /**
   * Current set of stop words.
   */
  Set<?> stopWords = DEFAULT_STOP_WORDS;

  /**
   * The boost configuration will be used to manage how :
   * - terms are boosted in the MLT query
   * - fields are boosted in the MLT query
   */
  BoostProperties boostConfiguration = new BoostProperties();

  /**
   * Returns an analyzer that will be used to parse source doc with. The default analyzer
   * is not set.
   *
   * @return the analyzer that will be used to parse source doc with.
   */
  Analyzer getAnalyzer() {
    return analyzer;
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
   * Sets the field names that will be used when generating the 'More Like This' query.
   * Set this to null for the field names to be determined at runtime from the IndexReader
   * provided in the constructor.
   *
   * @param fieldNames the field names that will be used when generating the 'More Like This'
   *                   query.
   */
  public void setFieldNamesRemovingBoost(String[] fieldNames) {
    this.fieldNames = Arrays.stream(fieldNames).map(fieldName -> fieldName.split("\\^")[0]).toArray(String[]::new);
  }

  /**
   * Describe the parameters that control how the "more like this" query is formed.
   */
  public String describeParams() {
    StringBuilder sb = new StringBuilder();
    sb.append("\t").append("maxQueryTerms  : ").append(this.maxQueryTerms).append("\n");
    sb.append("\t").append("minWordLen     : ").append(this.minWordLen).append("\n");
    sb.append("\t").append("maxWordLen     : ").append(this.maxWordLen).append("\n");
    sb.append("\t").append("fieldNames     : ");
    String delim = "";
    for (String fieldName : fieldNames) {
      sb.append(delim).append(fieldName);
      delim = ", ";
    }
    sb.append("\n");
    sb.append("\t").append("boost          : ").append(this.boostConfiguration.isBoostByTermScore()).append("\n");
    sb.append("\t").append("minTermFreq    : ").append(this.minTermFreq).append("\n");
    sb.append("\t").append("minDocFreq     : ").append(this.minDocFreq).append("\n");
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
