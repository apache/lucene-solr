package org.apache.lucene.search;

/**
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

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.spans.SpanQuery; // javadoc
import org.apache.lucene.util.TermContext;


/** 
 * Expert: Scoring API.
 *
 * <p>Similarity defines the components of Lucene scoring.
 * Overriding computation of these components is a convenient
 * way to alter Lucene scoring.
 *
 * @see org.apache.lucene.index.IndexWriterConfig#setSimilarityProvider(SimilarityProvider)
 * @see IndexSearcher#setSimilarityProvider(SimilarityProvider)
 */
public abstract class Similarity {
  
  public static final int NO_DOC_ID_PROVIDED = -1;

  /**
   * Computes the normalization value for a field, given the accumulated
   * state of term processing for this field (see {@link FieldInvertState}).
   * 
   * <p>Implementations should calculate a byte value based on the field
   * state and then return that value.
   *
   * <p>Matches in longer fields are less precise, so implementations of this
   * method usually return smaller values when <code>state.getLength()</code> is large,
   * and larger values when <code>state.getLength()</code> is small.
   * 
   * @lucene.experimental
   * 
   * @param state current processing state for this field
   * @return the calculated byte norm
   */
  public abstract byte computeNorm(FieldInvertState state);

  /** Computes the amount of a sloppy phrase match, based on an edit distance.
   * This value is summed for each sloppy phrase match in a document to form
   * the frequency to be used in scoring instead of the exact term count.
   *
   * <p>A phrase match with a small edit distance to a document passage more
   * closely matches the document, so implementations of this method usually
   * return larger values when the edit distance is small and smaller values
   * when it is large.
   *
   * @see PhraseQuery#setSlop(int)
   * @param distance the edit distance of this sloppy phrase match
   * @return the frequency increment for this match
   */
  public abstract float sloppyFreq(int distance);

  /**
   * Calculate a scoring factor based on the data in the payload.  Overriding implementations
   * are responsible for interpreting what is in the payload.  Lucene makes no assumptions about
   * what is in the byte array.
   * <p>
   * The default implementation returns 1.
   *
   * @param docId The docId currently being scored.  If this value is {@link #NO_DOC_ID_PROVIDED}, then it should be assumed that the PayloadQuery implementation does not provide document information
   * @param start The start position of the payload
   * @param end The end position of the payload
   * @param payload The payload byte array to be scored
   * @param offset The offset into the payload array
   * @param length The length in the array
   * @return An implementation dependent float to be used as a scoring factor
   *
   */
  // TODO: maybe switch this API to BytesRef?
  public float scorePayload(int docId, int start, int end, byte [] payload, int offset, int length)
  {
    return 1;
  }
  
  /**
   * Compute any collection-level stats (e.g. IDF, average document length, etc) needed for scoring a query.
   */
  public abstract Stats computeStats(IndexSearcher searcher, String fieldName, float queryBoost, TermContext... termContexts) throws IOException;
  
  /**
   * returns a new {@link Similarity.ExactDocScorer}.
   */
  public abstract ExactDocScorer exactDocScorer(Stats stats, String fieldName, AtomicReaderContext context) throws IOException;
  
  /**
   * returns a new {@link Similarity.SloppyDocScorer}.
   */
  public abstract SloppyDocScorer sloppyDocScorer(Stats stats, String fieldName, AtomicReaderContext context) throws IOException;
  
  /**
   * API for scoring exact queries such as {@link TermQuery} and 
   * exact {@link PhraseQuery}.
   * <p>
   * Term frequencies are integers (the term or phrase's tf)
   */
  public abstract class ExactDocScorer {
    /**
     * Score a single document
     * @param doc document id
     * @param freq term frequency
     * @return document's score
     */
    public abstract float score(int doc, int freq);
    
    /**
     * Explain the score for a single document
     * @param doc document id
     * @param freq Explanation of how the term frequency was computed
     * @return document's score
     */
    public Explanation explain(int doc, Explanation freq) {
      Explanation result = new Explanation(score(doc, (int)freq.getValue()), 
          "score(doc=" + doc + ",freq=" + freq.getValue() +"), with freq of:");
      result.addDetail(freq);
      return result;
    }
  }
  
  /**
   * API for scoring "sloppy" queries such as {@link SpanQuery} and 
   * sloppy {@link PhraseQuery}.
   * <p>
   * Term frequencies are floating point values.
   */
  public abstract class SloppyDocScorer {
    /**
     * Score a single document
     * @param doc document id
     * @param freq sloppy term frequency
     * @return document's score
     */
    public abstract float score(int doc, float freq);
    
    /**
     * Explain the score for a single document
     * @param doc document id
     * @param freq Explanation of how the sloppy term frequency was computed
     * @return document's score
     */
    public Explanation explain(int doc, Explanation freq) {
      Explanation result = new Explanation(score(doc, freq.getValue()), 
          "score(doc=" + doc + ",freq=" + freq.getValue() +"), with freq of:");
      result.addDetail(freq);
      return result;
    }
  }
  
  /** Stores the statistics for the indexed collection. This abstract
   * implementation is empty; descendants of {@code Similarity} should
   * subclass {@code Stats} and define the statistics they require in the
   * subclass. Examples include idf, average field length, etc.
   */
  public static abstract class Stats {
    
    /** The value for normalization of contained query clauses (e.g. sum of squared weights).
     * <p>
     * NOTE: a Similarity implementation might not use any query normalization at all,
     * its not required. However, if it wants to participate in query normalization,
     * it can return a value here.
     */
    public abstract float getValueForNormalization();
    
    /** Assigns the query normalization factor and boost from parent queries to this.
     * <p>
     * NOTE: a Similarity implementation might not use this normalized value at all,
     * its not required. However, its usually a good idea to at least incorporate 
     * the topLevelBoost (e.g. from an outer BooleanQuery) into its score.
     */
    public abstract void normalize(float queryNorm, float topLevelBoost);
  }
}
