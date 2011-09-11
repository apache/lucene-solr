package org.apache.lucene.search.similarities;

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

import org.apache.lucene.document.IndexDocValuesField; // javadoc
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader; // javadoc
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.Terms; // javadoc
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanQuery; // javadoc
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat; // javadoc
import org.apache.lucene.util.TermContext;


/** 
 * Similarity defines the components of Lucene scoring.
 * <p>
 * Expert: Scoring API.
 * <p>
 * This is a low-level API, you should only extend this API if you want to implement 
 * an information retrieval <i>model</i>.  If you are instead looking for a convenient way 
 * to alter Lucene's scoring, consider extending a higher-level implementation
 * such as {@link TFIDFSimilarity}, which implements the vector space model with this API, or 
 * just tweaking the default implementation: {@link DefaultSimilarity}.
 * <p>
 * Similarity determines how Lucene weights terms, and Lucene interacts with
 * this class at both <a href="#indextime">index-time</a> and 
 * <a href="#querytime">query-time</a>.
 * <p>
 * <a name="indextime"/>
 * At indexing time, the indexer calls {@link #computeNorm(FieldInvertState)}, allowing
 * the Similarity implementation to return a per-document byte for the field that will 
 * be later accessible via {@link IndexReader#norms(String)}.  Lucene makes no assumption
 * about what is in this byte, but it is most useful for encoding length normalization 
 * information.
 * <p>
 * Implementations should carefully consider how the normalization byte is encoded: while
 * Lucene's classical {@link TFIDFSimilarity} encodes a combination of index-time boost
 * and length normalization information with {@link SmallFloat}, this might not be suitable
 * for all purposes.
 * <p>
 * Many formulas require the use of average document length, which can be computed via a 
 * combination of {@link Terms#getSumTotalTermFreq()} and {@link IndexReader#maxDoc()},
 * <p>
 * Because index-time boost is handled entirely at the application level anyway,
 * an application can alternatively store the index-time boost separately using an 
 * {@link IndexDocValuesField}, and access this at query-time with 
 * {@link IndexReader#docValues(String)}.
 * <p>
 * Finally, using index-time boosts (either via folding into the normalization byte or
 * via IndexDocValues), is an inefficient way to boost the scores of different fields if the
 * boost will be the same for every document, instead the Similarity can simply take a constant
 * boost parameter <i>C</i>, and the SimilarityProvider can return different instances with
 * different boosts depending upon field name.
 * <p>
 * <a name="querytime"/>
 * At query-time, Queries interact with the Similarity via these steps:
 * <ol>
 *   <li>The {@link #computeStats(IndexSearcher, String, float, TermContext...)} method is called a single time,
 *       allowing the implementation to compute any statistics (such as IDF, average document length, etc)
 *       across <i>the entire collection</i>. The {@link TermContext}s passed in are already positioned
 *       to the terms involved with the raw statistics involved, so a Similarity can freely use any combination
 *       of term statistics without causing any additional I/O. Lucene makes no assumption about what is 
 *       stored in the returned {@link Similarity.Stats} object.
 *   <li>The query normalization process occurs a single time: {@link Similarity.Stats#getValueForNormalization()}
 *       is called for each query leaf node, {@link SimilarityProvider#queryNorm(float)} is called for the top-level
 *       query, and finally {@link Similarity.Stats#normalize(float, float)} passes down the normalization value
 *       and any top-level boosts (e.g. from enclosing {@link BooleanQuery}s).
 *   <li>For each segment in the index, the Query creates a {@link #exactDocScorer(Stats, String, IndexReader.AtomicReaderContext)}
 *       (for queries with exact frequencies such as TermQuerys and exact PhraseQueries) or a 
 *       {@link #sloppyDocScorer(Stats, String, IndexReader.AtomicReaderContext)} (for queries with sloppy frequencies such as
 *       SpanQuerys and sloppy PhraseQueries). The score() method is called for each matching document.
 * </ol>
 * <p>
 * <a name="explaintime"/>
 * When {@link IndexSearcher#explain(Query, int)} is called, queries consult the Similarity's DocScorer for an 
 * explanation of how it computed its score. The query passes in a the document id and an explanation of how the frequency
 * was computed.
 *
 * @see org.apache.lucene.index.IndexWriterConfig#setSimilarityProvider(SimilarityProvider)
 * @see IndexSearcher#setSimilarityProvider(SimilarityProvider)
 * @lucene.experimental
 */
public abstract class Similarity {
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
  public static abstract class ExactDocScorer {
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
  public static abstract class SloppyDocScorer {
    /**
     * Score a single document
     * @param doc document id
     * @param freq sloppy term frequency
     * @return document's score
     */
    public abstract float score(int doc, float freq);

    /** Computes the amount of a sloppy phrase match, based on an edit distance. */
    public abstract float computeSlopFactor(int distance);
    
    /** Calculate a scoring factor based on the data in the payload. */
    public abstract float computePayloadFactor(int doc, int start, int end, BytesRef payload);
    
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
