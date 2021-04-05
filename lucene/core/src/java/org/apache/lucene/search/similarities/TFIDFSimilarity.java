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
package org.apache.lucene.search.similarities;


import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.SmallFloat;


/**
 * Implementation of {@link Similarity} with the Vector Space Model.
 * <p>
 * Expert: Scoring API.
 * <p>TFIDFSimilarity defines the components of Lucene scoring.
 * Overriding computation of these components is a convenient
 * way to alter Lucene scoring.
 *
 * <p>Suggested reading:
 * <a href="http://nlp.stanford.edu/IR-book/html/htmledition/queries-as-vectors-1.html">
 * Introduction To Information Retrieval, Chapter 6</a>.
 *
 * <p>The following describes how Lucene scoring evolves from
 * underlying information retrieval models to (efficient) implementation.
 * We first brief on <i>VSM Score</i>, 
 * then derive from it <i>Lucene's Conceptual Scoring Formula</i>,
 * from which, finally, evolves <i>Lucene's Practical Scoring Function</i> 
 * (the latter is connected directly with Lucene classes and methods).    
 *
 * <p>Lucene combines
 * <a href="http://en.wikipedia.org/wiki/Standard_Boolean_model">
 * Boolean model (BM) of Information Retrieval</a>
 * with
 * <a href="http://en.wikipedia.org/wiki/Vector_Space_Model">
 * Vector Space Model (VSM) of Information Retrieval</a> -
 * documents "approved" by BM are scored by VSM.
 *
 * <p>In VSM, documents and queries are represented as
 * weighted vectors in a multi-dimensional space,
 * where each distinct index term is a dimension,
 * and weights are
 * <a href="http://en.wikipedia.org/wiki/Tfidf">Tf-idf</a> values.
 *
 * <p>VSM does not require weights to be <i>Tf-idf</i> values,
 * but <i>Tf-idf</i> values are believed to produce search results of high quality,
 * and so Lucene is using <i>Tf-idf</i>.
 * <i>Tf</i> and <i>Idf</i> are described in more detail below,
 * but for now, for completion, let's just say that
 * for given term <i>t</i> and document (or query) <i>x</i>,
 * <i>Tf(t,x)</i> varies with the number of occurrences of term <i>t</i> in <i>x</i>
 * (when one increases so does the other) and
 * <i>idf(t)</i> similarly varies with the inverse of the
 * number of index documents containing term <i>t</i>.
 *
 * <p><i>VSM score</i> of document <i>d</i> for query <i>q</i> is the
 * <a href="http://en.wikipedia.org/wiki/Cosine_similarity">
 * Cosine Similarity</a>
 * of the weighted query vectors <i>V(q)</i> and <i>V(d)</i>:
 *
 *  <br>&nbsp;<br>
 *  <table cellpadding="2" cellspacing="2" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="formatting only">
 *    <tr><td>
 *    <table cellpadding="1" cellspacing="0" border="1" style="margin-left:auto; margin-right:auto" summary="formatting only">
 *      <tr><td>
 *      <table cellpadding="2" cellspacing="2" border="0" style="margin-left:auto; margin-right:auto" summary="cosine similarity formula">
 *        <tr>
 *          <td style="vertical-align: middle; text-align: right" rowspan="1">
 *            cosine-similarity(q,d) &nbsp; = &nbsp;
 *          </td>
 *          <td style="vertical-align: middle; text-align: center">
 *            <table>
 *               <caption>cosine similarity formula</caption>
 *               <tr><td style="text-align: center"><small>V(q)&nbsp;&middot;&nbsp;V(d)</small></td></tr>
 *               <tr><td style="text-align: center">&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;</td></tr>
 *               <tr><td style="text-align: center"><small>|V(q)|&nbsp;|V(d)|</small></td></tr>
 *            </table>
 *          </td>
 *        </tr>
 *      </table>
 *      </td></tr>
 *    </table>
 *    </td></tr>
 *    <tr><td>
 *    <center><u>VSM Score</u></center>
 *    </td></tr>
 *  </table>
 *  <br>&nbsp;<br>
 *   
 *
 * Where <i>V(q)</i> &middot; <i>V(d)</i> is the
 * <a href="http://en.wikipedia.org/wiki/Dot_product">dot product</a>
 * of the weighted vectors,
 * and <i>|V(q)|</i> and <i>|V(d)|</i> are their
 * <a href="http://en.wikipedia.org/wiki/Euclidean_norm#Euclidean_norm">Euclidean norms</a>.
 *
 * <p>Note: the above equation can be viewed as the dot product of
 * the normalized weighted vectors, in the sense that dividing
 * <i>V(q)</i> by its euclidean norm is normalizing it to a unit vector.
 *
 * <p>Lucene refines <i>VSM score</i> for both search quality and usability:
 * <ul>
 *  <li>Normalizing <i>V(d)</i> to the unit vector is known to be problematic in that 
 *  it removes all document length information. 
 *  For some documents removing this info is probably ok, 
 *  e.g. a document made by duplicating a certain paragraph <i>10</i> times,
 *  especially if that paragraph is made of distinct terms. 
 *  But for a document which contains no duplicated paragraphs, 
 *  this might be wrong. 
 *  To avoid this problem, a different document length normalization 
 *  factor is used, which normalizes to a vector equal to or larger 
 *  than the unit vector: <i>doc-len-norm(d)</i>.
 *  </li>
 *
 *  <li>At indexing, users can specify that certain documents are more
 *  important than others, by assigning a document boost.
 *  For this, the score of each document is also multiplied by its boost value
 *  <i>doc-boost(d)</i>.
 *  </li>
 *
 *  <li>Lucene is field based, hence each query term applies to a single
 *  field, document length normalization is by the length of the certain field,
 *  and in addition to document boost there are also document fields boosts.
 *  </li>
 *
 *  <li>The same field can be added to a document during indexing several times,
 *  and so the boost of that field is the multiplication of the boosts of
 *  the separate additions (or parts) of that field within the document.
 *  </li>
 *
 *  <li>At search time users can specify boosts to each query, sub-query, and
 *  each query term, hence the contribution of a query term to the score of
 *  a document is multiplied by the boost of that query term <i>query-boost(q)</i>.
 *  </li>
 *
 *  <li>A document may match a multi term query without containing all
 *  the terms of that query (this is correct for some of the queries).
 *  </li>
 * </ul>
 *
 * <p>Under the simplifying assumption of a single field in the index,
 * we get <i>Lucene's Conceptual scoring formula</i>:
 *
 *  <br>&nbsp;<br>
 *  <table cellpadding="2" cellspacing="2" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="formatting only">
 *    <tr><td>
 *    <table cellpadding="1" cellspacing="0" border="1" style="margin-left:auto; margin-right:auto" summary="formatting only">
 *      <tr><td>
 *      <table cellpadding="2" cellspacing="2" border="0" style="margin-left:auto; margin-right:auto" summary="formatting only">
 *        <tr>
 *          <td style="vertical-align: middle; text-align: right" rowspan="1">
 *            score(q,d) &nbsp; = &nbsp;
 *            <span style="color: #CCCC00">query-boost(q)</span> &middot; &nbsp;
 *          </td>
 *          <td style="vertical-align: middle; text-align: center">
 *            <table>
 *               <caption>Lucene conceptual scoring formula</caption>
 *               <tr><td style="text-align: center"><small><span style="color: #993399">V(q)&nbsp;&middot;&nbsp;V(d)</span></small></td></tr>
 *               <tr><td style="text-align: center">&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;</td></tr>
 *               <tr><td style="text-align: center"><small><span style="color: #FF33CC">|V(q)|</span></small></td></tr>
 *            </table>
 *          </td>
 *          <td style="vertical-align: middle; text-align: right" rowspan="1">
 *            &nbsp; &middot; &nbsp; <span style="color: #3399FF">doc-len-norm(d)</span>
 *            &nbsp; &middot; &nbsp; <span style="color: #3399FF">doc-boost(d)</span>
 *          </td>
 *        </tr>
 *      </table>
 *      </td></tr>
 *    </table>
 *    </td></tr>
 *    <tr><td>
 *    <center><u>Lucene Conceptual Scoring Formula</u></center>
 *    </td></tr>
 *  </table>
 *  <br>&nbsp;<br>
 *
 * <p>The conceptual formula is a simplification in the sense that (1) terms and documents
 * are fielded and (2) boosts are usually per query term rather than per query.
 *
 * <p>We now describe how Lucene implements this conceptual scoring formula, and
 * derive from it <i>Lucene's Practical Scoring Function</i>.
 *  
 * <p>For efficient score computation some scoring components
 * are computed and aggregated in advance:
 *
 * <ul>
 *  <li><i>Query-boost</i> for the query (actually for each query term)
 *  is known when search starts.
 *  </li>
 *
 *  <li>Query Euclidean norm <i>|V(q)|</i> can be computed when search starts,
 *  as it is independent of the document being scored.
 *  From search optimization perspective, it is a valid question
 *  why bother to normalize the query at all, because all
 *  scored documents will be multiplied by the same <i>|V(q)|</i>,
 *  and hence documents ranks (their order by score) will not
 *  be affected by this normalization.
 *  There are two good reasons to keep this normalization:
 *  <ul>
 *   <li>Recall that
 *   <a href="http://en.wikipedia.org/wiki/Cosine_similarity">
 *   Cosine Similarity</a> can be used find how similar
 *   two documents are. One can use Lucene for e.g.
 *   clustering, and use a document as a query to compute
 *   its similarity to other documents.
 *   In this use case it is important that the score of document <i>d3</i>
 *   for query <i>d1</i> is comparable to the score of document <i>d3</i>
 *   for query <i>d2</i>. In other words, scores of a document for two
 *   distinct queries should be comparable.
 *   There are other applications that may require this.
 *   And this is exactly what normalizing the query vector <i>V(q)</i>
 *   provides: comparability (to a certain extent) of two or more queries.
 *   </li>
 *  </ul>
 *  </li>
 *
 *  <li>Document length norm <i>doc-len-norm(d)</i> and document
 *  boost <i>doc-boost(d)</i> are known at indexing time.
 *  They are computed in advance and their multiplication
 *  is saved as a single value in the index: <i>norm(d)</i>.
 *  (In the equations below, <i>norm(t in d)</i> means <i>norm(field(t) in doc d)</i>
 *  where <i>field(t)</i> is the field associated with term <i>t</i>.)
 *  </li>
 * </ul>
 *
 * <p><i>Lucene's Practical Scoring Function</i> is derived from the above.
 * The color codes demonstrate how it relates
 * to those of the <i>conceptual</i> formula:
 *
 * <table cellpadding="2" cellspacing="2" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="formatting only">
 *  <tr><td>
 *  <table cellpadding="" cellspacing="2" border="2" style="margin-left:auto; margin-right:auto" summary="formatting only">
 *  <tr><td>
 *   <table cellpadding="2" cellspacing="2" border="0" style="margin-left:auto; margin-right:auto" summary="Lucene conceptual scoring formula">
 *   <tr>
 *     <td style="vertical-align: middle; text-align: right" rowspan="1">
 *       score(q,d) &nbsp; = &nbsp;
 *       <big><big><big>&sum;</big></big></big>
 *     </td>
 *     <td style="vertical-align: middle; text-align: right" rowspan="1">
 *       <span style="font-size: larger">(</span>
 *       <A HREF="#formula_tf"><span style="color: #993399">tf(t in d)</span></A> &nbsp;&middot;&nbsp;
 *       <A HREF="#formula_idf"><span style="color: #993399">idf(t)</span></A><sup>2</sup> &nbsp;&middot;&nbsp;
 *       <A HREF="#formula_termBoost"><span style="color: #CCCC00">t.getBoost()</span></A>&nbsp;&middot;&nbsp;
 *       <A HREF="#formula_norm"><span style="color: #3399FF">norm(t,d)</span></A>
 *       <big><big>)</big></big>
 *     </td>
 *   </tr>
 *   <tr style="vertical-align: top">
 *    <td></td>
 *    <td align="center" style="text-align: center"><small>t in q</small></td>
 *    <td></td>
 *   </tr>
 *   </table>
 *  </td></tr>
 *  </table>
 * </td></tr>
 * <tr><td>
 *  <center><u>Lucene Practical Scoring Function</u></center>
 * </td></tr>
 * </table>
 *
 * <p> where
 * <ol>
 *    <li>
 *      <A NAME="formula_tf"></A>
 *      <b><i>tf(t in d)</i></b>
 *      correlates to the term's <i>frequency</i>,
 *      defined as the number of times term <i>t</i> appears in the currently scored document <i>d</i>.
 *      Documents that have more occurrences of a given term receive a higher score.
 *      Note that <i>tf(t in q)</i> is assumed to be <i>1</i> and therefore it does not appear in this equation,
 *      However if a query contains twice the same term, there will be
 *      two term-queries with that same term and hence the computation would still be correct (although
 *      not very efficient).
 *      The default computation for <i>tf(t in d)</i> in
 *      {@link org.apache.lucene.search.similarities.ClassicSimilarity#tf(float) ClassicSimilarity} is:
 *
 *      <br>&nbsp;<br>
 *      <table cellpadding="2" cellspacing="2" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="term frequency computation">
 *        <tr>
 *          <td style="vertical-align: middle; text-align: right" rowspan="1">
 *            {@link org.apache.lucene.search.similarities.ClassicSimilarity#tf(float) tf(t in d)} &nbsp; = &nbsp;
 *          </td>
 *          <td style="vertical-align: top; text-align: center" rowspan="1">
 *               frequency<sup><span style="font-size: larger">&frac12;</span></sup>
 *          </td>
 *        </tr>
 *      </table>
 *      <br>&nbsp;<br>
 *    </li>
 *
 *    <li>
 *      <A NAME="formula_idf"></A>
 *      <b><i>idf(t)</i></b> stands for Inverse Document Frequency. This value
 *      correlates to the inverse of <i>docFreq</i>
 *      (the number of documents in which the term <i>t</i> appears).
 *      This means rarer terms give higher contribution to the total score.
 *      <i>idf(t)</i> appears for <i>t</i> in both the query and the document,
 *      hence it is squared in the equation.
 *      The default computation for <i>idf(t)</i> in
 *      {@link org.apache.lucene.search.similarities.ClassicSimilarity#idf(long, long) ClassicSimilarity} is:
 *
 *      <br>&nbsp;<br>
 *      <table cellpadding="2" cellspacing="2" border="0" style="width:auto; margin-left:auto; margin-right:auto" summary="inverse document frequency computation">
 *        <tr>
 *          <td style="vertical-align: middle; text-align: right">
 *            {@link org.apache.lucene.search.similarities.ClassicSimilarity#idf(long, long) idf(t)}&nbsp; = &nbsp;
 *          </td>
 *          <td style="vertical-align: middle; text-align: center">
 *            1 + log <span style="font-size: larger">(</span>
 *          </td>
 *          <td style="vertical-align: middle; text-align: center">
 *            <table>
 *               <caption>inverse document frequency computation</caption>
 *               <tr><td style="text-align: center"><small>docCount+1</small></td></tr>
 *               <tr><td style="text-align: center">&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;</td></tr>
 *               <tr><td style="text-align: center"><small>docFreq+1</small></td></tr>
 *            </table>
 *          </td>
 *          <td style="vertical-align: middle; text-align: center">
 *            <span style="font-size: larger">)</span>
 *          </td>
 *        </tr>
 *      </table>
 *      <br>&nbsp;<br>
 *    </li>
 *
 *    <li>
 *      <A NAME="formula_termBoost"></A>
 *      <b><i>t.getBoost()</i></b>
 *      is a search time boost of term <i>t</i> in the query <i>q</i> as
 *      specified in the query text
 *      (see <A HREF="{@docRoot}/../queryparser/org/apache/lucene/queryparser/classic/package-summary.html#Boosting_a_Term">query syntax</A>),
 *      or as set by wrapping with
 *      {@link org.apache.lucene.search.BoostQuery#BoostQuery(org.apache.lucene.search.Query, float) BoostQuery}.
 *      Notice that there is really no direct API for accessing a boost of one term in a multi term query,
 *      but rather multi terms are represented in a query as multi
 *      {@link org.apache.lucene.search.TermQuery TermQuery} objects,
 *      and so the boost of a term in the query is accessible by calling the sub-query
 *      {@link org.apache.lucene.search.BoostQuery#getBoost() getBoost()}.
 *      <br>&nbsp;<br>
 *    </li>
 *
 *    <li>
 *      <A NAME="formula_norm"></A>
 *      <b><i>norm(t,d)</i></b> is an index-time boost factor that solely
 *      depends on the number of tokens of this field in the document, so
 *      that shorter fields contribute more to the score.
 *    </li>
 * </ol>
 *
 * @see org.apache.lucene.index.IndexWriterConfig#setSimilarity(Similarity)
 * @see IndexSearcher#setSimilarity(Similarity)
 */
public abstract class TFIDFSimilarity extends Similarity {

  /**
   * Sole constructor. (For invocation by subclass 
   * constructors, typically implicit.)
   */
  public TFIDFSimilarity() {}

  /** 
   * True if overlap tokens (tokens with a position of increment of zero) are
   * discounted from the document's length.
   */
  protected boolean discountOverlaps = true;

  /** Determines whether overlap tokens (Tokens with
   *  0 position increment) are ignored when computing
   *  norm.  By default this is true, meaning overlap
   *  tokens do not count when computing norms.
   *
   *  @lucene.experimental
   *
   *  @see #computeNorm
   */
  public void setDiscountOverlaps(boolean v) {
    discountOverlaps = v;
  }

  /**
   * Returns true if overlap tokens are discounted from the document's length. 
   * @see #setDiscountOverlaps 
   */
  public boolean getDiscountOverlaps() {
    return discountOverlaps;
  }

  /** Computes a score factor based on a term or phrase's frequency in a
   * document.  This value is multiplied by the {@link #idf(long, long)}
   * factor for each term in the query and these products are then summed to
   * form the initial score for a document.
   *
   * <p>Terms and phrases repeated in a document indicate the topic of the
   * document, so implementations of this method usually return larger values
   * when <code>freq</code> is large, and smaller values when <code>freq</code>
   * is small.
   *
   * @param freq the frequency of a term within a document
   * @return a score factor based on a term's within-document frequency
   */
  public abstract float tf(float freq);

  /**
   * Computes a score factor for a simple term and returns an explanation
   * for that score factor.
   * 
   * <p>
   * The default implementation uses:
   * 
   * <pre class="prettyprint">
   * idf(docFreq, docCount);
   * </pre>
   * 
   * Note that {@link CollectionStatistics#docCount()} is used instead of
   * {@link org.apache.lucene.index.IndexReader#numDocs() IndexReader#numDocs()} because also 
   * {@link TermStatistics#docFreq()} is used, and when the latter 
   * is inaccurate, so is {@link CollectionStatistics#docCount()}, and in the same direction.
   * In addition, {@link CollectionStatistics#docCount()} does not skew when fields are sparse.
   *   
   * @param collectionStats collection-level statistics
   * @param termStats term-level statistics for the term
   * @return an Explain object that includes both an idf score factor 
             and an explanation for the term.
   */
  public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats) {
    final long df = termStats.docFreq();
    final long docCount = collectionStats.docCount();
    final float idf = idf(df, docCount);
    return Explanation.match(idf, "idf(docFreq, docCount)", 
        Explanation.match(df, "docFreq, number of documents containing term"),
        Explanation.match(docCount, "docCount, total number of documents with field"));
  }

  /**
   * Computes a score factor for a phrase.
   * 
   * <p>
   * The default implementation sums the idf factor for
   * each term in the phrase.
   * 
   * @param collectionStats collection-level statistics
   * @param termStats term-level statistics for the terms in the phrase
   * @return an Explain object that includes both an idf 
   *         score factor for the phrase and an explanation 
   *         for each term.
   */
  public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats[]) {
    double idf = 0d; // sum into a double before casting into a float
    List<Explanation> subs = new ArrayList<>();
    for (final TermStatistics stat : termStats ) {
      Explanation idfExplain = idfExplain(collectionStats, stat);
      subs.add(idfExplain);
      idf += idfExplain.getValue().floatValue();
    }
    return Explanation.match((float) idf, "idf(), sum of:", subs);
  }

  /** Computes a score factor based on a term's document frequency (the number
   * of documents which contain the term).  This value is multiplied by the
   * {@link #tf(float)} factor for each term in the query and these products are
   * then summed to form the initial score for a document.
   *
   * <p>Terms that occur in fewer documents are better indicators of topic, so
   * implementations of this method usually return larger values for rare terms,
   * and smaller values for common terms.
   *
   * @param docFreq the number of documents which contain the term
   * @param docCount the total number of documents in the collection
   * @return a score factor based on the term's document frequency
   */
  public abstract float idf(long docFreq, long docCount);

  /**
   * Compute an index-time normalization value for this field instance.
   * 
   * @param length the number of terms in the field, optionally {@link #setDiscountOverlaps(boolean) discounting overlaps}
   * @return a length normalization value
   */
  public abstract float lengthNorm(int length);
  
  @Override
  public final long computeNorm(FieldInvertState state) {
    final int numTerms;
    if (state.getIndexOptions() == IndexOptions.DOCS && state.getIndexCreatedVersionMajor() >= 8) {
      numTerms = state.getUniqueTermCount();
    } else if (discountOverlaps) {
      numTerms = state.getLength() - state.getNumOverlap();
    } else {
      numTerms = state.getLength();
    }
    return SmallFloat.intToByte4(numTerms);
  }

  @Override
  public final SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    final Explanation idf = termStats.length == 1
    ? idfExplain(collectionStats, termStats[0])
    : idfExplain(collectionStats, termStats);
    float[] normTable = new float[256];
    for (int i = 1; i < 256; ++i) {
      int length = SmallFloat.byte4ToInt((byte) i);
      float norm = lengthNorm(length);
      normTable[i] = norm;
    }
    normTable[0] = 1f / normTable[255];
    return new TFIDFScorer(boost, idf, normTable);
  }

  
  /** Collection statistics for the TF-IDF model. The only statistic of interest
   * to this model is idf. */
  class TFIDFScorer extends SimScorer {
    /** The idf and its explanation */
    private final Explanation idf;
    private final float boost;
    private final float queryWeight;
    final float[] normTable;
    
    public TFIDFScorer(float boost, Explanation idf, float[] normTable) {
      // TODO: Validate?
      this.idf = idf;
      this.boost = boost;
      this.queryWeight = boost * idf.getValue().floatValue();
      this.normTable = normTable;
    }

    @Override
    public float score(float freq, long norm) {
      final float raw = tf(freq) * queryWeight; // compute tf(f)*weight
      float normValue = normTable[(int) (norm & 0xFF)];
      return raw * normValue;  // normalize for field
    }

    @Override
    public Explanation explain(Explanation freq, long norm) {
      return explainScore(freq, norm, normTable);
    }

    private Explanation explainScore(Explanation freq, long encodedNorm, float[] normTable) {
      List<Explanation> subs = new ArrayList<Explanation>();
      if (boost != 1F) {
        subs.add(Explanation.match(boost, "boost"));
      }
      subs.add(idf);
      Explanation tf = Explanation.match(tf(freq.getValue().floatValue()), "tf(freq="+freq.getValue()+"), with freq of:", freq);
      subs.add(tf);

      float norm = normTable[(int) (encodedNorm & 0xFF)];
      
      Explanation fieldNorm = Explanation.match(norm, "fieldNorm");
      subs.add(fieldNorm);
      
      return Explanation.match(
          queryWeight * tf.getValue().floatValue() * norm,
          "score(freq="+freq.getValue()+"), product of:",
          subs);
    }
  }  

}
