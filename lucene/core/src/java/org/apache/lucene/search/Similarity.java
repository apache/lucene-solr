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


import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation.IDFExplanation;
import org.apache.lucene.util.SmallFloat;
import org.apache.lucene.util.VirtualMethod;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;


/** 
 * Expert: Scoring API.
 *
 * <p>Similarity defines the components of Lucene scoring.
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
 *  <table cellpadding="2" cellspacing="2" border="0" align="center">
 *    <tr><td>
 *    <table cellpadding="1" cellspacing="0" border="1" align="center">
 *      <tr><td>
 *      <table cellpadding="2" cellspacing="2" border="0" align="center">
 *        <tr>
 *          <td valign="middle" align="right" rowspan="1">
 *            cosine-similarity(q,d) &nbsp; = &nbsp;
 *          </td>
 *          <td valign="middle" align="center">
 *            <table>
 *               <tr><td align="center"><small>V(q)&nbsp;&middot;&nbsp;V(d)</small></td></tr>
 *               <tr><td align="center">&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;</td></tr>
 *               <tr><td align="center"><small>|V(q)|&nbsp;|V(d)|</small></td></tr>
 *            </table>
 *          </td>
 *        </tr>
 *      </table>
 *      </td></tr>
 *    </table>
 *    </td></tr>
 *    <tr><td>
 *    <center><font=-1><u>VSM Score</u></font></center>
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
 *  the terms of that query (this is correct for some of the queries),
 *  and users can further reward documents matching more query terms
 *  through a coordination factor, which is usually larger when
 *  more terms are matched: <i>coord-factor(q,d)</i>.
 *  </li>
 * </ul>
 *
 * <p>Under the simplifying assumption of a single field in the index,
 * we get <i>Lucene's Conceptual scoring formula</i>:
 *
 *  <br>&nbsp;<br>
 *  <table cellpadding="2" cellspacing="2" border="0" align="center">
 *    <tr><td>
 *    <table cellpadding="1" cellspacing="0" border="1" align="center">
 *      <tr><td>
 *      <table cellpadding="2" cellspacing="2" border="0" align="center">
 *        <tr>
 *          <td valign="middle" align="right" rowspan="1">
 *            score(q,d) &nbsp; = &nbsp;
 *            <font color="#FF9933">coord-factor(q,d)</font> &middot; &nbsp;
 *            <font color="#CCCC00">query-boost(q)</font> &middot; &nbsp;
 *          </td>
 *          <td valign="middle" align="center">
 *            <table>
 *               <tr><td align="center"><small><font color="#993399">V(q)&nbsp;&middot;&nbsp;V(d)</font></small></td></tr>
 *               <tr><td align="center">&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;</td></tr>
 *               <tr><td align="center"><small><font color="#FF33CC">|V(q)|</font></small></td></tr>
 *            </table>
 *          </td>
 *          <td valign="middle" align="right" rowspan="1">
 *            &nbsp; &middot; &nbsp; <font color="#3399FF">doc-len-norm(d)</font>
 *            &nbsp; &middot; &nbsp; <font color="#3399FF">doc-boost(d)</font>
 *          </td>
 *        </tr>
 *      </table>
 *      </td></tr>
 *    </table>
 *    </td></tr>
 *    <tr><td>
 *    <center><font=-1><u>Lucene Conceptual Scoring Formula</u></font></center>
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
 *
 *   <li>Applying query normalization on the scores helps to keep the
 *   scores around the unit vector, hence preventing loss of score data
 *   because of floating point precision limitations.
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
 * <P>
 * <table cellpadding="2" cellspacing="2" border="0" align="center">
 *  <tr><td>
 *  <table cellpadding="" cellspacing="2" border="2" align="center">
 *  <tr><td>
 *   <table cellpadding="2" cellspacing="2" border="0" align="center">
 *   <tr>
 *     <td valign="middle" align="right" rowspan="1">
 *       score(q,d) &nbsp; = &nbsp;
 *       <A HREF="#formula_coord"><font color="#FF9933">coord(q,d)</font></A> &nbsp;&middot;&nbsp;
 *       <A HREF="#formula_queryNorm"><font color="#FF33CC">queryNorm(q)</font></A> &nbsp;&middot;&nbsp;
 *     </td>
 *     <td valign="bottom" align="center" rowspan="1">
 *       <big><big><big>&sum;</big></big></big>
 *     </td>
 *     <td valign="middle" align="right" rowspan="1">
 *       <big><big>(</big></big>
 *       <A HREF="#formula_tf"><font color="#993399">tf(t in d)</font></A> &nbsp;&middot;&nbsp;
 *       <A HREF="#formula_idf"><font color="#993399">idf(t)</font></A><sup>2</sup> &nbsp;&middot;&nbsp;
 *       <A HREF="#formula_termBoost"><font color="#CCCC00">t.getBoost()</font></A>&nbsp;&middot;&nbsp;
 *       <A HREF="#formula_norm"><font color="#3399FF">norm(t,d)</font></A>
 *       <big><big>)</big></big>
 *     </td>
 *   </tr>
 *   <tr valigh="top">
 *    <td></td>
 *    <td align="center"><small>t in q</small></td>
 *    <td></td>
 *   </tr>
 *   </table>
 *  </td></tr>
 *  </table>
 * </td></tr>
 * <tr><td>
 *  <center><font=-1><u>Lucene Practical Scoring Function</u></font></center>
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
 *      {@link org.apache.lucene.search.DefaultSimilarity#tf(float) DefaultSimilarity} is:
 *
 *      <br>&nbsp;<br>
 *      <table cellpadding="2" cellspacing="2" border="0" align="center">
 *        <tr>
 *          <td valign="middle" align="right" rowspan="1">
 *            {@link org.apache.lucene.search.DefaultSimilarity#tf(float) tf(t in d)} &nbsp; = &nbsp;
 *          </td>
 *          <td valign="top" align="center" rowspan="1">
 *               frequency<sup><big>&frac12;</big></sup>
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
 *      {@link org.apache.lucene.search.DefaultSimilarity#idf(int, int) DefaultSimilarity} is:
 *
 *      <br>&nbsp;<br>
 *      <table cellpadding="2" cellspacing="2" border="0" align="center">
 *        <tr>
 *          <td valign="middle" align="right">
 *            {@link org.apache.lucene.search.DefaultSimilarity#idf(int, int) idf(t)}&nbsp; = &nbsp;
 *          </td>
 *          <td valign="middle" align="center">
 *            1 + log <big>(</big>
 *          </td>
 *          <td valign="middle" align="center">
 *            <table>
 *               <tr><td align="center"><small>numDocs</small></td></tr>
 *               <tr><td align="center">&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;</td></tr>
 *               <tr><td align="center"><small>docFreq+1</small></td></tr>
 *            </table>
 *          </td>
 *          <td valign="middle" align="center">
 *            <big>)</big>
 *          </td>
 *        </tr>
 *      </table>
 *      <br>&nbsp;<br>
 *    </li>
 *
 *    <li>
 *      <A NAME="formula_coord"></A>
 *      <b><i>coord(q,d)</i></b>
 *      is a score factor based on how many of the query terms are found in the specified document.
 *      Typically, a document that contains more of the query's terms will receive a higher score
 *      than another document with fewer query terms.
 *      This is a search time factor computed in
 *      {@link #coord(int, int) coord(q,d)}
 *      by the Similarity in effect at search time.
 *      <br>&nbsp;<br>
 *    </li>
 *
 *    <li><b>
 *      <A NAME="formula_queryNorm"></A>
 *      <i>queryNorm(q)</i>
 *      </b>
 *      is a normalizing factor used to make scores between queries comparable.
 *      This factor does not affect document ranking (since all ranked documents are multiplied by the same factor),
 *      but rather just attempts to make scores from different queries (or even different indexes) comparable.
 *      This is a search time factor computed by the Similarity in effect at search time.
 *
 *      The default computation in
 *      {@link org.apache.lucene.search.DefaultSimilarity#queryNorm(float) DefaultSimilarity}
 *      produces a <a href="http://en.wikipedia.org/wiki/Euclidean_norm#Euclidean_norm">Euclidean norm</a>:
 *      <br>&nbsp;<br>
 *      <table cellpadding="1" cellspacing="0" border="0" align="center">
 *        <tr>
 *          <td valign="middle" align="right" rowspan="1">
 *            queryNorm(q)  &nbsp; = &nbsp;
 *            {@link org.apache.lucene.search.DefaultSimilarity#queryNorm(float) queryNorm(sumOfSquaredWeights)}
 *            &nbsp; = &nbsp;
 *          </td>
 *          <td valign="middle" align="center" rowspan="1">
 *            <table>
 *               <tr><td align="center"><big>1</big></td></tr>
 *               <tr><td align="center"><big>
 *                  &ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;&ndash;
 *               </big></td></tr>
 *               <tr><td align="center">sumOfSquaredWeights<sup><big>&frac12;</big></sup></td></tr>
 *            </table>
 *          </td>
 *        </tr>
 *      </table>
 *      <br>&nbsp;<br>
 *
 *      The sum of squared weights (of the query terms) is
 *      computed by the query {@link org.apache.lucene.search.Weight} object.
 *      For example, a {@link org.apache.lucene.search.BooleanQuery}
 *      computes this value as:
 *
 *      <br>&nbsp;<br>
 *      <table cellpadding="1" cellspacing="0" border="0"n align="center">
 *        <tr>
 *          <td valign="middle" align="right" rowspan="1">
 *            {@link org.apache.lucene.search.Weight#sumOfSquaredWeights() sumOfSquaredWeights} &nbsp; = &nbsp;
 *            {@link org.apache.lucene.search.Query#getBoost() q.getBoost()} <sup><big>2</big></sup>
 *            &nbsp;&middot;&nbsp;
 *          </td>
 *          <td valign="bottom" align="center" rowspan="1">
 *            <big><big><big>&sum;</big></big></big>
 *          </td>
 *          <td valign="middle" align="right" rowspan="1">
 *            <big><big>(</big></big>
 *            <A HREF="#formula_idf">idf(t)</A> &nbsp;&middot;&nbsp;
 *            <A HREF="#formula_termBoost">t.getBoost()</A>
 *            <big><big>) <sup>2</sup> </big></big>
 *          </td>
 *        </tr>
 *        <tr valigh="top">
 *          <td></td>
 *          <td align="center"><small>t in q</small></td>
 *          <td></td>
 *        </tr>
 *      </table>
 *      <br>&nbsp;<br>
 *
 *    </li>
 *
 *    <li>
 *      <A NAME="formula_termBoost"></A>
 *      <b><i>t.getBoost()</i></b>
 *      is a search time boost of term <i>t</i> in the query <i>q</i> as
 *      specified in the query text
 *      (see <A HREF="../../../../../../queryparsersyntax.html#Boosting a Term">query syntax</A>),
 *      or as set by application calls to
 *      {@link org.apache.lucene.search.Query#setBoost(float) setBoost()}.
 *      Notice that there is really no direct API for accessing a boost of one term in a multi term query,
 *      but rather multi terms are represented in a query as multi
 *      {@link org.apache.lucene.search.TermQuery TermQuery} objects,
 *      and so the boost of a term in the query is accessible by calling the sub-query
 *      {@link org.apache.lucene.search.Query#getBoost() getBoost()}.
 *      <br>&nbsp;<br>
 *    </li>
 *
 *    <li>
 *      <A NAME="formula_norm"></A>
 *      <b><i>norm(t,d)</i></b> encapsulates a few (indexing time) boost and length factors:
 *
 *      <ul>
 *        <li><b>Document boost</b> - set by calling
 *        {@link org.apache.lucene.document.Document#setBoost(float) doc.setBoost()}
 *        before adding the document to the index.
 *        </li>
 *        <li><b>Field boost</b> - set by calling
 *        {@link org.apache.lucene.document.Fieldable#setBoost(float) field.setBoost()}
 *        before adding the field to a document.
 *        </li>
 *        <li><b>lengthNorm</b> - computed
 *        when the document is added to the index in accordance with the number of tokens
 *        of this field in the document, so that shorter fields contribute more to the score.
 *        LengthNorm is computed by the Similarity class in effect at indexing.
 *        </li>
 *      </ul>
 *      The {@link #computeNorm} method is responsible for
 *      combining all of these factors into a single float.
 *
 *      <p>
 *      When a document is added to the index, all the above factors are multiplied.
 *      If the document has multiple fields with the same name, all their boosts are multiplied together:
 *
 *      <br>&nbsp;<br>
 *      <table cellpadding="1" cellspacing="0" border="0"n align="center">
 *        <tr>
 *          <td valign="middle" align="right" rowspan="1">
 *            norm(t,d) &nbsp; = &nbsp;
 *            {@link org.apache.lucene.document.Document#getBoost() doc.getBoost()}
 *            &nbsp;&middot;&nbsp;
 *            lengthNorm
 *            &nbsp;&middot;&nbsp;
 *          </td>
 *          <td valign="bottom" align="center" rowspan="1">
 *            <big><big><big>&prod;</big></big></big>
 *          </td>
 *          <td valign="middle" align="right" rowspan="1">
 *            {@link org.apache.lucene.document.Fieldable#getBoost() f.getBoost}()
 *          </td>
 *        </tr>
 *        <tr valigh="top">
 *          <td></td>
 *          <td align="center"><small>field <i><b>f</b></i> in <i>d</i> named as <i><b>t</b></i></small></td>
 *          <td></td>
 *        </tr>
 *      </table>
 *      <br>&nbsp;<br>
 *      However the resulted <i>norm</i> value is {@link #encodeNormValue(float) encoded} as a single byte
 *      before being stored.
 *      At search time, the norm byte value is read from the index
 *      {@link org.apache.lucene.store.Directory directory} and
 *      {@link #decodeNormValue(byte) decoded} back to a float <i>norm</i> value.
 *      This encoding/decoding, while reducing index size, comes with the price of
 *      precision loss - it is not guaranteed that <i>decode(encode(x)) = x</i>.
 *      For instance, <i>decode(encode(0.89)) = 0.75</i>.
 *      <br>&nbsp;<br>
 *      Compression of norm values to a single byte saves memory at search time, 
 *      because once a field is referenced at search time, its norms - for 
 *      all documents - are maintained in memory.
 *      <br>&nbsp;<br>
 *      The rationale supporting such lossy compression of norm values is that
 *      given the difficulty (and inaccuracy) of users to express their true information
 *      need by a query, only big differences matter.
 *      <br>&nbsp;<br>
 *      Last, note that search time is too late to modify this <i>norm</i> part of scoring, e.g. by
 *      using a different {@link Similarity} for search.
 *      <br>&nbsp;<br>
 *    </li>
 * </ol>
 *
 * @see #setDefault(Similarity)
 * @see org.apache.lucene.index.IndexWriter#setSimilarity(Similarity)
 * @see Searcher#setSimilarity(Similarity)
 */
public abstract class Similarity implements Serializable {

  // NOTE: this static code must precede setting the static defaultImpl:
  private static final VirtualMethod<Similarity> withoutDocFreqMethod =
    new VirtualMethod<Similarity>(Similarity.class, "idfExplain", Term.class, Searcher.class);
  private static final VirtualMethod<Similarity> withDocFreqMethod =
    new VirtualMethod<Similarity>(Similarity.class, "idfExplain", Term.class, Searcher.class, int.class);

  private final boolean hasIDFExplainWithDocFreqAPI =
    VirtualMethod.compareImplementationDistance(getClass(),
        withDocFreqMethod, withoutDocFreqMethod) >= 0; // its ok for both to be overridden
  /**
   * The Similarity implementation used by default.
   **/
  private static Similarity defaultImpl = new DefaultSimilarity();

  public static final int NO_DOC_ID_PROVIDED = -1;

  /** Set the default Similarity implementation used by indexing and search
   * code.
   *
   * @see Searcher#setSimilarity(Similarity)
   * @see org.apache.lucene.index.IndexWriter#setSimilarity(Similarity)
   */
  public static void setDefault(Similarity similarity) {
    Similarity.defaultImpl = similarity;
  }

  /** Return the default Similarity implementation used by indexing and search
   * code.
   *
   * <p>This is initially an instance of {@link DefaultSimilarity}.
   *
   * @see Searcher#setSimilarity(Similarity)
   * @see org.apache.lucene.index.IndexWriter#setSimilarity(Similarity)
   */
  public static Similarity getDefault() {
    return Similarity.defaultImpl;
  }

  /** Cache of decoded bytes. */
  private static final float[] NORM_TABLE = new float[256];

  static {
    for (int i = 0; i < 256; i++)
      NORM_TABLE[i] = SmallFloat.byte315ToFloat((byte)i);
  }

  /**
   * Decodes a normalization factor stored in an index.
   * @see #decodeNormValue(byte)
   * @deprecated Use {@link #decodeNormValue} instead.
   */
  @Deprecated
  public static float decodeNorm(byte b) {
    return NORM_TABLE[b & 0xFF];  // & 0xFF maps negative bytes to positive above 127
  }

  /** Decodes a normalization factor stored in an index.
   * <p>
   * <b>WARNING: If you override this method, you should change the default
   *    Similarity to your implementation with {@link Similarity#setDefault(Similarity)}. 
   *    Otherwise, your method may not always be called, especially if you omit norms 
   *    for some fields.</b>
   * @see #encodeNormValue(float)
   */
  public float decodeNormValue(byte b) {
    return NORM_TABLE[b & 0xFF];  // & 0xFF maps negative bytes to positive above 127
  }

  /** Returns a table for decoding normalization bytes.
   * @see #encodeNormValue(float)
   * @see #decodeNormValue(byte)
   * 
   * @deprecated Use instance methods for encoding/decoding norm values to enable customization.
   */
  @Deprecated
  public static float[] getNormDecoder() {
    return NORM_TABLE;
  }

  /**
   * Computes the normalization value for a field, given the accumulated
   * state of term processing for this field (see {@link FieldInvertState}).
   * 
   * <p>Implementations should calculate a float value based on the field
   * state and then return that value.
   *
   * <p>Matches in longer fields are less precise, so implementations of this
   * method usually return smaller values when <code>state.getLength()</code> is large,
   * and larger values when <code>state.getLength()</code> is small.
   * 
   * <p>Note that the return values are computed under 
   * {@link org.apache.lucene.index.IndexWriter#addDocument(org.apache.lucene.document.Document)} 
   * and then stored using
   * {@link #encodeNormValue(float)}.  
   * Thus they have limited precision, and documents
   * must be re-indexed if this method is altered.
   *
   * <p>For backward compatibility this method by default calls
   * {@link #lengthNorm(String, int)} passing
   * {@link FieldInvertState#getLength()} as the second argument, and
   * then multiplies this value by {@link FieldInvertState#getBoost()}.</p>
   * 
   * @lucene.experimental
   * 
   * @param field field name
   * @param state current processing state for this field
   * @return the calculated float norm
   */
  public abstract float computeNorm(String field, FieldInvertState state);
  
  /** Computes the normalization value for a field given the total number of
   * terms contained in a field.  These values, together with field boosts, are
   * stored in an index and multipled into scores for hits on each field by the
   * search code.
   *
   * <p>Matches in longer fields are less precise, so implementations of this
   * method usually return smaller values when <code>numTokens</code> is large,
   * and larger values when <code>numTokens</code> is small.
   * 
   * <p>Note that the return values are computed under 
   * {@link org.apache.lucene.index.IndexWriter#addDocument(org.apache.lucene.document.Document)} 
   * and then stored using
   * {@link #encodeNormValue(float)}.  
   * Thus they have limited precision, and documents
   * must be re-indexed if this method is altered.
   *
   * @param fieldName the name of the field
   * @param numTokens the total number of tokens contained in fields named
   * <i>fieldName</i> of <i>doc</i>.
   * @return a normalization factor for hits on this field of this document
   *
   * @see org.apache.lucene.document.Field#setBoost(float)
   *
   * @deprecated Please override computeNorm instead
   */
  @Deprecated
  public final float lengthNorm(String fieldName, int numTokens) {
    throw new UnsupportedOperationException("please use computeNorm instead");
  }

  /** Computes the normalization value for a query given the sum of the squared
   * weights of each of the query terms.  This value is multiplied into the
   * weight of each query term. While the classic query normalization factor is
   * computed as 1/sqrt(sumOfSquaredWeights), other implementations might
   * completely ignore sumOfSquaredWeights (ie return 1).
   *
   * <p>This does not affect ranking, but the default implementation does make scores
   * from different queries more comparable than they would be by eliminating the
   * magnitude of the Query vector as a factor in the score.
   *
   * @param sumOfSquaredWeights the sum of the squares of query term weights
   * @return a normalization factor for query weights
   */
  public abstract float queryNorm(float sumOfSquaredWeights);

  /** Encodes a normalization factor for storage in an index.
   *
   * <p>The encoding uses a three-bit mantissa, a five-bit exponent, and
   * the zero-exponent point at 15, thus
   * representing values from around 7x10^9 to 2x10^-9 with about one
   * significant decimal digit of accuracy.  Zero is also represented.
   * Negative numbers are rounded up to zero.  Values too large to represent
   * are rounded down to the largest representable value.  Positive values too
   * small to represent are rounded up to the smallest positive representable
   * value.
   * <p>
   * <b>WARNING: If you override this method, you should change the default
   * Similarity to your implementation with {@link Similarity#setDefault(Similarity)}. 
   * Otherwise, your method may not always be called, especially if you omit norms 
   * for some fields.</b>
   * @see org.apache.lucene.document.Field#setBoost(float)
   * @see org.apache.lucene.util.SmallFloat
   */
  public byte encodeNormValue(float f) {
    return SmallFloat.floatToByte315(f);
  }
  
  /**
   * Static accessor kept for backwards compability reason, use encodeNormValue instead.
   * @param f norm-value to encode
   * @return byte representing the given float
   * @deprecated Use {@link #encodeNormValue} instead.
   * 
   * @see #encodeNormValue(float)
   */
  @Deprecated
  public static byte encodeNorm(float f) {
    return SmallFloat.floatToByte315(f);
  }


  /** Computes a score factor based on a term or phrase's frequency in a
   * document.  This value is multiplied by the {@link #idf(int, int)}
   * factor for each term in the query and these products are then summed to
   * form the initial score for a document.
   *
   * <p>Terms and phrases repeated in a document indicate the topic of the
   * document, so implementations of this method usually return larger values
   * when <code>freq</code> is large, and smaller values when <code>freq</code>
   * is small.
   *
   * <p>The default implementation calls {@link #tf(float)}.
   *
   * @param freq the frequency of a term within a document
   * @return a score factor based on a term's within-document frequency
   */
  public float tf(int freq) {
    return tf((float)freq);
  }

  /** Computes the amount of a sloppy phrase match, based on an edit distance.
   * This value is summed for each sloppy phrase match in a document to form
   * the frequency that is passed to {@link #tf(float)}.
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

  /** Computes a score factor based on a term or phrase's frequency in a
   * document.  This value is multiplied by the {@link #idf(int, int)}
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
   * <pre>
   * idf(searcher.docFreq(term), searcher.maxDoc());
   * </pre>
   * 
   * Note that {@link Searcher#maxDoc()} is used instead of
   * {@link org.apache.lucene.index.IndexReader#numDocs() IndexReader#numDocs()} because also 
   * {@link Searcher#docFreq(Term)} is used, and when the latter 
   * is inaccurate, so is {@link Searcher#maxDoc()}, and in the same direction.
   * In addition, {@link Searcher#maxDoc()} is more efficient to compute
   *   
   * @param term the term in question
   * @param searcher the document collection being searched
   * @return an IDFExplain object that includes both an idf score factor 
             and an explanation for the term.
   * @throws IOException
   */
  public IDFExplanation idfExplain(final Term term, final Searcher searcher, int docFreq) throws IOException {

    if (!hasIDFExplainWithDocFreqAPI) {
      // Fallback to slow impl
      return idfExplain(term, searcher);
    }
    final int df = docFreq;
    final int max = searcher.maxDoc();
    final float idf = idf(df, max);
    return new IDFExplanation() {
        @Override
        public String explain() {
          return "idf(docFreq=" + df +
          ", maxDocs=" + max + ")";
        }
        @Override
        public float getIdf() {
          return idf;
        }};
  }

  /**
   * This method forwards to {@link
   * #idfExplain(Term,Searcher,int)} by passing
   * <code>searcher.docFreq(term)</code> as the docFreq.
   *
   * WARNING: if you subclass Similariary and override this
   * method then you may hit a peformance hit for certain
   * queries.  Better to override {@link
   * #idfExplain(Term,Searcher,int)} instead.
   */
  public IDFExplanation idfExplain(final Term term, final Searcher searcher) throws IOException {
    return idfExplain(term, searcher, searcher.docFreq(term));
  }

  /**
   * Computes a score factor for a phrase.
   * 
   * <p>
   * The default implementation sums the idf factor for
   * each term in the phrase.
   * 
   * @param terms the terms in the phrase
   * @param searcher the document collection being searched
   * @return an IDFExplain object that includes both an idf 
   *         score factor for the phrase and an explanation 
   *         for each term.
   * @throws IOException
   */
  public IDFExplanation idfExplain(Collection<Term> terms, Searcher searcher) throws IOException {
    final int max = searcher.maxDoc();
    float idf = 0.0f;
    final StringBuilder exp = new StringBuilder();
    for (final Term term : terms ) {
      final int df = searcher.docFreq(term);
      idf += idf(df, max);
      exp.append(" ");
      exp.append(term.text());
      exp.append("=");
      exp.append(df);
    }
    final float fIdf = idf;
    return new IDFExplanation() {
      @Override
      public float getIdf() {
        return fIdf;
      }
      @Override
      public String explain() {
        return exp.toString();
      }
    };
  }

  /** Computes a score factor based on a term's document frequency (the number
   * of documents which contain the term).  This value is multiplied by the
   * {@link #tf(int)} factor for each term in the query and these products are
   * then summed to form the initial score for a document.
   *
   * <p>Terms that occur in fewer documents are better indicators of topic, so
   * implementations of this method usually return larger values for rare terms,
   * and smaller values for common terms.
   *
   * @param docFreq the number of documents which contain the term
   * @param numDocs the total number of documents in the collection
   * @return a score factor based on the term's document frequency
   */
  public abstract float idf(int docFreq, int numDocs);

  /** Computes a score factor based on the fraction of all query terms that a
   * document contains.  This value is multiplied into scores.
   *
   * <p>The presence of a large portion of the query terms indicates a better
   * match with the query, so implementations of this method usually return
   * larger values when the ratio between these parameters is large and smaller
   * values when the ratio between them is small.
   *
   * @param overlap the number of query terms matched in the document
   * @param maxOverlap the total number of terms in the query
   * @return a score factor based on term overlap with the query
   */
  public abstract float coord(int overlap, int maxOverlap);

  /**
   * Calculate a scoring factor based on the data in the payload.  Overriding implementations
   * are responsible for interpreting what is in the payload.  Lucene makes no assumptions about
   * what is in the byte array.
   * <p>
   * The default implementation returns 1.
   *
   * @param docId The docId currently being scored.  If this value is {@link #NO_DOC_ID_PROVIDED}, then it should be assumed that the PayloadQuery implementation does not provide document information
   * @param fieldName The fieldName of the term this payload belongs to
   * @param start The start position of the payload
   * @param end The end position of the payload
   * @param payload The payload byte array to be scored
   * @param offset The offset into the payload array
   * @param length The length in the array
   * @return An implementation dependent float to be used as a scoring factor
   *
   */
  public float scorePayload(int docId, String fieldName, int start, int end, byte [] payload, int offset, int length)
  {
    return 1;
  }

}
