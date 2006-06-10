package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.SmallFloat;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

/** Expert: Scoring API.
 * <p>Subclasses implement search scoring.
 *
 * <p>The score of query <code>q</code> for document <code>d</code> is defined
 * in terms of these methods as follows:
 *
 * <table cellpadding="0" cellspacing="0" border="0">
 *  <tr>
 *    <td valign="middle" align="right" rowspan="2">score(q,d) =<br></td>
 *    <td valign="middle" align="center">
 *    <big><big><big><big><big>&Sigma;</big></big></big></big></big></td>
 *    <td valign="middle"><small>
 *    ( {@link #tf(int) tf}(t in d) *
 *    {@link #idf(Term,Searcher) idf}(t)^2 *
 *    {@link Query#getBoost getBoost}(t in q) *
 *    {@link org.apache.lucene.document.Field#getBoost getBoost}(t.field in d) *
 *    {@link #lengthNorm(String,int) lengthNorm}(t.field in d) )
 *    </small></td>
 *    <td valign="middle" rowspan="2">&nbsp;*
 *    {@link #coord(int,int) coord}(q,d) *
 *    {@link #queryNorm(float) queryNorm}(sumOfSqaredWeights)
 *    </td>
 *  </tr>
 *  <tr>
 *   <td valign="top" align="right">
 *    <small>t in q</small>
 *    </td>
 *  </tr>
 * </table>
 * 
 * <p> where
 * 
 * <table cellpadding="0" cellspacing="0" border="0">
 *  <tr>
 *    <td valign="middle" align="right" rowspan="2">sumOfSqaredWeights =<br></td>
 *    <td valign="middle" align="center">
 *    <big><big><big><big><big>&Sigma;</big></big></big></big></big></td>
 *    <td valign="middle"><small>
 *    ( {@link #idf(Term,Searcher) idf}(t) *
 *    {@link Query#getBoost getBoost}(t in q) )^2
 *    </small></td>
 *  </tr>
 *  <tr>
 *   <td valign="top" align="right">
 *    <small>t in q</small>
 *    </td>
 *  </tr>
 * </table>
 * 
 * <p> Note that the above formula is motivated by the cosine-distance or dot-product
 * between document and query vector, which is implemented by {@link DefaultSimilarity}.
 *
 * @see #setDefault(Similarity)
 * @see IndexWriter#setSimilarity(Similarity)
 * @see Searcher#setSimilarity(Similarity)
 */
public abstract class Similarity implements Serializable {
  /** The Similarity implementation used by default. */
  private static Similarity defaultImpl = new DefaultSimilarity();

  /** Set the default Similarity implementation used by indexing and search
   * code.
   *
   * @see Searcher#setSimilarity(Similarity)
   * @see IndexWriter#setSimilarity(Similarity)
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
   * @see IndexWriter#setSimilarity(Similarity)
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

  /** Decodes a normalization factor stored in an index.
   * @see #encodeNorm(float)
   */
  public static float decodeNorm(byte b) {
    return NORM_TABLE[b & 0xFF];  // & 0xFF maps negative bytes to positive above 127
  }

  /** Returns a table for decoding normalization bytes.
   * @see #encodeNorm(float)
   */
  public static float[] getNormDecoder() {
    return NORM_TABLE;
  }

  /** Computes the normalization value for a field given the total number of
   * terms contained in a field.  These values, together with field boosts, are
   * stored in an index and multipled into scores for hits on each field by the
   * search code.
   *
   * <p>Matches in longer fields are less precise, so implementations of this
   * method usually return smaller values when <code>numTokens</code> is large,
   * and larger values when <code>numTokens</code> is small.
   *
   * <p>That these values are computed under {@link
   * IndexWriter#addDocument(org.apache.lucene.document.Document)} and stored then using
   * {@link #encodeNorm(float)}.  Thus they have limited precision, and documents
   * must be re-indexed if this method is altered.
   *
   * @param fieldName the name of the field
   * @param numTokens the total number of tokens contained in fields named
   * <i>fieldName</i> of <i>doc</i>.
   * @return a normalization factor for hits on this field of this document
   *
   * @see org.apache.lucene.document.Field#setBoost(float)
   */
  public abstract float lengthNorm(String fieldName, int numTokens);

  /** Computes the normalization value for a query given the sum of the squared
   * weights of each of the query terms.  This value is then multipled into the
   * weight of each query term.
   *
   * <p>This does not affect ranking, but rather just attempts to make scores
   * from different queries comparable.
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
   *
   * @see org.apache.lucene.document.Field#setBoost(float)
   * @see SmallFloat
   */
  public static byte encodeNorm(float f) {
    return SmallFloat.floatToByte315(f);
  }


  /** Computes a score factor based on a term or phrase's frequency in a
   * document.  This value is multiplied by the {@link #idf(Term, Searcher)}
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
   * document.  This value is multiplied by the {@link #idf(Term, Searcher)}
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

  /** Computes a score factor for a simple term.
   *
   * <p>The default implementation is:<pre>
   *   return idf(searcher.docFreq(term), searcher.maxDoc());
   * </pre>
   *
   * Note that {@link Searcher#maxDoc()} is used instead of
   * {@link IndexReader#numDocs()} because it is proportional to
   * {@link Searcher#docFreq(Term)} , i.e., when one is inaccurate,
   * so is the other, and in the same direction.
   *
   * @param term the term in question
   * @param searcher the document collection being searched
   * @return a score factor for the term
   */
  public float idf(Term term, Searcher searcher) throws IOException {
    return idf(searcher.docFreq(term), searcher.maxDoc());
  }

  /** Computes a score factor for a phrase.
   *
   * <p>The default implementation sums the {@link #idf(Term,Searcher)} factor
   * for each term in the phrase.
   *
   * @param terms the terms in the phrase
   * @param searcher the document collection being searched
   * @return a score factor for the phrase
   */
  public float idf(Collection terms, Searcher searcher) throws IOException {
    float idf = 0.0f;
    Iterator i = terms.iterator();
    while (i.hasNext()) {
      idf += idf((Term)i.next(), searcher);
    }
    return idf;
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
}
