package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;
import java.util.Vector;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.IndexReader;

/** A Query that matches documents containing a particular sequence of terms.
  This may be combined with other terms with a {@link BooleanQuery}.
  */
public class PhraseQuery extends Query {
  private String field;
  private Vector terms = new Vector();
  private float idf = 0.0f;
  private float weight = 0.0f;

  private int slop = 0;


  /** Constructs an empty phrase query. */
  public PhraseQuery() {
  }

  /** Sets the number of other words permitted between words in query phrase.
    If zero, then this is an exact phrase search.  For larger values this works
    like a <code>WITHIN</code> or <code>NEAR</code> operator.

    <p>The slop is in fact an edit-distance, where the units correspond to
    moves of terms in the query phrase out of position.  For example, to switch
    the order of two words requires two moves (the first move places the words
    atop one another), so to permit re-orderings of phrases, the slop must be
    at least two.

    <p>More exact matches are scored higher than sloppier matches, thus search
    results are sorted by exactness.

    <p>The slop is zero by default, requiring exact matches.*/
  public void setSlop(int s) { slop = s; }
  /** Returns the slop.  See setSlop(). */
  public int getSlop() { return slop; }

  /** Adds a term to the end of the query phrase. */
  public void add(Term term) {
    if (terms.size() == 0)
      field = term.field();
    else if (term.field() != field)
      throw new IllegalArgumentException
	("All phrase terms must be in the same field: " + term);

    terms.addElement(term);
  }

  final float sumOfSquaredWeights(Searcher searcher) throws IOException {
    for (int i = 0; i < terms.size(); i++)	  // sum term IDFs
      idf += Similarity.idf((Term)terms.elementAt(i), searcher);

    weight = idf * boost;
    return weight * weight;			  // square term weights
  }

  final void normalize(float norm) {
    weight *= norm;				  // normalize for query
    weight *= idf;				  // factor from document
  }

  final Scorer scorer(IndexReader reader) throws IOException {
    if (terms.size() == 0)			  // optimize zero-term case
      return null;
    if (terms.size() == 1) {			  // optimize one-term case
      Term term = (Term)terms.elementAt(0);
      TermDocs docs = reader.termDocs(term);
      if (docs == null)
	return null;
      return new TermScorer(docs, reader.norms(term.field()), weight);
    }

    TermPositions[] tps = new TermPositions[terms.size()];
    for (int i = 0; i < terms.size(); i++) {
      TermPositions p = reader.termPositions((Term)terms.elementAt(i));
      if (p == null)
	return null;
      tps[i] = p;
    }

    if (slop == 0)				  // optimize exact case
      return new ExactPhraseScorer(tps, reader.norms(field), weight);
    else
      return
	new SloppyPhraseScorer(tps, slop, reader.norms(field), weight);

  }

  /** Prints a user-readable version of this query. */
  public String toString(String f) {
    StringBuffer buffer = new StringBuffer();
    if (!field.equals(f)) {
      buffer.append(field);
      buffer.append(":");
    }

    buffer.append("\"");
    for (int i = 0; i < terms.size(); i++) {
      buffer.append(((Term)terms.elementAt(i)).text());
      if (i != terms.size()-1)
	buffer.append(" ");
    }
    buffer.append("\"");

    if (slop != 0) {
      buffer.append("~");
      buffer.append(slop);
    }

    if (boost != 1.0f) {
      buffer.append("^");
      buffer.append(Float.toString(boost));
    }

    return buffer.toString();
  }
}
