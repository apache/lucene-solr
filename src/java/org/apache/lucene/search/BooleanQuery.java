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
import org.apache.lucene.index.IndexReader;

/** A Query that matches documents matching boolean combinations of other
  queries, typically {@link TermQuery}s or {@link PhraseQuery}s.
  */
final public class BooleanQuery extends Query {
  private Vector clauses = new Vector();

  /** Constructs an empty boolean query. */
  public BooleanQuery() {}

  /** Adds a clause to a boolean query.  Clauses may be:
    <ul>
    <li><code>required</code> which means that documents which <i>do not</i>
    match this sub-query will <it>not</it> match the boolean query;
    <li><code>prohibited</code> which means that documents which <i>do</i>
    match this sub-query will <it>not</it> match the boolean query; or
    <li>neither, in which case matched documents are neither prohibited from
    nor required to match the sub-query.
    </ul>
    It is an error to specify a clause as both <code>required</code> and
    <code>prohibited</code>.
    */
  public final void add(Query query, boolean required, boolean prohibited) {
    clauses.addElement(new BooleanClause(query, required, prohibited));
  }

  /** Adds a clause to a boolean query. */
  public final void add(BooleanClause clause) {
    clauses.addElement(clause);
  }

  void prepare(IndexReader reader) {
    for (int i = 0 ; i < clauses.size(); i++) {
      BooleanClause c = (BooleanClause)clauses.elementAt(i);
      c.query.prepare(reader);
    }
  }

  final float sumOfSquaredWeights(Searcher searcher)
       throws IOException {
    float sum = 0.0f;

    for (int i = 0 ; i < clauses.size(); i++) {
      BooleanClause c = (BooleanClause)clauses.elementAt(i);
      if (!c.prohibited)
	sum += c.query.sumOfSquaredWeights(searcher); // sum sub-query weights
    }

    return sum;
  }

  final void normalize(float norm) {
    for (int i = 0 ; i < clauses.size(); i++) {
      BooleanClause c = (BooleanClause)clauses.elementAt(i);
      if (!c.prohibited)
	c.query.normalize(norm);
    }
  }

  final Scorer scorer(IndexReader reader)
       throws IOException {

    if (clauses.size() == 1) {			  // optimize 1-term queries
      BooleanClause c = (BooleanClause)clauses.elementAt(0);
      if (!c.prohibited)			  // just return term scorer
	return c.query.scorer(reader);
    }

    BooleanScorer result = new BooleanScorer();

    int theMask = 1, thisMask;
    for (int i = 0 ; i < clauses.size(); i++) {
      BooleanClause c = (BooleanClause)clauses.elementAt(i);
      if (c.required || c.prohibited) {
	thisMask = theMask;
	theMask = theMask << 1;
      } else
	thisMask = 0;
      
      Scorer subScorer = c.query.scorer(reader);
      if (subScorer != null)
	result.add(subScorer, c.required, c.prohibited);
      else if (c.required)
	return null;
    }
    if (theMask == 0)
      throw new IndexOutOfBoundsException
	("More than 32 required/prohibited clauses in query.");

    return result;
  }

  /** Prints a user-readable version of this query. */
  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    for (int i = 0 ; i < clauses.size(); i++) {
      BooleanClause c = (BooleanClause)clauses.elementAt(i);
      if (c.prohibited)
	buffer.append("-");
      else if (c.required)
	buffer.append("+");

      Query subQuery = c.query;
      if (subQuery instanceof BooleanQuery) {	  // wrap sub-bools in parens
	BooleanQuery bq = (BooleanQuery)subQuery;
	buffer.append("(");
	buffer.append(c.query.toString(field));
	buffer.append(")");
      } else
	buffer.append(c.query.toString(field));

      if (i != clauses.size()-1)
	buffer.append(" ");
    }
    return buffer.toString();
  }

}
