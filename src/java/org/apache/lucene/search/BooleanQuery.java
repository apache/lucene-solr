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
public class BooleanQuery extends Query {
  private Vector clauses = new Vector();

  /** Constructs an empty boolean query. */
  public BooleanQuery() {}

  /** Adds a clause to a boolean query.  Clauses may be:
    <ul>
    <li><code>required</code> which means that documents which <i>do not</i>
    match this sub-query will <i>not</i> match the boolean query;
    <li><code>prohibited</code> which means that documents which <i>do</i>
    match this sub-query will <i>not</i> match the boolean query; or
    <li>neither, in which case matched documents are neither prohibited from
    nor required to match the sub-query.
    </ul>
    It is an error to specify a clause as both <code>required</code> and
    <code>prohibited</code>.
    */
  public void add(Query query, boolean required, boolean prohibited) {
    clauses.addElement(new BooleanClause(query, required, prohibited));
  }

  /** Adds a clause to a boolean query. */
  public void add(BooleanClause clause) {
    clauses.addElement(clause);
  }

  /** Returns the set of clauses in this query. */
  public BooleanClause[] getClauses() {
    return (BooleanClause[])clauses.toArray(new BooleanClause[0]);
  }

  private class BooleanWeight implements Weight {
    private Searcher searcher;
    private float norm;
    private Vector weights = new Vector();

    public BooleanWeight(Searcher searcher) {
      this.searcher = searcher;
      for (int i = 0 ; i < clauses.size(); i++) {
        BooleanClause c = (BooleanClause)clauses.elementAt(i);
        weights.add(c.query.createWeight(searcher));
      }
    }

    public Query getQuery() { return BooleanQuery.this; }
    public float getValue() { return getBoost(); }

    public float sumOfSquaredWeights() throws IOException {
      float sum = 0.0f;
      for (int i = 0 ; i < weights.size(); i++) {
        BooleanClause c = (BooleanClause)clauses.elementAt(i);
        Weight w = (Weight)weights.elementAt(i);
        if (!c.prohibited)
          sum += w.sumOfSquaredWeights();         // sum sub weights
      }
      
      sum *= getBoost() * getBoost();             // boost each sub-weight

      return sum ;
    }


    public void normalize(float norm) {
      norm *= getBoost();                         // incorporate boost
      for (int i = 0 ; i < weights.size(); i++) {
        BooleanClause c = (BooleanClause)clauses.elementAt(i);
        Weight w = (Weight)weights.elementAt(i);
        if (!c.prohibited)
          w.normalize(norm);
      }
    }

    public Scorer scorer(IndexReader reader) throws IOException {
      BooleanScorer result = new BooleanScorer(searcher.getSimilarity());

      for (int i = 0 ; i < weights.size(); i++) {
        BooleanClause c = (BooleanClause)clauses.elementAt(0);
        Weight w = (Weight)weights.elementAt(i);
        Scorer subScorer = w.scorer(reader);
        if (subScorer != null)
          result.add(subScorer, c.required, c.prohibited);
        else if (c.required)
          return null;
      }

      return result;
    }

    public Explanation explain(IndexReader reader, int doc)
      throws IOException {
      Explanation sumExpl = new Explanation();
      sumExpl.setDescription("sum of:");
      int coord = 0;
      int maxCoord = 0;
      float sum = 0.0f;
      for (int i = 0 ; i < weights.size(); i++) {
        BooleanClause c = (BooleanClause)clauses.elementAt(0);
        Weight w = (Weight)weights.elementAt(i);
        Explanation e = w.explain(reader, doc);
        if (!c.prohibited) maxCoord++;
        if (e.getValue() > 0) {
          if (!c.prohibited) {
            sumExpl.addDetail(e);
            sum += e.getValue();
            coord++;
          } else {
            return new Explanation(0.0f, "match prohibited");
          }
        } else if (c.required) {
          return new Explanation(0.0f, "match required");
        }
      }
      sumExpl.setValue(sum);

      if (coord == 1)                               // only one clause matched
        sumExpl = sumExpl.getDetails()[0];          // eliminate wrapper

      float coordFactor = searcher.getSimilarity().coord(coord, maxCoord);
      if (coordFactor == 1.0f)                      // coord is no-op
        return sumExpl;                             // eliminate wrapper
      else {
        Explanation result = new Explanation();
        result.setDescription("product of:");
        result.addDetail(sumExpl);
        result.addDetail(new Explanation(coordFactor,
                                         "coord("+coord+"/"+maxCoord+")"));
        result.setValue(sum*coordFactor);
        return result;
      }
    }
  }

  protected Weight createWeight(Searcher searcher) {
    return new BooleanWeight(searcher);
  }

  public Query rewrite(IndexReader reader) throws IOException {
    if (clauses.size() == 1) {                    // optimize 1-clause queries
      BooleanClause c = (BooleanClause)clauses.elementAt(0);
      if (!c.prohibited) {			  // just return clause
        Query clone = (Query)c.query.clone();     // have to clone to boost
        clone.setBoost(getBoost() * clone.getBoost());
        return clone;
      }
    }

    BooleanQuery clone = (BooleanQuery)this.clone(); // recursively clone
    boolean changed = false;
    for (int i = 0 ; i < clauses.size(); i++) {
      BooleanClause c = (BooleanClause)clauses.elementAt(i);
      Query q = c.query.rewrite(reader);
      if (q != c.query) {                         // rewrote
        changed = true;                           // replace in clone
        clone.clauses.setElementAt
          (new BooleanClause(q, c.required, c.prohibited), i);
      }
    }
    if (changed)
      return clone;                               // clauses rewrote
    else
      return this;                                // no clauses rewrote
  }


  public Object clone() {
    BooleanQuery clone = (BooleanQuery)super.clone();
    clone.clauses = (Vector)this.clauses.clone();
    return clone;
  }

  /** Prints a user-readable version of this query. */
  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    if (getBoost() != 1.0) {
      buffer.append("(");
    }

    for (int i = 0 ; i < clauses.size(); i++) {
      BooleanClause c = (BooleanClause)clauses.elementAt(i);
      if (c.prohibited)
	buffer.append("-");
      else if (c.required)
	buffer.append("+");

      Query subQuery = c.query;
      if (subQuery instanceof BooleanQuery) {	  // wrap sub-bools in parens
	buffer.append("(");
	buffer.append(c.query.toString(field));
	buffer.append(")");
      } else
	buffer.append(c.query.toString(field));

      if (i != clauses.size()-1)
	buffer.append(" ");
    }

    if (getBoost() != 1.0) {
      buffer.append(")^");
      buffer.append(getBoost());
    }

    return buffer.toString();
  }

  /** Returns true iff <code>o</code> is equal to this. */
  public boolean equals(Object o) {
    if (!(o instanceof BooleanQuery))
      return false;
    BooleanQuery other = (BooleanQuery)o;
    return (this.getBoost() == other.getBoost())
      &&  this.clauses.equals(other.clauses);
  }

  /** Returns a hash code value for this object.*/
  public int hashCode() {
    return Float.floatToIntBits(getBoost()) ^ clauses.hashCode();
  }

}
