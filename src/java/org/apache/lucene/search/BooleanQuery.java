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

import java.io.IOException;
import java.util.Vector;
import org.apache.lucene.index.IndexReader;

/** A Query that matches documents matching boolean combinations of other
  queries, typically {@link TermQuery}s or {@link PhraseQuery}s.
  */
public class BooleanQuery extends Query {

  /**
   * Default value is 1024.  Use <code>org.apache.lucene.maxClauseCount</code>
   * system property to override.
   */
  public static int maxClauseCount =
    Integer.parseInt(System.getProperty("org.apache.lucene.maxClauseCount",
      "1024"));

  /** Thrown when an attempt is made to add more than {@link
   * #getMaxClauseCount()} clauses. */
  public static class TooManyClauses extends RuntimeException {}

  /** Return the maximum number of clauses permitted, 1024 by default.
   * Attempts to add more than the permitted number of clauses cause {@link
   * TooManyClauses} to be thrown.*/
  public static int getMaxClauseCount() { return maxClauseCount; }

  /** Set the maximum number of clauses permitted. */
  public static void setMaxClauseCount(int maxClauseCount) {
    BooleanQuery.maxClauseCount = maxClauseCount;
  }

  private Vector clauses = new Vector();

  /** Constructs an empty boolean query. */
  public BooleanQuery() {}

  /** Adds a clause to a boolean query.  Clauses may be:
   * <ul>
   * <li><code>required</code> which means that documents which <i>do not</i>
   * match this sub-query will <i>not</i> match the boolean query;
   * <li><code>prohibited</code> which means that documents which <i>do</i>
   * match this sub-query will <i>not</i> match the boolean query; or
   * <li>neither, in which case matched documents are neither prohibited from
   * nor required to match the sub-query. However, a document must match at
   * least 1 sub-query to match the boolean query.
   * </ul>
   * It is an error to specify a clause as both <code>required</code> and
   * <code>prohibited</code>.
   *
   * @see #getMaxClauseCount()
   */
  public void add(Query query, boolean required, boolean prohibited) {
    add(new BooleanClause(query, required, prohibited));
  }

  /** Adds a clause to a boolean query.
    * @see #getMaxClauseCount()
   */
  public void add(BooleanClause clause) {
    if (clauses.size() >= maxClauseCount)
      throw new TooManyClauses();

    clauses.addElement(clause);
  }

  /** Returns the set of clauses in this query. */
  public BooleanClause[] getClauses() {
    return (BooleanClause[])clauses.toArray(new BooleanClause[0]);
  }

  private class BooleanWeight implements Weight {
    private Searcher searcher;
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
      // First see if the (faster) ConjunctionScorer will work.  This can be
      // used when all clauses are required.  Also, at this point a
      // BooleanScorer cannot be embedded in a ConjunctionScorer, as the hits
      // from a BooleanScorer are not always sorted by document number (sigh)
      // and hence BooleanScorer cannot implement skipTo() correctly, which is
      // required by ConjunctionScorer.
      boolean allRequired = true;
      boolean noneBoolean = true;
      for (int i = 0 ; i < weights.size(); i++) {
        BooleanClause c = (BooleanClause)clauses.elementAt(i);
        if (!c.required)
          allRequired = false;
        if (c.query instanceof BooleanQuery)
          noneBoolean = false;
      }

      if (allRequired && noneBoolean) {           // ConjunctionScorer is okay
        ConjunctionScorer result =
          new ConjunctionScorer(getSimilarity(searcher));
        for (int i = 0 ; i < weights.size(); i++) {
          Weight w = (Weight)weights.elementAt(i);
          Scorer subScorer = w.scorer(reader);
          if (subScorer == null)
            return null;
          result.add(subScorer);
        }
        return result;
      }

      // Use good-old BooleanScorer instead.
      BooleanScorer result = new BooleanScorer(getSimilarity(searcher));

      for (int i = 0 ; i < weights.size(); i++) {
        BooleanClause c = (BooleanClause)clauses.elementAt(i);
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
        BooleanClause c = (BooleanClause)clauses.elementAt(i);
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

      float coordFactor = getSimilarity(searcher).coord(coord, maxCoord);
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

        Query query = c.query.rewrite(reader);    // rewrite first

        if (getBoost() != 1.0f) {                 // incorporate boost
          if (query == c.query)                   // if rewrite was no-op
            query = (Query)query.clone();         // then clone before boost
          query.setBoost(getBoost() * query.getBoost());
        }

        return query;
      }
    }

    BooleanQuery clone = null;                    // recursively rewrite
    for (int i = 0 ; i < clauses.size(); i++) {
      BooleanClause c = (BooleanClause)clauses.elementAt(i);
      Query query = c.query.rewrite(reader);
      if (query != c.query) {                     // clause rewrote: must clone
        if (clone == null)
          clone = (BooleanQuery)this.clone();
        clone.clauses.setElementAt
          (new BooleanClause(query, c.required, c.prohibited), i);
      }
    }
    if (clone != null) {
      return clone;                               // some clauses rewrote
    } else
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
