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
import java.util.*;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultipleTermPositions;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ToStringUtils;

/**
 * MultiPhraseQuery is a generalized version of PhraseQuery, with an added
 * method {@link #add(Term[])}.
 * To use this class, to search for the phrase "Microsoft app*" first use
 * add(Term) on the term "Microsoft", then find all terms that have "app" as
 * prefix using IndexReader.terms(Term), and use MultiPhraseQuery.add(Term[]
 * terms) to add them to the query.
 *
 * @author Anders Nielsen
 * @version 1.0
 */
public class MultiPhraseQuery extends Query {
  private String field;
  private ArrayList termArrays = new ArrayList();
  private Vector positions = new Vector();

  private int slop = 0;

  /** Sets the phrase slop for this query.
   * @see PhraseQuery#setSlop(int)
   */
  public void setSlop(int s) { slop = s; }

  /** Sets the phrase slop for this query.
   * @see PhraseQuery#getSlop()
   */
  public int getSlop() { return slop; }

  /** Add a single term at the next position in the phrase.
   * @see PhraseQuery#add(Term)
   */
  public void add(Term term) { add(new Term[]{term}); }

  /** Add multiple terms at the next position in the phrase.  Any of the terms
   * may match.
   *
   * @see PhraseQuery#add(Term)
   */
  public void add(Term[] terms) {
    int position = 0;
    if (positions.size() > 0)
      position = ((Integer) positions.lastElement()).intValue() + 1;

    add(terms, position);
  }

  /**
   * Allows to specify the relative position of terms within the phrase.
   * 
   * @see PhraseQuery#add(Term, int)
   * @param terms
   * @param position
   */
  public void add(Term[] terms, int position) {
    if (termArrays.size() == 0)
      field = terms[0].field();

    for (int i = 0; i < terms.length; i++) {
      if (terms[i].field() != field) {
        throw new IllegalArgumentException(
            "All phrase terms must be in the same field (" + field + "): "
                + terms[i]);
      }
    }

    termArrays.add(terms);
    positions.addElement(new Integer(position));
  }

  /**
   * Returns a List<Term[]> of the terms in the multiphrase.
   * Do not modify the List or its contents.
   */
  public List getTermArrays() {
	  return Collections.unmodifiableList(termArrays);
  }

  /**
   * Returns the relative positions of terms in this phrase.
   */
  public int[] getPositions() {
    int[] result = new int[positions.size()];
    for (int i = 0; i < positions.size(); i++)
      result[i] = ((Integer) positions.elementAt(i)).intValue();
    return result;
  }

  // inherit javadoc
  public void extractTerms(Set terms) {
    for (Iterator iter = termArrays.iterator(); iter.hasNext();) {
      Term[] arr = (Term[])iter.next();
      for (int i=0; i<arr.length; i++) {
        terms.add(arr[i]);
      }
    }
  }


  private class MultiPhraseWeight implements Weight {
    private Similarity similarity;
    private float value;
    private float idf;
    private float queryNorm;
    private float queryWeight;

    public MultiPhraseWeight(Searcher searcher)
      throws IOException {
      this.similarity = getSimilarity(searcher);

      // compute idf
      Iterator i = termArrays.iterator();
      while (i.hasNext()) {
        Term[] terms = (Term[])i.next();
        for (int j=0; j<terms.length; j++) {
          idf += getSimilarity(searcher).idf(terms[j], searcher);
        }
      }
    }

    public Query getQuery() { return MultiPhraseQuery.this; }
    public float getValue() { return value; }

    public float sumOfSquaredWeights() {
      queryWeight = idf * getBoost();             // compute query weight
      return queryWeight * queryWeight;           // square it
    }

    public void normalize(float queryNorm) {
      this.queryNorm = queryNorm;
      queryWeight *= queryNorm;                   // normalize query weight
      value = queryWeight * idf;                  // idf for document 
    }

    public Scorer scorer(IndexReader reader) throws IOException {
      if (termArrays.size() == 0)                  // optimize zero-term case
        return null;

      TermPositions[] tps = new TermPositions[termArrays.size()];
      for (int i=0; i<tps.length; i++) {
        Term[] terms = (Term[])termArrays.get(i);

        TermPositions p;
        if (terms.length > 1)
          p = new MultipleTermPositions(reader, terms);
        else
          p = reader.termPositions(terms[0]);

        if (p == null)
          return null;

        tps[i] = p;
      }

      if (slop == 0)
        return new ExactPhraseScorer(this, tps, getPositions(), similarity,
                                     reader.norms(field));
      else
        return new SloppyPhraseScorer(this, tps, getPositions(), similarity,
                                      slop, reader.norms(field));
    }

    public Explanation explain(IndexReader reader, int doc)
      throws IOException {
      ComplexExplanation result = new ComplexExplanation();
      result.setDescription("weight("+getQuery()+" in "+doc+"), product of:");

      Explanation idfExpl = new Explanation(idf, "idf("+getQuery()+")");

      // explain query weight
      Explanation queryExpl = new Explanation();
      queryExpl.setDescription("queryWeight(" + getQuery() + "), product of:");

      Explanation boostExpl = new Explanation(getBoost(), "boost");
      if (getBoost() != 1.0f)
        queryExpl.addDetail(boostExpl);

      queryExpl.addDetail(idfExpl);

      Explanation queryNormExpl = new Explanation(queryNorm,"queryNorm");
      queryExpl.addDetail(queryNormExpl);

      queryExpl.setValue(boostExpl.getValue() *
                         idfExpl.getValue() *
                         queryNormExpl.getValue());

      result.addDetail(queryExpl);

      // explain field weight
      ComplexExplanation fieldExpl = new ComplexExplanation();
      fieldExpl.setDescription("fieldWeight("+getQuery()+" in "+doc+
                               "), product of:");

      Explanation tfExpl = scorer(reader).explain(doc);
      fieldExpl.addDetail(tfExpl);
      fieldExpl.addDetail(idfExpl);

      Explanation fieldNormExpl = new Explanation();
      byte[] fieldNorms = reader.norms(field);
      float fieldNorm =
        fieldNorms!=null ? Similarity.decodeNorm(fieldNorms[doc]) : 0.0f;
      fieldNormExpl.setValue(fieldNorm);
      fieldNormExpl.setDescription("fieldNorm(field="+field+", doc="+doc+")");
      fieldExpl.addDetail(fieldNormExpl);

      fieldExpl.setMatch(Boolean.valueOf(tfExpl.isMatch()));
      fieldExpl.setValue(tfExpl.getValue() *
                         idfExpl.getValue() *
                         fieldNormExpl.getValue());

      result.addDetail(fieldExpl);
      result.setMatch(fieldExpl.getMatch());

      // combine them
      result.setValue(queryExpl.getValue() * fieldExpl.getValue());

      if (queryExpl.getValue() == 1.0f)
        return fieldExpl;

      return result;
    }
  }

  public Query rewrite(IndexReader reader) {
    if (termArrays.size() == 1) {                 // optimize one-term case
      Term[] terms = (Term[])termArrays.get(0);
      BooleanQuery boq = new BooleanQuery(true);
      for (int i=0; i<terms.length; i++) {
        boq.add(new TermQuery(terms[i]), BooleanClause.Occur.SHOULD);
      }
      boq.setBoost(getBoost());
      return boq;
    } else {
      return this;
    }
  }

  protected Weight createWeight(Searcher searcher) throws IOException {
    return new MultiPhraseWeight(searcher);
  }

  /** Prints a user-readable version of this query. */
  public final String toString(String f) {
    StringBuffer buffer = new StringBuffer();
    if (!field.equals(f)) {
      buffer.append(field);
      buffer.append(":");
    }

    buffer.append("\"");
    Iterator i = termArrays.iterator();
    while (i.hasNext()) {
      Term[] terms = (Term[])i.next();
      if (terms.length > 1) {
        buffer.append("(");
        for (int j = 0; j < terms.length; j++) {
          buffer.append(terms[j].text());
          if (j < terms.length-1)
            buffer.append(" ");
        }
        buffer.append(")");
      } else {
        buffer.append(terms[0].text());
      }
      if (i.hasNext())
        buffer.append(" ");
    }
    buffer.append("\"");

    if (slop != 0) {
      buffer.append("~");
      buffer.append(slop);
    }

    buffer.append(ToStringUtils.boost(getBoost()));

    return buffer.toString();
  }


  /** Returns true if <code>o</code> is equal to this. */
  public boolean equals(Object o) {
    if (!(o instanceof MultiPhraseQuery)) return false;
    MultiPhraseQuery other = (MultiPhraseQuery)o;
    return this.getBoost() == other.getBoost()
      && this.slop == other.slop
      && this.termArrays.equals(other.termArrays)
      && this.positions.equals(other.positions);
  }

  /** Returns a hash code value for this object.*/
  public int hashCode() {
    return Float.floatToIntBits(getBoost())
      ^ slop
      ^ termArrays.hashCode()
      ^ positions.hashCode()
      ^ 0x4AC65113;
  }
}
