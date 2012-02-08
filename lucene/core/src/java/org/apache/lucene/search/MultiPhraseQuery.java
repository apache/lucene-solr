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
import java.util.*;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultipleTermPositions;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.Explanation.IDFExplanation;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ToStringUtils;

/**
 * MultiPhraseQuery is a generalized version of PhraseQuery, with an added
 * method {@link #add(Term[])}.
 * To use this class, to search for the phrase "Microsoft app*" first use
 * add(Term) on the term "Microsoft", then find all terms that have "app" as
 * prefix using IndexReader.terms(Term), and use MultiPhraseQuery.add(Term[]
 * terms) to add them to the query.
 *
 * @version 1.0
 */
public class MultiPhraseQuery extends Query {
  private String field;
  private ArrayList<Term[]> termArrays = new ArrayList<Term[]>();
  private ArrayList<Integer> positions = new ArrayList<Integer>();

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
      position = positions.get(positions.size()-1).intValue() + 1;

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
    positions.add(Integer.valueOf(position));
  }

  /**
   * Returns a List of the terms in the multiphrase.
   * Do not modify the List or its contents.
   */
  public List<Term[]> getTermArrays() {
	  return Collections.unmodifiableList(termArrays);
  }

  /**
   * Returns the relative positions of terms in this phrase.
   */
  public int[] getPositions() {
    int[] result = new int[positions.size()];
    for (int i = 0; i < positions.size(); i++)
      result[i] = positions.get(i).intValue();
    return result;
  }

  // inherit javadoc
  @Override
  public void extractTerms(Set<Term> terms) {
    for (final Term[] arr : termArrays) {
      for (final Term term: arr) {
        terms.add(term);
      }
    }
  }


  private class MultiPhraseWeight extends Weight {
    private Similarity similarity;
    private float value;
    private final IDFExplanation idfExp;
    private float idf;
    private float queryNorm;
    private float queryWeight;

    public MultiPhraseWeight(Searcher searcher)
      throws IOException {
      this.similarity = getSimilarity(searcher);

      // compute idf
      ArrayList<Term> allTerms = new ArrayList<Term>();
      for(final Term[] terms: termArrays) {
        for (Term term: terms) {
          allTerms.add(term);
        }
      }
      idfExp = similarity.idfExplain(allTerms, searcher);
      idf = idfExp.getIdf();
    }

    @Override
    public Query getQuery() { return MultiPhraseQuery.this; }

    @Override
    public float getValue() { return value; }

    @Override
    public float sumOfSquaredWeights() {
      queryWeight = idf * getBoost();             // compute query weight
      return queryWeight * queryWeight;           // square it
    }

    @Override
    public void normalize(float queryNorm) {
      this.queryNorm = queryNorm;
      queryWeight *= queryNorm;                   // normalize query weight
      value = queryWeight * idf;                  // idf for document 
    }

    @Override
    public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
      if (termArrays.size() == 0)                  // optimize zero-term case
        return null;

      PhraseQuery.PostingsAndFreq[] postingsFreqs = new PhraseQuery.PostingsAndFreq[termArrays.size()];

      for (int pos=0; pos<postingsFreqs.length; pos++) {
        Term[] terms = termArrays.get(pos);

        final TermPositions p;
        int docFreq;

        if (terms.length > 1) {
          p = new MultipleTermPositions(reader, terms);

          // coarse -- this overcounts since a given doc can
          // have more than one terms:
          docFreq = 0;
          for(int termIdx=0;termIdx<terms.length;termIdx++) {
            docFreq += reader.docFreq(terms[termIdx]);
          }
        } else {
          p = reader.termPositions(terms[0]);
          docFreq = reader.docFreq(terms[0]);

          if (p == null)
            return null;
        }

        postingsFreqs[pos] = new PhraseQuery.PostingsAndFreq(p, docFreq, positions.get(pos).intValue(), terms[0]);
      }

      // sort by increasing docFreq order
      if (slop == 0) {
        ArrayUtil.mergeSort(postingsFreqs);
      }

      if (slop == 0) {
        ExactPhraseScorer s = new ExactPhraseScorer(this, postingsFreqs, similarity,
                                                    reader.norms(field));
        if (s.noDocs) {
          return null;
        } else {
          return s;
        }
      } else {
        return new SloppyPhraseScorer(this, postingsFreqs, similarity,
                                      slop, reader.norms(field));
      }
    }

    @Override
    public Explanation explain(IndexReader reader, int doc)
      throws IOException {
      ComplexExplanation result = new ComplexExplanation();
      result.setDescription("weight("+getQuery()+" in "+doc+"), product of:");

      Explanation idfExpl = new Explanation(idf, "idf(" + field + ":" + idfExp.explain() +")");

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

      Scorer scorer = scorer(reader, true, false);
      if (scorer == null) {
        return new Explanation(0.0f, "no matching docs");
      }

      Explanation tfExplanation = new Explanation();
      int d = scorer.advance(doc);
      float phraseFreq;
      if (d == doc) {
        phraseFreq = scorer.freq();
      } else {
        phraseFreq = 0.0f;
      }

      tfExplanation.setValue(similarity.tf(phraseFreq));
      tfExplanation.setDescription("tf(phraseFreq=" + phraseFreq + ")");
      fieldExpl.addDetail(tfExplanation);
      fieldExpl.addDetail(idfExpl);

      Explanation fieldNormExpl = new Explanation();
      byte[] fieldNorms = reader.norms(field);
      float fieldNorm =
        fieldNorms!=null ? similarity.decodeNormValue(fieldNorms[doc]) : 1.0f;
      fieldNormExpl.setValue(fieldNorm);
      fieldNormExpl.setDescription("fieldNorm(field="+field+", doc="+doc+")");
      fieldExpl.addDetail(fieldNormExpl);

      fieldExpl.setMatch(Boolean.valueOf(tfExplanation.isMatch()));
      fieldExpl.setValue(tfExplanation.getValue() *
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

  @Override
  public Query rewrite(IndexReader reader) {
    if (termArrays.size() == 1) {                 // optimize one-term case
      Term[] terms = termArrays.get(0);
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

  @Override
  public Weight createWeight(Searcher searcher) throws IOException {
    return new MultiPhraseWeight(searcher);
  }

  /** Prints a user-readable version of this query. */
  @Override
  public final String toString(String f) {
    StringBuilder buffer = new StringBuilder();
    if (field == null || !field.equals(f)) {
      buffer.append(field);
      buffer.append(":");
    }

    buffer.append("\"");
    Iterator<Term[]> i = termArrays.iterator();
    while (i.hasNext()) {
      Term[] terms = i.next();
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
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MultiPhraseQuery)) return false;
    MultiPhraseQuery other = (MultiPhraseQuery)o;
    return this.getBoost() == other.getBoost()
      && this.slop == other.slop
      && termArraysEquals(this.termArrays, other.termArrays)
      && this.positions.equals(other.positions);
  }

  /** Returns a hash code value for this object.*/
  @Override
  public int hashCode() {
    return Float.floatToIntBits(getBoost())
      ^ slop
      ^ termArraysHashCode()
      ^ positions.hashCode()
      ^ 0x4AC65113;
  }
  
  // Breakout calculation of the termArrays hashcode
  private int termArraysHashCode() {
    int hashCode = 1;
    for (final Term[] termArray: termArrays) {
      hashCode = 31 * hashCode
          + (termArray == null ? 0 : Arrays.hashCode(termArray));
    }
    return hashCode;
  }

  // Breakout calculation of the termArrays equals
  private boolean termArraysEquals(List<Term[]> termArrays1, List<Term[]> termArrays2) {
    if (termArrays1.size() != termArrays2.size()) {
      return false;
    }
    ListIterator<Term[]> iterator1 = termArrays1.listIterator();
    ListIterator<Term[]> iterator2 = termArrays2.listIterator();
    while (iterator1.hasNext()) {
      Term[] termArray1 = iterator1.next();
      Term[] termArray2 = iterator2.next();
      if (!(termArray1 == null ? termArray2 == null : Arrays.equals(termArray1,
          termArray2))) {
        return false;
      }
    }
    return true;
  }
}
