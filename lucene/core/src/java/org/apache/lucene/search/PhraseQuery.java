package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

/** A Query that matches documents containing a particular sequence of terms.
 * A PhraseQuery is built by QueryParser for input like <code>"new york"</code>.
 * 
 * <p>This query may be combined with other terms or queries with a {@link BooleanQuery}.
 *
 * <b>NOTE</b>: Leading holes don't have any particular meaning for this query
 * and will be ignored. For instance this query:
 * <pre class="prettyprint">
 * PhraseQuery pq = new PhraseQuery();
 * pq.add(new Term("body", "one"), 4);
 * pq.add(new Term("body", "two"), 5);
 * </pre>
 * is equivalent to the below query:
 * <pre class="prettyprint">
 * PhraseQuery pq = new PhraseQuery();
 * pq.add(new Term("body", "one"), 0);
 * pq.add(new Term("body", "two"), 1);
 * </pre>
 */
public class PhraseQuery extends Query {
  private String field;
  private ArrayList<Term> terms = new ArrayList<>(4);
  private ArrayList<Integer> positions = new ArrayList<>(4);
  private int slop = 0;

  /** Constructs an empty phrase query. */
  public PhraseQuery() {}

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
  public void setSlop(int s) {
    if (s < 0) {
      throw new IllegalArgumentException("slop value cannot be negative");
    }
    slop = s; 
  }
  /** Returns the slop.  See setSlop(). */
  public int getSlop() { return slop; }

  /**
   * Adds a term to the end of the query phrase.
   * The relative position of the term is the one immediately after the last term added.
   */
  public void add(Term term) {
    int position = 0;
    if (positions.size() > 0) {
      position = positions.get(positions.size()-1) + 1;
    }

    add(term, position);
  }

  /**
   * Adds a term to the end of the query phrase.
   * The relative position of the term within the phrase is specified explicitly.
   * This allows e.g. phrases with more than one term at the same position
   * or phrases with gaps (e.g. in connection with stopwords).
   * 
   */
  public void add(Term term, int position) {
    if (positions.size() > 0) {
      final int previousPosition = positions.get(positions.size()-1);
      if (position < previousPosition) {
        throw new IllegalArgumentException("Positions must be added in order. Got position="
            + position + " while previous position was " + previousPosition);
      }
    } else if (position < 0) {
      throw new IllegalArgumentException("Positions must be positive, got " + position);
    }

    if (terms.size() == 0) {
      field = term.field();
    } else if (!term.field().equals(field)) {
      throw new IllegalArgumentException("All phrase terms must be in the same field: " + term);
    }

    terms.add(term);
    positions.add(Integer.valueOf(position));
  }

  /** Returns the set of terms in this phrase. */
  public Term[] getTerms() {
    return terms.toArray(new Term[0]);
  }

  /**
   * Returns the relative positions of terms in this phrase.
   */
  public int[] getPositions() {
      int[] result = new int[positions.size()];
      for(int i = 0; i < positions.size(); i++)
          result[i] = positions.get(i).intValue();
      return result;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (terms.isEmpty()) {
      BooleanQuery bq = new BooleanQuery();
      bq.setBoost(getBoost());
      return bq;
    } else if (terms.size() == 1) {
      TermQuery tq = new TermQuery(terms.get(0));
      tq.setBoost(getBoost());
      return tq;
    } else if (positions.get(0).intValue() != 0) {
      // PhraseWeight requires that positions start at 0 so we need to rebase
      // positions
      final Term[] terms = getTerms();
      final int[] positions = getPositions();
      PhraseQuery rewritten = new PhraseQuery();
      for (int i = 0; i < terms.length; ++i) {
        rewritten.add(terms[i], positions[i] - positions[0]);
      }
      rewritten.setBoost(getBoost());
      rewritten.setSlop(getSlop());
      return rewritten;
    } else {
      return super.rewrite(reader);
    }
  }

  static class PostingsAndFreq implements Comparable<PostingsAndFreq> {
    final PostingsEnum postings;
    final int position;
    final Term[] terms;
    final int nTerms; // for faster comparisons

    public PostingsAndFreq(PostingsEnum postings, int position, Term... terms) {
      this.postings = postings;
      this.position = position;
      nTerms = terms==null ? 0 : terms.length;
      if (nTerms>0) {
        if (terms.length==1) {
          this.terms = terms;
        } else {
          Term[] terms2 = new Term[terms.length];
          System.arraycopy(terms, 0, terms2, 0, terms.length);
          Arrays.sort(terms2);
          this.terms = terms2;
        }
      } else {
        this.terms = null;
      }
    }

    @Override
    public int compareTo(PostingsAndFreq other) {
      if (position != other.position) {
        return position - other.position;
      }
      if (nTerms != other.nTerms) {
        return nTerms - other.nTerms;
      }
      if (nTerms == 0) {
        return 0;
      }
      for (int i=0; i<terms.length; i++) {
        int res = terms[i].compareTo(other.terms[i]);
        if (res!=0) return res;
      }
      return 0;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + position;
      for (int i=0; i<nTerms; i++) {
        result = prime * result + terms[i].hashCode(); 
      }
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      PostingsAndFreq other = (PostingsAndFreq) obj;
      if (position != other.position) return false;
      if (terms == null) return other.terms == null;
      return Arrays.equals(terms, other.terms);
    }
  }

  private class PhraseWeight extends Weight {
    private final Similarity similarity;
    private final Similarity.SimWeight stats;
    private final boolean needsScores;
    private transient TermContext states[];

    public PhraseWeight(IndexSearcher searcher, boolean needsScores)
      throws IOException {
      super(PhraseQuery.this);
      final int[] positions = PhraseQuery.this.getPositions();
      if (positions.length < 2) {
        throw new IllegalStateException("PhraseWeight does not support less than 2 terms, call rewrite first");
      } else if (positions[0] != 0) {
        throw new IllegalStateException("PhraseWeight requires that the first position is 0, call rewrite first");
      }
      this.needsScores = needsScores;
      this.similarity = searcher.getSimilarity();
      final IndexReaderContext context = searcher.getTopReaderContext();
      states = new TermContext[terms.size()];
      TermStatistics termStats[] = new TermStatistics[terms.size()];
      for (int i = 0; i < terms.size(); i++) {
        final Term term = terms.get(i);
        states[i] = TermContext.build(context, term);
        termStats[i] = searcher.termStatistics(term, states[i]);
      }
      stats = similarity.computeWeight(getBoost(), searcher.collectionStatistics(field), termStats);
    }

    @Override
    public String toString() { return "weight(" + PhraseQuery.this + ")"; }

    @Override
    public float getValueForNormalization() {
      return stats.getValueForNormalization();
    }

    @Override
    public void normalize(float queryNorm, float topLevelBoost) {
      stats.normalize(queryNorm, topLevelBoost);
    }

    @Override
    public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
      assert !terms.isEmpty();
      final LeafReader reader = context.reader();
      final Bits liveDocs = acceptDocs;
      PostingsAndFreq[] postingsFreqs = new PostingsAndFreq[terms.size()];

      final Terms fieldTerms = reader.terms(field);
      if (fieldTerms == null) {
        return null;
      }

      if (fieldTerms.hasPositions() == false) {
        throw new IllegalStateException("field \"" + field + "\" was indexed without position data; cannot run PhraseQuery (phrase=" + getQuery() + ")");
      }

      // Reuse single TermsEnum below:
      final TermsEnum te = fieldTerms.iterator();
      
      for (int i = 0; i < terms.size(); i++) {
        final Term t = terms.get(i);
        final TermState state = states[i].get(context.ord);
        if (state == null) { /* term doesnt exist in this segment */
          assert termNotInReader(reader, t): "no termstate found but term exists in reader";
          return null;
        }
        te.seekExact(t.bytes(), state);
        PostingsEnum postingsEnum = te.postings(liveDocs, null, PostingsEnum.POSITIONS);
        postingsFreqs[i] = new PostingsAndFreq(postingsEnum, positions.get(i), t);
      }

      // sort by increasing docFreq order
      if (slop == 0) {
        ArrayUtil.timSort(postingsFreqs);
      }

      if (slop == 0) {  // optimize exact case
        return new ExactPhraseScorer(this, postingsFreqs, similarity.simScorer(stats, context), needsScores);
      } else {
        return new SloppyPhraseScorer(this, postingsFreqs, slop, similarity.simScorer(stats, context), needsScores);
      }
    }
    
    // only called from assert
    private boolean termNotInReader(LeafReader reader, Term term) throws IOException {
      return reader.docFreq(term) == 0;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context, context.reader().getLiveDocs());
      if (scorer != null) {
        int newDoc = scorer.advance(doc);
        if (newDoc == doc) {
          float freq = slop == 0 ? scorer.freq() : ((SloppyPhraseScorer)scorer).sloppyFreq();
          SimScorer docScorer = similarity.simScorer(stats, context);
          ComplexExplanation result = new ComplexExplanation();
          result.setDescription("weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], result of:");
          Explanation scoreExplanation = docScorer.explain(doc, new Explanation(freq, "phraseFreq=" + freq));
          result.addDetail(scoreExplanation);
          result.setValue(scoreExplanation.getValue());
          result.setMatch(true);
          return result;
        }
      }
      
      return new ComplexExplanation(false, 0.0f, "no matching term");
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new PhraseWeight(searcher, needsScores);
  }

  /**
   * @see org.apache.lucene.search.Query#extractTerms(Set)
   */
  @Override
  public void extractTerms(Set<Term> queryTerms) {
    queryTerms.addAll(terms);
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String f) {
    StringBuilder buffer = new StringBuilder();
    if (field != null && !field.equals(f)) {
      buffer.append(field);
      buffer.append(":");
    }

    buffer.append("\"");
    final int maxPosition;
    if (positions.isEmpty()) {
      maxPosition = -1;
    } else {
      maxPosition = positions.get(positions.size() - 1);
    }
    String[] pieces = new String[maxPosition + 1];
    for (int i = 0; i < terms.size(); i++) {
      int pos = positions.get(i).intValue();
      String s = pieces[pos];
      if (s == null) {
        s = (terms.get(i)).text();
      } else {
        s = s + "|" + (terms.get(i)).text();
      }
      pieces[pos] = s;
    }
    for (int i = 0; i < pieces.length; i++) {
      if (i > 0) {
        buffer.append(' ');
      }
      String s = pieces[i];
      if (s == null) {
        buffer.append('?');
      } else {
        buffer.append(s);
      }
    }
    buffer.append("\"");

    if (slop != 0) {
      buffer.append("~");
      buffer.append(slop);
    }

    buffer.append(ToStringUtils.boost(getBoost()));

    return buffer.toString();
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PhraseQuery))
      return false;
    PhraseQuery other = (PhraseQuery)o;
    return super.equals(o)
      && (this.slop == other.slop)
      &&  this.terms.equals(other.terms)
      && this.positions.equals(other.positions);
  }

  /** Returns a hash code value for this object.*/
  @Override
  public int hashCode() {
    return super.hashCode()
      ^ slop
      ^ terms.hashCode()
      ^ positions.hashCode();
  }

}
