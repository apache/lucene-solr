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
import java.util.Set;
import java.util.ArrayList;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.IndexReader.ReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Similarity.SloppyDocScorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TermContext;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;

/** A Query that matches documents containing a particular sequence of terms.
 * A PhraseQuery is built by QueryParser for input like <code>"new york"</code>.
 * 
 * <p>This query may be combined with other terms or queries with a {@link BooleanQuery}.
 */
public class PhraseQuery extends Query {
  private String field;
  private ArrayList<Term> terms = new ArrayList<Term>(4);
  private ArrayList<Integer> positions = new ArrayList<Integer>(4);
  private int maxPosition = 0;
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
  public void setSlop(int s) { slop = s; }
  /** Returns the slop.  See setSlop(). */
  public int getSlop() { return slop; }

  /**
   * Adds a term to the end of the query phrase.
   * The relative position of the term is the one immediately after the last term added.
   */
  public void add(Term term) {
    int position = 0;
    if(positions.size() > 0)
        position = positions.get(positions.size()-1).intValue() + 1;

    add(term, position);
  }

  /**
   * Adds a term to the end of the query phrase.
   * The relative position of the term within the phrase is specified explicitly.
   * This allows e.g. phrases with more than one term at the same position
   * or phrases with gaps (e.g. in connection with stopwords).
   * 
   * @param term
   * @param position
   */
  public void add(Term term, int position) {
    if (terms.size() == 0) {
      field = term.field();
    } else if (!term.field().equals(field)) {
      throw new IllegalArgumentException("All phrase terms must be in the same field: " + term);
    }

    terms.add(term);
    positions.add(Integer.valueOf(position));
    if (position > maxPosition) maxPosition = position;
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
    if (terms.size() == 1) {
      TermQuery tq = new TermQuery(terms.get(0));
      tq.setBoost(getBoost());
      return tq;
    } else
      return super.rewrite(reader);
  }

  static class PostingsAndFreq implements Comparable<PostingsAndFreq> {
    final DocsAndPositionsEnum postings;
    final int docFreq;
    final int position;
    final Term term;

    public PostingsAndFreq(DocsAndPositionsEnum postings, int docFreq, int position, Term term) {
      this.postings = postings;
      this.docFreq = docFreq;
      this.position = position;
      this.term = term;
    }

    public int compareTo(PostingsAndFreq other) {
      if (docFreq == other.docFreq) {
        if (position == other.position) {
          return term.compareTo(other.term);
        }
        return position - other.position;
      }
      return docFreq - other.docFreq;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + docFreq;
      result = prime * result + position;
      result = prime * result + ((term == null) ? 0 : term.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      PostingsAndFreq other = (PostingsAndFreq) obj;
      if (docFreq != other.docFreq) return false;
      if (position != other.position) return false;
      if (term == null) {
        if (other.term != null) return false;
      } else if (!term.equals(other.term)) return false;
      return true;
    }
  }

  private class PhraseWeight extends Weight {
    private final Similarity similarity;
    private final Similarity.Stats stats;
    private transient TermContext states[];

    public PhraseWeight(IndexSearcher searcher)
      throws IOException {
      this.similarity = searcher.getSimilarityProvider().get(field);
      final ReaderContext context = searcher.getTopReaderContext();
      states = new TermContext[terms.size()];
      for (int i = 0; i < terms.size(); i++)
        states[i] = TermContext.build(context, terms.get(i), true);
      stats = similarity.computeStats(searcher, field, getBoost(), states);
    }

    @Override
    public String toString() { return "weight(" + PhraseQuery.this + ")"; }

    @Override
    public Query getQuery() { return PhraseQuery.this; }

    @Override
    public float getValueForNormalization() {
      return stats.getValueForNormalization();
    }

    @Override
    public void normalize(float queryNorm, float topLevelBoost) {
      stats.normalize(queryNorm, topLevelBoost);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, ScorerContext scorerContext) throws IOException {
      if (terms.size() == 0)			  // optimize zero-term case
        return null;
      final IndexReader reader = context.reader;
      final Bits liveDocs = reader.getLiveDocs();
      PostingsAndFreq[] postingsFreqs = new PostingsAndFreq[terms.size()];
      for (int i = 0; i < terms.size(); i++) {
        final Term t = terms.get(i);
        final TermState state = states[i].get(context.ord);
        if (state == null) { /* term doesnt exist in this segment */
          assert termNotInReader(reader, field, t.bytes()) : "no termstate found but term exists in reader";
          return null;
        }
        DocsAndPositionsEnum postingsEnum = reader.termPositionsEnum(liveDocs,
                                                                     t.field(),
                                                                     t.bytes(),
                                                                     state);
        // PhraseQuery on a field that did not index
        // positions.
        if (postingsEnum == null) {
          assert (reader.termDocsEnum(liveDocs, t.field(), t.bytes(), state) != null) : "termstate found but no term exists in reader";
          // term does exist, but has no positions
          throw new IllegalStateException("field \"" + t.field() + "\" was indexed with Field.omitTermFreqAndPositions=true; cannot run PhraseQuery (term=" + t.text() + ")");
        }
        // get the docFreq without seeking
        TermsEnum te = reader.fields().terms(field).getThreadTermsEnum();
        te.seekExact(t.bytes(), state);
        postingsFreqs[i] = new PostingsAndFreq(postingsEnum, te.docFreq(), positions.get(i).intValue(), t);
      }

      // sort by increasing docFreq order
      if (slop == 0) {
        ArrayUtil.mergeSort(postingsFreqs);
      }

      if (slop == 0) {				  // optimize exact case
        ExactPhraseScorer s = new ExactPhraseScorer(this, postingsFreqs, similarity.exactDocScorer(stats, field, context));
        if (s.noDocs) {
          return null;
        } else {
          return s;
        }
      } else {
        return
          new SloppyPhraseScorer(this, postingsFreqs, similarity, slop, similarity.sloppyDocScorer(stats, field, context));
      }
    }
    
    private boolean termNotInReader(IndexReader reader, String field, BytesRef bytes) throws IOException {
      // only called from assert
      final Terms terms = reader.terms(field);
      return terms == null || terms.docFreq(bytes) == 0;
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context, ScorerContext.def());
      if (scorer != null) {
        int newDoc = scorer.advance(doc);
        if (newDoc == doc) {
          float freq = scorer.freq();
          SloppyDocScorer docScorer = similarity.sloppyDocScorer(stats, field, context);
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
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    if (terms.size() == 1) {			  // optimize one-term case
      Term term = terms.get(0);
      Query termQuery = new TermQuery(term);
      termQuery.setBoost(getBoost());
      return termQuery.createWeight(searcher);
    }
    return new PhraseWeight(searcher);
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
    return (this.getBoost() == other.getBoost())
      && (this.slop == other.slop)
      &&  this.terms.equals(other.terms)
      && this.positions.equals(other.positions);
  }

  /** Returns a hash code value for this object.*/
  @Override
  public int hashCode() {
    return Float.floatToIntBits(getBoost())
      ^ slop
      ^ terms.hashCode()
      ^ positions.hashCode();
  }

}
