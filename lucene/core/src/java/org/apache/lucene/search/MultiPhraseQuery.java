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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

/**
 * A generalized version of {@link PhraseQuery}, with the possibility of
 * adding more than one term at the same position that are treated as a disjunction (OR).
 * To use this class to search for the phrase "Microsoft app*" first create a Builder and use
 * {@link Builder#add(Term)} on the term "microsoft" (assuming lowercase analysis), then
 * find all terms that have "app" as prefix using {@link LeafReader#terms(String)},
 * seeking to "app" then iterating and collecting terms until there is no longer
 * that prefix, and finally use {@link Builder#add(Term[])} to add them.
 * {@link Builder#build()} returns the fully constructed (and immutable) MultiPhraseQuery.
 */
public class MultiPhraseQuery extends Query {
  /** A builder for multi-phrase queries */
  public static class Builder {
    private String field; // becomes non-null on first add() then is unmodified
    private final ArrayList<Term[]> termArrays;
    private final ArrayList<Integer> positions;
    private int slop;

    /** Default constructor. */
    public Builder() {
      this.field = null;
      this.termArrays = new ArrayList<>();
      this.positions = new ArrayList<>();
      this.slop = 0;
    }

    /** Copy constructor: this will create a builder that has the same
     *  configuration as the provided builder. */
    public Builder(MultiPhraseQuery multiPhraseQuery) {
      this.field = multiPhraseQuery.field;

      int length = multiPhraseQuery.termArrays.length;

      this.termArrays = new ArrayList<>(length);
      this.positions = new ArrayList<>(length);

      for (int i = 0 ; i < length ; ++i) {
        this.termArrays.add(multiPhraseQuery.termArrays[i]);
        this.positions.add(multiPhraseQuery.positions[i]);
      }

      this.slop = multiPhraseQuery.slop;
    }

    /** Sets the phrase slop for this query.
     * @see PhraseQuery#getSlop()
     */
    public Builder setSlop(int s) {
      if (s < 0) {
        throw new IllegalArgumentException("slop value cannot be negative");
      }
      slop = s;

      return this;
    }

    /** Add a single term at the next position in the phrase.
     */
    public Builder add(Term term) { return add(new Term[]{term}); }

    /** Add multiple terms at the next position in the phrase.  Any of the terms
     * may match (a disjunction).
     * The array is not copied or mutated, the caller should consider it
     * immutable subsequent to calling this method.
     */
    public Builder add(Term[] terms) {
      int position = 0;
      if (positions.size() > 0)
        position = positions.get(positions.size() - 1) + 1;

      return add(terms, position);
    }

    /**
     * Allows to specify the relative position of terms within the phrase.
     * The array is not copied or mutated, the caller should consider it
     * immutable subsequent to calling this method.
     */
    public Builder add(Term[] terms, int position) {
      Objects.requireNonNull(terms, "Term array must not be null");
      if (termArrays.size() == 0)
        field = terms[0].field();

      for (Term term : terms) {
        if (!term.field().equals(field)) {
          throw new IllegalArgumentException(
              "All phrase terms must be in the same field (" + field + "): " + term);
        }
      }

      termArrays.add(terms);
      positions.add(position);

      return this;
    }

    /** Builds a {@link MultiPhraseQuery}. */
    public MultiPhraseQuery build() {
      int[] positionsArray = new int[this.positions.size()];

      for (int i = 0; i < this.positions.size(); ++i) {
        positionsArray[i] = this.positions.get(i);
      }

      Term[][] termArraysArray = termArrays.toArray(new Term[termArrays.size()][]);

      return new MultiPhraseQuery(field, termArraysArray, positionsArray, slop);
    }
  }

  private final String field;
  private final Term[][] termArrays;
  private final int[] positions;
  private final int slop;

  private MultiPhraseQuery(String field, Term[][] termArrays, int[] positions, int slop) {
    // No argument checks here since they are provided by the MultiPhraseQuery.Builder
    this.field = field;
    this.termArrays = termArrays;
    this.positions = positions;
    this.slop = slop;
  }

  /** Sets the phrase slop for this query.
   * @see PhraseQuery#getSlop()
   */
  public int getSlop() { return slop; }

  /**
   * Returns the arrays of arrays of terms in the multi-phrase.
   * Do not modify!
   */
  public Term[][] getTermArrays() {
    return termArrays;
  }

  /**
   * Returns the relative positions of terms in this phrase.
   * Do not modify!
   */
  public int[] getPositions() {
    return positions;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (termArrays.length == 0) {
      return new MatchNoDocsQuery("empty MultiPhraseQuery");
    } else if (termArrays.length == 1) {                 // optimize one-term case
      Term[] terms = termArrays[0];
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (Term term : terms) {
        builder.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
      }
      return builder.build();
    } else {
      return super.rewrite(reader);
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field) == false) {
      return;
    }
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.MUST, this);
    for (Term[] terms : termArrays) {
      QueryVisitor sv = v.getSubVisitor(BooleanClause.Occur.SHOULD, this);
      sv.consumeTerms(this, terms);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    final Map<Term,TermStates> termStates = new HashMap<>();
    return new PhraseWeight(this, field, searcher, scoreMode) {

      @Override
      protected Similarity.SimScorer getStats(IndexSearcher searcher) throws IOException {
        final IndexReaderContext context = searcher.getTopReaderContext();

        // compute idf
        ArrayList<TermStatistics> allTermStats = new ArrayList<>();
        for(final Term[] terms: termArrays) {
          for (Term term: terms) {
            TermStates ts = termStates.get(term);
            if (ts == null) {
              ts = TermStates.build(context, term, scoreMode.needsScores());
              termStates.put(term, ts);
            }
            if (scoreMode.needsScores() && ts.docFreq() > 0) {
              allTermStats.add(searcher.termStatistics(term, ts.docFreq(), ts.totalTermFreq()));
            }
          }
        }
        if (allTermStats.isEmpty()) {
          return null; // none of the terms were found, we won't use sim at all
        } else {
          return similarity.scorer(
              boost,
              searcher.collectionStatistics(field),
              allTermStats.toArray(new TermStatistics[allTermStats.size()]));
        }
      }

      @Override
      protected PhraseMatcher getPhraseMatcher(LeafReaderContext context, SimScorer scorer, boolean exposeOffsets) throws IOException {
        assert termArrays.length != 0;
        final LeafReader reader = context.reader();

        PhraseQuery.PostingsAndFreq[] postingsFreqs = new PhraseQuery.PostingsAndFreq[termArrays.length];

        final Terms fieldTerms = reader.terms(field);
        if (fieldTerms == null) {
          return null;
        }

        // TODO: move this check to createWeight to happen earlier to the user?
        if (fieldTerms.hasPositions() == false) {
          throw new IllegalStateException("field \"" + field + "\" was indexed without position data;" +
              " cannot run MultiPhraseQuery (phrase=" + getQuery() + ")");
        }

        // Reuse single TermsEnum below:
        final TermsEnum termsEnum = fieldTerms.iterator();
        float totalMatchCost = 0;

        for (int pos=0; pos<postingsFreqs.length; pos++) {
          Term[] terms = termArrays[pos];
          List<PostingsEnum> postings = new ArrayList<>();

          for (Term term : terms) {
            TermState termState = termStates.get(term).get(context);
            if (termState != null) {
              termsEnum.seekExact(term.bytes(), termState);
              postings.add(termsEnum.postings(null, exposeOffsets ? PostingsEnum.ALL : PostingsEnum.POSITIONS));
              totalMatchCost += PhraseQuery.termPositionsCost(termsEnum);
            }
          }

          if (postings.isEmpty()) {
            return null;
          }

          final PostingsEnum postingsEnum;
          if (postings.size() == 1) {
            postingsEnum = postings.get(0);
          } else {
            postingsEnum = exposeOffsets ? new UnionFullPostingsEnum(postings) : new UnionPostingsEnum(postings);
          }

          postingsFreqs[pos] = new PhraseQuery.PostingsAndFreq(postingsEnum, new SlowImpactsEnum(postingsEnum), positions[pos], terms);
        }

        // sort by increasing docFreq order
        if (slop == 0) {
          ArrayUtil.timSort(postingsFreqs);
          return new ExactPhraseMatcher(postingsFreqs, scoreMode, scorer, totalMatchCost);
        }
        else {
          return new SloppyPhraseMatcher(postingsFreqs, slop, scoreMode, scorer, totalMatchCost, exposeOffsets);
        }

      }

      @Override
      public void extractTerms(Set<Term> terms) {
        for (final Term[] arr : termArrays) {
          Collections.addAll(terms, arr);
        }
      }
    };
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
    int lastPos = -1;

    for (int i = 0 ; i < termArrays.length ; ++i) {
      Term[] terms = termArrays[i];
      int position = positions[i];
      if (i != 0) {
        buffer.append(" ");
        for (int j=1; j<(position-lastPos); j++) {
          buffer.append("? ");
        }
      }
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
      lastPos = position;
    }
    buffer.append("\"");

    if (slop != 0) {
      buffer.append("~");
      buffer.append(slop);
    }

    return buffer.toString();
  }


  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(MultiPhraseQuery other) {
    return this.slop == other.slop && 
           termArraysEquals(this.termArrays, other.termArrays) && /* terms equal implies field equal */ 
           Arrays.equals(this.positions, other.positions);

  }

  /** Returns a hash code value for this object.*/
  @Override
  public int hashCode() {
    return classHash()
      ^ slop
      ^ termArraysHashCode() // terms equal implies field equal
      ^ Arrays.hashCode(positions);
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
  private boolean termArraysEquals(Term[][] termArrays1, Term[][] termArrays2) {
    if (termArrays1.length != termArrays2.length) {
      return false;
    }

    for (int i = 0 ; i < termArrays1.length ; ++i) {
      Term[] termArray1 = termArrays1[i];
      Term[] termArray2 = termArrays2[i];
      if (!(termArray1 == null ? termArray2 == null : Arrays.equals(termArray1,
          termArray2))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Takes the logical union of multiple PostingsEnum iterators.
   * <p>
   * Note: positions are merged during freq()
   */
  static class UnionPostingsEnum extends PostingsEnum {
    /** queue ordered by docid */
    final DocsQueue docsQueue;
    /** cost of this enum: sum of its subs */
    final long cost;

    /** queue ordered by position for current doc */
    final PositionsQueue posQueue = new PositionsQueue();
    /** current doc posQueue is working */
    int posQueueDoc = -2;
    /** list of subs (unordered) */
    final PostingsEnum[] subs;

    UnionPostingsEnum(Collection<PostingsEnum> subs) {
      docsQueue = new DocsQueue(subs.size());
      long cost = 0;
      for (PostingsEnum sub : subs) {
        docsQueue.add(sub);
        cost += sub.cost();
      }
      this.cost = cost;
      this.subs = subs.toArray(new PostingsEnum[subs.size()]);
    }

    @Override
    public int freq() throws IOException {
      int doc = docID();
      if (doc != posQueueDoc) {
        posQueue.clear();
        for (PostingsEnum sub : subs) {
          if (sub.docID() == doc) {
            int freq = sub.freq();
            for (int i = 0; i < freq; i++) {
              posQueue.add(sub.nextPosition());
            }
          }
        }
        posQueue.sort();
        posQueueDoc = doc;
      }
      return posQueue.size();
    }

    @Override
    public int nextPosition() throws IOException {
      return posQueue.next();
    }

    @Override
    public int docID() {
      return docsQueue.top().docID();
    }

    @Override
    public int nextDoc() throws IOException {
      PostingsEnum top = docsQueue.top();
      int doc = top.docID();

      do {
        top.nextDoc();
        top = docsQueue.updateTop();
      } while (top.docID() == doc);

      return top.docID();
    }

    @Override
    public int advance(int target) throws IOException {
      PostingsEnum top = docsQueue.top();

      do {
        top.advance(target);
        top = docsQueue.updateTop();
      } while (top.docID() < target);

      return top.docID();
    }

    @Override
    public long cost() {
      return cost;
    }

    @Override
    public int startOffset() throws IOException {
      return -1; // offsets are unsupported
    }

    @Override
    public int endOffset() throws IOException {
      return -1; // offsets are unsupported
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null; // payloads are unsupported
    }

    /**
     * disjunction of postings ordered by docid.
     */
    static class DocsQueue extends PriorityQueue<PostingsEnum> {
      DocsQueue(int size) {
        super(size);
      }

      @Override
      public final boolean lessThan(PostingsEnum a, PostingsEnum b) {
        return a.docID() < b.docID();
      }
    }

    /**
     * queue of terms for a single document. its a sorted array of
     * all the positions from all the postings
     */
    static class PositionsQueue {
      private int arraySize = 16;
      private int index = 0;
      private int size = 0;
      private int[] array = new int[arraySize];

      void add(int i) {
        if (size == arraySize)
          growArray();

        array[size++] = i;
      }

      int next() {
        return array[index++];
      }

      void sort() {
        Arrays.sort(array, index, size);
      }

      void clear() {
        index = 0;
        size = 0;
      }

      int size() {
        return size;
      }

      private void growArray() {
        int[] newArray = new int[arraySize * 2];
        System.arraycopy(array, 0, newArray, 0, arraySize);
        array = newArray;
        arraySize *= 2;
      }
    }
  }

  static class PostingsAndPosition {
    final PostingsEnum pe;
    int pos;
    int upto;

    PostingsAndPosition(PostingsEnum pe) {
      this.pe = pe;
    }
  }

  // Slower version of UnionPostingsEnum that delegates offsets and positions, for
  // use by MatchesIterator
  static class UnionFullPostingsEnum extends UnionPostingsEnum {

    int freq = -1;
    boolean started = false;

    final PriorityQueue<PostingsAndPosition> posQueue;
    final Collection<PostingsAndPosition> subs;

    UnionFullPostingsEnum(List<PostingsEnum> subs) {
      super(subs);
      this.posQueue = new PriorityQueue<PostingsAndPosition>(subs.size()) {
        @Override
        protected boolean lessThan(PostingsAndPosition a, PostingsAndPosition b) {
          return a.pos < b.pos;
        }
      };
      this.subs = new ArrayList<>();
      for (PostingsEnum pe : subs) {
        this.subs.add(new PostingsAndPosition(pe));
      }
    }

    @Override
    public int freq() throws IOException {
      int doc = docID();
      if (doc == posQueueDoc) {
        return freq;
      }
      freq = 0;
      started = false;
      posQueue.clear();
      for (PostingsAndPosition pp : subs) {
        if (pp.pe.docID() == doc) {
          pp.pos = pp.pe.nextPosition();
          pp.upto = pp.pe.freq();
          posQueue.add(pp);
          freq += pp.upto;
        }
      }
      return freq;
    }

    @Override
    public int nextPosition() throws IOException {
      if (started == false) {
        started = true;
        return posQueue.top().pos;
      }
      if (posQueue.top().upto == 1) {
        posQueue.pop();
        return posQueue.top().pos;
      }
      posQueue.top().pos = posQueue.top().pe.nextPosition();
      posQueue.top().upto--;
      posQueue.updateTop();
      return posQueue.top().pos;
    }

    @Override
    public int startOffset() throws IOException {
      return posQueue.top().pe.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return posQueue.top().pe.endOffset();
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return posQueue.top().pe.getPayload();
    }

  }
}
