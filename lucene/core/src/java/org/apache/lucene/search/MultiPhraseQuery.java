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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.PhraseQuery.TermDocsEnumFactory;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.ToStringUtils;

/**
 * MultiPhraseQuery is a generalized version of PhraseQuery, with an added
 * method {@link #add(Term[])}.
 * To use this class, to search for the phrase "Microsoft app*" first use
 * add(Term) on the term "Microsoft", then find all terms that have "app" as
 * prefix using IndexReader.terms(Term), and use MultiPhraseQuery.add(Term[]
 * terms) to add them to the query.
 *
 */
public class MultiPhraseQuery extends Query {
  private String field;
  private ArrayList<Term[]> termArrays = new ArrayList<>();
  private ArrayList<Integer> positions = new ArrayList<>();

  private int slop = 0;

  /** Sets the phrase slop for this query.
   * @see PhraseQuery#setSlop(int)
   */
  public void setSlop(int s) {
    if (s < 0) {
      throw new IllegalArgumentException("slop value cannot be negative");
    }
    slop = s; 
  }

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
   */
  public void add(Term[] terms, int position) {
    if (termArrays.size() == 0)
      field = terms[0].field();

    for (int i = 0; i < terms.length; i++) {
      if (!terms[i].field().equals(field)) {
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
    private final Similarity similarity;
    private final Similarity.SimWeight stats;
    private final Map<Term,TermContext> termContexts = new HashMap<>();

    public MultiPhraseWeight(IndexSearcher searcher)
      throws IOException {
      this.similarity = searcher.getSimilarity();
      final IndexReaderContext context = searcher.getTopReaderContext();
      
      // compute idf
      ArrayList<TermStatistics> allTermStats = new ArrayList<>();
      for(final Term[] terms: termArrays) {
        for (Term term: terms) {
          TermContext termContext = termContexts.get(term);
          if (termContext == null) {
            termContext = TermContext.build(context, term);
            termContexts.put(term, termContext);
          }
          allTermStats.add(searcher.termStatistics(term, termContext));
        }
      }
      stats = similarity.computeWeight(getBoost(),
          searcher.collectionStatistics(field), 
          allTermStats.toArray(new TermStatistics[allTermStats.size()]));
    }

    @Override
    public Query getQuery() { return MultiPhraseQuery.this; }

    @Override
    public float getValueForNormalization() {
      return stats.getValueForNormalization();
    }

    @Override
    public void normalize(float queryNorm, float topLevelBoost) {
      stats.normalize(queryNorm, topLevelBoost);
    }

    @Override
    public Scorer scorer(LeafReaderContext context, int flags, Bits acceptDocs) throws IOException {
      assert !termArrays.isEmpty();
      final LeafReader reader = context.reader();
      final Bits liveDocs = acceptDocs;

      if ((flags & DocsEnum.FLAG_POSITIONS) < DocsEnum.FLAG_POSITIONS)
        flags = DocsEnum.FLAG_POSITIONS;
      
      PhraseQuery.PostingsAndFreq[] postingsFreqs = new PhraseQuery.PostingsAndFreq[termArrays.size()];

      final Terms fieldTerms = reader.terms(field);
      if (fieldTerms == null) {
        return null;
      }

      // Reuse single TermsEnum below:
      final TermsEnum termsEnum = fieldTerms.iterator(null);

      for (int pos=0; pos<postingsFreqs.length; pos++) {
        Term[] terms = termArrays.get(pos);

        final DocsEnum postingsEnum;
        int docFreq;
        TermDocsEnumFactory factory;
        if (terms.length > 1) {
          postingsEnum = new UnionDocsAndPositionsEnum(liveDocs, context, terms, termContexts, termsEnum);

          // coarse -- this overcounts since a given doc can
          // have more than one term:
          docFreq = 0;
          for(int termIdx=0;termIdx<terms.length;termIdx++) {
            final Term term = terms[termIdx];
            TermState termState = termContexts.get(term).get(context.ord);
            if (termState == null) {
              // Term not in reader
              continue;
            }
            termsEnum.seekExact(term.bytes(), termState);
            docFreq += termsEnum.docFreq();
          }

          if (docFreq == 0) {
            // None of the terms are in this reader
            return null;
          }
          factory = new MultiTermDocsEnumFactory(liveDocs, context, terms, termContexts, termsEnum, flags);
        } else {
          final Term term = terms[0];
          TermState termState = termContexts.get(term).get(context.ord);
          if (termState == null) {
            // Term not in reader
            return null;
          }
          termsEnum.seekExact(term.bytes(), termState);
          postingsEnum = termsEnum.docsAndPositions(liveDocs, null, DocsEnum.FLAG_NONE);

          if (postingsEnum == null) {
            // term does exist, but has no positions
            assert termsEnum.docs(liveDocs, null, DocsEnum.FLAG_NONE) != null: "termstate found but no term exists in reader";
            throw new IllegalStateException("field \"" + term.field() + "\" was indexed without position data; cannot run PhraseQuery (term=" + term.text() + ")");
          }

          factory = new TermDocsEnumFactory(term.bytes(), termState, termsEnum, flags, acceptDocs);
        }
        
        postingsFreqs[pos] = new PhraseQuery.PostingsAndFreq(postingsEnum, factory, termsEnum.docFreq() , positions.get(pos).intValue(), terms);
      }

      // sort by increasing docFreq order
      if (slop == 0) {
        ArrayUtil.timSort(postingsFreqs);
      }

      if (slop == 0) {
        return new ExactPhraseScorer(this, postingsFreqs, similarity.simScorer(stats, context));
      } else {
        return new SloppyPhraseScorer(this, postingsFreqs, slop, similarity.simScorer(stats, context));
      }
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context, DocsEnum.FLAG_POSITIONS, context.reader().getLiveDocs());
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
  public Query rewrite(IndexReader reader) {
    if (termArrays.isEmpty()) {
      BooleanQuery bq = new BooleanQuery();
      bq.setBoost(getBoost());
      return bq;
    } else if (termArrays.size() == 1) {                 // optimize one-term case
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
  public Weight createWeight(IndexSearcher searcher) throws IOException {
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
    int k = 0;
    Iterator<Term[]> i = termArrays.iterator();
    int lastPos = -1;
    boolean first = true;
    while (i.hasNext()) {
      Term[] terms = i.next();
      int position = positions.get(k);
      if (first) {
        first = false;
      } else {
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
      ++k;
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

  private static class MultiTermDocsEnumFactory extends TermDocsEnumFactory {

    LeafReaderContext context;
    Term[] terms;
    Map<Term, TermContext> termContexts;

    MultiTermDocsEnumFactory(Bits liveDocs, LeafReaderContext context, Term[] terms,
                             Map<Term,TermContext> termContexts, TermsEnum termsEnum, int flags) throws IOException {
      super(termsEnum, flags, liveDocs);
      this.context = context;
      this.terms = terms;
      this.termContexts = termContexts;
    }

    @Override
    public DocsEnum docsAndPositionsEnum() throws IOException {
      return new UnionDocsAndPositionsEnum(liveDocs, context, terms, termContexts, termsEnum, flags);
    }

  }
}

/**
 * Takes the logical union of multiple DocsEnum iterators.
 */

// TODO: if ever we allow subclassing of the *PhraseScorer
class UnionDocsAndPositionsEnum extends DocsEnum {

  private static final class DocsQueue extends PriorityQueue<DocsEnum> {
    DocsQueue(List<DocsEnum> docsEnums) throws IOException {
      super(docsEnums.size());

      Iterator<DocsEnum> i = docsEnums.iterator();
      while (i.hasNext()) {
        DocsEnum postings = i.next();
        if (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          add(postings);
        }
      }
    }

    @Override
    public final boolean lessThan(DocsEnum a, DocsEnum b) {
      return a.docID() < b.docID();
    }
  }

  // TODO: Reimplement this as int[_arraySize * 3], storing position at i * 3,
  // startOffset at i * 3 + 1 and endOffset at i * 3 + 2.  Will need to also
  // implement a new SorterTemplate to sort the array.

  private static final class PositionQueue {
    private int _arraySize = 48;
    private int _index = 0;
    private int _lastIndex = 0;
    private int[] _array = new int[_arraySize];
    
    final void add(int pos, int start, int end) {
      if (_lastIndex * 3 == _arraySize)
        growArray();

      _array[_lastIndex * 3] = pos;
      _array[_lastIndex * 3 + 1] = start;
      _array[_lastIndex * 3 + 2] = end;
      _lastIndex += 1;
    }

    final int next() {
      return _array[_index++ * 3];
    }

    final int startPosition() {
      return _array[(_index - 1) * 3];
    }

    final int endPosition() {
      return _array[(_index - 1) * 3];
    }

    final int startOffset() {
      return _array[(_index - 1) * 3 + 1];
    }

    final int endOffset() {
      return _array[(_index - 1) * 3 + 2];
    }

    final void sort() {
      //Arrays.sort(_array, _index, _lastIndex);
      sorter.sort(_index, _lastIndex);
    }

    final void clear() {
      _index = 0;
      _lastIndex = 0;
    }

    final int size() {
      return (_lastIndex - _index);
    }

    private void growArray() {
      int[] newArray = new int[_arraySize * 2];
      System.arraycopy(_array, 0, newArray, 0, _arraySize);
      _array = newArray;
      _arraySize *= 2;
    }

    private IntroSorter sorter = new IntroSorter() {
      private int pivot;

      @Override
      protected void swap(int i, int j) {
        int ti = _array[i * 3];
        int ts = _array[i * 3 + 1];
        int te = _array[i * 3 + 2];
        _array[i * 3] = _array[j * 3];
        _array[i * 3 + 1] = _array[j * 3 + 1];
        _array[i * 3 + 2] = _array[j * 3 + 2];
        _array[j * 3] = ti;
        _array[j * 3 + 1] = ts;
        _array[j * 3 + 2] = te;
      }

      @Override
      protected int compare(int i, int j) {
        return _array[i * 3] - _array[j * 3];
      }

      @Override
      protected void setPivot(int i) {
        pivot = i;
      }

      @Override
      protected int comparePivot(int j) {
        return pivot - _array[j * 3];
      }
    };
  }

  private int _doc = -1;
  private int _freq;
  private DocsQueue _queue;
  private PositionQueue _posList;
  private int posPending;
  private long cost;

  public UnionDocsAndPositionsEnum(Bits liveDocs, LeafReaderContext context, Term[] terms,
                                   Map<Term,TermContext> termContexts, TermsEnum termsEnum) throws IOException {
    this(liveDocs, context, terms, termContexts, termsEnum, DocsEnum.FLAG_POSITIONS);
  }

  public UnionDocsAndPositionsEnum(Bits liveDocs, LeafReaderContext context, Term[] terms, Map<Term,TermContext> termContexts, TermsEnum termsEnum, int flags) throws IOException {
    List<DocsEnum> docsEnums = new LinkedList<>();
    for (int i = 0; i < terms.length; i++) {
      final Term term = terms[i];
      TermState termState = termContexts.get(term).get(context.ord);
      if (termState == null) {
        // Term doesn't exist in reader
        continue;
      }
      termsEnum.seekExact(term.bytes(), termState);
      DocsEnum postings = termsEnum.docsAndPositions(liveDocs, null, DocsEnum.FLAG_NONE);
      if (postings == null) {
        // term does exist, but has no positions
        throw new IllegalStateException("field \"" + term.field() + "\" was indexed without position data; cannot run PhraseQuery (term=" + term.text() + ")");
      }
      cost += postings.cost();
      docsEnums.add(postings);
    }

    _queue = new DocsQueue(docsEnums);
    _posList = new PositionQueue();
  }

  @Override
  public final int nextDoc() throws IOException {
    if (_queue.size() == 0) {
      return NO_MORE_DOCS;
    }

    // TODO: move this init into positions(): if the search
    // doesn't need the positions for this doc then don't
    // waste CPU merging them:
    _posList.clear();
    _doc = _queue.top().docID();

    // merge sort all positions together
    DocsEnum postings;
    do {
      postings = _queue.top();

      final int freq = postings.freq();
      for (int i = 0; i < freq; i++) {
        _posList.add(postings.nextPosition(), postings.startOffset(), postings.endOffset());
      }

      if (postings.nextDoc() != NO_MORE_DOCS) {
        _queue.updateTop();
      } else {
        _queue.pop();
      }
    } while (_queue.size() > 0 && _queue.top().docID() == _doc);

    _posList.sort();
    _freq = _posList.size();
    posPending = _freq;

    return _doc;
  }

  @Override
  public int nextPosition() {
    if (posPending == 0)
      return NO_MORE_POSITIONS;
    posPending--;
    return _posList.next();
  }

  @Override
  public int startPosition() throws IOException {
    return _posList.startPosition();
  }

  @Override
  public int endPosition() throws IOException {
    return _posList.endPosition();
  }

  @Override
  public int startOffset() {
    return _posList.startOffset();
  }

  @Override
  public int endOffset() {
    return _posList.endOffset();
  }

  @Override
  public BytesRef getPayload() {
    return null;
  }

  @Override
  public final int advance(int target) throws IOException {
    while (_queue.top() != null && target > _queue.top().docID()) {
      DocsEnum postings = _queue.pop();
      if (postings.advance(target) != NO_MORE_DOCS) {
        _queue.add(postings);
      }
    }
    return nextDoc();
  }

  @Override
  public final int freq() throws IOException {
    return _freq;
  }

  @Override
  public final int docID() {
    return _doc;
  }

  @Override
  public long cost() {
    return cost;
  }
}
