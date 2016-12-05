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
package org.apache.lucene.search.uhighlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.highlight.WeightedSpanTerm;
import org.apache.lucene.search.highlight.WeightedSpanTermExtractor;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.BytesRef;

/**
 * Helps the {@link FieldOffsetStrategy} with strict position highlighting (e.g. highlight phrases correctly).
 * This is a stateful class holding information about the query, but it can (and is) re-used across highlighting
 * documents.  Despite this state; it's immutable after construction.  The approach taken in this class is very similar
 * to the standard Highlighter's {@link WeightedSpanTermExtractor} which is in fact re-used here.  However, we ought to
 * completely rewrite it to use the SpanCollector interface to collect offsets directly. We'll get better
 * phrase accuracy.
 *
 * @lucene.internal
 */
public class PhraseHelper {

  public static final PhraseHelper NONE = new PhraseHelper(new MatchAllDocsQuery(), "_ignored_",
      (s) -> false, spanQuery -> null, query -> null, true);

  //TODO it seems this ought to be a general thing on Spans?
  private static final Comparator<? super Spans> SPANS_COMPARATOR = (o1, o2) -> {
    int cmp = Integer.compare(o1.docID(), o2.docID());
    if (cmp != 0) {
      return cmp;
    }
    if (o1.docID() == DocIdSetIterator.NO_MORE_DOCS) {
      return 0; // don't ask for start/end position; not sure if we can even call those methods
    }
    cmp = Integer.compare(o1.startPosition(), o2.startPosition());
    if (cmp != 0) {
      return cmp;
    } else {
      return Integer.compare(o1.endPosition(), o2.endPosition());
    }
  };

  private final String fieldName;
  private final Set<Term> positionInsensitiveTerms; // (TermQuery terms)
  private final Set<SpanQuery> spanQueries;
  private final boolean willRewrite;
  private final Predicate<String> fieldMatcher;

  /**
   * Constructor.
   * {@code rewriteQueryPred} is an extension hook to override the default choice of
   * {@link WeightedSpanTermExtractor#mustRewriteQuery(SpanQuery)}. By default unknown query types are rewritten,
   * so use this to return {@link Boolean#FALSE} if you know the query doesn't need to be rewritten.
   * Similarly, {@code preExtractRewriteFunction} is also an extension hook for extract to allow different queries
   * to be set before the {@link WeightedSpanTermExtractor}'s extraction is invoked.
   * {@code ignoreQueriesNeedingRewrite} effectively ignores any query clause that needs to be "rewritten", which is
   * usually limited to just a {@link SpanMultiTermQueryWrapper} but could be other custom ones.
   * {@code fieldMatcher} The field name predicate to use for extracting the query part that must be highlighted.
   */
  public PhraseHelper(Query query, String field, Predicate<String> fieldMatcher, Function<SpanQuery, Boolean> rewriteQueryPred,
                      Function<Query, Collection<Query>> preExtractRewriteFunction,
                      boolean ignoreQueriesNeedingRewrite) {
    this.fieldName = field;
    this.fieldMatcher = fieldMatcher;
    // filter terms to those we want
    positionInsensitiveTerms = new FieldFilteringTermSet();
    spanQueries = new HashSet<>();

    // TODO Have toSpanQuery(query) Function as an extension point for those with custom Query impls

    boolean[] mustRewriteHolder = {false}; // boolean wrapped in 1-ary array so it's mutable from inner class

    // For TermQueries or other position insensitive queries, collect the Terms.
    // For other Query types, WSTE will convert to an equivalent SpanQuery.  NOT extracting position spans here.
    new WeightedSpanTermExtractor(field) {
      //anonymous constructor
      {
        setExpandMultiTermQuery(true); //necessary for mustRewriteQuery(spanQuery) to work.

        try {
          extract(query, 1f, null); // null because we won't actually extract right now; we're not collecting
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      protected void extract(Query query, float boost, Map<String, WeightedSpanTerm> terms) throws IOException {
        Collection<Query> newQueriesToExtract = preExtractRewriteFunction.apply(query);
        if (newQueriesToExtract != null) {
          for (Query newQuery : newQueriesToExtract) {
            extract(newQuery, boost, terms);
          }
        } else {
          super.extract(query, boost, terms);
        }
      }

      @Override
      protected boolean isQueryUnsupported(Class<? extends Query> clazz) {
        if (clazz.isAssignableFrom(MultiTermQuery.class)) {
          return true; //We do MTQ processing separately in MultiTermHighlighting.java
        }
        return true; //TODO set to false and provide a hook to customize certain queries.
      }

      @Override
      protected void extractWeightedTerms(Map<String, WeightedSpanTerm> terms, Query query, float boost)
          throws IOException {
        query.createWeight(UnifiedHighlighter.EMPTY_INDEXSEARCHER, false)
            .extractTerms(positionInsensitiveTerms);
      }

      @Override
      protected void extractWeightedSpanTerms(Map<String, WeightedSpanTerm> terms, SpanQuery spanQuery,
                                              float boost) throws IOException {
        // if this span query isn't for this field, skip it.
        Set<String> fieldNameSet = new HashSet<>();//TODO reuse.  note: almost always size 1
        collectSpanQueryFields(spanQuery, fieldNameSet);
        for (String spanField : fieldNameSet) {
          if (!fieldMatcher.test(spanField)) {
            return;
          }
        }

        // TODO allow users to override the answer to mustRewriteQuery
        boolean mustRewriteQuery = mustRewriteQuery(spanQuery);
        if (ignoreQueriesNeedingRewrite && mustRewriteQuery) {
          return;// ignore this query
        }
        mustRewriteHolder[0] |= mustRewriteQuery;

        spanQueries.add(spanQuery);
      }

      @Override
      protected boolean mustRewriteQuery(SpanQuery spanQuery) {
        Boolean rewriteQ = rewriteQueryPred.apply(spanQuery);// allow to override
        return rewriteQ != null ? rewriteQ : super.mustRewriteQuery(spanQuery);
      }
    }; // calling the constructor triggered the extraction/visiting we want.  Hacky; yes.

    willRewrite = mustRewriteHolder[0];
  }

  Set<SpanQuery> getSpanQueries() {
    return spanQueries;
  }

  /**
   * If there is no position sensitivity then use of the instance of this class can be ignored.
   */
  boolean hasPositionSensitivity() {
    return spanQueries.isEmpty() == false;
  }

  /**
   * Rewrite is needed for handling a {@link SpanMultiTermQueryWrapper} (MTQ / wildcards) or some
   * custom things.  When true, the resulting term list will probably be different than what it was known
   * to be initially.
   */
  boolean willRewrite() {
    return willRewrite;
  }

  /**
   * Collect a list of pre-positioned {@link Spans} for each term, given a reader that has just one document.
   * It returns no mapping for query terms that occurs in a position insensitive way which therefore don't
   * need to be filtered.
   */
  Map<BytesRef, Spans> getTermToSpans(LeafReader leafReader, int doc)
      throws IOException {
    if (spanQueries.isEmpty()) {
      return Collections.emptyMap();
    }
    final LeafReader filteredReader = new SingleFieldFilterLeafReader(leafReader, fieldName);
    // for each SpanQuery, collect the member spans into a map.
    Map<BytesRef, Spans> result = new HashMap<>();
    for (SpanQuery spanQuery : spanQueries) {
      getTermToSpans(spanQuery, filteredReader.getContext(), doc, result);
    }
    return result;
  }

  // code extracted & refactored from WSTE.extractWeightedSpanTerms()
  private void getTermToSpans(SpanQuery spanQuery, LeafReaderContext readerContext,
                              int doc, Map<BytesRef, Spans> result)
      throws IOException {
    // note: in WSTE there was some field specific looping that seemed pointless so that isn't here.
    final IndexSearcher searcher = new IndexSearcher(readerContext.reader());
    searcher.setQueryCache(null);
    if (willRewrite) {
      spanQuery = (SpanQuery) searcher.rewrite(spanQuery); // searcher.rewrite loops till done
    }

    // Get the underlying query terms
    TreeSet<Term> termSet = new FieldFilteringTermSet(); // sorted so we can loop over results in order shortly...
    searcher.createWeight(spanQuery, false).extractTerms(termSet);//needsScores==false

    // Get Spans by running the query against the reader
    // TODO it might make sense to re-use/cache the Spans instance, to advance forward between docs
    SpanWeight spanWeight = (SpanWeight) searcher.createNormalizedWeight(spanQuery, false);
    Spans spans = spanWeight.getSpans(readerContext, SpanWeight.Postings.POSITIONS);
    if (spans == null) {
      return;
    }
    TwoPhaseIterator twoPhaseIterator = spans.asTwoPhaseIterator();
    if (twoPhaseIterator != null) {
      if (twoPhaseIterator.approximation().advance(doc) != doc || !twoPhaseIterator.matches()) {
        return;
      }
    } else if (spans.advance(doc) != doc) { // preposition, and return doing nothing if find none
      return;
    }

    // Consume the Spans into a cache.  This instance is used as a source for multiple cloned copies.
    // It's important we do this and not re-use the same original Spans instance since these will be iterated
    // independently later on; sometimes in ways that prevents sharing the original Spans.
    CachedSpans cachedSpansSource = new CachedSpans(spans); // consumes spans for this doc only and caches
    spans = null;// we don't use it below

    // Map terms to a Spans instance (aggregate if necessary)
    for (final Term queryTerm : termSet) {
      // note: we expect that at least one query term will pass these filters. This is because the collected
      //   spanQuery list were already filtered by these conditions.
      if (positionInsensitiveTerms.contains(queryTerm)) {
        continue;
      }
      // copy-constructor refers to same data (shallow) but has iteration state from the beginning
      CachedSpans cachedSpans = new CachedSpans(cachedSpansSource);
      // Add the span to whatever span may or may not exist
      Spans existingSpans = result.get(queryTerm.bytes());
      if (existingSpans != null) {
        if (existingSpans instanceof MultiSpans) {
          ((MultiSpans) existingSpans).addSpans(cachedSpans);
        } else { // upgrade to MultiSpans
          MultiSpans multiSpans = new MultiSpans();
          multiSpans.addSpans(existingSpans);
          multiSpans.addSpans(cachedSpans);
          result.put(queryTerm.bytes(), multiSpans);
        }
      } else {
        result.put(queryTerm.bytes(), cachedSpans);
      }
    }
  }

  /**
   * Returns terms as a List, but expanded to any terms in phraseHelper' keySet if present.  That can only
   * happen if willRewrite() is true.
   */
  List<BytesRef> expandTermsIfRewrite(BytesRef[] terms, Map<BytesRef, Spans> strictPhrasesTermToSpans) {
    if (willRewrite()) {
      Set<BytesRef> allTermSet = new LinkedHashSet<>(terms.length + strictPhrasesTermToSpans.size());
      Collections.addAll(allTermSet, terms);//FYI already sorted; will keep order
      if (allTermSet.addAll(strictPhrasesTermToSpans.keySet())) { // true if any were added
        List<BytesRef> sourceTerms = Arrays.asList(allTermSet.toArray(new BytesRef[allTermSet.size()]));
        sourceTerms.sort(Comparator.naturalOrder());
        return sourceTerms;
      }
    }
    return Arrays.asList(terms); // no rewrite; use original terms
  }

  /**
   * Returns a filtered postings where the position must be in the given Spans.
   * The Spans must be in a positioned state (not initial) and should not be shared between other terms.
   * {@code postingsEnum} should be positioned at the
   * document (the same one as the spans) but it hasn't iterated the positions yet.
   * The Spans should be the result of a simple
   * lookup from {@link #getTermToSpans(LeafReader, int)}, and so it could be null which could mean
   * either it's completely filtered or that there should be no filtering; this class knows what to do.
   * <p>
   * Due to limitations in filtering, the {@link PostingsEnum#freq()} is un-changed even if some positions
   * get filtered.  So when {@link PostingsEnum#nextPosition()} is called or {@code startOffset} or {@code
   * endOffset} beyond the "real" positions, these methods returns {@link Integer#MAX_VALUE}.
   * <p>
   * <b>This will return null if it's completely filtered out (i.e. effectively has no postings).</b>
   */
  PostingsEnum filterPostings(BytesRef term, PostingsEnum postingsEnum, Spans spans)
      throws IOException {
    if (spans == null) {
      if (hasPositionSensitivity() == false || positionInsensitiveTerms.contains(new Term(fieldName, term))) {
        return postingsEnum; // no filtering
      } else {
        return null; // completely filtered out
      }
    }
    if (postingsEnum.docID() != spans.docID()) {
      throw new IllegalStateException("Spans & Postings doc ID misaligned or not positioned");
    }

    return new FilterLeafReader.FilterPostingsEnum(postingsEnum) {
      // freq() is max times nextPosition can be called. We'll set this var to -1 when exhausted.
      int remainingPositions = postingsEnum.freq();

      @Override
      public String toString() {
        String where;
        try {
          where = "[" + startOffset() + ":" + endOffset() + "]";
        } catch (IOException e) {
          where = "[" + e + "]";
        }
        return "'" + term.utf8ToString() + "'@" + where + " filtered by " + spans;
      }

      @Override
      public int nextDoc() throws IOException {
        throw new IllegalStateException("not expected"); // don't need to implement; just used on one doc
      }

      @Override
      public int advance(int target) throws IOException {
        throw new IllegalStateException("not expected"); // don't need to implement; just used on one doc
      }

      @Override
      public int nextPosition() throws IOException {
        // loop over posting positions...
        NEXT_POS_LOOP:
        while (remainingPositions > 0) {
          final int thisPos = super.nextPosition();
          remainingPositions--;

          // loop spans forward (if necessary) while the span end is behind thisPos
          while (spans.endPosition() <= thisPos) {
            if (spans.nextStartPosition() == Spans.NO_MORE_POSITIONS) { // advance
              break NEXT_POS_LOOP;
            }
            assert spans.docID() == postingsEnum.docID();
          }

          // is this position within the span?
          if (thisPos >= spans.startPosition()) {
            assert thisPos < spans.endPosition(); // guaranteed by previous loop
            return thisPos; // yay!
          }
          // else continue and try the next position
        }
        remainingPositions = -1; // signify done
        return Integer.MAX_VALUE;
      }

      @Override
      public int startOffset() throws IOException {
        return remainingPositions >= 0 ? super.startOffset() : Integer.MAX_VALUE;
      }

      @Override
      public int endOffset() throws IOException {
        return remainingPositions >= 0 ? super.endOffset() : Integer.MAX_VALUE;
      }
    };
  }

  /**
   * Simple TreeSet that filters out Terms not matching the provided predicate on {@code add()}.
   */
  private class FieldFilteringTermSet extends TreeSet<Term> {
    @Override
    public boolean add(Term term) {
      if (fieldMatcher.test(term.field())) {
        if (term.field().equals(fieldName)) {
          return super.add(term);
        } else {
          return super.add(new Term(fieldName, term.bytes()));
        }
      } else {
        return false;
      }
    }
  }

  /**
   * A single {@link Spans} view over multiple spans.  At least one span is mandatory, but you should probably
   * supply more than one.  Furthermore, the given spans are expected to be positioned to a document already
   * via a call to next or advance).
   */  // TODO move to Lucene core as a Spans utility class?
  static class MultiSpans extends Spans {
    final PriorityQueue<Spans> spansQueue = new PriorityQueue<>(SPANS_COMPARATOR);
    long cost;

    void addSpans(Spans spans) {
      if (spans.docID() < 0 || spans.docID() == NO_MORE_DOCS) {
        throw new IllegalArgumentException("Expecting given spans to be in a positioned state.");
      }
      spansQueue.add(spans);
      cost = Math.max(cost, spans.cost());
    }

    // DocIdSetIterator methods:

    @Override
    public int nextDoc() throws IOException {
      if (spansQueue.isEmpty()) {
        return NO_MORE_DOCS;
      }
      return advance(spansQueue.peek().docID() + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (spansQueue.isEmpty()) {
        return NO_MORE_DOCS;
      }
      while (true) {
        Spans spans = spansQueue.peek();
        if (spans.docID() >= target) {
          return spans.docID();
        }
        spansQueue.remove(); // must remove before modify state
        if (spans.advance(target) != NO_MORE_DOCS) { // ... otherwise it's not re-added
          spansQueue.add(spans);
        } else if (spansQueue.isEmpty()) {
          return NO_MORE_DOCS;
        }
      }
    }

    @Override
    public int docID() {
      if (spansQueue.isEmpty()) {
        return NO_MORE_DOCS;
      }
      return spansQueue.peek().docID();
    }

    @Override
    public long cost() {
      return cost;
    }

    // Spans methods:

    @Override
    public int nextStartPosition() throws IOException {
      // advance any spans at the initial position per document
      boolean atDocStart = false;
      while (spansQueue.peek().startPosition() == -1) {
        atDocStart = true;
        Spans headSpans = spansQueue.remove(); // remove because we will change state
        headSpans.nextStartPosition();
        spansQueue.add(headSpans);
      }
      if (!atDocStart) {
        Spans headSpans = spansQueue.remove(); // remove because we will change state
        headSpans.nextStartPosition();
        spansQueue.add(headSpans);
      }
      return startPosition();
    }

    @Override
    public int startPosition() {
      return spansQueue.peek().startPosition();
    }

    @Override
    public int endPosition() {
      return spansQueue.peek().endPosition();
    }

    @Override
    public int width() {
      return spansQueue.peek().width();
    }

    @Override
    public void collect(SpanCollector collector) throws IOException {
      spansQueue.peek().collect(collector);
    }

    @Override
    public float positionsCost() {
      return 100f;// no idea; and we can't delegate due to not allowing to call it dependent on TwoPhaseIterator
    }
  }

  /**
   * This reader will just delegate every call to a single field in the wrapped
   * LeafReader. This way we ensure that all queries going through this reader target the same field.
  */
  static final class SingleFieldFilterLeafReader extends FilterLeafReader {
    final String fieldName;
    SingleFieldFilterLeafReader(LeafReader in, String fieldName) {
      super(in);
      this.fieldName = fieldName;
    }

    @Override
    public FieldInfos getFieldInfos() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Fields fields() throws IOException {
      return new FilterFields(super.fields()) {
        @Override
        public Terms terms(String field) throws IOException {
          return super.terms(fieldName);
        }

        @Override
        public Iterator<String> iterator() {
          return Collections.singletonList(fieldName).iterator();
        }

        @Override
        public int size() {
          return 1;
        }
      };
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
      return super.getNumericDocValues(fieldName);
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
      return super.getBinaryDocValues(fieldName);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
      return super.getSortedDocValues(fieldName);
    }

    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
      return super.getNormValues(fieldName);
    }
  }


  /**
   * A Spans based on a list of cached spans for one doc.  It is pre-positioned to this doc.
   */
  private static class CachedSpans extends Spans {

    private static class CachedSpan {
      final int start;
      final int end;

      CachedSpan(int start, int end) {
        this.start = start;
        this.end = end;
      }
    }

    final int docId;
    final ArrayList<CachedSpan> cachedSpanList;
    int index = -1;

    CachedSpans(Spans spans) throws IOException {
      this.docId = spans.docID();
      assert this.docId != -1;
      // Consume the spans for this doc into a list.  There's always at least one; the first/current one.
      cachedSpanList = new ArrayList<>();
      while (spans.nextStartPosition() != NO_MORE_POSITIONS) {
        cachedSpanList.add(new CachedSpan(spans.startPosition(), spans.endPosition()));
      }
      assert !cachedSpanList.isEmpty(); // bad Span impl?
    }

    /**
     * Clone; reset iteration state.
     */
    CachedSpans(CachedSpans cloneMe) {
      docId = cloneMe.docId;
      cachedSpanList = cloneMe.cachedSpanList;
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException("Not expected");
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException("Not expected");
    }

    @Override
    public int docID() {
      return docId;
    }

    @Override
    public long cost() {
      return 1;
    }

    @Override
    public int nextStartPosition() throws IOException {
      index++;
      return startPosition();
    }

    @Override
    public int startPosition() {
      return index < 0 ?
          -1 : index >= cachedSpanList.size() ?
          NO_MORE_POSITIONS : cachedSpanList.get(index).start;
    }

    @Override
    public int endPosition() {
      return index < 0 ?
          -1 : index >= cachedSpanList.size() ?
          NO_MORE_POSITIONS : cachedSpanList.get(index).end;
    }

    @Override
    public int width() {
      return endPosition() - startPosition();
    }

    @Override
    public void collect(SpanCollector collector) throws IOException {
      throw new UnsupportedOperationException("Not expected");
    }

    @Override
    public float positionsCost() {
      return 1f;
    }

  } // class CachedSpans
}
