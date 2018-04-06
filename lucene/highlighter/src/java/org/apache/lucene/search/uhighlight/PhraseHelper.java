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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.highlight.WeightedSpanTerm;
import org.apache.lucene.search.highlight.WeightedSpanTermExtractor;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanScorer;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

/**
 * Helps the {@link FieldOffsetStrategy} with position sensitive queries (e.g. highlight phrases correctly).
 * This is a stateful class holding information about the query, but it can (and is) re-used across highlighting
 * documents.  Despite this state, it's immutable after construction.
 *
 * @lucene.internal
 */
// TODO rename to SpanHighlighting ?
public class PhraseHelper {

  public static final PhraseHelper NONE = new PhraseHelper(new MatchAllDocsQuery(), "_ignored_",
      (s) -> false, spanQuery -> null, query -> null, true);

  private final String fieldName;
  private final Set<BytesRef> positionInsensitiveTerms; // (TermQuery terms)
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
    positionInsensitiveTerms = new HashSet<>();
    spanQueries = new HashSet<>();

    // TODO Have toSpanQuery(query) Function as an extension point for those with custom Query impls

    boolean[] mustRewriteHolder = {false}; // boolean wrapped in 1-ary array so it's mutable from inner class

    // When we call Weight.extractTerms, we do it on clauses that are NOT position sensitive.
    // We only want the to track a Set of bytes for the Term, not Term class with field part.
    Set<Term> extractPosInsensitiveTermsTarget = new TreeSet<Term>() {
      @Override
      public boolean add(Term term) {
        // don't call super.add; we don't actually use the superclass
        if (fieldMatcher.test(term.field())) {
          return positionInsensitiveTerms.add(term.bytes());
        } else {
          return false;
        }
      }
    };

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

      // called on Query types that are NOT position sensitive, e.g. TermQuery
      @Override
      protected void extractWeightedTerms(Map<String, WeightedSpanTerm> terms, Query query, float boost)
          throws IOException {
        query.createWeight(UnifiedHighlighter.EMPTY_INDEXSEARCHER, false, boost)
            .extractTerms(extractPosInsensitiveTermsTarget);
      }

      // called on SpanQueries. Some other position-sensitive queries like PhraseQuery are converted beforehand
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

  public Set<SpanQuery> getSpanQueries() {
    return spanQueries;
  }

  /**
   * If there is no position sensitivity then use of the instance of this class can be ignored.
   */
  public boolean hasPositionSensitivity() {
    return spanQueries.isEmpty() == false;
  }

  /**
   * Rewrite is needed for handling a {@link SpanMultiTermQueryWrapper} (MTQ / wildcards) or some
   * custom things.  When true, the resulting term list will probably be different than what it was known
   * to be initially.
   */
  public boolean willRewrite() {
    return willRewrite;
  }

  /** Returns the terms that are position-insensitive (sorted). */
  public BytesRef[] getAllPositionInsensitiveTerms() {
    BytesRef[] result = positionInsensitiveTerms.toArray(new BytesRef[positionInsensitiveTerms.size()]);
    Arrays.sort(result);
    return result;
  }

  /** Given the internal SpanQueries, produce a number of OffsetsEnum into the {@code results} param. */
  public void createOffsetsEnumsForSpans(LeafReader leafReader, int docId, List<OffsetsEnum> results) throws IOException {
    leafReader = new SingleFieldWithOffsetsFilterLeafReader(leafReader, fieldName);
    //TODO avoid searcher and do what it does to rewrite & get weight?
    IndexSearcher searcher = new IndexSearcher(leafReader);
    searcher.setQueryCache(null);

    // for each SpanQuery, grab it's Spans and put it into a PriorityQueue
    PriorityQueue<Spans> spansPriorityQueue = new PriorityQueue<Spans>(spanQueries.size()) {
      @Override
      protected boolean lessThan(Spans a, Spans b) {
        return a.startPosition() <= b.startPosition();
      }
    };
    for (Query query : spanQueries) {
      Weight weight = searcher.createWeight(searcher.rewrite(query), false, 1);
      Scorer scorer = weight.scorer(leafReader.getContext());
      if (scorer == null) {
        continue;
      }
      TwoPhaseIterator twoPhaseIterator = scorer.twoPhaseIterator();
      if (twoPhaseIterator != null) {
        if (twoPhaseIterator.approximation().advance(docId) != docId || !twoPhaseIterator.matches()) {
          continue;
        }
      } else if (scorer.iterator().advance(docId) != docId) { // preposition, and return doing nothing if find none
        continue;
      }

      Spans spans = ((SpanScorer) scorer).getSpans();
      assert spans.docID() == docId;
      if (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
        spansPriorityQueue.add(spans);
      }
    }

    // Iterate the Spans in the PriorityQueue, collecting as we go.  By using a PriorityQueue ordered by position,
    //   the underlying offsets in our collector will be mostly appended to the end of arrays (efficient).
    // note: alternatively it'd interesting if we produced one OffsetsEnum that internally advanced
    //   this PriorityQueue when nextPosition is called; it would cap what we have to cache for large docs and
    //   exiting early (due to maxLen) is easy.
    //   But at least we have an accurate "freq" and it shouldn't be too much data to collect.  Even SpanScorer
    //   navigates the spans fully to compute a good freq (and thus score)!
    OffsetSpanCollector spanCollector = new OffsetSpanCollector();
    while (spansPriorityQueue.size() > 0) {
      Spans spans = spansPriorityQueue.top();
      //TODO limit to a capped endOffset length somehow so we can break this loop early
      spans.collect(spanCollector);

      if (spans.nextStartPosition() == Spans.NO_MORE_POSITIONS) {
        spansPriorityQueue.pop();
      } else {
        spansPriorityQueue.updateTop();
      }
    }
    results.addAll(spanCollector.termToOffsetsEnums.values());
  }

  /**
   * Needed to support the ability to highlight a query irrespective of the field a query refers to
   * (aka requireFieldMatch=false).
   * This reader will just delegate every call to a single field in the wrapped
   * LeafReader. This way we ensure that all queries going through this reader target the same field.
   */
  private static final class SingleFieldWithOffsetsFilterLeafReader extends FilterLeafReader {
    final String fieldName;

    SingleFieldWithOffsetsFilterLeafReader(LeafReader in, String fieldName) {
      super(in);
      this.fieldName = fieldName;
    }

    @Override
    public FieldInfos getFieldInfos() {
      throw new UnsupportedOperationException();//TODO merge them
    }

    @Override
    public Terms terms(String field) throws IOException {
      // ensure the underlying PostingsEnum returns offsets.  It's sad we have to do this to use the SpanCollector.
      return new FilterTerms(super.terms(fieldName)) {
        @Override
        public TermsEnum iterator() throws IOException {
          return new FilterTermsEnum(in.iterator()) {
            @Override
            public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
              return super.postings(reuse, flags | PostingsEnum.OFFSETS);
            }
          };
        }
      };
    }

    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
      return super.getNormValues(fieldName);
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

  private class OffsetSpanCollector implements SpanCollector {
    Map<BytesRef, SpanCollectedOffsetsEnum> termToOffsetsEnums = new HashMap<>();

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      if (!fieldMatcher.test(term.field())) {
        return;
      }

      SpanCollectedOffsetsEnum offsetsEnum = termToOffsetsEnums.get(term.bytes());
      if (offsetsEnum == null) {
        // If it's pos insensitive we handle it outside of PhraseHelper.  term.field() is from the Query.
        if (positionInsensitiveTerms.contains(term.bytes())) {
          return;
        }
        offsetsEnum = new SpanCollectedOffsetsEnum(term.bytes(), postings.freq());
        termToOffsetsEnums.put(term.bytes(), offsetsEnum);
      }
      offsetsEnum.add(postings.startOffset(), postings.endOffset());
    }

    @Override
    public void reset() { // called when at a new position.  We don't care.
    }
  }

  private static class SpanCollectedOffsetsEnum extends OffsetsEnum {
    // TODO perhaps optionally collect (and expose) payloads?
    private final BytesRef term;
    private final int[] startOffsets;
    private final int[] endOffsets;
    private int numPairs = 0;
    private int enumIdx = -1;

    private SpanCollectedOffsetsEnum(BytesRef term, int postingsFreq) {
      this.term = term;
      this.startOffsets = new int[postingsFreq]; // hopefully not wasteful?  At least we needn't resize it.
      this.endOffsets = new int[postingsFreq];
    }

    // called from collector before it's navigated
    void add(int startOffset, int endOffset) {
      assert enumIdx == -1 : "bad state";

      // loop backwards since we expect a match at the end or close to it.  We expect O(1) not O(N).
      int pairIdx = numPairs - 1;
      for (; pairIdx >= 0; pairIdx--) {
        int iStartOffset = startOffsets[pairIdx];
        int iEndOffset = endOffsets[pairIdx];
        int cmp = Integer.compare(iStartOffset, startOffset);
        if (cmp == 0) {
          cmp = Integer.compare(iEndOffset, endOffset);
        }
        if (cmp == 0) {
          return; // we already have this offset-pair for this term
        } else if (cmp < 0) {
          break; //we will insert offsetPair to the right of pairIdx
        }
      }
      // pairIdx is now one position to the left of where we insert the new pair
      // shift right any pairs by one to make room
      final int shiftLen = numPairs - (pairIdx + 1);
      if (shiftLen > 0) {
        System.arraycopy(startOffsets, pairIdx + 2, startOffsets, pairIdx + 3, shiftLen);
        System.arraycopy(endOffsets, pairIdx + 2, endOffsets, pairIdx + 3, shiftLen);
      }
      // now we can place the offset pair
      startOffsets[pairIdx + 1] = startOffset;
      endOffsets[pairIdx + 1] = endOffset;
      numPairs++;
    }

    @Override
    public boolean nextPosition() throws IOException {
      return ++enumIdx < numPairs;
    }

    @Override
    public int freq() throws IOException {
      return numPairs;
    }

    @Override
    public BytesRef getTerm() throws IOException {
      return term;
    }

    @Override
    public int startOffset() throws IOException {
      return startOffsets[enumIdx];
    }

    @Override
    public int endOffset() throws IOException {
      return endOffsets[enumIdx];
    }
  }

}
