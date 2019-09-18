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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.mutable.MutableValueBool;

/**
 * A generalized version of {@link PhraseQuery}, built with one or more {@link MultiTermQuery}
 * that provides term expansions for multi-terms (one of the expanded terms must match).
 * <p>
 * Its main advantage is to control the total number of expansions across all {@link MultiTermQuery}
 * and across all segments.
 * <p>
 * Use the {@link Builder} to build a {@link PhraseWildcardQuery}.
 * <p>
 * This query is similar to {@link MultiPhraseQuery}, but it handles, controls and optimizes the
 * multi-term expansions.
 * <p>
 * This query is equivalent to building an ordered {@link org.apache.lucene.search.spans.SpanNearQuery}
 * with a list of {@link org.apache.lucene.search.spans.SpanTermQuery} and
 * {@link org.apache.lucene.search.spans.SpanMultiTermQueryWrapper}.
 * But it optimizes the multi-term expansions and the segment accesses.
 * It first resolves the single-terms to early stop if some does not match. Then
 * it expands each multi-term sequentially, stopping immediately if one does not
 * match. It detects the segments that do not match to skip them for the next
 * expansions. This often avoid expanding the other multi-terms on some or
 * even all segments. And finally it controls the total number of expansions.
 * <p>
 * Immutable.
 * @lucene.experimental
 */
public class PhraseWildcardQuery extends Query {

  protected static final Query NO_MATCH_QUERY = new MatchNoDocsQuery("Empty " + PhraseWildcardQuery.class.getSimpleName());

  protected final String field;
  protected final List<PhraseTerm> phraseTerms;
  protected final int slop;
  protected final int maxMultiTermExpansions;
  protected final boolean segmentOptimizationEnabled;

  protected PhraseWildcardQuery(
      String field,
      List<PhraseTerm> phraseTerms,
      int slop,
      int maxMultiTermExpansions,
      boolean segmentOptimizationEnabled) {
    this.field = field;
    this.phraseTerms = phraseTerms;
    this.slop = slop;
    this.maxMultiTermExpansions = maxMultiTermExpansions;
    this.segmentOptimizationEnabled = segmentOptimizationEnabled;
  }

  public String getField() {
    return field;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (phraseTerms.isEmpty()) {
      return NO_MATCH_QUERY;
    }
    if (phraseTerms.size() == 1) {
      return phraseTerms.get(0).getQuery();
    }
    return super.rewrite(reader);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (!visitor.acceptField(field)) {
      return;
    }
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.MUST, this);
    for (PhraseTerm phraseTerm : phraseTerms) {
      phraseTerm.getQuery().visit(v);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    IndexReader reader = searcher.getIndexReader();

    // Build a list of segments ordered by terms size (number of terms).
    // The first segments to be searched are the smaller ones, which are by
    // design containing the most recent documents. Any segment in this list
    // may also be removed in the PhraseTerm.collectTermData() calls below
    // if one of the phrase term does not match in the segment. This allows
    // to early stop expanding multi-terms on removed segments.
    // Additionally there is a global multi-term expansion limit across all multi-terms
    // and all segments. So this is important to first start with the smallest
    // segments to give back non-used expansion credits to the next multi-terms,
    // as this is more probable with the small segments.
    List<LeafReaderContext> sizeSortedSegments =
        new SegmentTermsSizeComparator().createTermsSizeSortedCopyOf(reader.leaves());

    // TermsData will contain the collected TermState and TermStatistics for all the terms
    // of the phrase. It is filled during PhraseTerm.collectTermData() calls below.
    TermsData termsData = createTermsData(sizeSortedSegments.size());

    // Iterate the phrase terms, and collect the TermState for single-terms.
    // - Early stop if a single term does not match.
    int numMultiTerms = 0;
    for (PhraseTerm phraseTerm : phraseTerms) {
      if (phraseTerm.hasExpansions()) {
        numMultiTerms++;
      } else {
        assert TestCounters.get().incSingleTermAnalysisCount();
        int numMatches = phraseTerm.collectTermData(this, searcher, sizeSortedSegments, termsData);
        if (numMatches == 0) {
          // Early stop here because the single term does not match in any segment.
          // So the whole phrase query cannot match.
          return earlyStopWeight();
        }
      }
    }

    // Iterate the phrase terms and collect the TermState for multi-terms.
    // - Early stop if a multi-term does not match.
    // - Expand the multi-terms only when required.
    int remainingExpansions = maxMultiTermExpansions;
    int remainingMultiTerms = numMultiTerms;
    for (PhraseTerm phraseTerm : phraseTerms) {
      if (phraseTerm.hasExpansions()) {
        assert TestCounters.get().incMultiTermAnalysisCount();
        assert remainingExpansions >= 0 && remainingExpansions <= maxMultiTermExpansions;
        assert remainingMultiTerms > 0;
        // Consider the remaining expansions allowed for all remaining multi-terms.
        // Divide it evenly to get the expansion limit for the current multi-term.
        int maxExpansionsForTerm = remainingExpansions / remainingMultiTerms;
        int numExpansions = phraseTerm.collectTermData(this, searcher, sizeSortedSegments, remainingMultiTerms, maxExpansionsForTerm, termsData);
        assert numExpansions >= 0 && numExpansions <= maxExpansionsForTerm;
        if (numExpansions == 0) {
          // Early stop here because the multi-term does not match in any segment.
          // So the whole phrase query cannot match.
          return earlyStopWeight();
        }
        // Deduct the effectively used expansions. This may give more expansion
        // credits to the next multi-terms.
        remainingExpansions -= numExpansions;
        remainingMultiTerms--;
      }
    }
    assert remainingMultiTerms == 0;
    assert remainingExpansions >= 0;

//    TestCounters.get().printTestCounters(termsData);

    return termsData.areAllTermsMatching() ?
        createPhraseWeight(searcher, scoreMode, boost, termsData)
        : noMatchWeight();
  }

  /**
   * Creates new {@link TermsData}.
   */
  protected TermsData createTermsData(int numSegments) {
    return new TermsData(phraseTerms.size(), numSegments);
  }

  protected Weight earlyStopWeight() {
    assert TestCounters.get().incQueryEarlyStopCount();
    return noMatchWeight();
  }

  protected Weight noMatchWeight() {
    return new ConstantScoreWeight(this, 0) {
      @Override
      public Scorer scorer(LeafReaderContext leafReaderContext) {
        return null;
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  PhraseWeight createPhraseWeight(IndexSearcher searcher, ScoreMode scoreMode,
                                            float boost, TermsData termsData) throws IOException {
    return new PhraseWeight(this, field, searcher, scoreMode) {

      @Override
      protected Similarity.SimScorer getStats(IndexSearcher searcher) throws IOException {
        if (termsData.termStatsList.isEmpty()) {
          return null;
        }
        return searcher.getSimilarity().scorer(
            boost,
            searcher.collectionStatistics(field),
            termsData.termStatsList.toArray(new TermStatistics[0]));
      }

      @Override
      protected PhraseMatcher getPhraseMatcher(LeafReaderContext leafReaderContext, Similarity.SimScorer scorer, boolean exposeOffsets) throws IOException {
        Terms fieldTerms = leafReaderContext.reader().terms(field);
        if (fieldTerms == null) {
          return null;
        }
        TermsEnum termsEnum = fieldTerms.iterator();
        float totalMatchCost = 0;

        PhraseQuery.PostingsAndFreq[] postingsFreqs = new PhraseQuery.PostingsAndFreq[phraseTerms.size()];
        for (int termPosition = 0; termPosition < postingsFreqs.length; termPosition++) {
          TermData termData = termsData.getTermData(termPosition);
          assert termData != null;
          List<TermBytesTermState> termStates = termData.getTermStatesForSegment(leafReaderContext);
          if (termStates == null) {
            // If the current phrase term does not match in the segment, then the phrase cannot match on the segment.
            // So early stop by returning a null scorer.
            return null;
          }
          assert !termStates.isEmpty();

          List<PostingsEnum> postingsEnums = new ArrayList<>(termStates.size());
          for (TermBytesTermState termBytesTermState : termStates) {
            termsEnum.seekExact(termBytesTermState.termBytes, termBytesTermState.termState);
            postingsEnums.add(termsEnum.postings(null, exposeOffsets ? PostingsEnum.ALL : PostingsEnum.POSITIONS));
            totalMatchCost += PhraseQuery.termPositionsCost(termsEnum);
          }
          PostingsEnum unionPostingsEnum;
          if (postingsEnums.size() == 1) {
            unionPostingsEnum = postingsEnums.get(0);
          } else {
            unionPostingsEnum = exposeOffsets ? new MultiPhraseQuery.UnionFullPostingsEnum(postingsEnums) : new MultiPhraseQuery.UnionPostingsEnum(postingsEnums);
          }
          postingsFreqs[termPosition] = new PhraseQuery.PostingsAndFreq(unionPostingsEnum, new SlowImpactsEnum(unionPostingsEnum), termPosition, termData.terms);
        }

        if (slop == 0) {
          // Sort by increasing docFreq order.
          ArrayUtil.timSort(postingsFreqs);
          return new ExactPhraseMatcher(postingsFreqs, scoreMode, scorer, totalMatchCost);
        } else {
          return new SloppyPhraseMatcher(postingsFreqs, slop, scoreMode, scorer, totalMatchCost, exposeOffsets);
        }
      }

      @Override
      public void extractTerms(Set<Term> terms) {
        for (int i = 0, size = phraseTerms.size(); i < size; i++) {
          terms.addAll(termsData.getTermData(i).terms);
        }
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PhraseWildcardQuery)) {
      return false;
    }
    PhraseWildcardQuery pwq = (PhraseWildcardQuery) o;
    return slop == pwq.slop && phraseTerms.equals(pwq.phraseTerms);
  }

  @Override
  public int hashCode() {
    return classHash() ^ slop ^ phraseTerms.hashCode();
  }

  @Override
  public final String toString(String omittedField) {
    StringBuilder builder = new StringBuilder();
    builder.append("phraseWildcard(");

    if (field == null || !field.equals(omittedField)) {
      builder.append(field).append(':');
    }

    builder.append('\"');
    for (int i = 0; i < phraseTerms.size(); i++) {
      if (i != 0) {
        builder.append(' ');
      }
      phraseTerms.get(i).toString(builder);
    }
    builder.append('\"');

    if (slop != 0) {
      builder.append('~');
      builder.append(slop);
    }

    builder.append(")");
    return builder.toString();
  }

  /**
   * Collects the {@link TermState} and {@link TermStatistics} for a single-term
   * without expansion.
   *
   * @param termsData receives the collected data.
   */
  protected int collectSingleTermData(
      SingleTerm singleTerm,
      IndexSearcher searcher,
      List<LeafReaderContext> segments,
      TermsData termsData) throws IOException {
    TermData termData = termsData.getOrCreateTermData(singleTerm.termPosition);
    Term term = singleTerm.term;
    termData.terms.add(term);
    TermStates termStates = TermStates.build(searcher.getIndexReader().getContext(), term, true);

    // Collect TermState per segment.
    int numMatches = 0;
    Iterator<LeafReaderContext> segmentIterator = segments.iterator();
    while (segmentIterator.hasNext()) {
      LeafReaderContext leafReaderContext = segmentIterator.next();
      assert TestCounters.get().incSegmentUseCount();
      boolean termMatchesInSegment = false;
      Terms terms = leafReaderContext.reader().terms(term.field());
      if (terms != null) {
        checkTermsHavePositions(terms);
        TermState termState = termStates.get(leafReaderContext);
        if (termState != null) {
          termMatchesInSegment = true;
          numMatches++;
          termData.setTermStatesForSegment(leafReaderContext, Collections.singletonList(new TermBytesTermState(term.bytes(), termState)));
        }
      }
      if (!termMatchesInSegment && shouldOptimizeSegments()) {
        // Remove this segment from the list because the phrase cannot match on it.
        segmentIterator.remove();
        assert TestCounters.get().incSegmentSkipCount();
      }
    }
    // Collect the term stats across all segments.
    if (termStates.docFreq() > 0) {
      termsData.termStatsList.add(searcher.termStatistics(term, termStates.docFreq(), termStates.totalTermFreq()));
    }
    return numMatches;
  }

  /**
   * Collects the {@link TermState} and {@link TermStatistics} for a multi-term
   * with expansion.
   *
   * @param remainingMultiTerms the number of remaining multi-terms to process,
   *                            including the current one, excluding the multi-terms already processed.
   * @param termsData           receives the collected data.
   */
  protected int collectMultiTermData(
      MultiTerm multiTerm,
      IndexSearcher searcher,
      List<LeafReaderContext> segments,
      int remainingMultiTerms, // Unused here but leveraged by extending classes.
      int maxExpansionsForTerm,
      TermsData termsData) throws IOException {
    TermData termData = termsData.getOrCreateTermData(multiTerm.termPosition);
    Map<BytesRef, TermStats> termStatsMap = createTermStatsMap(multiTerm);
    int numExpansions = 0;
    Iterator<LeafReaderContext> segmentIterator = segments.iterator();
    MutableValueBool shouldStopSegmentIteration = new MutableValueBool();

    while (segmentIterator.hasNext() && !shouldStopSegmentIteration.value) {
      LeafReaderContext leafReaderContext = segmentIterator.next();
      int remainingExpansions = maxExpansionsForTerm - numExpansions;
      assert remainingExpansions >= 0;
      List<TermBytesTermState> termStates = collectMultiTermDataForSegment(
          multiTerm, leafReaderContext, remainingExpansions, shouldStopSegmentIteration, termStatsMap);

      if (!termStates.isEmpty()) {
        assert termStates.size() <= remainingExpansions;
        numExpansions += termStates.size();
        assert numExpansions <= maxExpansionsForTerm;
        termData.setTermStatesForSegment(leafReaderContext, termStates);

      } else if (shouldOptimizeSegments()) {
        // Remove this segment from the list because the phrase cannot match on it.
        segmentIterator.remove();
        assert TestCounters.get().incSegmentSkipCount();
      }
    }

    // Collect the term stats across all segments.
    collectMultiTermStats(searcher, termStatsMap, termsData, termData);
    return numExpansions;
  }

  protected boolean shouldOptimizeSegments() {
    return segmentOptimizationEnabled;
  }

  /**
   * Creates a {@link TermStats} map for a {@link MultiTerm}.
   */
  protected Map<BytesRef, TermStats> createTermStatsMap(MultiTerm multiTerm) { // multiTerm param can be used by sub-classes.
    return new HashMap<>();
  }

  /**
   * Collects the {@link TermState} list and {@link TermStatistics} for a multi-term
   * on a specific index segment.
   *
   * @param remainingExpansions        the number of remaining expansions allowed
   *                                   for the segment.
   * @param shouldStopSegmentIteration to be set to true to stop the segment
   *                                   iteration calling this method repeatedly.
   * @param termStatsMap               receives the collected {@link TermStats} across all segments.
   */
  protected List<TermBytesTermState> collectMultiTermDataForSegment(
      MultiTerm multiTerm,
      LeafReaderContext leafReaderContext,
      int remainingExpansions,
      MutableValueBool shouldStopSegmentIteration,
      Map<BytesRef, TermStats> termStatsMap) throws IOException {
    TermsEnum termsEnum = createTermsEnum(multiTerm, leafReaderContext);
    if (termsEnum == null) {
      return Collections.emptyList();
    }
    assert TestCounters.get().incSegmentUseCount();
    List<TermBytesTermState> termStates = new ArrayList<>();
    while (termsEnum.next() != null && remainingExpansions > 0) {
      // Collect term stats for the segment.
      TermStats termStats = termStatsMap.get(termsEnum.term());
      if (termStats == null) {
        BytesRef termBytes = BytesRef.deepCopyOf(termsEnum.term());
        termStats = new TermStats(termBytes);
        termStatsMap.put(termBytes, termStats);
      }
      // Accumulate stats the same way TermStates.accumulateStatistics() does.
      // Sum the stats per term for all segments the same way TermStates.build() does.
      termStats.addStats(termsEnum.docFreq(), termsEnum.totalTermFreq());

      // Collect TermState per segment.
      termStates.add(new TermBytesTermState(termStats.termBytes, termsEnum.termState()));
      remainingExpansions--;
      assert TestCounters.get().incExpansionCount();
    }
    assert remainingExpansions >= 0;
    shouldStopSegmentIteration.value = remainingExpansions == 0;
    return termStates;
  }

  /**
   * Creates the {@link TermsEnum} for the given {@link MultiTerm} and segment.
   *
   * @return null if there is no term for this query field in the segment.
   */
  protected TermsEnum createTermsEnum(MultiTerm multiTerm, LeafReaderContext leafReaderContext) throws IOException {
    Terms terms = leafReaderContext.reader().terms(field);
    if (terms == null) {
      return null;
    }
    checkTermsHavePositions(terms);
    TermsEnum termsEnum = multiTerm.query.getTermsEnum(terms);
    assert termsEnum != null;
    return termsEnum;
  }

  /**
   * Collect the term stats across all segments.
   *
   * @param termStatsMap input map of already collected {@link TermStats}.
   * @param termsData    receives the {@link TermStatistics} computed for all {@link TermStats}.
   * @param termData     receives all the collected {@link Term}.
   */
  protected void collectMultiTermStats(
      IndexSearcher searcher,
      Map<BytesRef, TermStats> termStatsMap,
      TermsData termsData,
      TermData termData) throws IOException {
    // Collect term stats across all segments.
    // Collect stats the same way MultiPhraseQuery.MultiPhraseWeight constructor does, for all terms and all segments.
    for (Map.Entry<BytesRef, TermStats> termStatsEntry : termStatsMap.entrySet()) {
      Term term = new Term(field, termStatsEntry.getKey());
      termData.terms.add(term);
      TermStats termStats = termStatsEntry.getValue();
      if (termStats.docFreq > 0) {
        termsData.termStatsList.add(searcher.termStatistics(term, termStats.docFreq, termStats.totalTermFreq));
      }
    }
  }

  protected void checkTermsHavePositions(Terms terms) {
    if (!terms.hasPositions()) {
      throw new IllegalStateException("field \"" + field + "\" was indexed without position data;" +
          " cannot run " + PhraseWildcardQuery.class.getSimpleName());
    }
  }

  /**
   * Builds a {@link PhraseWildcardQuery}.
   */
  public static class Builder {

    protected final String field;
    protected final List<PhraseTerm> phraseTerms;
    protected int slop;
    protected final int maxMultiTermExpansions;
    protected final boolean segmentOptimizationEnabled;

    /**
     * @param field                  The query field.
     * @param maxMultiTermExpansions The maximum number of expansions across all multi-terms and across all segments.
     *                               It counts expansions for each segments individually, that allows optimizations per
     *                               segment and unused expansions are credited to next segments. This is different from
     *                               {@link MultiPhraseQuery} and {@link org.apache.lucene.search.spans.SpanMultiTermQueryWrapper}
     *                               which have an expansion limit per multi-term.
     */
    public Builder(String field, int maxMultiTermExpansions) {
      this(field, maxMultiTermExpansions, true);
    }

    /**
     * @param field                      The query field.
     * @param maxMultiTermExpansions     The maximum number of expansions across all multi-terms and across all segments.
     *                                   It counts expansions for each segments individually, that allows optimizations per
     *                                   segment and unused expansions are credited to next segments. This is different from
     *                                   {@link MultiPhraseQuery} and {@link org.apache.lucene.search.spans.SpanMultiTermQueryWrapper}
     *                                   which have an expansion limit per multi-term.
     * @param segmentOptimizationEnabled Whether to enable the segment optimization which consists in ignoring a segment
     *                                   for further analysis as soon as a term is not present inside it. This optimizes
     *                                   the query execution performance but changes the scoring. The result ranking is
     *                                   preserved.
     */
    public Builder(String field, int maxMultiTermExpansions, boolean segmentOptimizationEnabled) {
      this.field = field;
      this.maxMultiTermExpansions = maxMultiTermExpansions;
      this.segmentOptimizationEnabled = segmentOptimizationEnabled;
      phraseTerms = new ArrayList<>();
    }

    /**
     * Adds a single term at the next position in the phrase.
     */
    public Builder addTerm(BytesRef termBytes) {
      return addTerm(new Term(field, termBytes));
    }

    /**
     * Adds a single term at the next position in the phrase.
     */
    public Builder addTerm(Term term) {
      if (!term.field().equals(field)) {
        throw new IllegalArgumentException(term.getClass().getSimpleName()
            + " field \"" + term.field() + "\" cannot be different from the "
            + PhraseWildcardQuery.class.getSimpleName() + " field \"" + field + "\"");
      }
      phraseTerms.add(new SingleTerm(term, phraseTerms.size()));
      return this;
    }

    /**
     * Adds a multi-term at the next position in the phrase.
     * Any of the terms returned by the provided {@link MultiTermQuery} enumeration
     * may match (expansion as a disjunction).
     */
    public Builder addMultiTerm(MultiTermQuery multiTermQuery) {
      if (!multiTermQuery.getField().equals(field)) {
        throw new IllegalArgumentException(multiTermQuery.getClass().getSimpleName()
            + " field \"" + multiTermQuery.getField() + "\" cannot be different from the "
            + PhraseWildcardQuery.class.getSimpleName() + " field \"" + field + "\"");
      }
      phraseTerms.add(new MultiTerm(multiTermQuery, phraseTerms.size()));
      return this;
    }

    /**
     * Sets the phrase slop.
     */
    public Builder setSlop(int slop) {
      if (slop < 0) {
        throw new IllegalArgumentException("slop value cannot be negative");
      }
      this.slop = slop;
      return this;
    }

    /**
     * Builds a {@link PhraseWildcardQuery}.
     */
    public PhraseWildcardQuery build() {
      return new PhraseWildcardQuery(field, phraseTerms, slop, maxMultiTermExpansions, segmentOptimizationEnabled);
    }
  }

  /**
   * All {@link PhraseTerm} are light and immutable. They do not hold query
   * processing data such as {@link TermsData}. That way, the {@link PhraseWildcardQuery}
   * is immutable and light itself and can be used safely as a key of the query cache.
   */
  protected abstract static class PhraseTerm {

    protected final int termPosition;

    protected PhraseTerm(int termPosition) {
      this.termPosition = termPosition;
    }

    protected abstract boolean hasExpansions();

    protected abstract Query getQuery();

    /**
     * Collects {@link TermState} and {@link TermStatistics} for the term without expansion.
     * It must be called only if {@link #hasExpansions()} returns false.
     * Simplified version of {@code #collectTermData(PhraseWildcardQuery, IndexSearcher, List, int, int, TermsData)}
     * with less arguments. This method throws {@link UnsupportedOperationException} if not overridden.
     */
    protected int collectTermData(
        PhraseWildcardQuery query,
        IndexSearcher searcher,
        List<LeafReaderContext> segments,
        TermsData termsData) throws IOException {
      throw new UnsupportedOperationException();
    }

    /**
     * Collects {@link TermState} and {@link TermStatistics} for the term (potentially expanded).
     *
     * @param termsData {@link TermsData} to update with the collected terms and stats.
     * @return The number of expansions or matches in all segments; or 0 if this term
     * does not match in any segment, in this case the phrase query can immediately stop.
     */
    protected abstract int collectTermData(
        PhraseWildcardQuery query,
        IndexSearcher searcher,
        List<LeafReaderContext> segments,
        int remainingMultiTerms,
        int maxExpansionsForTerm,
        TermsData termsData) throws IOException;

    protected abstract void toString(StringBuilder builder);

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();
  }

  /**
   * Phrase term with no expansion.
   */
  protected static class SingleTerm extends PhraseTerm {

    protected final Term term;

    protected SingleTerm(Term term, int termPosition) {
      super(termPosition);
      this.term = term;
    }

    @Override
    protected boolean hasExpansions() {
      return false;
    }

    @Override
    protected Query getQuery() {
      return new TermQuery(term);
    }

    @Override
    protected int collectTermData(
        PhraseWildcardQuery query,
        IndexSearcher searcher,
        List<LeafReaderContext> segments,
        TermsData termsData) throws IOException {
      return collectTermData(query, searcher, segments, 0, 0, termsData);
    }

    @Override
    protected int collectTermData(
        PhraseWildcardQuery query,
        IndexSearcher searcher,
        List<LeafReaderContext> segments,
        int remainingMultiTerms,
        int maxExpansionsForTerm,
        TermsData termsData) throws IOException {
      return query.collectSingleTermData(this, searcher, segments, termsData);
    }

    @Override
    protected void toString(StringBuilder builder) {
      builder.append(term.text());
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SingleTerm)) {
        return false;
      }
      SingleTerm singleTerm = (SingleTerm) o;
      return term.equals(singleTerm.term);
    }

    @Override
    public int hashCode() {
      return term.hashCode();
    }
  }

  /**
   * Phrase term with expansions.
   */
  protected static class MultiTerm extends PhraseTerm {

    protected final MultiTermQuery query;

    protected MultiTerm(MultiTermQuery query, int termPosition) {
      super(termPosition);
      this.query = query;
    }

    @Override
    protected boolean hasExpansions() {
      return true;
    }

    @Override
    protected Query getQuery() {
      return query;
    }

    @Override
    protected int collectTermData(
        PhraseWildcardQuery query,
        IndexSearcher searcher,
        List<LeafReaderContext> segments,
        int remainingMultiTerms,
        int maxExpansionsForTerm,
        TermsData termsData) throws IOException {
      return query.collectMultiTermData(this, searcher, segments, remainingMultiTerms, maxExpansionsForTerm, termsData);
    }

    @Override
    protected void toString(StringBuilder builder) {
      builder.append(query.toString(query.field));
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof MultiTerm)) {
        return false;
      }
      MultiTerm multiTerm = (MultiTerm) o;
      return query.equals(multiTerm.query);
    }

    @Override
    public int hashCode() {
      return query.hashCode();
    }
  }

  /**
   * Holds the {@link TermState} and {@link TermStatistics} for all the matched
   * and collected {@link Term}, for all phrase terms, for all segments.
   */
  protected static class TermsData {

    protected final int numTerms;
    protected final int numSegments;
    protected final List<TermStatistics> termStatsList;
    protected final TermData[] termDataPerPosition;
    protected int numTermsMatching;

    protected TermsData(int numTerms, int numSegments) {
      this.numTerms = numTerms;
      this.numSegments = numSegments;
      termStatsList = new ArrayList<>();
      termDataPerPosition = new TermData[numTerms];
    }

    protected TermData getOrCreateTermData(int termPosition) {
      TermData termData = termDataPerPosition[termPosition];
      if (termData == null) {
        termData = new TermData(numSegments, this);
        termDataPerPosition[termPosition] = termData;
      }
      return termData;
    }

    protected TermData getTermData(int termPosition) {
      return termDataPerPosition[termPosition];
    }

    protected boolean areAllTermsMatching() {
      assert numTermsMatching <= numTerms;
      return numTermsMatching == numTerms;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("TermsData(");
      builder.append("numSegments=").append(numSegments);
      builder.append(", termDataPerPosition=").append(Arrays.asList(termDataPerPosition));
      builder.append(", termsStatsList=[");
      for (TermStatistics termStatistics : termStatsList) {
        builder.append("{")
            .append(termStatistics.term().utf8ToString())
            .append(", ").append(termStatistics.docFreq())
            .append(", ").append(termStatistics.totalTermFreq())
            .append("}");
      }
      builder.append("]");
      builder.append(")");
      return builder.toString();
    }
  }

  /**
   * Holds the {@link TermState} for all the collected {@link Term},
   * for a specific phrase term, for all segments.
   */
  protected static class TermData {

    protected final int numSegments;
    protected final TermsData termsData;
    protected List<TermBytesTermState>[] termStatesPerSegment;
    protected final List<Term> terms;

    protected TermData(int numSegments, TermsData termsData) {
      this.numSegments = numSegments;
      this.termsData = termsData;
      terms = new ArrayList<>();
    }

    /**
     * Sets the collected list of {@link TermBytesTermState} for the given segment.
     */
    @SuppressWarnings("unchecked")
    protected void setTermStatesForSegment(LeafReaderContext leafReaderContext, List<TermBytesTermState> termStates) {
      if (termStatesPerSegment == null) {
        termStatesPerSegment = (List<TermBytesTermState>[]) new List[numSegments];
        termsData.numTermsMatching++;
      }
      termStatesPerSegment[leafReaderContext.ord] = termStates;
    }

    /**
     * @return The collected list of {@link TermBytesTermState} for the given segment;
     * or null if this phrase term does not match in the given segment.
     */
    protected List<TermBytesTermState> getTermStatesForSegment(LeafReaderContext leafReaderContext) {
      assert termStatesPerSegment != null : "No TermState for any segment; the query should have been stopped before";
      return termStatesPerSegment[leafReaderContext.ord];
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("TermData(");
      builder.append("termStates=");
      if (termStatesPerSegment == null) {
        builder.append("null");
      } else {
        builder.append(Arrays.asList(termStatesPerSegment));
      }
      builder.append(", terms=").append(terms);
      builder.append(")");
      return builder.toString();
    }
  }

  /**
   * Holds a pair of term bytes - term state.
   */
  public static class TermBytesTermState {

    protected final BytesRef termBytes;
    protected final TermState termState;

    public TermBytesTermState(BytesRef termBytes, TermState termState) {
      this.termBytes = termBytes;
      this.termState = termState;
    }

    @Override
    public String toString() {
      return "\"" + termBytes.utf8ToString() + "\"->" + termState;
    }
  }

  /**
   * Accumulates the doc freq and total term freq.
   */
  public static class TermStats {

    protected final BytesRef termBytes;
    protected int docFreq;
    protected long totalTermFreq;

    protected TermStats(BytesRef termBytes) {
      this.termBytes = termBytes;
    }

    public BytesRef getTermBytes() {
      return termBytes;
    }

    protected void addStats(int docFreq, long totalTermFreq) {
      this.docFreq += docFreq;
      if (this.totalTermFreq >= 0 && totalTermFreq >= 0) {
        this.totalTermFreq += totalTermFreq;
      } else {
        this.totalTermFreq = -1;
      }
    }
  }

  /**
   * Compares segments based of the number of terms they contain.
   * <p>
   * This is used to sort segments incrementally by number of terms. This
   * way the first segment to search is the smallest, so a term has the lowest
   * probability to match in this segment. And if the term does not match,
   * we credit unused expansions when searching the other next segments.
   */
  protected class SegmentTermsSizeComparator implements Comparator<LeafReaderContext> {

    private static final String COMPARISON_ERROR_MESSAGE = "Segment comparison error";

    @Override
    public int compare(LeafReaderContext leafReaderContext1, LeafReaderContext leafReaderContext2) {
      try {
        return Long.compare(getTermsSize(leafReaderContext1), getTermsSize(leafReaderContext2));
      } catch (IOException e) {
        throw new RuntimeException(COMPARISON_ERROR_MESSAGE, e);
      }
    }

    protected List<LeafReaderContext> createTermsSizeSortedCopyOf(List<LeafReaderContext> segments) throws IOException {
      List<LeafReaderContext> copy = new ArrayList<>(segments);
      try {
        copy.sort(this);
      } catch (RuntimeException e) {
        if (COMPARISON_ERROR_MESSAGE.equals(e.getMessage())) {
          throw (IOException) e.getCause();
        }
        throw e;
      }
      return copy;
    }

    private long getTermsSize(LeafReaderContext leafReaderContext) throws IOException {
      Terms terms = leafReaderContext.reader().terms(field);
      return terms == null ? 0 : terms.size();
    }
  }

  /**
   * Test counters incremented when assertions are enabled. Used only when testing.
   */
  protected static class TestCounters {

    private static final TestCounters SINGLETON = new TestCounters();

    protected long singleTermAnalysisCount;
    protected long multiTermAnalysisCount;
    protected long expansionCount;
    protected long segmentUseCount;
    protected long segmentSkipCount;
    protected long queryEarlyStopCount;

    protected static TestCounters get() {
      return SINGLETON;
    }

    protected boolean incSingleTermAnalysisCount() {
      singleTermAnalysisCount++;
      return true;
    }

    protected boolean incMultiTermAnalysisCount() {
      multiTermAnalysisCount++;
      return true;
    }

    protected boolean incExpansionCount() {
      expansionCount++;
      return true;
    }

    protected boolean incSegmentUseCount() {
      segmentUseCount++;
      return true;
    }

    protected boolean incSegmentSkipCount() {
      segmentSkipCount++;
      return true;
    }

    protected boolean incQueryEarlyStopCount() {
      queryEarlyStopCount++;
      return true;
    }

    protected void clear() {
      singleTermAnalysisCount = 0;
      multiTermAnalysisCount = 0;
      expansionCount = 0;
      segmentUseCount = 0;
      segmentSkipCount = 0;
      queryEarlyStopCount = 0;
    }

//    protected void printTestCounters(TermsData termsData) {
//      System.out.println("singleTermAnalysisCount=" + singleTermAnalysisCount);
//      System.out.println("multiTermAnalysisCount=" + multiTermAnalysisCount);
//      System.out.println("expansionCount=" + expansionCount);
//      System.out.println("segmentUseCount=" + segmentUseCount);
//      System.out.println("segmentSkipCount=" + segmentSkipCount);
//      System.out.println("queryEarlyStopCount=" + queryEarlyStopCount);
//      System.out.println(termsData);
//    }
  }
}