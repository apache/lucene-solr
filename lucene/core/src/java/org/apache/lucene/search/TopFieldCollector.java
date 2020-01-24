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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.search.MaxScoreAccumulator.DocAndScore;
import org.apache.lucene.search.TotalHits.Relation;

/**
 * A {@link Collector} that sorts by {@link SortField} using
 * {@link FieldComparator}s.
 * <p>
 * See the {@link #create(org.apache.lucene.search.Sort, int, int)} method
 * for instantiating a TopFieldCollector.
 *
 * @lucene.experimental
 */
public abstract class TopFieldCollector extends TopDocsCollector<Entry> {

  // TODO: one optimization we could do is to pre-fill
  // the queue with sentinel value that guaranteed to
  // always compare lower than a real hit; this would
  // save having to check queueFull on each insert

  private static abstract class MultiComparatorLeafCollector implements LeafCollector {

    final LeafFieldComparator comparator;
    final int reverseMul;
    Scorable scorer;

    MultiComparatorLeafCollector(LeafFieldComparator[] comparators, int[] reverseMul) {
      if (comparators.length == 1) {
        this.reverseMul = reverseMul[0];
        this.comparator = comparators[0];
      } else {
        this.reverseMul = 1;
        this.comparator = new MultiLeafFieldComparator(comparators, reverseMul);
      }
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      comparator.setScorer(scorer);
      this.scorer = scorer;
    }
  }

  static boolean canEarlyTerminate(Sort searchSort, Sort indexSort) {
    return canEarlyTerminateOnDocId(searchSort) ||
           canEarlyTerminateOnPrefix(searchSort, indexSort);
  }

  private static boolean canEarlyTerminateOnDocId(Sort searchSort) {
    final SortField[] fields1 = searchSort.getSort();
    return SortField.FIELD_DOC.equals(fields1[0]);
  }

  private static boolean canEarlyTerminateOnPrefix(Sort searchSort, Sort indexSort) {
    if (indexSort != null) {
      final SortField[] fields1 = searchSort.getSort();
      final SortField[] fields2 = indexSort.getSort();
      // early termination is possible if fields1 is a prefix of fields2
      if (fields1.length > fields2.length) {
        return false;
      }
      return Arrays.asList(fields1).equals(Arrays.asList(fields2).subList(0, fields1.length));
    } else {
      return false;
    }
  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, with tracking
   * document scores and maxScore.
   */
  private static class SimpleFieldCollector extends TopFieldCollector {
    final Sort sort;
    final FieldValueHitQueue<Entry> queue;

    public SimpleFieldCollector(Sort sort, FieldValueHitQueue<Entry> queue, int numHits,
                                HitsThresholdChecker hitsThresholdChecker,
                                MaxScoreAccumulator minScoreAcc) {
      super(queue, numHits, hitsThresholdChecker, sort.needsScores(), minScoreAcc);
      this.sort = sort;
      this.queue = queue;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      docBase = context.docBase;

      final LeafFieldComparator[] comparators = queue.getComparators(context);
      final int[] reverseMul = queue.getReverseMul();
      final Sort indexSort = context.reader().getMetaData().getSort();
      final boolean canEarlyTerminate = canEarlyTerminate(sort, indexSort);

      return new MultiComparatorLeafCollector(comparators, reverseMul) {

        boolean collectedAllCompetitiveHits = false;

        @Override
        public void setScorer(Scorable scorer) throws IOException {
          super.setScorer(scorer);
          minCompetitiveScore = 0f;
          updateMinCompetitiveScore(scorer);
          if (minScoreAcc != null) {
            updateGlobalMinCompetitiveScore(scorer);
          }
        }

        @Override
        public void collect(int doc) throws IOException {
          ++totalHits;
          hitsThresholdChecker.incrementHitCount();

          if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
            updateGlobalMinCompetitiveScore(scorer);
          }

          if (queueFull) {
            if (collectedAllCompetitiveHits || reverseMul * comparator.compareBottom(doc) <= 0) {
              // since docs are visited in doc Id order, if compare is 0, it means
              // this document is largest than anything else in the queue, and
              // therefore not competitive.
              if (canEarlyTerminate) {
                if (hitsThresholdChecker.isThresholdReached()) {
                  totalHitsRelation = Relation.GREATER_THAN_OR_EQUAL_TO;
                  throw new CollectionTerminatedException();
                } else {
                  collectedAllCompetitiveHits = true;
                }
              } else if (totalHitsRelation == Relation.EQUAL_TO) {
                // we can start setting the min competitive score if the
                // threshold is reached for the first time here.
                updateMinCompetitiveScore(scorer);
              }
              return;
            }

            // This hit is competitive - replace bottom element in queue & adjustTop
            comparator.copy(bottom.slot, doc);
            updateBottom(doc);
            comparator.setBottom(bottom.slot);
            updateMinCompetitiveScore(scorer);
          } else {
            // Startup transient: queue hasn't gathered numHits yet
            final int slot = totalHits - 1;

            // Copy hit into queue
            comparator.copy(slot, doc);
            add(slot, doc);
            if (queueFull) {
              comparator.setBottom(bottom.slot);
              updateMinCompetitiveScore(scorer);
            }
          }
        }

      };
    }

  }

  /*
   * Implements a TopFieldCollector when after != null.
   */
  private final static class PagingFieldCollector extends TopFieldCollector {

    final Sort sort;
    int collectedHits;
    final FieldValueHitQueue<Entry> queue;
    final FieldDoc after;

    public PagingFieldCollector(Sort sort, FieldValueHitQueue<Entry> queue, FieldDoc after, int numHits,
                                HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
      super(queue, numHits, hitsThresholdChecker, sort.needsScores(), minScoreAcc);
      this.sort = sort;
      this.queue = queue;
      this.after = after;

      FieldComparator<?>[] comparators = queue.comparators;
      // Tell all comparators their top value:
      for(int i=0;i<comparators.length;i++) {
        @SuppressWarnings("unchecked")
        FieldComparator<Object> comparator = (FieldComparator<Object>) comparators[i];
        comparator.setTopValue(after.fields[i]);
      }
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      docBase = context.docBase;
      final int afterDoc = after.doc - docBase;
      final Sort indexSort = context.reader().getMetaData().getSort();
      final boolean canEarlyTerminate = canEarlyTerminate(sort, indexSort);
      return new MultiComparatorLeafCollector(queue.getComparators(context), queue.getReverseMul()) {

        boolean collectedAllCompetitiveHits = false;

        @Override
        public void setScorer(Scorable scorer) throws IOException {
          super.setScorer(scorer);
          minCompetitiveScore = 0f;
          updateMinCompetitiveScore(scorer);
          if (minScoreAcc != null) {
            updateGlobalMinCompetitiveScore(scorer);
          }
        }

        @Override
        public void collect(int doc) throws IOException {
          //System.out.println("  collect doc=" + doc);

          totalHits++;
          hitsThresholdChecker.incrementHitCount();

          if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
            updateGlobalMinCompetitiveScore(scorer);
          }

          if (queueFull) {
            // Fastmatch: return if this hit is no better than
            // the worst hit currently in the queue:
            if (collectedAllCompetitiveHits || reverseMul * comparator.compareBottom(doc) <= 0) {
              // since docs are visited in doc Id order, if compare is 0, it means
              // this document is largest than anything else in the queue, and
              // therefore not competitive.
              if (canEarlyTerminate) {
                if (hitsThresholdChecker.isThresholdReached()) {
                  totalHitsRelation = Relation.GREATER_THAN_OR_EQUAL_TO;
                  throw new CollectionTerminatedException();
                } else {
                  collectedAllCompetitiveHits = true;
                }
              } else if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
                // we can start setting the min competitive score if the
                // threshold is reached for the first time here.
                updateMinCompetitiveScore(scorer);
              }
              return;
            }
          }

          final int topCmp = reverseMul * comparator.compareTop(doc);
          if (topCmp > 0 || (topCmp == 0 && doc <= afterDoc)) {
            // Already collected on a previous page
            if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
              // we just reached totalHitsThreshold, we can start setting the min
              // competitive score now
              updateMinCompetitiveScore(scorer);
            }
            return;
          }

          if (queueFull) {
            // This hit is competitive - replace bottom element in queue & adjustTop
            comparator.copy(bottom.slot, doc);

            updateBottom(doc);

            comparator.setBottom(bottom.slot);
            updateMinCompetitiveScore(scorer);
          } else {
            collectedHits++;

            // Startup transient: queue hasn't gathered numHits yet
            final int slot = collectedHits - 1;
            //System.out.println("    slot=" + slot);
            // Copy hit into queue
            comparator.copy(slot, doc);

            bottom = pq.add(new Entry(slot, docBase + doc));
            queueFull = collectedHits == numHits;
            if (queueFull) {
              comparator.setBottom(bottom.slot);
              updateMinCompetitiveScore(scorer);
            }
          }
        }
      };
    }

  }

  private static final ScoreDoc[] EMPTY_SCOREDOCS = new ScoreDoc[0];

  final int numHits;
  final HitsThresholdChecker hitsThresholdChecker;
  final FieldComparator.RelevanceComparator firstComparator;
  final boolean canSetMinScore;

  // an accumulator that maintains the maximum of the segment's minimum competitive scores
  final MaxScoreAccumulator minScoreAcc;
  // the current local minimum competitive score already propagated to the underlying scorer
  float minCompetitiveScore;

  final int numComparators;
  FieldValueHitQueue.Entry bottom = null;
  boolean queueFull;
  int docBase;
  final boolean needsScores;
  final ScoreMode scoreMode;

  // Declaring the constructor private prevents extending this class by anyone
  // else. Note that the class cannot be final since it's extended by the
  // internal versions. If someone will define a constructor with any other
  // visibility, then anyone will be able to extend the class, which is not what
  // we want.
  private TopFieldCollector(FieldValueHitQueue<Entry> pq, int numHits,
                            HitsThresholdChecker hitsThresholdChecker, boolean needsScores,
                            MaxScoreAccumulator minScoreAcc) {
    super(pq);
    this.needsScores = needsScores;
    this.numHits = numHits;
    this.hitsThresholdChecker = hitsThresholdChecker;
    this.numComparators = pq.getComparators().length;
    FieldComparator<?> fieldComparator = pq.getComparators()[0];
    int reverseMul = pq.reverseMul[0];
    if (fieldComparator.getClass().equals(FieldComparator.RelevanceComparator.class)
          && reverseMul == 1 // if the natural sort is preserved (sort by descending relevance)
          && hitsThresholdChecker.getHitsThreshold() != Integer.MAX_VALUE) {
      firstComparator = (FieldComparator.RelevanceComparator) fieldComparator;
      scoreMode = ScoreMode.TOP_SCORES;
      canSetMinScore = true;
    } else {
      firstComparator = null;
      scoreMode = needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
      canSetMinScore = false;
    }
    this.minScoreAcc = minScoreAcc;
  }

  @Override
  public ScoreMode scoreMode() {
    return scoreMode;
  }

  protected void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
    assert minScoreAcc != null;
    if (canSetMinScore
          && hitsThresholdChecker.isThresholdReached()) {
      // we can start checking the global maximum score even
      // if the local queue is not full because the threshold
      // is reached.
      DocAndScore maxMinScore = minScoreAcc.get();
      if (maxMinScore != null && maxMinScore.score > minCompetitiveScore) {
        scorer.setMinCompetitiveScore(maxMinScore.score);
        minCompetitiveScore = maxMinScore.score;
        totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
      }
    }
  }

  protected void updateMinCompetitiveScore(Scorable scorer) throws IOException {
    if (canSetMinScore
          && queueFull
          && hitsThresholdChecker.isThresholdReached()) {
      assert bottom != null && firstComparator != null;
      float minScore = firstComparator.value(bottom.slot);
      if (minScore > minCompetitiveScore) {
        scorer.setMinCompetitiveScore(minScore);
        minCompetitiveScore = minScore;
        totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        if (minScoreAcc != null) {
          minScoreAcc.accumulate(bottom.doc, minScore);
        }
      }
    }
  }

  /**
   * Creates a new {@link TopFieldCollector} from the given
   * arguments.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>.
   *
   * @param sort
   *          the sort criteria (SortFields).
   * @param numHits
   *          the number of results to collect.
   * @param totalHitsThreshold
   *          the number of docs to count accurately. If the query matches more than
   *          {@code totalHitsThreshold} hits then its hit count will be a
   *          lower bound. On the other hand if the query matches less than or exactly
   *          {@code totalHitsThreshold} hits then the hit count of the result will
   *          be accurate. {@link Integer#MAX_VALUE} may be used to make the hit
   *          count accurate, but this will also make query processing slower.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   */
  public static TopFieldCollector create(Sort sort, int numHits, int totalHitsThreshold) {
    return create(sort, numHits, null, totalHitsThreshold);
  }

  /**
   * Creates a new {@link TopFieldCollector} from the given
   * arguments.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>.
   *
   * @param sort
   *          the sort criteria (SortFields).
   * @param numHits
   *          the number of results to collect.
   * @param after
   *          only hits after this FieldDoc will be collected
   * @param totalHitsThreshold
   *          the number of docs to count accurately. If the query matches more than
   *          {@code totalHitsThreshold} hits then its hit count will be a
   *          lower bound. On the other hand if the query matches less than or exactly
   *          {@code totalHitsThreshold} hits then the hit count of the result will
   *          be accurate. {@link Integer#MAX_VALUE} may be used to make the hit
   *          count accurate, but this will also make query processing slower.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   */
  public static TopFieldCollector create(Sort sort, int numHits, FieldDoc after, int totalHitsThreshold) {
    if (totalHitsThreshold < 0) {
      throw new IllegalArgumentException("totalHitsThreshold must be >= 0, got " + totalHitsThreshold);
    }

    return create(sort, numHits, after, HitsThresholdChecker.create(totalHitsThreshold), null /* bottomValueChecker */);
  }

  /**
   * Same as above with additional parameters to allow passing in the threshold checker and the max score accumulator.
   */
  static TopFieldCollector create(Sort sort, int numHits, FieldDoc after,
                                         HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {

    if (sort.fields.length == 0) {
      throw new IllegalArgumentException("Sort must contain at least one field");
    }

    if (numHits <= 0) {
      throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }

    if (hitsThresholdChecker == null) {
      throw new IllegalArgumentException("hitsThresholdChecker should not be null");
    }

    FieldValueHitQueue<Entry> queue = FieldValueHitQueue.create(sort.fields, numHits);

    if (after == null) {
      return new SimpleFieldCollector(sort, queue, numHits, hitsThresholdChecker, minScoreAcc);
    } else {
      if (after.fields == null) {
        throw new IllegalArgumentException("after.fields wasn't set; you must pass fillFields=true for the previous search");
      }

      if (after.fields.length != sort.getSort().length) {
        throw new IllegalArgumentException("after.fields has " + after.fields.length + " values but sort has " + sort.getSort().length);
      }

      return new PagingFieldCollector(sort, queue, after, numHits, hitsThresholdChecker, minScoreAcc);
    }
  }

  /**
   * Create a CollectorManager which uses a shared hit counter to maintain number of hits
   * and a shared {@link MaxScoreAccumulator} to propagate the minimum score accross segments if
   * the primary sort is by relevancy.
   */
  public static CollectorManager<TopFieldCollector, TopFieldDocs> createSharedManager(Sort sort, int numHits, FieldDoc after,
                                                                                 int totalHitsThreshold) {
    return new CollectorManager<>() {

      private final HitsThresholdChecker hitsThresholdChecker = HitsThresholdChecker.createShared(totalHitsThreshold);
      private final MaxScoreAccumulator minScoreAcc = new MaxScoreAccumulator();

      @Override
      public TopFieldCollector newCollector() throws IOException {
        return create(sort, numHits, after, hitsThresholdChecker, minScoreAcc);
      }

      @Override
      public TopFieldDocs reduce(Collection<TopFieldCollector> collectors) throws IOException {
        final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
        int i = 0;
        for (TopFieldCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(sort, 0, numHits, topDocs);
      }
    };
  }

  /**
   * Populate {@link ScoreDoc#score scores} of the given {@code topDocs}.
   * @param topDocs   the top docs to populate
   * @param searcher  the index searcher that has been used to compute {@code topDocs}
   * @param query     the query that has been used to compute {@code topDocs}
   * @throws IllegalArgumentException if there is evidence that {@code topDocs}
   *             have been computed against a different searcher or a different query.
   * @lucene.experimental
   */
  public static void populateScores(ScoreDoc[] topDocs, IndexSearcher searcher, Query query) throws IOException {
    // Get the score docs sorted in doc id order
    topDocs = topDocs.clone();
    Arrays.sort(topDocs, Comparator.comparingInt(scoreDoc -> scoreDoc.doc));

    final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1);
    List<LeafReaderContext> contexts = searcher.getIndexReader().leaves();
    LeafReaderContext currentContext = null;
    Scorer currentScorer = null;
    for (ScoreDoc scoreDoc : topDocs) {
      if (currentContext == null || scoreDoc.doc >= currentContext.docBase + currentContext.reader().maxDoc()) {
        Objects.checkIndex(scoreDoc.doc, searcher.getIndexReader().maxDoc());
        int newContextIndex = ReaderUtil.subIndex(scoreDoc.doc, contexts);
        currentContext = contexts.get(newContextIndex);
        final ScorerSupplier scorerSupplier = weight.scorerSupplier(currentContext);
        if (scorerSupplier == null) {
          throw new IllegalArgumentException("Doc id " + scoreDoc.doc + " doesn't match the query");
        }
        currentScorer = scorerSupplier.get(1); // random-access
      }
      final int leafDoc = scoreDoc.doc - currentContext.docBase;
      assert leafDoc >= 0;
      final int advanced = currentScorer.iterator().advance(leafDoc);
      if (leafDoc != advanced) {
        throw new IllegalArgumentException("Doc id " + scoreDoc.doc + " doesn't match the query");
      }
      scoreDoc.score = currentScorer.score();
    }
  }

  final void add(int slot, int doc) {
    bottom = pq.add(new Entry(slot, docBase + doc));
    queueFull = totalHits == numHits;
  }

  final void updateBottom(int doc) {
    // bottom.score is already set to Float.NaN in add().
    bottom.doc = docBase + doc;
    bottom = pq.updateTop();
  }

  /*
   * Only the following callback methods need to be overridden since
   * topDocs(int, int) calls them to return the results.
   */

  @Override
  protected void populateResults(ScoreDoc[] results, int howMany) {
    // avoid casting if unnecessary.
    FieldValueHitQueue<Entry> queue = (FieldValueHitQueue<Entry>) pq;
    for (int i = howMany - 1; i >= 0; i--) {
      results[i] = queue.fillFields(queue.pop());
    }
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      results = EMPTY_SCOREDOCS;
    }

    // If this is a maxScoring tracking collector and there were no results,
    return new TopFieldDocs(new TotalHits(totalHits, totalHitsRelation), results, ((FieldValueHitQueue<Entry>) pq).getFields());
  }

  @Override
  public TopFieldDocs topDocs() {
    return (TopFieldDocs) super.topDocs();
  }

  /** Return whether collection terminated early. */
  public boolean isEarlyTerminated() {
    return totalHitsRelation == Relation.GREATER_THAN_OR_EQUAL_TO;
  }
}
