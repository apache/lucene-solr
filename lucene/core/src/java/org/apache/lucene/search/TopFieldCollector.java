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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.util.PriorityQueue;

/**
 * A {@link Collector} that sorts by {@link SortField} using
 * {@link FieldComparator}s.
 * <p>
 * See the {@link #create(org.apache.lucene.search.Sort, int, boolean, boolean, boolean, boolean)} method
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
    final boolean mayNeedScoresTwice;
    Scorer scorer;

    MultiComparatorLeafCollector(LeafFieldComparator[] comparators, int[] reverseMul, boolean mayNeedScoresTwice) {
      if (comparators.length == 1) {
        this.reverseMul = reverseMul[0];
        this.comparator = comparators[0];
      } else {
        this.reverseMul = 1;
        this.comparator = new MultiLeafFieldComparator(comparators, reverseMul);
      }
      this.mayNeedScoresTwice = mayNeedScoresTwice;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      if (mayNeedScoresTwice && scorer instanceof ScoreCachingWrappingScorer == false) {
        scorer = new ScoreCachingWrappingScorer(scorer);
      }
      comparator.setScorer(scorer);
      this.scorer = scorer;
    }
  }

  static boolean canEarlyTerminate(Sort searchSort, Sort indexSort) {
    final SortField[] fields1 = searchSort.getSort();
    final SortField[] fields2 = indexSort.getSort();
    // early termination is possible if fields1 is a prefix of fields2
    if (fields1.length > fields2.length) {
      return false;
    }
    return Arrays.asList(fields1).equals(Arrays.asList(fields2).subList(0, fields1.length));
  }

  static int estimateRemainingHits(int hitCount, int doc, int maxDoc) {
    double hitRatio = (double) hitCount / (doc + 1);
    int remainingDocs = maxDoc - doc - 1;
    int remainingHits = (int) (remainingDocs * hitRatio);
    return remainingHits;
  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, with tracking
   * document scores and maxScore.
   */
  private static class SimpleFieldCollector extends TopFieldCollector {

    final Sort sort;
    final FieldValueHitQueue<Entry> queue;
    final boolean trackDocScores;
    final boolean trackMaxScore;
    final boolean mayNeedScoresTwice;
    final boolean trackTotalHits;

    public SimpleFieldCollector(Sort sort, FieldValueHitQueue<Entry> queue, int numHits, boolean fillFields,
        boolean trackDocScores, boolean trackMaxScore, boolean trackTotalHits) {
      super(queue, numHits, fillFields, sort.needsScores() || trackDocScores || trackMaxScore);
      this.sort = sort;
      this.queue = queue;
      if (trackMaxScore) {
        maxScore = Float.NEGATIVE_INFINITY; // otherwise we would keep NaN
      }
      this.trackDocScores = trackDocScores;
      this.trackMaxScore = trackMaxScore;
      // If one of the sort fields needs scores, and if we also track scores, then
      // we might call scorer.score() several times per doc so wrapping the scorer
      // to cache scores would help
      this.mayNeedScoresTwice = sort.needsScores() && (trackDocScores || trackMaxScore);
      this.trackTotalHits = trackTotalHits;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      docBase = context.docBase;

      final LeafFieldComparator[] comparators = queue.getComparators(context);
      final int[] reverseMul = queue.getReverseMul();
      final Sort indexSort = context.reader().getMetaData().getSort();
      final boolean canEarlyTerminate = trackTotalHits == false &&
          trackMaxScore == false &&
          indexSort != null &&
          canEarlyTerminate(sort, indexSort);
      final int initialTotalHits = totalHits;

      return new MultiComparatorLeafCollector(comparators, reverseMul, mayNeedScoresTwice) {

        @Override
        public void collect(int doc) throws IOException {
          float score = Float.NaN;
          if (trackMaxScore) {
            score = scorer.score();
            if (score > maxScore) {
              maxScore = score;
            }
          }

          ++totalHits;
          if (queueFull) {
            if (reverseMul * comparator.compareBottom(doc) <= 0) {
              // since docs are visited in doc Id order, if compare is 0, it means
              // this document is largest than anything else in the queue, and
              // therefore not competitive.
              if (canEarlyTerminate) {
                // scale totalHits linearly based on the number of docs
                // and terminate collection
                totalHits += estimateRemainingHits(totalHits - initialTotalHits, doc, context.reader().maxDoc());
                earlyTerminated = true;
                throw new CollectionTerminatedException();
              } else {
                // just move to the next doc
                return;
              }
            }

            if (trackDocScores && !trackMaxScore) {
              score = scorer.score();
            }

            // This hit is competitive - replace bottom element in queue & adjustTop
            comparator.copy(bottom.slot, doc);
            updateBottom(doc, score);
            comparator.setBottom(bottom.slot);
          } else {
            // Startup transient: queue hasn't gathered numHits yet
            final int slot = totalHits - 1;

            if (trackDocScores && !trackMaxScore) {
              score = scorer.score();
            }

            // Copy hit into queue
            comparator.copy(slot, doc);
            add(slot, doc, score);
            if (queueFull) {
              comparator.setBottom(bottom.slot);
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
    final boolean trackDocScores;
    final boolean trackMaxScore;
    final FieldDoc after;
    final boolean mayNeedScoresTwice;
    final boolean trackTotalHits;

    public PagingFieldCollector(Sort sort, FieldValueHitQueue<Entry> queue, FieldDoc after, int numHits, boolean fillFields,
                                boolean trackDocScores, boolean trackMaxScore, boolean trackTotalHits) {
      super(queue, numHits, fillFields, trackDocScores || trackMaxScore || sort.needsScores());
      this.sort = sort;
      this.queue = queue;
      this.trackDocScores = trackDocScores;
      this.trackMaxScore = trackMaxScore;
      this.after = after;
      this.mayNeedScoresTwice = sort.needsScores() && (trackDocScores || trackMaxScore);
      this.trackTotalHits = trackTotalHits;

      // Must set maxScore to NEG_INF, or otherwise Math.max always returns NaN.
      if (trackMaxScore) {
        maxScore = Float.NEGATIVE_INFINITY;
      }

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
      final boolean canEarlyTerminate = trackTotalHits == false &&
          trackMaxScore == false &&
          indexSort != null &&
          canEarlyTerminate(sort, indexSort);
      final int initialTotalHits = totalHits;
      return new MultiComparatorLeafCollector(queue.getComparators(context), queue.getReverseMul(), mayNeedScoresTwice) {

        @Override
        public void collect(int doc) throws IOException {
          //System.out.println("  collect doc=" + doc);

          totalHits++;

          float score = Float.NaN;
          if (trackMaxScore) {
            score = scorer.score();
            if (score > maxScore) {
              maxScore = score;
            }
          }

          if (queueFull) {
            // Fastmatch: return if this hit is no better than
            // the worst hit currently in the queue:
            final int cmp = reverseMul * comparator.compareBottom(doc);
            if (cmp <= 0) {
              // not competitive since documents are visited in doc id order
              if (canEarlyTerminate) {
                // scale totalHits linearly based on the number of docs
                // and terminate collection
                totalHits += estimateRemainingHits(totalHits - initialTotalHits, doc, context.reader().maxDoc());
                earlyTerminated = true;
                throw new CollectionTerminatedException();
              } else {
                // just move to the next doc
                return;
              }
            }
          }

          final int topCmp = reverseMul * comparator.compareTop(doc);
          if (topCmp > 0 || (topCmp == 0 && doc <= afterDoc)) {
            // Already collected on a previous page
            return;
          }

          if (queueFull) {
            // This hit is competitive - replace bottom element in queue & adjustTop
            comparator.copy(bottom.slot, doc);

            // Compute score only if it is competitive.
            if (trackDocScores && !trackMaxScore) {
              score = scorer.score();
            }
            updateBottom(doc, score);

            comparator.setBottom(bottom.slot);
          } else {
            collectedHits++;

            // Startup transient: queue hasn't gathered numHits yet
            final int slot = collectedHits - 1;
            //System.out.println("    slot=" + slot);
            // Copy hit into queue
            comparator.copy(slot, doc);

            // Compute score only if it is competitive.
            if (trackDocScores && !trackMaxScore) {
              score = scorer.score();
            }
            bottom = pq.add(new Entry(slot, docBase + doc, score));
            queueFull = collectedHits == numHits;
            if (queueFull) {
              comparator.setBottom(bottom.slot);
            }
          }
        }
      };
    }

  }

  private static final ScoreDoc[] EMPTY_SCOREDOCS = new ScoreDoc[0];

  private final boolean fillFields;

  /*
   * Stores the maximum score value encountered, needed for normalizing. If
   * document scores are not tracked, this value is initialized to NaN.
   */
  float maxScore = Float.NaN;

  final int numHits;
  FieldValueHitQueue.Entry bottom = null;
  boolean queueFull;
  int docBase;
  boolean earlyTerminated = false;
  final boolean needsScores;

  // Declaring the constructor private prevents extending this class by anyone
  // else. Note that the class cannot be final since it's extended by the
  // internal versions. If someone will define a constructor with any other
  // visibility, then anyone will be able to extend the class, which is not what
  // we want.
  private TopFieldCollector(PriorityQueue<Entry> pq, int numHits, boolean fillFields, boolean needsScores) {
    super(pq);
    this.needsScores = needsScores;
    this.numHits = numHits;
    this.fillFields = fillFields;
  }

  @Override
  public boolean needsScores() {
    return needsScores;
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
   * @param fillFields
   *          specifies whether the actual field values should be returned on
   *          the results (FieldDoc).
   * @param trackDocScores
   *          specifies whether document scores should be tracked and set on the
   *          results. Note that if set to false, then the results' scores will
   *          be set to Float.NaN. Setting this to true affects performance, as
   *          it incurs the score computation on each competitive result.
   *          Therefore if document scores are not required by the application,
   *          it is recommended to set it to false.
   * @param trackMaxScore
   *          specifies whether the query's maxScore should be tracked and set
   *          on the resulting {@link TopDocs}. Note that if set to false,
   *          {@link TopDocs#getMaxScore()} returns Float.NaN. Setting this to
   *          true affects performance as it incurs the score computation on
   *          each result. Also, setting this true automatically sets
   *          <code>trackDocScores</code> to true as well.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   * @deprecated Use {@link #create(Sort, int, boolean, boolean, boolean, boolean)}
   */
  @Deprecated
  public static TopFieldCollector create(Sort sort, int numHits,
      boolean fillFields, boolean trackDocScores, boolean trackMaxScore) {
    return create(sort, numHits, fillFields, trackDocScores, trackMaxScore, true);
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
   * @param fillFields
   *          specifies whether the actual field values should be returned on
   *          the results (FieldDoc).
   * @param trackDocScores
   *          specifies whether document scores should be tracked and set on the
   *          results. Note that if set to false, then the results' scores will
   *          be set to Float.NaN. Setting this to true affects performance, as
   *          it incurs the score computation on each competitive result.
   *          Therefore if document scores are not required by the application,
   *          it is recommended to set it to false.
   * @param trackMaxScore
   *          specifies whether the query's maxScore should be tracked and set
   *          on the resulting {@link TopDocs}. Note that if set to false,
   *          {@link TopDocs#getMaxScore()} returns Float.NaN. Setting this to
   *          true affects performance as it incurs the score computation on
   *          each result. Also, setting this true automatically sets
   *          <code>trackDocScores</code> to true as well.
   * @param trackTotalHits
   *          specifies whether the total number of hits should be tracked. If
   *          set to false, the value of {@link TopFieldDocs#totalHits} will be
   *          approximated.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   */
  public static TopFieldCollector create(Sort sort, int numHits,
      boolean fillFields, boolean trackDocScores, boolean trackMaxScore, boolean trackTotalHits) {
    return create(sort, numHits, null, fillFields, trackDocScores, trackMaxScore, trackTotalHits);
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
   * @param fillFields
   *          specifies whether the actual field values should be returned on
   *          the results (FieldDoc).
   * @param trackDocScores
   *          specifies whether document scores should be tracked and set on the
   *          results. Note that if set to false, then the results' scores will
   *          be set to Float.NaN. Setting this to true affects performance, as
   *          it incurs the score computation on each competitive result.
   *          Therefore if document scores are not required by the application,
   *          it is recommended to set it to false.
   * @param trackMaxScore
   *          specifies whether the query's maxScore should be tracked and set
   *          on the resulting {@link TopDocs}. Note that if set to false,
   *          {@link TopDocs#getMaxScore()} returns Float.NaN. Setting this to
   *          true affects performance as it incurs the score computation on
   *          each result. Also, setting this true automatically sets
   *          <code>trackDocScores</code> to true as well.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   * @deprecated Use {@link #create(Sort, int, FieldDoc, boolean, boolean, boolean, boolean)}
   */
  @Deprecated
  public static TopFieldCollector create(Sort sort, int numHits, FieldDoc after,
      boolean fillFields, boolean trackDocScores, boolean trackMaxScore) {
    return create(sort, numHits, after, fillFields, trackDocScores, trackMaxScore, true);
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
   * @param fillFields
   *          specifies whether the actual field values should be returned on
   *          the results (FieldDoc).
   * @param trackDocScores
   *          specifies whether document scores should be tracked and set on the
   *          results. Note that if set to false, then the results' scores will
   *          be set to Float.NaN. Setting this to true affects performance, as
   *          it incurs the score computation on each competitive result.
   *          Therefore if document scores are not required by the application,
   *          it is recommended to set it to false.
   * @param trackMaxScore
   *          specifies whether the query's maxScore should be tracked and set
   *          on the resulting {@link TopDocs}. Note that if set to false,
   *          {@link TopDocs#getMaxScore()} returns Float.NaN. Setting this to
   *          true affects performance as it incurs the score computation on
   *          each result. Also, setting this true automatically sets
   *          <code>trackDocScores</code> to true as well.
   * @param trackTotalHits
   *          specifies whether the total number of hits should be tracked. If
   *          set to false, the value of {@link TopFieldDocs#totalHits} will be
   *          approximated.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   */
  public static TopFieldCollector create(Sort sort, int numHits, FieldDoc after,
      boolean fillFields, boolean trackDocScores, boolean trackMaxScore, boolean trackTotalHits) {

    if (sort.fields.length == 0) {
      throw new IllegalArgumentException("Sort must contain at least one field");
    }

    if (numHits <= 0) {
      throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }

    FieldValueHitQueue<Entry> queue = FieldValueHitQueue.create(sort.fields, numHits);

    if (after == null) {
      return new SimpleFieldCollector(sort, queue, numHits, fillFields, trackDocScores, trackMaxScore, trackTotalHits);
    } else {
      if (after.fields == null) {
        throw new IllegalArgumentException("after.fields wasn't set; you must pass fillFields=true for the previous search");
      }

      if (after.fields.length != sort.getSort().length) {
        throw new IllegalArgumentException("after.fields has " + after.fields.length + " values but sort has " + sort.getSort().length);
      }

      return new PagingFieldCollector(sort, queue, after, numHits, fillFields, trackDocScores, trackMaxScore, trackTotalHits);
    }
  }

  final void add(int slot, int doc, float score) {
    bottom = pq.add(new Entry(slot, docBase + doc, score));
    queueFull = totalHits == numHits;
  }

  final void updateBottom(int doc) {
    // bottom.score is already set to Float.NaN in add().
    bottom.doc = docBase + doc;
    bottom = pq.updateTop();
  }

  final void updateBottom(int doc, float score) {
    bottom.doc = docBase + doc;
    bottom.score = score;
    bottom = pq.updateTop();
  }

  /*
   * Only the following callback methods need to be overridden since
   * topDocs(int, int) calls them to return the results.
   */

  @Override
  protected void populateResults(ScoreDoc[] results, int howMany) {
    if (fillFields) {
      // avoid casting if unnecessary.
      FieldValueHitQueue<Entry> queue = (FieldValueHitQueue<Entry>) pq;
      for (int i = howMany - 1; i >= 0; i--) {
        results[i] = queue.fillFields(queue.pop());
      }
    } else {
      for (int i = howMany - 1; i >= 0; i--) {
        Entry entry = pq.pop();
        results[i] = new FieldDoc(entry.doc, entry.score);
      }
    }
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      results = EMPTY_SCOREDOCS;
      // Set maxScore to NaN, in case this is a maxScore tracking collector.
      maxScore = Float.NaN;
    }

    // If this is a maxScoring tracking collector and there were no results,
    return new TopFieldDocs(totalHits, results, ((FieldValueHitQueue<Entry>) pq).getFields(), maxScore);
  }

  @Override
  public TopFieldDocs topDocs() {
    return (TopFieldDocs) super.topDocs();
  }

  /** Return whether collection terminated early. */
  public boolean isEarlyTerminated() {
    return earlyTerminated;
  }
}
