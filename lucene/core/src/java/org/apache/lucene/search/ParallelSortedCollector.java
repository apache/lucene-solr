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
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldValueHitQueue.Entry;
import org.apache.lucene.search.TotalHits.Relation;

/**
 * A {@link Collector} for results sorted by field, optimized for early termination in
 * the case where the {@link Sort} matches the index and the search is executed in parallel,
 * using multiple threads.
 *
 * @lucene.experimental
 */
abstract class ParallelSortedCollector extends TopDocsCollector<Entry> {

  private static final ScoreDoc[] EMPTY_SCOREDOCS = new ScoreDoc[0];

  final int numHits;
  final Sort sort;
  final HitsThresholdChecker hitsThresholdChecker;
  final FieldComparator<?> firstComparator;

  // the current local minimum competitive score already propagated to the underlying scorer
  float minCompetitiveScore;

  // Enables global early termination with concurrent threads using minimum competitive scores and
  // collected counts of all segments
  final MaxScoreTerminator maxScoreTerminator;

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
  private ParallelSortedCollector(FieldValueHitQueue<Entry> pq, int numHits, Sort sort,
                                  HitsThresholdChecker hitsThresholdChecker, boolean needsScores,
                                  MaxScoreTerminator maxScoreTerminator) {
    super(pq);
    this.needsScores = needsScores;
    this.numHits = numHits;
    this.sort = sort;
    this.hitsThresholdChecker = hitsThresholdChecker;
    this.maxScoreTerminator = maxScoreTerminator;
    numComparators = pq.getComparators().length;
    firstComparator = pq.getComparators()[0];
    scoreMode = needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
  }

  private abstract class TopFieldLeafCollector implements LeafCollector {

    final LeafFieldComparator comparator;
    final int firstReverseMul;
    final int reverseMul;
    final LeafReaderContext context;
    final MaxScoreTerminator.LeafState leafTerminationState;

    private double score;
    Scorable scorer;

    TopFieldLeafCollector(FieldValueHitQueue<Entry> queue, LeafReaderContext context) throws IOException {
      LeafFieldComparator[] comparators = queue.getComparators(context);
      firstReverseMul = queue.reverseMul[0];
      if (comparators.length == 1) {
        this.reverseMul = queue.reverseMul[0];
        this.comparator = comparators[0];
      } else {
        this.reverseMul = 1;
        this.comparator = new MultiLeafFieldComparator(comparators, queue.reverseMul);
      }
      this.context = context;
      leafTerminationState = maxScoreTerminator.addLeafState();
    }

    void countHit() {
      ++totalHits;
      // TODO: replace hitsThresholdChecker with something simpler
      hitsThresholdChecker.incrementHitCount();
    }

    void collectHitIfCompetitive(int doc) throws IOException {
      if (reverseMul * comparator.compareBottom(doc) > 0) {
        comparator.copy(bottom.slot, doc);
        score = getComparatorValue(bottom.slot);
        //System.out.printf("leaf=%d doc=%d score=%f\n", context.ord, context.docBase + doc, score);
        updateBottom(doc);
        comparator.setBottom(bottom.slot);
      } else {
        // The comparator has no score from this document. We can use any noncompetitive value
        // to induce this leaf to be terminated.
        //System.out.printf("leaf=%d doc=%d (noncompetitive) score=%f\n", context.ord, context.docBase + doc, score);
        score = getComparatorValue(bottom.slot) + 1;
      }
    }

    void collectAnyHit(int doc, int hitsCollected) throws IOException {
      // Startup transient: queue hasn't gathered numHits yet
      int slot = hitsCollected - 1;
      // Copy hit into queue
      comparator.copy(slot, doc);
      // compute the doc's score before it gets moved by updating the priority queue
      score = getComparatorValue(slot);
      add(slot, doc);
      if (queueFull) {
        comparator.setBottom(bottom.slot);
      }
    }

    private double getComparatorValue(int slot) {
      // We could avoid this cast by genericizing MaxScoreAccumulator and having its Number type
      // co-vary with this FieldComparator; also - should DocComparator extend
      // NumericComparator<Integer>?
      return firstReverseMul * ((Number) firstComparator.value(slot)).doubleValue();
    }

    void updateTerminationState(int doc) {
      leafTerminationState.update(score, context.docBase + doc);
      if ((leafTerminationState.resultCount & maxScoreTerminator.intervalMask) == 0) {
        //System.out.println("scoreboard update leaf=" + context.ord + " doc=" + context.docBase + "+" + doc + " total=" + totalHits);
        if (maxScoreTerminator.updateLeafState(leafTerminationState)) {
          // Stop if across all segments we have collected enough, and our scores are no longer competitive
          totalHitsRelation = Relation.GREATER_THAN_OR_EQUAL_TO;
          //System.out.println("scoreboard terminate leaf " + context.ord + " doc=" + context.docBase + "+" + doc + " totalHits=" + totalHits + " score=" + (long) score);
          throw new CollectionTerminatedException();
        }
      }
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = scorer;
      comparator.setScorer(scorer);
      minCompetitiveScore = 0f;
    }
  }

  static boolean isApplicable(Sort sort, int numHits, List<LeafReaderContext> leafContexts) {
    for (LeafReaderContext leafContext : leafContexts) {
      if (TopFieldCollector.canEarlyTerminate(sort, leafContext) == false) {
        return false;
      }
    }
    return MaxScoreTerminator.isApplicable(sort, numHits);
  }

  /*
   * Implements a ParallelSortedCollector for the first page of results (after == null).
   */
  private static class TopCollector extends ParallelSortedCollector {
    final FieldValueHitQueue<Entry> queue;

    TopCollector(Sort sort, FieldValueHitQueue<Entry> queue, int numHits,
                 HitsThresholdChecker hitsThresholdChecker,
                 MaxScoreTerminator maxScoreTerminator) {
      super(queue, numHits, sort, hitsThresholdChecker, sort.needsScores(), maxScoreTerminator);
      this.queue = queue;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      docBase = context.docBase;

      return new TopFieldLeafCollector(queue, context) {

        @Override
        public void collect(int doc) throws IOException {
          countHit();
          if (queueFull) {
            collectHitIfCompetitive(doc);
          } else {
            collectAnyHit(doc, totalHits);
          }
          updateTerminationState(doc);
        }
      };
    }
  }

  /*
   * Implements a ParallelSortedCollector when after != null.
   */
  private final static class PagingCollector extends ParallelSortedCollector {

    int collectedHits;
    final FieldValueHitQueue<Entry> queue;
    final FieldDoc after;

    PagingCollector(Sort sort, FieldValueHitQueue<Entry> queue, FieldDoc after, int numHits,
                    HitsThresholdChecker hitsThresholdChecker,
                    MaxScoreTerminator maxScoreTerminator) {
      super(queue, numHits, sort, hitsThresholdChecker, sort.needsScores(), maxScoreTerminator);
      this.queue = queue;
      this.after = after;

      FieldComparator<?>[] comparators = queue.comparators;
      // Tell all comparators their top value:
      for(int i=0; i<comparators.length; i++) {
        @SuppressWarnings("unchecked")
        FieldComparator<Object> comparator = (FieldComparator<Object>) comparators[i];
        comparator.setTopValue(after.fields[i]);
      }
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      docBase = context.docBase;
      final int afterDoc = after.doc - docBase;

      return new TopFieldLeafCollector(queue, context) {

        @Override
        public void collect(int doc) throws IOException {
          countHit();
          final int topCmp = reverseMul * comparator.compareTop(doc);
          if (topCmp < 0 || (topCmp == 0 && doc > afterDoc)) {
            if (queueFull) {
              collectHitIfCompetitive(doc);
            } else {
              collectedHits++;
              collectAnyHit(doc, collectedHits);
            }
          }
          updateTerminationState(doc);
        }
      };
    }
  }

  @Override
  public ScoreMode scoreMode() {
    return scoreMode;
  }

  /**
   * Same as above with additional parameters to allow passing in the threshold checker and the max score accumulator.
   */
  static ParallelSortedCollector create(Sort sort, int numHits, FieldDoc after, HitsThresholdChecker hitsThresholdChecker,
                                        MaxScoreTerminator maxScoreTerminator) {

    if (sort.fields.length == 0) {
      throw new IllegalArgumentException("Sort must contain at least one field");
    }

    if (numHits <= 0) {
      throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }

    assert hitsThresholdChecker != null;
    assert maxScoreTerminator != null;

    FieldValueHitQueue<Entry> queue = FieldValueHitQueue.create(sort.fields, numHits);

    if (after == null) {
      return new TopCollector(sort, queue, numHits, hitsThresholdChecker, maxScoreTerminator);
    } else {
      if (after.fields == null) {
        throw new IllegalArgumentException("after.fields wasn't set; you must pass fillFields=true for the previous search");
      }

      if (after.fields.length != sort.getSort().length) {
        throw new IllegalArgumentException("after.fields has " + after.fields.length + " values but sort has " + sort.getSort().length);
      }

      return new PagingCollector(sort, queue, after, numHits, hitsThresholdChecker, maxScoreTerminator);
    }
  }

  /**
   * Create a CollectorManager which uses a shared hit counter to maintain number of hits, a shared {@link
   * MaxScoreAccumulator} to propagate the minimum score across segments if the primary sort is by relevancy, and a
   * shared {@link MaxScoreTerminator} that maintains per-collector statistics to facilitate early termination when
   * primary sort matches the index sort.
   * @param sort the criteria to sort results by; must be based on numeric fields and a prefix of the index sort
   * @param numHits how many hits to collect
   * @param after the position after which to start collecting hits, or null to collect starting with the first
   * @param totalHitsThreshold a minimum number of hits to count
   * @param numThreads how many threads will collect hits. Setting the wrong value will still
   * collect hits correctly, but may compromise performance due to thread contention (if too small),
   * or less-than-optimal early termination, if too large.
   */
  public static CollectorManager<ParallelSortedCollector, TopFieldDocs> createManager(Sort sort, int numHits, FieldDoc after,
                                                                                      int totalHitsThreshold, Integer numThreads) {
    return new CollectorManager<>() {

      private final HitsThresholdChecker hitsThresholdChecker = HitsThresholdChecker.createShared(totalHitsThreshold);
      private final MaxScoreTerminator maxScoreTerminator = new MaxScoreTerminator(numHits, totalHitsThreshold, numThreads);

      @Override
      public ParallelSortedCollector newCollector() {
        return create(sort, numHits, after, hitsThresholdChecker, maxScoreTerminator);
      }

      @Override
      public TopFieldDocs reduce(Collection<ParallelSortedCollector> collectors) {
        final TopFieldDocs[] topDocs = new TopFieldDocs[collectors.size()];
        int i = 0;
        for (ParallelSortedCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(sort, 0, numHits, topDocs);
      }
    };
  }

  private void add(int slot, int doc) {
    bottom = pq.add(new Entry(slot, docBase + doc));
    // The queue is full either when totalHits == numHits (in SimpleFieldCollector), in which case
    // slot = totalHits - 1, or when hitsCollected == numHits (in PagingFieldCollector this is hits
    // on the current page) and slot = hitsCollected - 1.
    assert slot < numHits;
    queueFull = slot == numHits - 1;
  }

  private void updateBottom(int doc) {
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

  /**
   * <p>MaxScoreTerminator is used by TopFieldCollector when the query sort is a prefix of the index sort (in which case
   * we can apply early termination), multiple threads are used for collection, and <code>numHits</code> is relatively
   * high. It is notified periodically by leaf collectors calling {@link #updateLeafState} with their worst (ie maximum)
   * score and how many hits they have collected.  When enough hits are collected, MaxScoreTerminator notifies
   * noncompetitive leaf collectors when they can stop (early terminate) by returning true from {@link
   * #updateLeafState}.</p>
   *
   * <p>Used by TopFieldCollector to orchestrate early termination (when query sort matches index sort) based on the worst
   * competitive score across all leaf collectors. Once we have globally collected the number of hits required to satisfy
   * the query (<code>collectionThreshold</code>, below, typically <code>max(numHits, 1000)</code>) then the worst
   * collected score across threads is a global lower bound on the score that must be met by any hit (we already have
   * sufficient hits to satisfy the query with score better than that, so any later hit with a worse score will be
   * discarded, and any Collector retrieving such a hit can be terminated). Further, once a collector terminates, the same
   * logic applies to the remaining collectors, which can result in raising the bound further. The termination bound used
   * is the minimum minimum score among the top collectors (ranked by their minimum scores) that together have at least
   * <code>collectionThreshold</code> hits.</p>
   *
   * <p>MaxScoreTerminator implements this termination strategy by tracking the worst score, and number of hits collected,
   * of each LeafCollector in {@link LeafState} objects that are shared with the leaf collectors, and updated by the
   * collectors asynchronously. The leaf collectors check in periodically (every {@link #interval}th hit collected), at
   * which time the leaf states are sorted, the termination bound is updated, and the collector is notified whether it can
   * terminate. The interval must be a power of 2 (since a mask is used to check the period), and should be set as low as
   * possible while avoiding thread contention on this synchronized method. A good heuristic seems to be the least power
   * of 2 greater than than the number of threads in use.</p>
   */
  static class MaxScoreTerminator {

    // This strategy performs best for higher N. The threshold below was determined empirically. The best tradeoff is
    // probably related to number of threads used by the searcher and the overall counting threshold.
    private static final int TERMINATION_STRATEGY_HIT_THRESHOLD = 50;

    // By default we use 2^5-1 to check the remainder with a bitwise operation, but for best performance
    // the actual value should always be set by calling setIntervalBits()
    private static final int DEFAULT_INTERVAL_BITS = 5;
    private int intervalMask;
    int interval;

    /** The worst score for each leaf */
    private final List<LeafState> leafStates = new ArrayList<>();
    private final List<LeafState> scratch = new ArrayList<>();

    /** How many hits were requested: Collector's numHits. */
    private final int numHits;

    /** the worst hit over all */
    private final LeafState thresholdState;

    /** The total number of docs to collect: the max of the Collector's numHits and its early termination threshold. */
    final int totalToCollect;

    /** An upper bound on the number of docs "excluded" from max-score accounting due to early termination. */
    private int numExcludedBound;

    /** A lower bound on the total hits collected by all leaves */
    private volatile int totalCollected;

    /**
     * @param numHits the number of hits to be returned
     * @param collectionThreshold collect at least this many, even if numHits is less, for the sake of counting
     * @param numThreads the number of threads that will be updating this concurrently. Controls the update interval.
     *                   If null, an internal default is used.
     */
    MaxScoreTerminator(int numHits, int collectionThreshold, Integer numThreads) {
      this.numHits = numHits;
      this.totalToCollect = Math.max(numHits, collectionThreshold);
      thresholdState = new LeafState();
      thresholdState.score = Double.MAX_VALUE;
      thresholdState.docid = Integer.MAX_VALUE;
      if (numThreads != null) {
        int numThreadsLog2 = 31 - Integer.numberOfLeadingZeros(numThreads);
        setIntervalBits(numThreadsLog2 + 1);
      } else {
        setIntervalBits(DEFAULT_INTERVAL_BITS);
      }
    }

    /**
     * @param sort the query sort
     * @param numHits the number of hits requested
     * @return whether the Sort is compatible with early termination using {@link MaxScoreTerminator} and the number of hits
     * requested is large enough to make this optimization likely to help.
     */
    static boolean isApplicable(Sort sort, int numHits) {
      return numHits > TERMINATION_STRATEGY_HIT_THRESHOLD && sortIsNumeric(sort);
    }

    private static boolean sortIsNumeric(Sort sort) {
      switch (sort.getSort()[0].getType()) {
        case DOC:
        case INT:
        case FLOAT:
        case LONG:
        case DOUBLE:
          return true;
        default:
          return false;
      }
    }

    synchronized LeafState addLeafState() {
      LeafState newLeafState = new LeafState();
      newLeafState.index = leafStates.size();
      leafStates.add(newLeafState);
      scratch.add(new LeafState());
      return newLeafState;
    }

    /**
     * Set the update interval to 2^bitCount and the intervalMask to 2^bitCount-1. This controls the
     * rate at which multiple threads report their worst scores. For best performance this should be
     * set to the nearest power of 2 &gt; the number of expected calling threads.
     * @param bitCount the number of bits in the interval/intervalMask
     */
    void setIntervalBits(int bitCount) {
      interval = 1 << bitCount;
      intervalMask = interval - 1;
    }

    /**
     * Must be called by leaf collectors on every interval hits to update their progress; they should
     * call when ((leafCollector.numCollected &amp; this.intervalMask) == 0).
     * @param newLeafState the leaf collector's current lowest score
     * @return whether the collector should terminate
     */
    synchronized boolean updateLeafState(LeafState newLeafState) {
      if (newLeafState.isActive()) {
        // Only count hits from this leaf if it is still active: otherwise its hits are not competitive and do not count
        // towards meeting the threshold for terminating the remaining leaves
        totalCollected += interval;
      }
      // (1) Until we collect totalToCollect we can't terminate anything.
      // (2) after that, any single leaf that has collected numHits can be terminated,
      // (3) and we may be able to remove the worst leaves even if they have not yet collected numHits, as long as the
      // remaining leaves have collected totalToCollect.
      if (totalCollected >= totalToCollect) {
        if (newLeafState.resultCount >= numHits) {
          // This leaf has collected enough hits all on its own
          return true;
        }
        // We may be ready to terminate some leaves (although they may not include the one that just called
        // updateLeafState(), such leaves will eventually be terminated here).
        excludeSuperfluousLeaves();
        if (newLeafState.compareTo(thresholdState) <= 0) {
          // Tell the current leaf collector to terminate since it can no longer contribute any top hits
          //System.out.println("  terminate " + newLeafState + " b/c it is > " + thresholdState);
          return true;
        }
      }
      return false;
    }

    private void excludeSuperfluousLeaves() {
      if (leafStates.size() > 1 && totalCollected >= numHits + numExcludedBound) {
        // Copy the current states since they are being updated concurrently and we need stable values
        PriorityQueue<LeafState> queue = new PriorityQueue<>();
        for (LeafState leafState : leafStates) {
          if (leafState.isActive()) {
            LeafState copy = scratch.get(leafState.index);
            copy.updateFrom(leafState);
            queue.add(copy);
          }
        }
        if (queue.size() < 2) {
          return;
        }
        LeafState worstLeafState;
        do {
          worstLeafState = queue.remove();
          //System.out.println(" exclude " + worstLeafState + " because " + totalCollected + " >= " + numHits + "+" + numExcludedBound);
          // We don't need the worst leaf in order to get enough results, so remember how many results
          // (upper bound) it accounted for, setting a new threshold for hits collected
          numExcludedBound += worstLeafState.resultCount;
          leafStates.get(worstLeafState.index).deactivate();
        } while (queue.size() > 1 && totalCollected >= numHits + numExcludedBound);
        // And update the score threshold if the worst leaf remaining after exclusion has a lower max score
        if (worstLeafState.compareTo(thresholdState) > 0) {
          //System.out.println("  new threshold: " + worstLeafState);
          thresholdState.updateFrom(worstLeafState);
        }
      }
    }

    /**
     * For tracking the worst scoring hit and total number of hits collected by each leaf
     */
    class LeafState implements Comparable<LeafState> {

      int docid = -1;             // the global docid
      double score = -Double.MAX_VALUE;
      int resultCount;
      int index;
      boolean active = true;

      void update(double score, int docid) {
        // scores are nondecreasing
        assert score > this.score || (score == this.score && docid >= this.docid) :
          "descending (score,docid): (" + score + "," + docid + ") < (" + this.score + "," + this.docid + ")";
        this.score = score;
        this.docid = docid;
        resultCount += 1;
      }

      void updateFrom(LeafState other) {
        this.score = other.score;
        this.docid = other.docid;
        this.resultCount = other.resultCount;
        this.index = other.index;
      }

      boolean isActive() {
        return active;
      }

      void deactivate() {
        active = false;
      }

      @Override
      public int compareTo(LeafState o) {
        int cmp = Double.compare(score, o.score);
        if (cmp == 0) {
          // maybe we do not need this since we only terminate when scores become strictly worse (not >=)
          cmp = Integer.compare(docid, o.docid);
        }
        return -cmp;
      }

      @Override
      public String toString() {
        return "LeafState<" + index + " docid=" + docid + " score=" + score + ", count=" + resultCount + ">";
      }

    }

  }
}
