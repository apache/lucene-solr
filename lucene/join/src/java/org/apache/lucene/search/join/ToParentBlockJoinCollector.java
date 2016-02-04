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
package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldValueHitQueue;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Scorer.ChildScorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.ArrayUtil;


/** Collects parent document hits for a Query containing one more more
 *  BlockJoinQuery clauses, sorted by the
 *  specified parent Sort.  Note that this cannot perform
 *  arbitrary joins; rather, it requires that all joined
 *  documents are indexed as a doc block (using {@link
 *  IndexWriter#addDocuments} or {@link
 *  IndexWriter#updateDocuments}).  Ie, the join is computed
 *  at index time.
 *
 *  <p>This collector MUST be used with {@link ToParentBlockJoinIndexSearcher},
 *  in order to work correctly.
 *
 *  <p>The parent Sort must only use
 *  fields from the parent documents; sorting by field in
 *  the child documents is not supported.</p>
 *
 *  <p>You should only use this
 *  collector if one or more of the clauses in the query is
 *  a {@link ToParentBlockJoinQuery}.  This collector will find those query
 *  clauses and record the matching child documents for the
 *  top scoring parent documents.</p>
 *
 *  <p>Multiple joins (star join) and nested joins and a mix
 *  of the two are allowed, as long as in all cases the
 *  documents corresponding to a single row of each joined
 *  parent table were indexed as a doc block.</p>
 *
 *  <p>For the simple star join you can retrieve the
 *  {@link TopGroups} instance containing each {@link ToParentBlockJoinQuery}'s
 *  matching child documents for the top parent groups,
 *  using {@link #getTopGroups}.  Ie,
 *  a single query, which will contain two or more
 *  {@link ToParentBlockJoinQuery}'s as clauses representing the star join,
 *  can then retrieve two or more {@link TopGroups} instances.</p>
 *
 *  <p>For nested joins, the query will run correctly (ie,
 *  match the right parent and child documents), however,
 *  because TopGroups is currently unable to support nesting
 *  (each group is not able to hold another TopGroups), you
 *  are only able to retrieve the TopGroups of the first
 *  join.  The TopGroups of the nested joins will not be
 *  correct.
 *
 *  See {@link org.apache.lucene.search.join} for a code
 *  sample.
 *
 * @lucene.experimental
 */
public class ToParentBlockJoinCollector implements Collector {

  private final Sort sort;

  // Maps each BlockJoinQuery instance to its "slot" in
  // joinScorers and in OneGroup's cached doc/scores/count:
  private final Map<Query,Integer> joinQueryID = new HashMap<>();
  private final int numParentHits;
  private final FieldValueHitQueue<OneGroup> queue;
  private final FieldComparator<?>[] comparators;
  private final boolean trackMaxScore;
  private final boolean trackScores;

  private ToParentBlockJoinQuery.BlockJoinScorer[] joinScorers = new ToParentBlockJoinQuery.BlockJoinScorer[0];
  private boolean queueFull;

  private OneGroup bottom;
  private int totalHitCount;
  private float maxScore = Float.NaN;

  /**  Creates a ToParentBlockJoinCollector.  The provided sort must
   *  not be null.  If you pass true trackScores, all
   *  ToParentBlockQuery instances must not use
   *  ScoreMode.None. */
  public ToParentBlockJoinCollector(Sort sort, int numParentHits, boolean trackScores, boolean trackMaxScore) throws IOException {
    // TODO: allow null sort to be specialized to relevance
    // only collector
    this.sort = sort;
    this.trackMaxScore = trackMaxScore;
    if (trackMaxScore) {
      maxScore = Float.MIN_VALUE;
    }
    //System.out.println("numParentHits=" + numParentHits);
    this.trackScores = trackScores;
    this.numParentHits = numParentHits;
    queue = FieldValueHitQueue.create(sort.getSort(), numParentHits);
    comparators = queue.getComparators();
  }
  
  private static final class OneGroup extends FieldValueHitQueue.Entry {
    public OneGroup(int comparatorSlot, int parentDoc, float parentScore, int numJoins, boolean doScores) {
      super(comparatorSlot, parentDoc, parentScore);
      //System.out.println("make OneGroup parentDoc=" + parentDoc);
      docs = new int[numJoins][];
      for(int joinID=0;joinID<numJoins;joinID++) {
        docs[joinID] = new int[5];
      }
      if (doScores) {
        scores = new float[numJoins][];
        for(int joinID=0;joinID<numJoins;joinID++) {
          scores[joinID] = new float[5];
        }
      }
      counts = new int[numJoins];
    }
    LeafReaderContext readerContext;
    int[][] docs;
    float[][] scores;
    int[] counts;
  }

  @Override
  public LeafCollector getLeafCollector(final LeafReaderContext context)
      throws IOException {
    final LeafFieldComparator[] comparators = queue.getComparators(context);
    final int[] reverseMul = queue.getReverseMul();
    final int docBase = context.docBase;
    return new LeafCollector() {

      private Scorer scorer;

      @Override
      public void setScorer(Scorer scorer) throws IOException {
        //System.out.println("C.setScorer scorer=" + scorer);
        // Since we invoke .score(), and the comparators likely
        // do as well, cache it so it's only "really" computed
        // once:
        if (scorer instanceof ScoreCachingWrappingScorer == false) {
          scorer = new ScoreCachingWrappingScorer(scorer);
        }
        this.scorer = scorer;
        for (LeafFieldComparator comparator : comparators) {
          comparator.setScorer(scorer);
        }
        Arrays.fill(joinScorers, null);

        Queue<Scorer> queue = new LinkedList<>();
        //System.out.println("\nqueue: add top scorer=" + scorer);
        queue.add(scorer);
        while ((scorer = queue.poll()) != null) {
          //System.out.println("  poll: " + scorer + "; " + scorer.getWeight().getQuery());
          if (scorer instanceof ToParentBlockJoinQuery.BlockJoinScorer) {
            enroll((ToParentBlockJoinQuery) scorer.getWeight().getQuery(), (ToParentBlockJoinQuery.BlockJoinScorer) scorer);
          }

          for (ChildScorer sub : scorer.getChildren()) {
            //System.out.println("  add sub: " + sub.child + "; " + sub.child.getWeight().getQuery());
            queue.add(sub.child);
          }
        }
      }
      
      @Override
      public void collect(int parentDoc) throws IOException {
      //System.out.println("\nC parentDoc=" + parentDoc);
        totalHitCount++;

        float score = Float.NaN;

        if (trackMaxScore) {
          score = scorer.score();
          maxScore = Math.max(maxScore, score);
        }

        // TODO: we could sweep all joinScorers here and
        // aggregate total child hit count, so we can fill this
        // in getTopGroups (we wire it to 0 now)

        if (queueFull) {
          //System.out.println("  queueFull");
          // Fastmatch: return if this hit is not competitive
          int c = 0;
          for (int i = 0; i < comparators.length; ++i) {
            c = reverseMul[i] * comparators[i].compareBottom(parentDoc);
            if (c != 0) {
              break;
            }
          }
          if (c <= 0) { // in case of equality, this hit is not competitive as docs are visited in order
            // Definitely not competitive.
            //System.out.println("    skip");
            return;
          }

          //System.out.println("    competes!  doc=" + (docBase + parentDoc));

          // This hit is competitive - replace bottom element in queue & adjustTop
          for (LeafFieldComparator comparator : comparators) {
            comparator.copy(bottom.slot, parentDoc);
          }
          if (!trackMaxScore && trackScores) {
            score = scorer.score();
          }
          bottom.doc = docBase + parentDoc;
          bottom.readerContext = context;
          bottom.score = score;
          copyGroups(bottom);
          bottom = queue.updateTop();

          for (LeafFieldComparator comparator : comparators) {
            comparator.setBottom(bottom.slot);
          }
        } else {
          // Startup transient: queue is not yet full:
          final int comparatorSlot = totalHitCount - 1;

          // Copy hit into queue
          for (LeafFieldComparator comparator : comparators) {
            comparator.copy(comparatorSlot, parentDoc);
          }
          //System.out.println("  startup: new OG doc=" + (docBase+parentDoc));
          if (!trackMaxScore && trackScores) {
            score = scorer.score();
          }
          final OneGroup og = new OneGroup(comparatorSlot, docBase+parentDoc, score, joinScorers.length, trackScores);
          og.readerContext = context;
          copyGroups(og);
          bottom = queue.add(og);
          queueFull = totalHitCount == numParentHits;
          if (queueFull) {
            // End of startup transient: queue just filled up:
            for (LeafFieldComparator comparator : comparators) {
              comparator.setBottom(bottom.slot);
            }
          }
        }
      }
      
      // Pulls out child doc and scores for all join queries:
      private void copyGroups(OneGroup og) {
        // While rare, it's possible top arrays could be too
        // short if join query had null scorer on first
        // segment(s) but then became non-null on later segments
        final int numSubScorers = joinScorers.length;
        if (og.docs.length < numSubScorers) {
          // While rare, this could happen if join query had
          // null scorer on first segment(s) but then became
          // non-null on later segments
          og.docs = ArrayUtil.grow(og.docs);
        }
        if (og.counts.length < numSubScorers) {
          og.counts = ArrayUtil.grow(og.counts);
        }
        if (trackScores && og.scores.length < numSubScorers) {
          og.scores = ArrayUtil.grow(og.scores);
        }

        //System.out.println("\ncopyGroups parentDoc=" + og.doc);
        for(int scorerIDX = 0;scorerIDX < numSubScorers;scorerIDX++) {
          final ToParentBlockJoinQuery.BlockJoinScorer joinScorer = joinScorers[scorerIDX];
          //System.out.println("  scorer=" + joinScorer);
          if (joinScorer != null && docBase + joinScorer.getParentDoc() == og.doc) {
            og.counts[scorerIDX] = joinScorer.getChildCount();
            //System.out.println("    count=" + og.counts[scorerIDX]);
            og.docs[scorerIDX] = joinScorer.swapChildDocs(og.docs[scorerIDX]);
            assert og.docs[scorerIDX].length >= og.counts[scorerIDX]: "length=" + og.docs[scorerIDX].length + " vs count=" + og.counts[scorerIDX];
            //System.out.println("    len=" + og.docs[scorerIDX].length);
            /*
              for(int idx=0;idx<og.counts[scorerIDX];idx++) {
              System.out.println("    docs[" + idx + "]=" + og.docs[scorerIDX][idx]);
              }
            */
            if (trackScores) {
              //System.out.println("    copy scores");
              og.scores[scorerIDX] = joinScorer.swapChildScores(og.scores[scorerIDX]);
              assert og.scores[scorerIDX].length >= og.counts[scorerIDX]: "length=" + og.scores[scorerIDX].length + " vs count=" + og.counts[scorerIDX];
            }
          } else {
            og.counts[scorerIDX] = 0;
          }
        }
      }
    };
  }

  private void enroll(ToParentBlockJoinQuery query, ToParentBlockJoinQuery.BlockJoinScorer scorer) {
    scorer.trackPendingChildHits();
    final Integer slot = joinQueryID.get(query);
    if (slot == null) {
      joinQueryID.put(query, joinScorers.length);
      //System.out.println("found JQ: " + query + " slot=" + joinScorers.length);
      final ToParentBlockJoinQuery.BlockJoinScorer[] newArray = new ToParentBlockJoinQuery.BlockJoinScorer[1+joinScorers.length];
      System.arraycopy(joinScorers, 0, newArray, 0, joinScorers.length);
      joinScorers = newArray;
      joinScorers[joinScorers.length-1] = scorer;
    } else {
      joinScorers[slot] = scorer;
    }
  }

  private OneGroup[] sortedGroups;

  private void sortQueue() {
    sortedGroups = new OneGroup[queue.size()];
    for(int downTo=queue.size()-1;downTo>=0;downTo--) {
      sortedGroups[downTo] = queue.pop();
    }
  }

  /** Returns the TopGroups for the specified
   *  BlockJoinQuery. The groupValue of each GroupDocs will
   *  be the parent docID for that group.
   *  The number of documents within each group is calculated as minimum of <code>maxDocsPerGroup</code>
   *  and number of matched child documents for that group.
   *  Returns null if no groups matched.
   *
   * @param query Search query
   * @param withinGroupSort Sort criteria within groups
   * @param offset Parent docs offset
   * @param maxDocsPerGroup Upper bound of documents per group number
   * @param withinGroupOffset Offset within each group of child docs
   * @param fillSortFields Specifies whether to add sort fields or not
   * @return TopGroups for specified query
   * @throws IOException if there is a low-level I/O error
   */
  public TopGroups<Integer> getTopGroups(ToParentBlockJoinQuery query, Sort withinGroupSort, int offset,
                                         int maxDocsPerGroup, int withinGroupOffset, boolean fillSortFields)
    throws IOException {

    final Integer _slot = joinQueryID.get(query);
    if (_slot == null && totalHitCount == 0) {
      return null;
    }

    if (sortedGroups == null) {
      if (offset >= queue.size()) {
        return null;
      }
      sortQueue();
    } else if (offset > sortedGroups.length) {
      return null;
    }

    return accumulateGroups(_slot == null ? -1 : _slot.intValue(), offset, maxDocsPerGroup, withinGroupOffset, withinGroupSort, fillSortFields);
  }

  /**
   *  Accumulates groups for the BlockJoinQuery specified by its slot.
   *
   * @param slot Search query's slot
   * @param offset Parent docs offset
   * @param maxDocsPerGroup Upper bound of documents per group number
   * @param withinGroupOffset Offset within each group of child docs
   * @param withinGroupSort Sort criteria within groups
   * @param fillSortFields Specifies whether to add sort fields or not
   * @return TopGroups for the query specified by slot
   * @throws IOException if there is a low-level I/O error
   */
  @SuppressWarnings({"unchecked","rawtypes"})
  private TopGroups<Integer> accumulateGroups(int slot, int offset, int maxDocsPerGroup,
                                              int withinGroupOffset, Sort withinGroupSort, boolean fillSortFields) throws IOException {
    final GroupDocs<Integer>[] groups = new GroupDocs[sortedGroups.length - offset];
    final FakeScorer fakeScorer = new FakeScorer();

    int totalGroupedHitCount = 0;
    //System.out.println("slot=" + slot);

    for(int groupIDX=offset;groupIDX<sortedGroups.length;groupIDX++) {
      final OneGroup og = sortedGroups[groupIDX];
      final int numChildDocs;
      if (slot == -1 || slot >= og.counts.length) {
        numChildDocs = 0;
      } else {
        numChildDocs = og.counts[slot];
      }

      // Number of documents in group should be bounded to prevent redundant memory allocation
      final int numDocsInGroup = Math.max(1, Math.min(numChildDocs, maxDocsPerGroup));
      //System.out.println("parent doc=" + og.doc + " numChildDocs=" + numChildDocs + " maxDocsPG=" + maxDocsPerGroup);

      // At this point we hold all docs w/ in each group,
      // unsorted; we now sort them:
      final TopDocsCollector<?> collector;
      if (withinGroupSort == null) {
        //System.out.println("sort by score");
        // Sort by score
        if (!trackScores) {
          throw new IllegalArgumentException("cannot sort by relevance within group: trackScores=false");
        }
        collector = TopScoreDocCollector.create(numDocsInGroup);
      } else {
        // Sort by fields
        collector = TopFieldCollector.create(withinGroupSort, numDocsInGroup, fillSortFields, trackScores, trackMaxScore);
      }

      LeafCollector leafCollector = collector.getLeafCollector(og.readerContext);
      leafCollector.setScorer(fakeScorer);
      for(int docIDX=0;docIDX<numChildDocs;docIDX++) {
        //System.out.println("docIDX=" + docIDX + " vs " + og.docs[slot].length);
        final int doc = og.docs[slot][docIDX];
        fakeScorer.doc = doc;
        if (trackScores) {
          fakeScorer.score = og.scores[slot][docIDX];
        }
        leafCollector.collect(doc);
      }
      totalGroupedHitCount += numChildDocs;

      final Object[] groupSortValues;

      if (fillSortFields) {
        groupSortValues = new Object[comparators.length];
        for(int sortFieldIDX=0;sortFieldIDX<comparators.length;sortFieldIDX++) {
          groupSortValues[sortFieldIDX] = comparators[sortFieldIDX].value(og.slot);
        }
      } else {
        groupSortValues = null;
      }

      final TopDocs topDocs = collector.topDocs(withinGroupOffset, numDocsInGroup);

      groups[groupIDX-offset] = new GroupDocs<>(og.score,
                                                       topDocs.getMaxScore(),
                                                       numChildDocs,
                                                       topDocs.scoreDocs,
                                                       og.doc,
                                                       groupSortValues);
    }

    return new TopGroups<>(new TopGroups<>(sort.getSort(),
                                                       withinGroupSort == null ? null : withinGroupSort.getSort(),
                                                       0, totalGroupedHitCount, groups, maxScore),
                                  totalHitCount);
  }

  /** Returns the TopGroups for the specified BlockJoinQuery.
   *  The groupValue of each GroupDocs will be the parent docID for that group.
   *  The number of documents within each group
   *  equals to the total number of matched child documents for that group.
   *  Returns null if no groups matched.
   *
   * @param query Search query
   * @param withinGroupSort Sort criteria within groups
   * @param offset Parent docs offset
   * @param withinGroupOffset Offset within each group of child docs
   * @param fillSortFields Specifies whether to add sort fields or not
   * @return TopGroups for specified query
   * @throws IOException if there is a low-level I/O error
   */
  public TopGroups<Integer> getTopGroupsWithAllChildDocs(ToParentBlockJoinQuery query, Sort withinGroupSort, int offset,
                                                         int withinGroupOffset, boolean fillSortFields)
    throws IOException {

    return getTopGroups(query, withinGroupSort, offset, Integer.MAX_VALUE, withinGroupOffset, fillSortFields);
  }
  
  /**
   * Returns the highest score across all collected parent hits, as long as
   * <code>trackMaxScores=true</code> was passed
   * {@link #ToParentBlockJoinCollector(Sort, int, boolean, boolean) on
   * construction}. Else, this returns <code>Float.NaN</code>
   */
  public float getMaxScore() {
    return maxScore;
  }

  @Override
  public boolean needsScores() {
    // needed so that eg. BooleanQuery does not rewrite its MUST clauses to
    // FILTER since the filter scorers are hidden in Scorer.getChildren().
    return true;
  }
}
