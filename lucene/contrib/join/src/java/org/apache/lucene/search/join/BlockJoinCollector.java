package org.apache.lucene.search.join;

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;       // javadocs
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldValueHitQueue;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.Weight;
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
 *  <p>The parent Sort must only use
 *  fields from the parent documents; sorting by field in
 *  the child documents is not supported.</p>
 *
 *  <p>You should only use this
 *  collector if one or more of the clauses in the query is
 *  a {@link BlockJoinQuery}.  This collector will find those query
 *  clauses and record the matching child documents for the
 *  top scoring parent documents.</p>
 *
 *  <p>Multiple joins (star join) and nested joins and a mix
 *  of the two are allowed, as long as in all cases the
 *  documents corresponding to a single row of each joined
 *  parent table were indexed as a doc block.</p>
 *
 *  <p>For the simple star join you can retrieve the
 *  {@link TopGroups} instance containing each {@link BlockJoinQuery}'s
 *  matching child documents for the top parent groups,
 *  using {@link #getTopGroups}.  Ie,
 *  a single query, which will contain two or more
 *  {@link BlockJoinQuery}'s as clauses representing the star join,
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
public class BlockJoinCollector extends Collector {

  private final Sort sort;

  // Maps each BlockJoinQuery instance to its "slot" in
  // joinScorers and in OneGroup's cached doc/scores/count:
  private final Map<Query,Integer> joinQueryID = new HashMap<Query,Integer>();
  private final int numParentHits;
  private final FieldValueHitQueue<OneGroup> queue;
  private final FieldComparator[] comparators;
  private final int[] reverseMul;
  private final int compEnd;
  private final boolean trackMaxScore;
  private final boolean trackScores;

  private int docBase;
  private BlockJoinQuery.BlockJoinScorer[] joinScorers = new BlockJoinQuery.BlockJoinScorer[0];
  private IndexReader currentReader;
  private Scorer scorer;
  private boolean queueFull;

  private OneGroup bottom;
  private int totalHitCount;
  private float maxScore = Float.NaN;

  /*  Creates a BlockJoinCollector.  The provided sort must
   *  not be null. */
  public BlockJoinCollector(Sort sort, int numParentHits, boolean trackScores, boolean trackMaxScore) throws IOException {
    // TODO: allow null sort to be specialized to relevance
    // only collector
    this.sort = sort;
    this.trackMaxScore = trackMaxScore;
    this.trackScores = trackScores;
    this.numParentHits = numParentHits;
    queue = FieldValueHitQueue.create(sort.getSort(), numParentHits);
    comparators = queue.getComparators();
    reverseMul = queue.getReverseMul();
    compEnd = comparators.length - 1;
  }
  
  private static final class OneGroup extends FieldValueHitQueue.Entry {
    public OneGroup(int comparatorSlot, int parentDoc, float parentScore, int numJoins, boolean doScores) {
      super(comparatorSlot, parentDoc, parentScore);
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
    IndexReader reader;
    int docBase;
    int[][] docs;
    float[][] scores;
    int[] counts;
  };

  @Override
  public void collect(int parentDoc) throws IOException {
    //System.out.println("C parentDoc=" + parentDoc);
    totalHitCount++;

    float score = Float.NaN;

    if (trackMaxScore) {
      score = scorer.score();
      if (score > maxScore) {
        maxScore = score;
      }
    }

    // TODO: we could sweep all joinScorers here and
    // aggregate total child hit count, so we can fill this
    // in getTopGroups (we wire it to 0 now)

    if (queueFull) {
      //System.out.println("  queueFull");
      // Fastmatch: return if this hit is not competitive
      for (int i = 0;; i++) {
        final int c = reverseMul[i] * comparators[i].compareBottom(parentDoc);
        if (c < 0) {
          // Definitely not competitive.
          //System.out.println("    skip");
          return;
        } else if (c > 0) {
          // Definitely competitive.
          break;
        } else if (i == compEnd) {
          // Here c=0. If we're at the last comparator, this doc is not
          // competitive, since docs are visited in doc Id order, which means
          // this doc cannot compete with any other document in the queue.
          //System.out.println("    skip");
          return;
        }
      }

      //System.out.println("    competes!  doc=" + (docBase + parentDoc));

      // This hit is competitive - replace bottom element in queue & adjustTop
      for (int i = 0; i < comparators.length; i++) {
        comparators[i].copy(bottom.slot, parentDoc);
      }
      if (!trackMaxScore && trackScores) {
        score = scorer.score();
      }
      bottom.doc = docBase + parentDoc;
      bottom.reader = currentReader;
      bottom.docBase = docBase;
      bottom.score = score;
      copyGroups(bottom);
      bottom = queue.updateTop();

      for (int i = 0; i < comparators.length; i++) {
        comparators[i].setBottom(bottom.slot);
      }
    } else {
      // Startup transient: queue is not yet full:
      final int comparatorSlot = totalHitCount - 1;

      // Copy hit into queue
      for (int i = 0; i < comparators.length; i++) {
        comparators[i].copy(comparatorSlot, parentDoc);
      }
      //System.out.println("  startup: new OG doc=" + (docBase+parentDoc));
      final OneGroup og = new OneGroup(comparatorSlot, docBase+parentDoc, score, joinScorers.length, trackScores);
      og.reader = currentReader;
      og.docBase = docBase;
      copyGroups(og);
      bottom = queue.add(og);
      queueFull = totalHitCount == numParentHits;
      if (queueFull) {
        // End of startup transient: queue just filled up:
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
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

    //System.out.println("copyGroups parentDoc=" + og.doc);
    for(int scorerIDX = 0;scorerIDX < numSubScorers;scorerIDX++) {
      final BlockJoinQuery.BlockJoinScorer joinScorer = joinScorers[scorerIDX];
      //System.out.println("  scorer=" + joinScorer);
      if (joinScorer != null) {
        og.counts[scorerIDX] = joinScorer.getChildCount();
        //System.out.println("    count=" + og.counts[scorerIDX]);
        og.docs[scorerIDX] = joinScorer.swapChildDocs(og.docs[scorerIDX]);
        /*
        for(int idx=0;idx<og.counts[scorerIDX];idx++) {
          System.out.println("    docs[" + idx + "]=" + og.docs[scorerIDX][idx]);
        }
        */
        if (trackScores) {
          og.scores[scorerIDX] = joinScorer.swapChildScores(og.scores[scorerIDX]);
        }
      }
    }
  }

  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    currentReader = reader;
    this.docBase = docBase;
    for (int compIDX = 0; compIDX < comparators.length; compIDX++) {
      comparators[compIDX].setNextReader(reader, docBase);
    }
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  @Override
  public void setScorer(Scorer scorer) {
    //System.out.println("C.setScorer scorer=" + scorer);
    // Since we invoke .score(), and the comparators likely
    // do as well, cache it so it's only "really" computed
    // once:
    this.scorer = new ScoreCachingWrappingScorer(scorer);
    for (int compIDX = 0; compIDX < comparators.length; compIDX++) {
      comparators[compIDX].setScorer(this.scorer);
    }
    Arrays.fill(joinScorers, null);

    // Find any BlockJoinScorers out there:
    scorer.visitScorers(new Scorer.ScorerVisitor<Query,Query,Scorer>() {
        private void enroll(BlockJoinQuery query, BlockJoinQuery.BlockJoinScorer scorer) {
          final Integer slot = joinQueryID.get(query);
          if (slot == null) {
            joinQueryID.put(query, joinScorers.length);
            //System.out.println("found JQ: " + query + " slot=" + joinScorers.length);
            final BlockJoinQuery.BlockJoinScorer[] newArray = new BlockJoinQuery.BlockJoinScorer[1+joinScorers.length];
            System.arraycopy(joinScorers, 0, newArray, 0, joinScorers.length);
            joinScorers = newArray;
            joinScorers[joinScorers.length-1] = scorer;
          } else {
            joinScorers[slot] = scorer;
          }
        }

        @Override
        public void visitOptional(Query parent, Query child, Scorer scorer) {
          //System.out.println("visitOpt");
          if (child instanceof BlockJoinQuery) {
            enroll((BlockJoinQuery) child,
                   (BlockJoinQuery.BlockJoinScorer) scorer);
          }
        }

        @Override
        public void visitRequired(Query parent, Query child, Scorer scorer) {
          //System.out.println("visitReq parent=" + parent + " child=" + child + " scorer=" + scorer);
          if (child instanceof BlockJoinQuery) {
            enroll((BlockJoinQuery) child,
                   (BlockJoinQuery.BlockJoinScorer) scorer);
          }
        }

        @Override
        public void visitProhibited(Query parent, Query child, Scorer scorer) {
          //System.out.println("visitProh");
          if (child instanceof BlockJoinQuery) {
            enroll((BlockJoinQuery) child,
                   (BlockJoinQuery.BlockJoinScorer) scorer);
          }
        }
      });
  }

  private final static class FakeScorer extends Scorer {

    float score;
    int doc;

    public FakeScorer() {
      super((Weight) null);
    }

    @Override
    public float score() {
      return score;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextDoc() {
      throw new UnsupportedOperationException();
    }
  }

  private OneGroup[] sortedGroups;

  private void sortQueue() {
    sortedGroups = new OneGroup[queue.size()];
    for(int downTo=queue.size()-1;downTo>=0;downTo--) {
      sortedGroups[downTo] = queue.pop();
    }
  }

  /** Return the TopGroups for the specified
   *  BlockJoinQuery.  The groupValue of each GroupDocs will
   *  be the parent docID for that group.  Note that the
   *  {@link GroupDocs#totalHits}, which would be the
   *  total number of child documents matching that parent,
   *  is not computed (will always be 0).  Returns null if
   *  no groups matched. */
  @SuppressWarnings("unchecked")
  public TopGroups<Integer> getTopGroups(BlockJoinQuery query, Sort withinGroupSort, int offset, int maxDocsPerGroup, int withinGroupOffset, boolean fillSortFields) 

    throws IOException {

    final Integer _slot = joinQueryID.get(query);
    if (_slot == null) {
      if (totalHitCount == 0) {
        return null;
      } else {
        throw new IllegalArgumentException("the Query did not contain the provided BlockJoinQuery");
      }
    }

    // unbox once
    final int slot = _slot;

    if (offset >= queue.size()) {
      return null;
    }
    int totalGroupedHitCount = 0;

    if (sortedGroups == null) {
      sortQueue();
    }

    final FakeScorer fakeScorer = new FakeScorer();

    final GroupDocs<Integer>[] groups = new GroupDocs[sortedGroups.length - offset];

    for(int groupIDX=offset;groupIDX<sortedGroups.length;groupIDX++) {
      final OneGroup og = sortedGroups[groupIDX];

      // At this point we hold all docs w/ in each group,
      // unsorted; we now sort them:
      final TopDocsCollector collector;
      if (withinGroupSort == null) {
        // Sort by score
        if (!trackScores) {
          throw new IllegalArgumentException("cannot sort by relevance within group: trackScores=false");
        }
        collector = TopScoreDocCollector.create(maxDocsPerGroup, true);
      } else {
        // Sort by fields
        collector = TopFieldCollector.create(withinGroupSort, maxDocsPerGroup, fillSortFields, trackScores, trackMaxScore, true);
      }

      collector.setScorer(fakeScorer);
      collector.setNextReader(og.reader, og.docBase);
      final int numChildDocs = og.counts[slot];
      for(int docIDX=0;docIDX<numChildDocs;docIDX++) {
        final int doc = og.docs[slot][docIDX];
        fakeScorer.doc = doc;
        if (trackScores) {
          fakeScorer.score = og.scores[slot][docIDX];
        }
        collector.collect(doc);
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

      final TopDocs topDocs = collector.topDocs(withinGroupOffset, maxDocsPerGroup);

      groups[groupIDX-offset] = new GroupDocs<Integer>(topDocs.getMaxScore(),
                                                       og.counts[slot],
                                                       topDocs.scoreDocs,
                                                       og.doc,
                                                       groupSortValues);
    }

    return new TopGroups<Integer>(new TopGroups<Integer>(sort.getSort(),
                                                         withinGroupSort == null ? null : withinGroupSort.getSort(),
                                                         0, totalGroupedHitCount, groups),
                                  totalHitCount);
  }
}
