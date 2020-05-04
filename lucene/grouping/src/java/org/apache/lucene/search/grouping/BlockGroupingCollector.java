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
package org.apache.lucene.search.grouping;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.PriorityQueue;

// TODO: this sentence is too long for the class summary.
/** BlockGroupingCollector performs grouping with a
 *  single pass collector, as long as you are grouping by a
 *  doc block field, ie all documents sharing a given group
 *  value were indexed as a doc block using the atomic
 *  {@link IndexWriter#addDocuments IndexWriter.addDocuments()} 
 *  or {@link IndexWriter#updateDocuments IndexWriter.updateDocuments()} 
 *  API.
 *
 *  <p>This results in faster performance (~25% faster QPS)
 *  than the two-pass grouping collectors, with the tradeoff
 *  being that the documents in each group must always be
 *  indexed as a block.  This collector also fills in
 *  TopGroups.totalGroupCount without requiring the separate
 *  {@link org.apache.lucene.search.grouping.AllGroupsCollector}.  However, this collector does
 *  not fill in the groupValue of each group; this field
 *  will always be null.
 *
 *  <p><b>NOTE</b>: this collector makes no effort to verify
 *  the docs were in fact indexed as a block, so it's up to
 *  you to ensure this was the case.
 *
 *  <p>See {@link org.apache.lucene.search.grouping} for more
 *  details including a full code example.</p>
 *
 * @lucene.experimental
 */

// TODO: TopGroups.merge() won't work with TopGroups returned by this collector, because
// each block will be on a different shard.  Add a specialized merge() static method
// to this collector?

public class BlockGroupingCollector extends SimpleCollector {

  private int[] pendingSubDocs;
  private float[] pendingSubScores;
  private int subDocUpto;

  private final Sort groupSort;
  private final int topNGroups;
  private final Weight lastDocPerGroup;

  // TODO: specialize into 2 classes, static "create" method:
  private final boolean needsScores;

  private final FieldComparator<?>[] comparators;
  private final LeafFieldComparator[] leafComparators;
  private final int[] reversed;
  private final int compIDXEnd;
  private int bottomSlot;
  private boolean queueFull;
  private LeafReaderContext currentReaderContext;

  private int topGroupDoc;
  private int totalHitCount;
  private int totalGroupCount;
  private int docBase;
  private int groupEndDocID;
  private DocIdSetIterator lastDocPerGroupBits;
  private Scorable scorer;
  private final GroupQueue groupQueue;
  private boolean groupCompetes;

  private static final class OneGroup {
    LeafReaderContext readerContext;
    //int groupOrd;
    int topGroupDoc;
    int[] docs;
    float[] scores;
    int count;
    int comparatorSlot;
  }
  
  // Sorts by groupSort.  Not static -- uses comparators, reversed
  private final class GroupQueue extends PriorityQueue<OneGroup> {

    public GroupQueue(int size) {
      super(size);
    }

    @Override
    protected boolean lessThan(final OneGroup group1, final OneGroup group2) {

      //System.out.println("    ltcheck");
      assert group1 != group2;
      assert group1.comparatorSlot != group2.comparatorSlot;

      final int numComparators = comparators.length;
      for (int compIDX=0;compIDX < numComparators; compIDX++) {
        final int c = reversed[compIDX] * comparators[compIDX].compare(group1.comparatorSlot, group2.comparatorSlot);
        if (c != 0) {
          // Short circuit
          return c > 0;
        }
      }

      // Break ties by docID; lower docID is always sorted first
      return group1.topGroupDoc > group2.topGroupDoc;
    }
  }

  // Called when we transition to another group; if the
  // group is competitive we insert into the group queue
  private void processGroup() throws IOException {
    totalGroupCount++;
    //System.out.println("    processGroup ord=" + lastGroupOrd + " competes=" + groupCompetes + " count=" + subDocUpto + " groupDoc=" + topGroupDoc);
    if (groupCompetes) {
      if (!queueFull) {
        // Startup transient: always add a new OneGroup
        final OneGroup og = new OneGroup();
        og.count = subDocUpto;
        og.topGroupDoc = docBase + topGroupDoc;
        og.docs = pendingSubDocs;
        pendingSubDocs = new int[10];
        if (needsScores) {
          og.scores = pendingSubScores;
          pendingSubScores = new float[10];
        }
        og.readerContext = currentReaderContext;
        //og.groupOrd = lastGroupOrd;
        og.comparatorSlot = bottomSlot;
        final OneGroup bottomGroup = groupQueue.add(og);
        //System.out.println("      ADD group=" + getGroupString(lastGroupOrd) + " newBottom=" + getGroupString(bottomGroup.groupOrd));
        queueFull = groupQueue.size() == topNGroups;
        if (queueFull) {
          // Queue just became full; now set the real bottom
          // in the comparators:
          bottomSlot = bottomGroup.comparatorSlot;
          //System.out.println("    set bottom=" + bottomSlot);
          for (int i = 0; i < comparators.length; i++) {
            leafComparators[i].setBottom(bottomSlot);
          }
          //System.out.println("     QUEUE FULL");
        } else {
          // Queue not full yet -- just advance bottomSlot:
          bottomSlot = groupQueue.size();
        }
      } else {
        // Replace bottom element in PQ and then updateTop
        final OneGroup og = groupQueue.top();
        assert og != null;
        og.count = subDocUpto;
        og.topGroupDoc = docBase + topGroupDoc;
        // Swap pending docs
        final int[] savDocs = og.docs;
        og.docs = pendingSubDocs;
        pendingSubDocs = savDocs;
        if (needsScores) {
          // Swap pending scores
          final float[] savScores = og.scores;
          og.scores = pendingSubScores;
          pendingSubScores = savScores;
        }
        og.readerContext = currentReaderContext;
        //og.groupOrd = lastGroupOrd;
        bottomSlot = groupQueue.updateTop().comparatorSlot;

        //System.out.println("    set bottom=" + bottomSlot);
        for (int i = 0; i < comparators.length; i++) {
          leafComparators[i].setBottom(bottomSlot);
        }
      }
    }
    subDocUpto = 0;
  }

  /**
   * Create the single pass collector.
   *
   *  @param groupSort The {@link Sort} used to sort the
   *    groups.  The top sorted document within each group
   *    according to groupSort, determines how that group
   *    sorts against other groups.  This must be non-null,
   *    ie, if you want to groupSort by relevance use
   *    Sort.RELEVANCE.
   *  @param topNGroups How many top groups to keep.
   *  @param needsScores true if the collected documents
   *    require scores, either because relevance is included
   *    in the withinGroupSort or because you plan to pass true
   *    for either getSscores or getMaxScores to {@link
   *    #getTopGroups}
   *  @param lastDocPerGroup a {@link Weight} that marks the
   *    last document in each group.
   */
  public BlockGroupingCollector(Sort groupSort, int topNGroups, boolean needsScores, Weight lastDocPerGroup) {

    if (topNGroups < 1) {
      throw new IllegalArgumentException("topNGroups must be >= 1 (got " + topNGroups + ")");
    }

    groupQueue = new GroupQueue(topNGroups);
    pendingSubDocs = new int[10];
    if (needsScores) {
      pendingSubScores = new float[10];
    }

    this.needsScores = needsScores;
    this.lastDocPerGroup = lastDocPerGroup;

    this.groupSort = groupSort;
    
    this.topNGroups = topNGroups;

    final SortField[] sortFields = groupSort.getSort();
    comparators = new FieldComparator<?>[sortFields.length];
    leafComparators = new LeafFieldComparator[sortFields.length];
    compIDXEnd = comparators.length - 1;
    reversed = new int[sortFields.length];
    for (int i = 0; i < sortFields.length; i++) {
      final SortField sortField = sortFields[i];
      comparators[i] = sortField.getComparator(topNGroups, i);
      reversed[i] = sortField.getReverse() ? -1 : 1;
    }
  }

  // TODO: maybe allow no sort on retrieving groups?  app
  // may want to simply process docs in the group itself?
  // typically they will be presented as a "single" result
  // in the UI?

  /** Returns the grouped results.  Returns null if the
   *  number of groups collected is &lt;= groupOffset.
   *
   *  <p><b>NOTE</b>: This collector is unable to compute
   *  the groupValue per group so it will always be null.
   *  This is normally not a problem, as you can obtain the
   *  value just like you obtain other values for each
   *  matching document (eg, via stored fields, via
   *  DocValues, etc.)
   *
   *  @param withinGroupSort The {@link Sort} used to sort
   *    documents within each group.
   *  @param groupOffset Which group to start from
   *  @param withinGroupOffset Which document to start from
   *    within each group
   *  @param maxDocsPerGroup How many top documents to keep
   *     within each group.
   */
  public TopGroups<?> getTopGroups(Sort withinGroupSort, int groupOffset, int withinGroupOffset, int maxDocsPerGroup) throws IOException {

    //if (queueFull) {
    //System.out.println("getTopGroups groupOffset=" + groupOffset + " topNGroups=" + topNGroups);
    //}
    if (subDocUpto != 0) {
      processGroup();
    }
    if (groupOffset >= groupQueue.size()) {
      return null;
    }
    int totalGroupedHitCount = 0;

    final ScoreAndDoc fakeScorer = new ScoreAndDoc();

    float maxScore = Float.MIN_VALUE;

    @SuppressWarnings({"unchecked","rawtypes"})
    final GroupDocs<Object>[] groups = new GroupDocs[groupQueue.size() - groupOffset];
    for(int downTo=groupQueue.size()-groupOffset-1;downTo>=0;downTo--) {
      final OneGroup og = groupQueue.pop();

      // At this point we hold all docs w/ in each group,
      // unsorted; we now sort them:
      final TopDocsCollector<?> collector;
      if (withinGroupSort.equals(Sort.RELEVANCE)) {
        // Sort by score
        if (!needsScores) {
          throw new IllegalArgumentException("cannot sort by relevance within group: needsScores=false");
        }
        collector = TopScoreDocCollector.create(maxDocsPerGroup, Integer.MAX_VALUE);
      } else {
        // Sort by fields
        collector = TopFieldCollector.create(withinGroupSort, maxDocsPerGroup, Integer.MAX_VALUE); // TODO: disable exact counts?
      }

      float groupMaxScore = needsScores ? Float.NEGATIVE_INFINITY : Float.NaN;
      LeafCollector leafCollector = collector.getLeafCollector(og.readerContext);
      leafCollector.setScorer(fakeScorer);
      for(int docIDX=0;docIDX<og.count;docIDX++) {
        final int doc = og.docs[docIDX];
        fakeScorer.doc = doc;
        if (needsScores) {
          fakeScorer.score = og.scores[docIDX];
          groupMaxScore = Math.max(groupMaxScore, fakeScorer.score);
        }
        leafCollector.collect(doc);
      }
      totalGroupedHitCount += og.count;

      final Object[] groupSortValues;

      groupSortValues = new Comparable<?>[comparators.length];
      for(int sortFieldIDX=0;sortFieldIDX<comparators.length;sortFieldIDX++) {
        groupSortValues[sortFieldIDX] = comparators[sortFieldIDX].value(og.comparatorSlot);
      }

      final TopDocs topDocs = collector.topDocs(withinGroupOffset, maxDocsPerGroup);

      // TODO: we could aggregate scores across children
      // by Sum/Avg instead of passing NaN:
      groups[downTo] = new GroupDocs<>(Float.NaN,
                                             groupMaxScore,
                                             new TotalHits(og.count, TotalHits.Relation.EQUAL_TO),
                                             topDocs.scoreDocs,
                                             null,
                                             groupSortValues);
      maxScore = Math.max(maxScore, groupMaxScore);
    }

    /*
    while (groupQueue.size() != 0) {
      final OneGroup og = groupQueue.pop();
      //System.out.println("  leftover: og ord=" + og.groupOrd + " count=" + og.count);
      totalGroupedHitCount += og.count;
    }
    */

    return new TopGroups<>(new TopGroups<>(groupSort.getSort(),
                                       withinGroupSort.getSort(),
                                       totalHitCount, totalGroupedHitCount, groups, maxScore),
                         totalGroupCount);
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    this.scorer = scorer;
    for (LeafFieldComparator comparator : leafComparators) {
      comparator.setScorer(scorer);
    }
  }

  @Override
  public void collect(int doc) throws IOException {

    // System.out.println("C " + doc);

    if (doc > groupEndDocID) {
      // Group changed
      if (subDocUpto != 0) {
        processGroup();
      }
      groupEndDocID = lastDocPerGroupBits.advance(doc);
      //System.out.println("  adv " + groupEndDocID + " " + lastDocPerGroupBits);
      subDocUpto = 0;
      groupCompetes = !queueFull;
    }

    totalHitCount++;

    // Always cache doc/score within this group:
    if (subDocUpto == pendingSubDocs.length) {
      pendingSubDocs = ArrayUtil.grow(pendingSubDocs);
    }
    pendingSubDocs[subDocUpto] = doc;
    if (needsScores) {
      if (subDocUpto == pendingSubScores.length) {
        pendingSubScores = ArrayUtil.grow(pendingSubScores);
      }
      pendingSubScores[subDocUpto] = scorer.score();
    }
    subDocUpto++;

    if (groupCompetes) {
      if (subDocUpto == 1) {
        assert !queueFull;

        //System.out.println("    init copy to bottomSlot=" + bottomSlot);
        for (LeafFieldComparator fc : leafComparators) {
          fc.copy(bottomSlot, doc);
          fc.setBottom(bottomSlot);
        }        
        topGroupDoc = doc;
      } else {
        // Compare to bottomSlot
        for (int compIDX = 0;; compIDX++) {
          final int c = reversed[compIDX] * leafComparators[compIDX].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive -- done
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (compIDX == compIDXEnd) {
            // Ties with bottom, except we know this docID is
            // > docID in the queue (docs are visited in
            // order), so not competitive:
            return;
          }
        }

        //System.out.println("       best w/in group!");
        
        for (LeafFieldComparator fc : leafComparators) {
          fc.copy(bottomSlot, doc);
          // Necessary because some comparators cache
          // details of bottom slot; this forces them to
          // re-cache:
          fc.setBottom(bottomSlot);
        }        
        topGroupDoc = doc;
      }
    } else {
      // We're not sure this group will make it into the
      // queue yet
      for (int compIDX = 0;; compIDX++) {
        final int c = reversed[compIDX] * leafComparators[compIDX].compareBottom(doc);
        if (c < 0) {
          // Definitely not competitive -- done
          //System.out.println("    doc doesn't compete w/ top groups");
          return;
        } else if (c > 0) {
          // Definitely competitive.
          break;
        } else if (compIDX == compIDXEnd) {
          // Ties with bottom, except we know this docID is
          // > docID in the queue (docs are visited in
          // order), so not competitive:
          //System.out.println("    doc doesn't compete w/ top groups");
          return;
        }
      }
      groupCompetes = true;
      for (LeafFieldComparator fc : leafComparators) {
        fc.copy(bottomSlot, doc);
        // Necessary because some comparators cache
        // details of bottom slot; this forces them to
        // re-cache:
        fc.setBottom(bottomSlot);
      }
      topGroupDoc = doc;
      //System.out.println("        doc competes w/ top groups");
    }
  }

  @Override
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    if (subDocUpto != 0) {
      processGroup();
    }
    subDocUpto = 0;
    docBase = readerContext.docBase;
    //System.out.println("setNextReader base=" + docBase + " r=" + readerContext.reader);
    Scorer s = lastDocPerGroup.scorer(readerContext);
    if (s == null) {
      lastDocPerGroupBits = null;
    } else {
      lastDocPerGroupBits = s.iterator();
    }
    groupEndDocID = -1;

    currentReaderContext = readerContext;
    for (int i=0; i<comparators.length; i++) {
      leafComparators[i] = comparators[i].getLeafComparator(readerContext);
    }
  }

  @Override
  public ScoreMode scoreMode() {
    return needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
  }

  private static class ScoreAndDoc extends Scorable {

    float score;
    int doc = -1;

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public float score() {
      return score;
    }

  }
}
