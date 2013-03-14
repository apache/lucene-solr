package org.apache.lucene.search.grouping;

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


import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;

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
 *  {@link org.apache.lucene.search.grouping.term.TermAllGroupsCollector}.  However, this collector does
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

public class BlockGroupingCollector extends Collector {

  private int[] pendingSubDocs;
  private float[] pendingSubScores;
  private int subDocUpto;

  private final Sort groupSort;
  private final int topNGroups;
  private final Filter lastDocPerGroup;

  // TODO: specialize into 2 classes, static "create" method:
  private final boolean needsScores;

  private final FieldComparator<?>[] comparators;
  private final int[] reversed;
  private final int compIDXEnd;
  private int bottomSlot;
  private boolean queueFull;
  private AtomicReaderContext currentReaderContext;

  private int topGroupDoc;
  private int totalHitCount;
  private int totalGroupCount;
  private int docBase;
  private int groupEndDocID;
  private DocIdSetIterator lastDocPerGroupBits;
  private Scorer scorer;
  private final GroupQueue groupQueue;
  private boolean groupCompetes;

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
    public int freq() {
      throw new UnsupportedOperationException(); // TODO: wtf does this class do?
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

    @Override
    public long cost() {
      return 1;
    }
  }

  private static final class OneGroup {
    AtomicReaderContext readerContext;
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
  private void processGroup() {
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
            comparators[i].setBottom(bottomSlot);
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
          comparators[i].setBottom(bottomSlot);
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
   *  @param lastDocPerGroup a {@link Filter} that marks the
   *    last document in each group.
   */
  public BlockGroupingCollector(Sort groupSort, int topNGroups, boolean needsScores, Filter lastDocPerGroup) throws IOException {

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
    // TODO: allow null groupSort to mean "by relevance",
    // and specialize it?
    this.groupSort = groupSort;
    
    this.topNGroups = topNGroups;

    final SortField[] sortFields = groupSort.getSort();
    comparators = new FieldComparator<?>[sortFields.length];
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
   *  number of groups collected is <= groupOffset.
   *
   *  <p><b>NOTE</b>: This collector is unable to compute
   *  the groupValue per group so it will always be null.
   *  This is normally not a problem, as you can obtain the
   *  value just like you obtain other values for each
   *  matching document (eg, via stored fields, via
   *  FieldCache, etc.)
   *
   *  @param withinGroupSort The {@link Sort} used to sort
   *    documents within each group.  Passing null is
   *    allowed, to sort by relevance.
   *  @param groupOffset Which group to start from
   *  @param withinGroupOffset Which document to start from
   *    within each group
   *  @param maxDocsPerGroup How many top documents to keep
   *     within each group.
   *  @param fillSortFields If true then the Comparable
   *     values for the sort fields will be set
   */
  public TopGroups<?> getTopGroups(Sort withinGroupSort, int groupOffset, int withinGroupOffset, int maxDocsPerGroup, boolean fillSortFields) throws IOException {

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

    final FakeScorer fakeScorer = new FakeScorer();

    float maxScore = Float.MIN_VALUE;

    @SuppressWarnings({"unchecked","rawtypes"})
    final GroupDocs<Object>[] groups = new GroupDocs[groupQueue.size() - groupOffset];
    for(int downTo=groupQueue.size()-groupOffset-1;downTo>=0;downTo--) {
      final OneGroup og = groupQueue.pop();

      // At this point we hold all docs w/ in each group,
      // unsorted; we now sort them:
      final TopDocsCollector<?> collector;
      if (withinGroupSort == null) {
        // Sort by score
        if (!needsScores) {
          throw new IllegalArgumentException("cannot sort by relevance within group: needsScores=false");
        }
        collector = TopScoreDocCollector.create(maxDocsPerGroup, true);
      } else {
        // Sort by fields
        collector = TopFieldCollector.create(withinGroupSort, maxDocsPerGroup, fillSortFields, needsScores, needsScores, true);
      }

      collector.setScorer(fakeScorer);
      collector.setNextReader(og.readerContext);
      for(int docIDX=0;docIDX<og.count;docIDX++) {
        final int doc = og.docs[docIDX];
        fakeScorer.doc = doc;
        if (needsScores) {
          fakeScorer.score = og.scores[docIDX];
        }
        collector.collect(doc);
      }
      totalGroupedHitCount += og.count;

      final Object[] groupSortValues;

      if (fillSortFields) {
        groupSortValues = new Comparable<?>[comparators.length];
        for(int sortFieldIDX=0;sortFieldIDX<comparators.length;sortFieldIDX++) {
          groupSortValues[sortFieldIDX] = comparators[sortFieldIDX].value(og.comparatorSlot);
        }
      } else {
        groupSortValues = null;
      }

      final TopDocs topDocs = collector.topDocs(withinGroupOffset, maxDocsPerGroup);

      // TODO: we could aggregate scores across children
      // by Sum/Avg instead of passing NaN:
      groups[downTo] = new GroupDocs<Object>(Float.NaN,
                                             topDocs.getMaxScore(),
                                             og.count,
                                             topDocs.scoreDocs,
                                             null,
                                             groupSortValues);
      maxScore = Math.max(maxScore, topDocs.getMaxScore());
    }

    /*
    while (groupQueue.size() != 0) {
      final OneGroup og = groupQueue.pop();
      //System.out.println("  leftover: og ord=" + og.groupOrd + " count=" + og.count);
      totalGroupedHitCount += og.count;
    }
    */

    return new TopGroups<Object>(new TopGroups<Object>(groupSort.getSort(),
                                       withinGroupSort == null ? null : withinGroupSort.getSort(),
                                       totalHitCount, totalGroupedHitCount, groups, maxScore),
                         totalGroupCount);
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
    for (FieldComparator<?> comparator : comparators) {
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
        for (FieldComparator<?> fc : comparators) {
          fc.copy(bottomSlot, doc);
          fc.setBottom(bottomSlot);
        }        
        topGroupDoc = doc;
      } else {
        // Compare to bottomSlot
        for (int compIDX = 0;; compIDX++) {
          final int c = reversed[compIDX] * comparators[compIDX].compareBottom(doc);
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
        
        for (FieldComparator<?> fc : comparators) {
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
        final int c = reversed[compIDX] * comparators[compIDX].compareBottom(doc);
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
      for (FieldComparator<?> fc : comparators) {
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
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
    if (subDocUpto != 0) {
      processGroup();
    }
    subDocUpto = 0;
    docBase = readerContext.docBase;
    //System.out.println("setNextReader base=" + docBase + " r=" + readerContext.reader);
    lastDocPerGroupBits = lastDocPerGroup.getDocIdSet(readerContext, readerContext.reader().getLiveDocs()).iterator();
    groupEndDocID = -1;

    currentReaderContext = readerContext;
    for (int i=0; i<comparators.length; i++) {
      comparators[i] = comparators[i].setNextReader(readerContext);
    }
  }
}
