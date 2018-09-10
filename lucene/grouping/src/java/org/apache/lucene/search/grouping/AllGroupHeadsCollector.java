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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.FixedBitSet;

/**
 * This collector specializes in collecting the most relevant document (group head) for each
 * group that matches the query.
 *
 * Clients should create new collectors by calling {@link #newCollector(GroupSelector, Sort)}
 *
 * @lucene.experimental
 */
@SuppressWarnings({"unchecked","rawtypes"})
public abstract class AllGroupHeadsCollector<T> extends SimpleCollector {

  private final GroupSelector<T> groupSelector;
  protected final Sort sort;

  protected final int[] reversed;
  protected final int compIDXEnd;

  protected Map<T, GroupHead<T>> heads = new HashMap<>();

  protected LeafReaderContext context;
  protected Scorable scorer;

  /**
   * Create a new AllGroupHeadsCollector based on the type of within-group Sort required
   * @param selector a GroupSelector to define the groups
   * @param sort     the within-group sort to use to choose the group head document
   * @param <T>      the group value type
   */
  public static <T> AllGroupHeadsCollector<T> newCollector(GroupSelector<T> selector, Sort sort) {
    if (sort.equals(Sort.RELEVANCE))
      return new ScoringGroupHeadsCollector<>(selector, sort);
    return new SortingGroupHeadsCollector<>(selector, sort);
  }

  private AllGroupHeadsCollector(GroupSelector<T> selector, Sort sort) {
    this.groupSelector = selector;
    this.sort = sort;
    this.reversed = new int[sort.getSort().length];
    final SortField[] sortFields = sort.getSort();
    for (int i = 0; i < sortFields.length; i++) {
      reversed[i] = sortFields[i].getReverse() ? -1 : 1;
    }
    this.compIDXEnd = this.reversed.length - 1;
  }

  /**
   * @param maxDoc The maxDoc of the top level {@link IndexReader}.
   * @return a {@link FixedBitSet} containing all group heads.
   */
  public FixedBitSet retrieveGroupHeads(int maxDoc) {
    FixedBitSet bitSet = new FixedBitSet(maxDoc);

    Collection<? extends GroupHead<T>> groupHeads = getCollectedGroupHeads();
    for (GroupHead groupHead : groupHeads) {
      bitSet.set(groupHead.doc);
    }

    return bitSet;
  }

  /**
   * @return an int array containing all group heads. The size of the array is equal to number of collected unique groups.
   */
  public int[] retrieveGroupHeads() {
    Collection<? extends GroupHead<T>> groupHeads = getCollectedGroupHeads();
    int[] docHeads = new int[groupHeads.size()];

    int i = 0;
    for (GroupHead groupHead : groupHeads) {
      docHeads[i++] = groupHead.doc;
    }

    return docHeads;
  }

  /**
   * @return the number of group heads found for a query.
   */
  public int groupHeadsSize() {
    return getCollectedGroupHeads().size();
  }

  /**
   * Returns the collected group heads.
   * Subsequent calls should return the same group heads.
   *
   * @return the collected group heads
   */
  protected Collection<? extends GroupHead<T>> getCollectedGroupHeads() {
    return heads.values();
  }

  @Override
  public void collect(int doc) throws IOException {
    groupSelector.advanceTo(doc);
    T groupValue = groupSelector.currentValue();
    if (heads.containsKey(groupValue) == false) {
      groupValue = groupSelector.copyValue();
      heads.put(groupValue, newGroupHead(doc, groupValue, context, scorer));
      return;
    }

    GroupHead<T> groupHead = heads.get(groupValue);
    // Ok now we need to check if the current doc is more relevant than top doc for this group
    for (int compIDX = 0; ; compIDX++) {
      final int c = reversed[compIDX] * groupHead.compare(compIDX, doc);
      if (c < 0) {
        // Definitely not competitive. So don't even bother to continue
        return;
      } else if (c > 0) {
        // Definitely competitive.
        break;
      } else if (compIDX == compIDXEnd) {
        // Here c=0. If we're at the last comparator, this doc is not
        // competitive, since docs are visited in doc Id order, which means
        // this doc cannot compete with any other document in the queue.
        return;
      }
    }
    groupHead.updateDocHead(doc);
  }

  @Override
  public ScoreMode scoreMode() {
    return sort.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    groupSelector.setNextReader(context);
    this.context = context;
    for (GroupHead<T> head : heads.values()) {
      head.setNextReader(context);
    }
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    this.scorer = scorer;
    for (GroupHead<T> head : heads.values()) {
      head.setScorer(scorer);
    }
  }

  /**
   * Create a new GroupHead for the given group value, initialized with a doc, context and scorer
   */
  protected abstract GroupHead<T> newGroupHead(int doc, T value, LeafReaderContext context, Scorable scorer) throws IOException;

  /**
   * Represents a group head. A group head is the most relevant document for a particular group.
   * The relevancy is based is usually based on the sort.
   *
   * The group head contains a group value with its associated most relevant document id.
   */
  public static abstract class GroupHead<T> {

    public final T groupValue;
    public int doc;

    protected int docBase;

    /**
     * Create a new GroupHead for the given value
     */
    protected GroupHead(T groupValue, int doc, int docBase) {
      this.groupValue = groupValue;
      this.doc = doc + docBase;
      this.docBase = docBase;
    }

    /**
     * Called for each segment
     */
    protected void setNextReader(LeafReaderContext ctx) throws IOException {
      this.docBase = ctx.docBase;
    }

    /**
     * Called for each segment
     */
    protected abstract void setScorer(Scorable scorer) throws IOException;

    /**
     * Compares the specified document for a specified comparator against the current most relevant document.
     *
     * @param compIDX The comparator index of the specified comparator.
     * @param doc The specified document.
     * @return -1 if the specified document wasn't competitive against the current most relevant document, 1 if the
     *         specified document was competitive against the current most relevant document. Otherwise 0.
     * @throws IOException If I/O related errors occur
     */
    protected abstract int compare(int compIDX, int doc) throws IOException;

    /**
     * Updates the current most relevant document with the specified document.
     *
     * @param doc The specified document
     * @throws IOException If I/O related errors occur
     */
    protected abstract void updateDocHead(int doc) throws IOException;

  }

  /**
   * General implementation using a {@link FieldComparator} to select the group head
   */
  private static class SortingGroupHeadsCollector<T> extends AllGroupHeadsCollector<T> {

    protected SortingGroupHeadsCollector(GroupSelector<T> selector, Sort sort) {
      super(selector, sort);
    }

    @Override
    protected GroupHead<T> newGroupHead(int doc, T value, LeafReaderContext ctx, Scorable scorer) throws IOException {
      return new SortingGroupHead<>(sort, value, doc, ctx, scorer);
    }
  }

  private static class SortingGroupHead<T> extends GroupHead<T> {

    final FieldComparator[] comparators;
    final LeafFieldComparator[] leafComparators;

    protected SortingGroupHead(Sort sort, T groupValue, int doc, LeafReaderContext context, Scorable scorer) throws IOException {
      super(groupValue, doc, context.docBase);
      final SortField[] sortFields = sort.getSort();
      comparators = new FieldComparator[sortFields.length];
      leafComparators = new LeafFieldComparator[sortFields.length];
      for (int i = 0; i < sortFields.length; i++) {
        comparators[i] = sortFields[i].getComparator(1, i);
        leafComparators[i] = comparators[i].getLeafComparator(context);
        leafComparators[i].setScorer(scorer);
        leafComparators[i].copy(0, doc);
        leafComparators[i].setBottom(0);
      }
    }

    @Override
    public void setNextReader(LeafReaderContext ctx) throws IOException {
      super.setNextReader(ctx);
      for (int i = 0; i < comparators.length; i++) {
        leafComparators[i] = comparators[i].getLeafComparator(ctx);
      }
    }

    @Override
    protected void setScorer(Scorable scorer) throws IOException {
      for (LeafFieldComparator c : leafComparators) {
        c.setScorer(scorer);
      }
    }

    @Override
    public int compare(int compIDX, int doc) throws IOException {
      return leafComparators[compIDX].compareBottom(doc);
    }

    @Override
    public void updateDocHead(int doc) throws IOException {
      for (LeafFieldComparator comparator : leafComparators) {
        comparator.copy(0, doc);
        comparator.setBottom(0);
      }
      this.doc = doc + docBase;
    }
  }

  /**
   * Specialized implementation for sorting by score
   */
  private static class ScoringGroupHeadsCollector<T> extends AllGroupHeadsCollector<T> {

    protected ScoringGroupHeadsCollector(GroupSelector<T> selector, Sort sort) {
      super(selector, sort);
    }

    @Override
    protected GroupHead<T> newGroupHead(int doc, T value, LeafReaderContext context, Scorable scorer) throws IOException {
      return new ScoringGroupHead<>(scorer, value, doc, context.docBase);
    }
  }

  private static class ScoringGroupHead<T> extends GroupHead<T> {

    private Scorable scorer;
    private float topScore;

    protected ScoringGroupHead(Scorable scorer, T groupValue, int doc, int docBase) throws IOException {
      super(groupValue, doc, docBase);
      assert scorer.docID() == doc;
      this.scorer = scorer;
      this.topScore = scorer.score();
    }

    @Override
    protected void setScorer(Scorable scorer) {
      this.scorer = scorer;
    }

    @Override
    protected int compare(int compIDX, int doc) throws IOException {
      assert scorer.docID() == doc;
      assert compIDX == 0;
      float score = scorer.score();
      int c = Float.compare(score, topScore);
      if (c > 0)
        topScore = score;
      return c;
    }

    @Override
    protected void updateDocHead(int doc) throws IOException {
      this.doc = doc + docBase;
    }
  }

}
