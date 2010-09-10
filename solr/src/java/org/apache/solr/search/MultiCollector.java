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

package org.apache.solr.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.solr.search.function.DocValues;
import org.apache.solr.search.function.ValueSource;

import java.io.IOException;
import java.util.*;

public class MultiCollector extends Collector {
  final Collector[] collectors;
  final boolean acceptsDocsOutOfOrder;

  public static Collector wrap(List<? extends Collector> collectors) {
    return collectors.size() == 1 ? collectors.get(0) : new MultiCollector(collectors);  
  }

  public static Collector[] subCollectors(Collector collector) {
    if (collector instanceof MultiCollector)
      return ((MultiCollector)collector).collectors;
    return new Collector[]{collector};
  }

  public MultiCollector(List<? extends Collector> collectors) {
    this(collectors.toArray(new Collector[collectors.size()]));
  }

  public MultiCollector(Collector[] collectors) {
    this.collectors = collectors;

    boolean acceptsDocsOutOfOrder = true;
    for (Collector collector : collectors) {
      if (collector.acceptsDocsOutOfOrder() == false) {
        acceptsDocsOutOfOrder = false;
        break;
      }
    }
    this.acceptsDocsOutOfOrder = acceptsDocsOutOfOrder;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    for (Collector collector : collectors)
      collector.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    for (Collector collector : collectors)
      collector.collect(doc);
  }

  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    for (Collector collector : collectors)
      collector.setNextReader(reader, docBase);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return acceptsDocsOutOfOrder;
  }
}






class SearchGroup {
  public MutableValue groupValue;
  int matches;
  int topDoc;
  // float topDocScore;  // currently unused
  int comparatorSlot;

  // currently only used when sort != sort.group
  FieldComparator[] sortGroupComparators;
  int[] sortGroupReversed;

  /***
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return groupValue.equalsSameType(((SearchGroup)obj).groupValue);
  }
  ***/
}



/** Finds the top set of groups, grouped by groupByVS when sort == group.sort */
class TopGroupCollector extends Collector {
  final int nGroups;
  final HashMap<MutableValue, SearchGroup> groupMap;
  TreeSet<SearchGroup> orderedGroups;
  final ValueSource vs;
  final Map context;
  final FieldComparator[] comparators;
  final int[] reversed;

  DocValues docValues;
  DocValues.ValueFiller filler;
  MutableValue mval;
  Scorer scorer;
  int docBase;
  int spareSlot;

  int matches;

  public TopGroupCollector(ValueSource groupByVS, Map vsContext, Sort sort, int nGroups) throws IOException {
    this.vs = groupByVS;
    this.context = vsContext;
    this.nGroups = nGroups;

    SortField[] sortFields = sort.getSort();
    this.comparators = new FieldComparator[sortFields.length];
    this.reversed = new int[sortFields.length];
    for (int i = 0; i < sortFields.length; i++) {
      SortField sortField = sortFields[i];
      reversed[i] = sortField.getReverse() ? -1 : 1;
      // use nGroups + 1 so we have a spare slot to use for comparing (tracked by this.spareSlot)
      comparators[i] = sortField.getComparator(nGroups + 1, i);
    }
    this.spareSlot = nGroups;

    this.groupMap = new HashMap<MutableValue, SearchGroup>(nGroups);
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
    for (FieldComparator fc : comparators)
      fc.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    matches++;
    filler.fillValue(doc);
    SearchGroup group = groupMap.get(mval);
    if (group == null) {
      int num = groupMap.size();
      if (groupMap.size() < nGroups) {
        SearchGroup sg = new SearchGroup();
        sg.groupValue = mval.duplicate();
        sg.comparatorSlot = num++;
        sg.matches = 1;
        sg.topDoc = docBase + doc;
        // sg.topDocScore = scorer.score();
        for (FieldComparator fc : comparators)
          fc.copy(sg.comparatorSlot, doc);
        groupMap.put(sg.groupValue, sg);
        return;
      }

      if (orderedGroups == null) {
        buildSet();
      }


      for (int i = 0;; i++) {
        final int c = reversed[i] * comparators[i].compareBottom(doc);
        if (c < 0) {
          // Definitely not competitive.
          return;
        } else if (c > 0) {
          // Definitely competitive.
          break;
        } else if (i == comparators.length - 1) {
          // Here c=0. If we're at the last comparator, this doc is not
          // competitive, since docs are visited in doc Id order, which means
          // this doc cannot compete with any other document in the queue.
          return;
        }
      }

      // remove current smallest group
      SearchGroup smallest = orderedGroups.pollLast();
      groupMap.remove(smallest.groupValue);

      // reuse the removed SearchGroup
      smallest.groupValue.copy(mval);
      smallest.matches = 1;
      smallest.topDoc = docBase + doc;
      // smallest.topDocScore = scorer.score();
      for (FieldComparator fc : comparators)
        fc.copy(smallest.comparatorSlot, doc);

      groupMap.put(smallest.groupValue, smallest);
      orderedGroups.add(smallest);

      for (FieldComparator fc : comparators)
        fc.setBottom(orderedGroups.last().comparatorSlot);

      return;
    }

    //
    // update existing group
    //

    group.matches++; // TODO: these aren't valid if the group is every discarded then re-added.  keep track if there have been discards?

    for (int i = 0;; i++) {
      FieldComparator fc = comparators[i];
      fc.copy(spareSlot, doc);

      final int c = reversed[i] * fc.compare(group.comparatorSlot, spareSlot);
      if (c < 0) {
        // Definitely not competitive.
        return;
      } else if (c > 0) {
        // Definitely competitive.
        // Set remaining comparators
        for (int j=i+1; j<comparators.length; j++)
          comparators[j].copy(spareSlot, doc);
        break;
      } else if (i == comparators.length - 1) {
        // Here c=0. If we're at the last comparator, this doc is not
        // competitive, since docs are visited in doc Id order, which means
        // this doc cannot compete with any other document in the queue.
        return;
      }
    }

    // remove before updating the group since lookup is done via comparators
    // TODO: optimize this
    if (orderedGroups != null)
      orderedGroups.remove(group);

    group.topDoc = docBase + doc;
    // group.topDocScore = scorer.score();
    int tmp = spareSlot; spareSlot = group.comparatorSlot; group.comparatorSlot=tmp;  // swap slots

    // re-add the changed group
    if (orderedGroups != null)
      orderedGroups.add(group);
  }

  void buildSet() {
    Comparator<SearchGroup> comparator = new Comparator<SearchGroup>() {
      public int compare(SearchGroup o1, SearchGroup o2) {
        for (int i = 0;; i++) {
          FieldComparator fc = comparators[i];
          int c = reversed[i] * fc.compare(o1.comparatorSlot, o2.comparatorSlot);
          if (c != 0) {
            return c;
          } else if (i == comparators.length - 1) {
            return o1.topDoc - o2.topDoc;
          }
        }
      }
    };

    orderedGroups = new TreeSet<SearchGroup>(comparator);
    orderedGroups.addAll(groupMap.values());
    if (orderedGroups.size() == 0) return;
    for (FieldComparator fc : comparators)
      fc.setBottom(orderedGroups.last().comparatorSlot);
  }

  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    this.docBase = docBase;
    docValues = vs.getValues(context, reader);
    filler = docValues.getValueFiller();
    mval = filler.getValue();
    for (FieldComparator fc : comparators)
      fc.setNextReader(reader, docBase);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  public int getMatches() {
    return matches;
  }
}


/**
 * This class allows a different sort within a group than what is used between groups.
 * Sorting between groups is done by the sort value of the first (highest ranking)
 * document in that group.
 */
class TopGroupSortCollector extends TopGroupCollector {

  IndexReader reader;
  Sort groupSort;

  public TopGroupSortCollector(ValueSource groupByVS, Map vsContext, Sort sort, Sort groupSort, int nGroups) throws IOException {
    super(groupByVS, vsContext, sort, nGroups);
    this.groupSort = groupSort;
  }

  void constructComparators(FieldComparator[] comparators, int[] reversed, SortField[] sortFields, int size) throws IOException {
    for (int i = 0; i < sortFields.length; i++) {
      SortField sortField = sortFields[i];
      reversed[i] = sortField.getReverse() ? -1 : 1;
      comparators[i] = sortField.getComparator(size, i);
      if (scorer != null) comparators[i].setScorer(scorer);
      if (reader != null) comparators[i] = comparators[i].setNextReader(reader, docBase);
    }
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    super.setScorer(scorer);
    for (SearchGroup searchGroup : groupMap.values()) {
      for (FieldComparator fc : searchGroup.sortGroupComparators) {
        fc.setScorer(scorer);
      }
    }
  }

  @Override
  public void collect(int doc) throws IOException {
    matches++;
    filler.fillValue(doc);
    SearchGroup group = groupMap.get(mval);
    if (group == null) {
      int num = groupMap.size();
      if (groupMap.size() < nGroups) {
        SearchGroup sg = new SearchGroup();
        SortField[] sortGroupFields = groupSort.getSort();
        sg.sortGroupComparators = new FieldComparator[sortGroupFields.length];
        sg.sortGroupReversed = new int[sortGroupFields.length];
        constructComparators(sg.sortGroupComparators, sg.sortGroupReversed, sortGroupFields, 1);

        sg.groupValue = mval.duplicate();
        sg.comparatorSlot = num++;
        sg.matches = 1;
        sg.topDoc = docBase + doc;
        // sg.topDocScore = scorer.score();
        for (FieldComparator fc : comparators)
          fc.copy(sg.comparatorSlot, doc);
        for (FieldComparator fc : sg.sortGroupComparators) {
          fc.copy(0, doc);
          fc.setBottom(0);
        }
        groupMap.put(sg.groupValue, sg);
        return;
      }

      if (orderedGroups == null) {
        buildSet();
      }

      SearchGroup leastSignificantGroup = orderedGroups.last();
      for (int i = 0;; i++) {
        final int c = leastSignificantGroup.sortGroupReversed[i] * leastSignificantGroup.sortGroupComparators[i].compareBottom(doc);
        if (c < 0) {
          // Definitely not competitive.
          return;
        } else if (c > 0) {
          // Definitely competitive.
          break;
        } else if (i == leastSignificantGroup.sortGroupComparators.length - 1) {
          // Here c=0. If we're at the last comparator, this doc is not
          // competitive, since docs are visited in doc Id order, which means
          // this doc cannot compete with any other document in the queue.
          return;
        }
      }

      // remove current smallest group
      SearchGroup smallest = orderedGroups.pollLast();
      groupMap.remove(smallest.groupValue);

      // reuse the removed SearchGroup
      smallest.groupValue.copy(mval);
      smallest.matches = 1;
      smallest.topDoc = docBase + doc;
      // smallest.topDocScore = scorer.score();
      for (FieldComparator fc : comparators)
        fc.copy(smallest.comparatorSlot, doc);
      for (FieldComparator fc : smallest.sortGroupComparators) {
        fc.copy(0, doc);
        fc.setBottom(0);
      }

      groupMap.put(smallest.groupValue, smallest);
      orderedGroups.add(smallest);

      for (FieldComparator fc : comparators)
        fc.setBottom(orderedGroups.last().comparatorSlot);
      for (FieldComparator fc : smallest.sortGroupComparators)
        fc.setBottom(0);

      return;
    }

    //
    // update existing group
    //

    group.matches++; // TODO: these aren't valid if the group is every discarded then re-added.  keep track if there have been discards?

    for (int i = 0;; i++) {
      FieldComparator fc = group.sortGroupComparators[i];

      final int c = group.sortGroupReversed[i] * fc.compareBottom(doc);
      if (c < 0) {
        // Definitely not competitive.
        return;
      } else if (c > 0) {
        // Definitely competitive.
        // Set remaining comparators
        for (int j = 0; j < group.sortGroupComparators.length; j++) {
          group.sortGroupComparators[j].copy(0, doc);
          group.sortGroupComparators[j].setBottom(0);
        }
        for (FieldComparator comparator : comparators) comparator.copy(spareSlot, doc);
        break;
      } else if (i == group.sortGroupComparators.length - 1) {
        // Here c=0. If we're at the last comparator, this doc is not
        // competitive, since docs are visited in doc Id order, which means
        // this doc cannot compete with any other document in the queue.
        return;
      }
    }

    // remove before updating the group since lookup is done via comparators
    // TODO: optimize this
    if (orderedGroups != null)
      orderedGroups.remove(group);

    group.topDoc = docBase + doc;
    // group.topDocScore = scorer.score();
    int tmp = spareSlot; spareSlot = group.comparatorSlot; group.comparatorSlot=tmp;  // swap slots

    // re-add the changed group
    if (orderedGroups != null)
      orderedGroups.add(group);
  }

  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    super.setNextReader(reader, docBase);
    this.reader = reader;
    for (SearchGroup searchGroup : groupMap.values()) {
      for (FieldComparator fc : searchGroup.sortGroupComparators) {
        fc.setNextReader(reader, docBase);
      }
    }
  }

}


class Phase2GroupCollector extends Collector {
  final HashMap<MutableValue, SearchGroupDocs> groupMap;
  final ValueSource vs;
  final Map context;

  DocValues docValues;
  DocValues.ValueFiller filler;
  MutableValue mval;
  Scorer scorer;
  int docBase;

  // TODO: may want to decouple from the phase1 collector
  public Phase2GroupCollector(TopGroupCollector topGroups, ValueSource groupByVS, Map vsContext, Sort sort, int docsPerGroup, boolean getScores) throws IOException {
    boolean getSortFields = false;

    groupMap = new HashMap<MutableValue, SearchGroupDocs>(topGroups.groupMap.size());
    for (SearchGroup group : topGroups.groupMap.values()) {
      SearchGroupDocs groupDocs = new SearchGroupDocs();
      groupDocs.groupValue = group.groupValue;
      groupDocs.collector = TopFieldCollector.create(sort, docsPerGroup, getSortFields, getScores, getScores, true);
      groupMap.put(groupDocs.groupValue, groupDocs);
    }

    this.vs = groupByVS;
    this.context = vsContext;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
    for (SearchGroupDocs group : groupMap.values())
      group.collector.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    filler.fillValue(doc);
    SearchGroupDocs group = groupMap.get(mval);
    if (group == null) return;
    group.matches++;
    group.collector.collect(doc);
  }

  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    this.docBase = docBase;
    docValues = vs.getValues(context, reader);
    filler = docValues.getValueFiller();
    mval = filler.getValue();
    for (SearchGroupDocs group : groupMap.values())
      group.collector.setNextReader(reader, docBase);
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }
}

// TODO: merge with SearchGroup or not?
// ad: don't need to build a new hashmap
// disad: blows up the size of SearchGroup if we need many of them, and couples implementations
class SearchGroupDocs {
  public MutableValue groupValue;
  int matches;
  TopFieldCollector collector;
}

