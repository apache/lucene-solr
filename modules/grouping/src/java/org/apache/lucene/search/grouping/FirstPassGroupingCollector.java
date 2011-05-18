package org.apache.lucene.search.grouping;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;

/** FirstPassGroupingCollector is the first of two passes necessary
 *  to collect grouped hits.  This pass gathers the top N sorted
 *  groups.
 *
 *  <p>See {@link org.apache.lucene.search.grouping} for more
 *  details including a full code example.</p>
 *
 * @lucene.experimental
 */

public class FirstPassGroupingCollector extends Collector {

  private final String groupField;
  private final Sort groupSort;
  private final FieldComparator[] comparators;
  private final int[] reversed;
  private final int topNGroups;
  private final HashMap<BytesRef, CollectedSearchGroup> groupMap;
  private final BytesRef scratchBytesRef = new BytesRef();
  private final int compIDXEnd;

  // Set once we reach topNGroups unique groups:
  private TreeSet<CollectedSearchGroup> orderedGroups;
  private int docBase;
  private int spareSlot;
  private FieldCache.DocTermsIndex index;

  /**
   * Create the first pass collector.
   *
   *  @param groupField The field used to group
   *    documents. This field must be single-valued and
   *    indexed (FieldCache is used to access its value
   *    per-document).
   *  @param groupSort The {@link Sort} used to sort the
   *    groups.  The top sorted document within each group
   *    according to groupSort, determines how that group
   *    sorts against other groups.  This must be non-null,
   *    ie, if you want to groupSort by relevance use
   *    Sort.RELEVANCE.
   *  @param topNGroups How many top groups to keep.
   */
  public FirstPassGroupingCollector(String groupField, Sort groupSort, int topNGroups) throws IOException {
    if (topNGroups < 1) {
      throw new IllegalArgumentException("topNGroups must be >= 1 (got " + topNGroups + ")");
    }

    this.groupField = groupField;
    // TODO: allow null groupSort to mean "by relevance",
    // and specialize it?
    this.groupSort = groupSort;

    this.topNGroups = topNGroups;

    final SortField[] sortFields = groupSort.getSort();
    comparators = new FieldComparator[sortFields.length];
    compIDXEnd = comparators.length - 1;
    reversed = new int[sortFields.length];
    for (int i = 0; i < sortFields.length; i++) {
      final SortField sortField = sortFields[i];

      // use topNGroups + 1 so we have a spare slot to use for comparing (tracked by this.spareSlot):
      comparators[i] = sortField.getComparator(topNGroups + 1, i);
      reversed[i] = sortField.getReverse() ? -1 : 1;
    }

    spareSlot = topNGroups;
    groupMap = new HashMap<BytesRef, CollectedSearchGroup>(topNGroups);
  }

  /** Returns top groups, starting from offset.  This may
   *  return null, if no groups were collected, or if the
   *  number of unique groups collected is <= offset. */
  public Collection<SearchGroup> getTopGroups(int groupOffset, boolean fillFields) {

    //System.out.println("FP.getTopGroups groupOffset=" + groupOffset + " fillFields=" + fillFields + " groupMap.size()=" + groupMap.size());

    if (groupOffset < 0) {
      throw new IllegalArgumentException("groupOffset must be >= 0 (got " + groupOffset + ")");
    }

    if (groupMap.size() <= groupOffset) {
      return null;
    }

    if (orderedGroups == null) {
      buildSortedSet();
    }

    final Collection<SearchGroup> result = new ArrayList<SearchGroup>();
    int upto = 0;
    final int sortFieldCount = groupSort.getSort().length;
    for(CollectedSearchGroup group : orderedGroups) {
      if (upto++ < groupOffset) {
        continue;
      }
      //System.out.println("  group=" + (group.groupValue == null ? "null" : group.groupValue.utf8ToString()));
      SearchGroup searchGroup = new SearchGroup();
      searchGroup.groupValue = group.groupValue;
      if (fillFields) {
        searchGroup.sortValues = new Comparable[sortFieldCount];
        for(int sortFieldIDX=0;sortFieldIDX<sortFieldCount;sortFieldIDX++) {
          searchGroup.sortValues[sortFieldIDX] = comparators[sortFieldIDX].value(group.comparatorSlot);
        }
      }
      result.add(searchGroup);
    }
    //System.out.println("  return " + result.size() + " groups");
    return result;
  }

  public String getGroupField() {
    return groupField;
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    for (FieldComparator comparator : comparators) {
      comparator.setScorer(scorer);
    }
  }

  @Override
  public void collect(int doc) throws IOException {
    //System.out.println("FP.collect doc=" + doc);

    // If orderedGroups != null we already have collected N groups and
    // can short circuit by comparing this document to the bottom group,
    // without having to find what group this document belongs to.
    
    // Even if this document belongs to a group in the top N, we'll know that
    // we don't have to update that group.

    // Downside: if the number of unique groups is very low, this is
    // wasted effort as we will most likely be updating an existing group.
    if (orderedGroups != null) {
      for (int compIDX = 0;; compIDX++) {
        final int c = reversed[compIDX] * comparators[compIDX].compareBottom(doc);
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
    }

    // TODO: should we add option to mean "ignore docs that
    // don't have the group field" (instead of stuffing them
    // under null group)?
    final int ord = index.getOrd(doc);
    //System.out.println("  ord=" + ord);

    final BytesRef br = ord == 0 ? null : index.lookup(ord, scratchBytesRef);
    //System.out.println("  group=" + (br == null ? "null" : br.utf8ToString()));

    final CollectedSearchGroup group = groupMap.get(br);

    if (group == null) {

      // First time we are seeing this group, or, we've seen
      // it before but it fell out of the top N and is now
      // coming back

      if (groupMap.size() < topNGroups) {

        // Still in startup transient: we have not
        // seen enough unique groups to start pruning them;
        // just keep collecting them

        // Add a new CollectedSearchGroup:
        CollectedSearchGroup sg = new CollectedSearchGroup();
        sg.groupValue = ord == 0 ? null : new BytesRef(scratchBytesRef);
        sg.comparatorSlot = groupMap.size();
        sg.topDoc = docBase + doc;
        for (FieldComparator fc : comparators) {
          fc.copy(sg.comparatorSlot, doc);
        }
        groupMap.put(sg.groupValue, sg);

        if (groupMap.size() == topNGroups) {
          // End of startup transient: we now have max
          // number of groups; from here on we will drop
          // bottom group when we insert new one:
          buildSortedSet();
        }

        return;
      }

      // We already tested that the document is competitive, so replace
      // the bottom group with this new group.

      // java 6-only: final CollectedSearchGroup bottomGroup = orderedGroups.pollLast();
      final CollectedSearchGroup bottomGroup = orderedGroups.last();
      orderedGroups.remove(bottomGroup);
      assert orderedGroups.size() == topNGroups -1;

      groupMap.remove(bottomGroup.groupValue);

      // reuse the removed CollectedSearchGroup
      if (br == null) {
        bottomGroup.groupValue = null;
      } else if (bottomGroup.groupValue != null) {
        bottomGroup.groupValue.copy(br);
      } else {
        bottomGroup.groupValue = new BytesRef(br);
      }
      bottomGroup.topDoc = docBase + doc;

      for (FieldComparator fc : comparators) {
        fc.copy(bottomGroup.comparatorSlot, doc);
      }

      groupMap.put(bottomGroup.groupValue, bottomGroup);
      orderedGroups.add(bottomGroup);
      assert orderedGroups.size() == topNGroups;

      final int lastComparatorSlot = orderedGroups.last().comparatorSlot;
      for (FieldComparator fc : comparators) {
        fc.setBottom(lastComparatorSlot);
      }

      return;
    }

    // Update existing group:
    for (int compIDX = 0;; compIDX++) {
      final FieldComparator fc = comparators[compIDX];
      fc.copy(spareSlot, doc);

      final int c = reversed[compIDX] * fc.compare(group.comparatorSlot, spareSlot);
      if (c < 0) {
        // Definitely not competitive.
        return;
      } else if (c > 0) {
        // Definitely competitive; set remaining comparators:
        for (int compIDX2=compIDX+1; compIDX2<comparators.length; compIDX2++) {
          comparators[compIDX2].copy(spareSlot, doc);
        }
        break;
      } else if (compIDX == compIDXEnd) {
        // Here c=0. If we're at the last comparator, this doc is not
        // competitive, since docs are visited in doc Id order, which means
        // this doc cannot compete with any other document in the queue.
        return;
      }
    }

    // Remove before updating the group since lookup is done via comparators
    // TODO: optimize this

    final CollectedSearchGroup prevLast;
    if (orderedGroups != null) {
      prevLast = orderedGroups.last();
      orderedGroups.remove(group);
      assert orderedGroups.size() == topNGroups-1;
    } else {
      prevLast = null;
    }

    group.topDoc = docBase + doc;

    // Swap slots
    final int tmp = spareSlot;
    spareSlot = group.comparatorSlot;
    group.comparatorSlot = tmp;

    // Re-add the changed group
    if (orderedGroups != null) {
      orderedGroups.add(group);
      assert orderedGroups.size() == topNGroups;
      final CollectedSearchGroup newLast = orderedGroups.last();
      // If we changed the value of the last group, or changed which group was last, then update bottom:
      if (group == newLast || prevLast != newLast) {
        for (FieldComparator fc : comparators) {
          fc.setBottom(newLast.comparatorSlot);
        }
      }
    }
  }

  private void buildSortedSet() {
    final Comparator<CollectedSearchGroup> comparator = new Comparator<CollectedSearchGroup>() {
      public int compare(CollectedSearchGroup o1, CollectedSearchGroup o2) {
        for (int compIDX = 0;; compIDX++) {
          FieldComparator fc = comparators[compIDX];
          final int c = reversed[compIDX] * fc.compare(o1.comparatorSlot, o2.comparatorSlot);
          if (c != 0) {
            return c;
          } else if (compIDX == compIDXEnd) {
            return o1.topDoc - o2.topDoc;
          }
        }
      }
    };

    orderedGroups = new TreeSet<CollectedSearchGroup>(comparator);
    orderedGroups.addAll(groupMap.values());
    assert orderedGroups.size() > 0;

    for (FieldComparator fc : comparators) {
      fc.setBottom(orderedGroups.last().comparatorSlot);
    }
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  @Override
  public void setNextReader(AtomicReaderContext readerContext) throws IOException {
    docBase = readerContext.docBase;
    index = FieldCache.DEFAULT.getTermsIndex(readerContext.reader, groupField);
    
    for (int i=0; i<comparators.length; i++) {
      comparators[i] = comparators[i].setNextReader(readerContext);
    }
  }
}

class CollectedSearchGroup extends SearchGroup {
  int topDoc;
  int comparatorSlot;
}
