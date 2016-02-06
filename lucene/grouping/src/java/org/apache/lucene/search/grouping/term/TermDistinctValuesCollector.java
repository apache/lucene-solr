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
package org.apache.lucene.search.grouping.term;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.grouping.AbstractDistinctValuesCollector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * A term based implementation of {@link org.apache.lucene.search.grouping.AbstractDistinctValuesCollector} that relies
 * on {@link SortedDocValues} to count the distinct values per group.
 *
 * @lucene.experimental
 */
public class TermDistinctValuesCollector extends AbstractDistinctValuesCollector<TermDistinctValuesCollector.GroupCount> {

  private final String groupField;
  private final String countField;
  private final List<GroupCount> groups;
  private final SentinelIntSet ordSet;
  private final GroupCount groupCounts[];

  private SortedDocValues groupFieldTermIndex;
  private SortedDocValues countFieldTermIndex;

  /**
   * Constructs {@link TermDistinctValuesCollector} instance.
   *
   * @param groupField The field to group by
   * @param countField The field to count distinct values for
   * @param groups The top N groups, collected during the first phase search
   */
  public TermDistinctValuesCollector(String groupField, String countField, Collection<SearchGroup<BytesRef>> groups) {
    this.groupField = groupField;
    this.countField = countField;
    this.groups = new ArrayList<>(groups.size());
    for (SearchGroup<BytesRef> group : groups) {
      this.groups.add(new GroupCount(group.groupValue));
    }
    ordSet = new SentinelIntSet(groups.size(), -2);
    groupCounts = new GroupCount[ordSet.keys.length];
  }

  @Override
  public void collect(int doc) throws IOException {
    int slot = ordSet.find(groupFieldTermIndex.getOrd(doc));
    if (slot < 0) {
      return;
    }

    GroupCount gc = groupCounts[slot];
    int countOrd = countFieldTermIndex.getOrd(doc);
    if (doesNotContainOrd(countOrd, gc.ords)) {
      if (countOrd == -1) {
        gc.uniqueValues.add(null);
      } else {
        BytesRef term = BytesRef.deepCopyOf(countFieldTermIndex.lookupOrd(countOrd));
        gc.uniqueValues.add(term);
      }

      gc.ords = Arrays.copyOf(gc.ords, gc.ords.length + 1);
      gc.ords[gc.ords.length - 1] = countOrd;
      if (gc.ords.length > 1) {
        Arrays.sort(gc.ords);
      }
    }
  }

  private boolean doesNotContainOrd(int ord, int[] ords) {
    if (ords.length == 0) {
      return true;
    } else if (ords.length == 1) {
      return ord != ords[0];
    }
    return Arrays.binarySearch(ords, ord) < 0;
  }

  @Override
  public List<GroupCount> getGroups() {
    return groups;
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    groupFieldTermIndex = DocValues.getSorted(context.reader(), groupField);
    countFieldTermIndex = DocValues.getSorted(context.reader(), countField);
    ordSet.clear();
    for (GroupCount group : groups) {
      int groupOrd = group.groupValue == null ? -1 : groupFieldTermIndex.lookupTerm(group.groupValue);
      if (group.groupValue != null && groupOrd < 0) {
        continue;
      }

      groupCounts[ordSet.put(groupOrd)] = group;
      group.ords = new int[group.uniqueValues.size()];
      Arrays.fill(group.ords, -2);
      int i = 0;
      for (BytesRef value : group.uniqueValues) {
        int countOrd = value == null ? -1 : countFieldTermIndex.lookupTerm(value);
        if (value == null || countOrd >= 0) {
          group.ords[i++] = countOrd;
        }
      }
    }
  }

  /** Holds distinct values for a single group.
   *
   * @lucene.experimental */
  public static class GroupCount extends AbstractDistinctValuesCollector.GroupCount<BytesRef> {

    int[] ords;

    GroupCount(BytesRef groupValue) {
      super(groupValue);
    }
  }

}
