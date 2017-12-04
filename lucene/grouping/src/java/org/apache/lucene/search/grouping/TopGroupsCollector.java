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

import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;

/**
 * A second-pass collector that collects the TopDocs for each group, and
 * returns them as a {@link TopGroups} object
 *
 * @param <T> the type of the group value
 */
public class TopGroupsCollector<T> extends SecondPassGroupingCollector<T> {

  private final Sort groupSort;
  private final Sort withinGroupSort;
  private final int maxDocsPerGroup;

  /**
   * Create a new TopGroupsCollector
   * @param groupSelector     the group selector used to define groups
   * @param groups            the groups to collect TopDocs for
   * @param groupSort         the order in which groups are returned
   * @param withinGroupSort   the order in which documents are sorted in each group
   * @param maxDocsPerGroup   the maximum number of docs to collect for each group
   * @param getScores         if true, record the scores of all docs in each group
   * @param getMaxScores      if true, record the maximum score for each group
   * @param fillSortFields    if true, record the sort field values for all docs
   */
  public TopGroupsCollector(GroupSelector<T> groupSelector, Collection<SearchGroup<T>> groups, Sort groupSort, Sort withinGroupSort,
                            int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields) {
    super(groupSelector, groups,
        new TopDocsReducer<>(withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields));
    this.groupSort = Objects.requireNonNull(groupSort);
    this.withinGroupSort = Objects.requireNonNull(withinGroupSort);
    this.maxDocsPerGroup = maxDocsPerGroup;

  }

  private static class TopDocsReducer<T> extends GroupReducer<T, TopDocsCollector<?>> {

    private final Supplier<TopDocsCollector<?>> supplier;
    private final boolean needsScores;

    TopDocsReducer(Sort withinGroupSort,
                   int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields) {
      this.needsScores = getScores || getMaxScores || withinGroupSort.needsScores();
      this.supplier = withinGroupSort == Sort.RELEVANCE ?
          () -> TopScoreDocCollector.create(maxDocsPerGroup) :
          () -> TopFieldCollector.create(withinGroupSort, maxDocsPerGroup, fillSortFields, getScores, getMaxScores, true); // TODO: disable exact counts?
    }

    @Override
    public boolean needsScores() {
      return needsScores;
    }

    @Override
    protected TopDocsCollector<?> newCollector() {
      return supplier.get();
    }
  }

  /**
   * Get the TopGroups recorded by this collector
   * @param withinGroupOffset the offset within each group to start collecting documents
   */
  public TopGroups<T> getTopGroups(int withinGroupOffset) {
    @SuppressWarnings({"unchecked","rawtypes"})
    final GroupDocs<T>[] groupDocsResult = (GroupDocs<T>[]) new GroupDocs[groups.size()];

    int groupIDX = 0;
    float maxScore = Float.MIN_VALUE;
    for(SearchGroup<T> group : groups) {
      TopDocsCollector<?> collector = (TopDocsCollector<?>) groupReducer.getCollector(group.groupValue);
      final TopDocs topDocs = collector.topDocs(withinGroupOffset, maxDocsPerGroup);
      groupDocsResult[groupIDX++] = new GroupDocs<>(Float.NaN,
          topDocs.getMaxScore(),
          topDocs.totalHits,
          topDocs.scoreDocs,
          group.groupValue,
          group.sortValues);
      maxScore = Math.max(maxScore, topDocs.getMaxScore());
    }

    return new TopGroups<>(groupSort.getSort(),
        withinGroupSort.getSort(),
        totalHitCount, totalGroupedHitCount, groupDocsResult,
        maxScore);
  }


}
