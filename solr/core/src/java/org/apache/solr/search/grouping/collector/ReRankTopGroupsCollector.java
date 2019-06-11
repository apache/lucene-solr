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
package org.apache.solr.search.grouping.collector;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.GroupReducer;
import org.apache.lucene.search.grouping.GroupSelector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.TopGroupsCollector;
import org.apache.solr.search.AbstractReRankQuery;
import org.apache.solr.search.RankQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReRankTopGroupsCollector<T> extends TopGroupsCollector<T> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Sort groupSort;
  private Sort withinGroupSort;
  private int maxDocsPerGroup;

  /**
   * Create a new TopGroupsCollector
   * @param groupSelector     the group selector used to define groups
   * @param groups            the groups to collect TopDocs for
   * @param groupSort         the order in which groups are returned
   * @param withinGroupSort   the order in which documents are sorted in each group
   * @param maxDocsPerGroup   the maximum number of docs to collect for each group
   * @param getScores         if true, record the scores of all docs in each group
   * @param getMaxScores      if true, record the maximum score for each group
   * @param query             the rankQuery if provided by the user, null otherwise
   * @param searcher          an index searcher
   */
  public ReRankTopGroupsCollector(GroupSelector<T> groupSelector, Collection<SearchGroup<T>> groups, Sort groupSort, Sort withinGroupSort,
                                  int maxDocsPerGroup, boolean getScores, boolean getMaxScores, RankQuery query, IndexSearcher searcher) {
    super(new ReRankTopGroupsCollector.TopDocsReducer<>(withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, query, searcher), groupSelector, groups, groupSort, withinGroupSort, maxDocsPerGroup);
    this.groupSort = Objects.requireNonNull(groupSort);
    this.withinGroupSort = Objects.requireNonNull(withinGroupSort);
    this.maxDocsPerGroup = maxDocsPerGroup;
  }

  private static class TopDocsReducer<T> extends GroupReducer<T, TopDocsCollector<?>> {

    private final Supplier<TopDocsCollector<?>> supplier;
    private final boolean needsScores;
    private final RankQuery query;
    private final IndexSearcher searcher;
    private final Sort groupSort;
    private final int maxDocsPerGroup;

    TopDocsReducer(Sort withinGroupSort,
                   int maxDocsPerGroup, boolean getScores, boolean getMaxScores, RankQuery query, IndexSearcher searcher) {
      this.needsScores = getScores || getMaxScores || withinGroupSort.needsScores();
      if (withinGroupSort == Sort.RELEVANCE) {
        this.supplier = () -> TopScoreDocCollector.create(maxDocsPerGroup, Integer.MAX_VALUE);
      }
      else {
        this.supplier = () -> TopFieldCollector.create(withinGroupSort, maxDocsPerGroup, Integer.MAX_VALUE); // TODO: disable exact counts?
      }
      this.query = query;
      this.searcher = searcher;
      this.groupSort = withinGroupSort;
      this.maxDocsPerGroup = maxDocsPerGroup;

    }


    @Override
    public boolean needsScores() {
      return needsScores;
    }

    @Override
    protected TopDocsCollector<?> newCollector() {
      TopDocsCollector<?> collector = supplier.get();
      final int len;
      if (query instanceof AbstractReRankQuery){
        len = ((AbstractReRankQuery) query).getReRankDocs();
      } else {
        len = maxDocsPerGroup;
      }
      try {
        collector = this.query.getTopDocsCollector(len, groupSort, searcher);
      } catch (IOException e) {
        // this should never happen
        log.error("Cannot rerank groups ", e);
      }
      return collector;
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
      float topDocsMaxScore = topDocs.scoreDocs.length == 0 ? Float.NaN : topDocs.scoreDocs[0].score;
      groupDocsResult[groupIDX++] = new GroupDocs<>(Float.NaN,
          topDocsMaxScore,
          topDocs.totalHits,
          topDocs.scoreDocs,
          group.groupValue,
          group.sortValues);
      if (! Float.isNaN(topDocsMaxScore)) {
        maxScore = Math.max(maxScore, topDocsMaxScore);
      }
    }
    return new TopGroups<>(groupSort.getSort(),
        withinGroupSort.getSort(),
        totalHitCount, totalGroupedHitCount, groupDocsResult,
        maxScore);
  }
}
