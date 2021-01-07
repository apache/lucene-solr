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
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.ArrayUtil;

/**
 * A second-pass collector that collects the TopDocs for each group, and returns them as a {@link
 * TopGroups} object
 *
 * @param <T> the type of the group value
 */
public class TopGroupsCollector<T> extends SecondPassGroupingCollector<T> {

  private final Sort groupSort;
  private final Sort withinGroupSort;
  private final int maxDocsPerGroup;

  /**
   * Create a new TopGroupsCollector
   *
   * @param groupSelector the group selector used to define groups
   * @param groups the groups to collect TopDocs for
   * @param groupSort the order in which groups are returned
   * @param withinGroupSort the order in which documents are sorted in each group
   * @param maxDocsPerGroup the maximum number of docs to collect for each group
   * @param getMaxScores if true, record the maximum score for each group
   */
  public TopGroupsCollector(
      GroupSelector<T> groupSelector,
      Collection<SearchGroup<T>> groups,
      Sort groupSort,
      Sort withinGroupSort,
      int maxDocsPerGroup,
      boolean getMaxScores) {
    super(
        groupSelector,
        groups,
        new TopDocsReducer<>(withinGroupSort, maxDocsPerGroup, getMaxScores));
    this.groupSort = Objects.requireNonNull(groupSort);
    this.withinGroupSort = Objects.requireNonNull(withinGroupSort);
    this.maxDocsPerGroup = maxDocsPerGroup;
  }

  private static class MaxScoreCollector extends SimpleCollector {
    private Scorable scorer;
    private float maxScore = Float.MIN_VALUE;
    private boolean collectedAnyHits = false;

    public MaxScoreCollector() {}

    public float getMaxScore() {
      return collectedAnyHits ? maxScore : Float.NaN;
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }

    @Override
    public void setScorer(Scorable scorer) {
      this.scorer = scorer;
    }

    @Override
    public void collect(int doc) throws IOException {
      collectedAnyHits = true;
      maxScore = Math.max(scorer.score(), maxScore);
    }
  }

  private static class TopDocsAndMaxScoreCollector extends FilterCollector {
    private final TopDocsCollector<?> topDocsCollector;
    private final MaxScoreCollector maxScoreCollector;
    private final boolean sortedByScore;

    public TopDocsAndMaxScoreCollector(
        boolean sortedByScore,
        TopDocsCollector<?> topDocsCollector,
        MaxScoreCollector maxScoreCollector) {
      super(MultiCollector.wrap(topDocsCollector, maxScoreCollector));
      this.sortedByScore = sortedByScore;
      this.topDocsCollector = topDocsCollector;
      this.maxScoreCollector = maxScoreCollector;
    }
  }

  private static class TopDocsReducer<T> extends GroupReducer<T, TopDocsAndMaxScoreCollector> {

    private final Supplier<TopDocsAndMaxScoreCollector> supplier;
    private final boolean needsScores;

    TopDocsReducer(Sort withinGroupSort, int maxDocsPerGroup, boolean getMaxScores) {
      this.needsScores = getMaxScores || withinGroupSort.needsScores();
      if (withinGroupSort == Sort.RELEVANCE) {
        supplier =
            () ->
                new TopDocsAndMaxScoreCollector(
                    true, TopScoreDocCollector.create(maxDocsPerGroup, Integer.MAX_VALUE), null);
      } else {
        supplier =
            () -> {
              TopFieldCollector topDocsCollector =
                  TopFieldCollector.create(
                      withinGroupSort,
                      maxDocsPerGroup,
                      Integer.MAX_VALUE); // TODO: disable exact counts?
              MaxScoreCollector maxScoreCollector = getMaxScores ? new MaxScoreCollector() : null;
              return new TopDocsAndMaxScoreCollector(false, topDocsCollector, maxScoreCollector);
            };
      }
    }

    @Override
    public boolean needsScores() {
      return needsScores;
    }

    @Override
    protected TopDocsAndMaxScoreCollector newCollector() {
      return supplier.get();
    }
  }

  /**
   * Get the TopGroups recorded by this collector
   *
   * @param withinGroupOffset the offset within each group to start collecting documents
   */
  public TopGroups<T> getTopGroups(int withinGroupOffset) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    final GroupDocs<T>[] groupDocsResult = (GroupDocs<T>[]) new GroupDocs[groups.size()];

    int groupIDX = 0;
    float maxScore = Float.MIN_VALUE;
    for (SearchGroup<T> group : groups) {
      TopDocsAndMaxScoreCollector collector =
          (TopDocsAndMaxScoreCollector) groupReducer.getCollector(group.groupValue);
      final TopDocs topDocs;
      final float groupMaxScore;
      if (collector.sortedByScore) {
        TopDocs allTopDocs = collector.topDocsCollector.topDocs();
        groupMaxScore =
            allTopDocs.scoreDocs.length == 0 ? Float.NaN : allTopDocs.scoreDocs[0].score;
        if (allTopDocs.scoreDocs.length <= withinGroupOffset) {
          topDocs = new TopDocs(allTopDocs.totalHits, new ScoreDoc[0]);
        } else {
          topDocs =
              new TopDocs(
                  allTopDocs.totalHits,
                  ArrayUtil.copyOfSubArray(
                      allTopDocs.scoreDocs,
                      withinGroupOffset,
                      Math.min(allTopDocs.scoreDocs.length, withinGroupOffset + maxDocsPerGroup)));
        }
      } else {
        topDocs = collector.topDocsCollector.topDocs(withinGroupOffset, maxDocsPerGroup);
        if (collector.maxScoreCollector == null) {
          groupMaxScore = Float.NaN;
        } else {
          groupMaxScore = collector.maxScoreCollector.getMaxScore();
        }
      }

      groupDocsResult[groupIDX++] =
          new GroupDocs<>(
              Float.NaN,
              groupMaxScore,
              topDocs.totalHits,
              topDocs.scoreDocs,
              group.groupValue,
              group.sortValues);
      maxScore = Math.max(maxScore, groupMaxScore);
    }

    return new TopGroups<>(
        groupSort.getSort(),
        withinGroupSort.getSort(),
        totalHitCount,
        totalGroupedHitCount,
        groupDocsResult,
        maxScore);
  }
}
