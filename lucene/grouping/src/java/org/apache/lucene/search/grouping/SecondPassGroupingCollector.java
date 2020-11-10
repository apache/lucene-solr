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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

/**
 * SecondPassGroupingCollector runs over an already collected set of
 * groups, further applying a {@link GroupReducer} to each group
 *
 * @see TopGroupsCollector
 * @see DistinctValuesCollector
 *
 * @lucene.experimental
 */
public class SecondPassGroupingCollector<T> extends SimpleCollector {

  protected final GroupSelector<T> groupSelector;
  protected final Collection<SearchGroup<T>> groups;
  protected final GroupReducer<T, ?> groupReducer;

  protected int totalHitCount;
  protected int totalGroupedHitCount;

  /**
   * Create a new SecondPassGroupingCollector
   * @param groupSelector   the GroupSelector that defines groups for this search
   * @param groups          the groups to collect documents for
   * @param reducer         the reducer to apply to each group
   */
  public SecondPassGroupingCollector(GroupSelector<T> groupSelector, Collection<SearchGroup<T>> groups, GroupReducer<T, ?> reducer) {

    //System.out.println("SP init");
    if (groups.isEmpty()) {
      throw new IllegalArgumentException("no groups to collect (groups is empty)");
    }

    this.groupSelector = Objects.requireNonNull(groupSelector);
    this.groupSelector.setGroups(groups);

    this.groups = Objects.requireNonNull(groups);
    this.groupReducer = reducer;
    reducer.setGroups(groups);
  }

  /**
   * @return the GroupSelector used in this collector
   */
  public GroupSelector<T> getGroupSelector() {
    return groupSelector;
  }

  @Override
  public ScoreMode scoreMode() {
    return groupReducer.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    groupSelector.setScorer(scorer);
    groupReducer.setScorer(scorer);
  }

  @Override
  public void collect(int doc) throws IOException {
    totalHitCount++;
    if (groupSelector.advanceTo(doc) == GroupSelector.State.SKIP)
      return;
    totalGroupedHitCount++;
    T value = groupSelector.currentValue();
    groupReducer.collect(value, doc);
  }

  @Override
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    groupReducer.setNextReader(readerContext);
    groupSelector.setNextReader(readerContext);
  }

}
