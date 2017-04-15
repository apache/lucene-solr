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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SimpleCollector;

/**
 * A second pass grouping collector that keeps track of distinct values for a specified field for the top N group.
 *
 * @lucene.experimental
 */
public class DistinctValuesCollector<T, R> extends SecondPassGroupingCollector<T> {

  /**
   * Create a DistinctValuesCollector
   * @param groupSelector the group selector to determine the top-level groups
   * @param groups        the top-level groups to collect for
   * @param valueSelector a group selector to determine which values to collect per-group
   */
  public DistinctValuesCollector(GroupSelector<T> groupSelector, Collection<SearchGroup<T>> groups,
                                       GroupSelector<R> valueSelector) {
    super(groupSelector, groups, new DistinctValuesReducer<>(valueSelector));
  }

  private static class ValuesCollector<R> extends SimpleCollector {

    final GroupSelector<R> valueSelector;
    final Set<R> values = new HashSet<>();

    private ValuesCollector(GroupSelector<R> valueSelector) {
      this.valueSelector = valueSelector;
    }

    @Override
    public void collect(int doc) throws IOException {
      if (valueSelector.advanceTo(doc) == GroupSelector.State.ACCEPT) {
        R value = valueSelector.currentValue();
        if (values.contains(value) == false)
          values.add(valueSelector.copyValue());
      }
      else {
        if (values.contains(null) == false)
          values.add(null);
      }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      valueSelector.setNextReader(context);
    }

    @Override
    public boolean needsScores() {
      return false;
    }
  }

  private static class DistinctValuesReducer<T, R> extends GroupReducer<T, ValuesCollector<R>> {

    final GroupSelector<R> valueSelector;

    private DistinctValuesReducer(GroupSelector<R> valueSelector) {
      this.valueSelector = valueSelector;
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    protected ValuesCollector<R> newCollector() {
      return new ValuesCollector<>(valueSelector);
    }
  }

  /**
   * Returns all unique values for each top N group.
   *
   * @return all unique values for each top N group
   */
  public List<GroupCount<T, R>> getGroups() {
    List<GroupCount<T, R>> counts = new ArrayList<>();
    for (SearchGroup<T> group : groups) {
      @SuppressWarnings("unchecked")
      ValuesCollector<R> vc = (ValuesCollector<R>) groupReducer.getCollector(group.groupValue);
      counts.add(new GroupCount<>(group.groupValue, vc.values));
    }
    return counts;
  }

  /**
   * Returned by {@link DistinctValuesCollector#getGroups()},
   * representing the value and set of distinct values for the group.
   */
  public static class GroupCount<T, R> {

    public final T groupValue;
    public final Set<R> uniqueValues;

    public GroupCount(T groupValue, Set<R> values) {
      this.groupValue = groupValue;
      this.uniqueValues = values;
    }
  }

}
