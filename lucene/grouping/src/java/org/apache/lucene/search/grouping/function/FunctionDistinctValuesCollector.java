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
package org.apache.lucene.search.grouping.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.grouping.AbstractDistinctValuesCollector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.mutable.MutableValue;

import java.io.IOException;
import java.util.*;

/**
 * Function based implementation of {@link org.apache.lucene.search.grouping.AbstractDistinctValuesCollector}.
 *
 * @lucene.experimental
 */
public class FunctionDistinctValuesCollector extends AbstractDistinctValuesCollector<FunctionDistinctValuesCollector.GroupCount> {

  private final Map<?, ?> vsContext;
  private final ValueSource groupSource;
  private final ValueSource countSource;
  private final Map<MutableValue, GroupCount> groupMap;

  private FunctionValues.ValueFiller groupFiller;
  private FunctionValues.ValueFiller countFiller;
  private MutableValue groupMval;
  private MutableValue countMval;

  public FunctionDistinctValuesCollector(Map<?, ?> vsContext, ValueSource groupSource, ValueSource countSource, Collection<SearchGroup<MutableValue>> groups) {
    this.vsContext = vsContext;
    this.groupSource = groupSource;
    this.countSource = countSource;
    groupMap = new LinkedHashMap<>();
    for (SearchGroup<MutableValue> group : groups) {
      groupMap.put(group.groupValue, new GroupCount(group.groupValue));
    }
  }

  @Override
  public List<GroupCount> getGroups() {
    return new ArrayList<>(groupMap.values());
  }

  @Override
  public void collect(int doc) throws IOException {
    groupFiller.fillValue(doc);
    GroupCount groupCount = groupMap.get(groupMval);
    if (groupCount != null) {
      countFiller.fillValue(doc);
      groupCount.uniqueValues.add(countMval.duplicate());
    }
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    FunctionValues values = groupSource.getValues(vsContext, context);
    groupFiller = values.getValueFiller();
    groupMval = groupFiller.getValue();
    values = countSource.getValues(vsContext, context);
    countFiller = values.getValueFiller();
    countMval = countFiller.getValue();
  }

  /** Holds distinct values for a single group.
   *
   * @lucene.experimental */
  public static class GroupCount extends AbstractDistinctValuesCollector.GroupCount<MutableValue> {

    GroupCount(MutableValue groupValue) {
      super(groupValue);
    }

  }

}
