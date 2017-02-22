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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AllGroupHeadsCollector;
import org.apache.lucene.search.grouping.AllGroupsCollector;
import org.apache.lucene.search.grouping.FirstPassGroupingCollector;
import org.apache.lucene.search.grouping.SecondPassGroupingCollector;
import org.apache.lucene.search.grouping.Grouper;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.mutable.MutableValue;

/**
 * Collector factory for grouping by ValueSource
 */
public class FunctionGrouper extends Grouper<MutableValue> {

  private final ValueSource valueSource;
  private final Map<?, ?> context;

  /**
   * Create a Grouper for the provided ValueSource and context
   */
  public FunctionGrouper(ValueSource valueSource, Map<?, ?> context) {
    this.valueSource = valueSource;
    this.context = context;
  }

  @Override
  public FirstPassGroupingCollector<MutableValue> getFirstPassCollector(Sort sort, int count) throws IOException {
    return new FunctionFirstPassGroupingCollector(valueSource, context, sort, count);
  }

  @Override
  public AllGroupHeadsCollector<MutableValue> getGroupHeadsCollector(Sort sort) {
    return new FunctionAllGroupHeadsCollector(valueSource, context, sort);
  }

  @Override
  public AllGroupsCollector<MutableValue> getAllGroupsCollector() {
    return new FunctionAllGroupsCollector(valueSource, context);
  }

  @Override
  public SecondPassGroupingCollector<MutableValue> getSecondPassCollector(Collection<SearchGroup<MutableValue>> searchGroups, Sort groupSort, Sort withinGroupSort, int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields) throws IOException {
    return new FunctionSecondPassGroupingCollector(searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields, valueSource, context);
  }
}
