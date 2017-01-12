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
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.SecondPassGroupingCollector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.search.grouping.TopGroups; //javadoc

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Concrete implementation of {@link SecondPassGroupingCollector} that groups based on
 * {@link ValueSource} instances.
 *
 * @lucene.experimental
 */
public class FunctionSecondPassGroupingCollector extends SecondPassGroupingCollector<MutableValue> {

  private final ValueSource groupByVS;
  private final Map<?, ?> vsContext;

  private FunctionValues.ValueFiller filler;
  private MutableValue mval;

  /**
   * Constructs a {@link FunctionSecondPassGroupingCollector} instance.
   *
   * @param searchGroups The {@link SearchGroup} instances collected during the first phase.
   * @param groupSort The group sort
   * @param withinGroupSort The sort inside a group
   * @param maxDocsPerGroup The maximum number of documents to collect inside a group
   * @param getScores Whether to include the scores
   * @param getMaxScores Whether to include the maximum score
   * @param fillSortFields Whether to fill the sort values in {@link TopGroups#withinGroupSort}
   * @param groupByVS The {@link ValueSource} to group by
   * @param vsContext The value source context
   * @throws IOException IOException When I/O related errors occur
   */
  public FunctionSecondPassGroupingCollector(Collection<SearchGroup<MutableValue>> searchGroups, Sort groupSort, Sort withinGroupSort, int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields, ValueSource groupByVS, Map<?, ?> vsContext) throws IOException {
    super(searchGroups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
    this.groupByVS = groupByVS;
    this.vsContext = vsContext;
  }

  @Override
  protected SearchGroupDocs<MutableValue> retrieveGroup(int doc) throws IOException {
    filler.fillValue(doc);
    return groupMap.get(mval);
  }

  @Override
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    super.doSetNextReader(readerContext);
    FunctionValues values = groupByVS.getValues(vsContext, readerContext);
    filler = values.getValueFiller();
    mval = filler.getValue();
  }

}
