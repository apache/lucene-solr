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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.grouping.AllGroupHeadsCollector;
import org.apache.lucene.util.mutable.MutableValue;

/**
 * An implementation of {@link AllGroupHeadsCollector} for retrieving the most relevant groups when grouping
 * by {@link ValueSource}.
 *
 * @lucene.experimental
 */
public class FunctionAllGroupHeadsCollector extends AllGroupHeadsCollector<MutableValue> {

  private final ValueSource groupBy;
  private final Map<?, ?> vsContext;
  private final Map<MutableValue, FunctionGroupHead> groups;
  private final Sort sortWithinGroup;

  private FunctionValues.ValueFiller filler;
  private MutableValue mval;
  private LeafReaderContext readerContext;
  private Scorer scorer;

  /**
   * Constructs a {@link FunctionAllGroupHeadsCollector} instance.
   *
   * @param groupBy The {@link ValueSource} to group by
   * @param vsContext The ValueSource context
   * @param sortWithinGroup The sort within a group
   */
  public FunctionAllGroupHeadsCollector(ValueSource groupBy, Map<?, ?> vsContext, Sort sortWithinGroup) {
    super(sortWithinGroup.getSort().length);
    groups = new HashMap<>();
    this.sortWithinGroup = sortWithinGroup;
    this.groupBy = groupBy;
    this.vsContext = vsContext;

    final SortField[] sortFields = sortWithinGroup.getSort();
    for (int i = 0; i < sortFields.length; i++) {
      reversed[i] = sortFields[i].getReverse() ? -1 : 1;
    }
  }

  @Override
  protected void retrieveGroupHeadAndAddIfNotExist(int doc) throws IOException {
    filler.fillValue(doc);
    FunctionGroupHead groupHead = groups.get(mval);
    if (groupHead == null) {
      MutableValue groupValue = mval.duplicate();
      groupHead = new FunctionGroupHead(groupValue, sortWithinGroup, doc);
      groups.put(groupValue, groupHead);
      temporalResult.stop = true;
    } else {
      temporalResult.stop = false;
    }
    this.temporalResult.groupHead = groupHead;
  }

  @Override
  protected Collection<FunctionGroupHead> getCollectedGroupHeads() {
    return groups.values();
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
    for (FunctionGroupHead groupHead : groups.values()) {
      for (LeafFieldComparator comparator : groupHead.leafComparators) {
        comparator.setScorer(scorer);
      }
    }
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    this.readerContext = context;
    FunctionValues values = groupBy.getValues(vsContext, context);
    filler = values.getValueFiller();
    mval = filler.getValue();

    for (FunctionGroupHead groupHead : groups.values()) {
      for (int i = 0; i < groupHead.comparators.length; i++) {
        groupHead.leafComparators[i] = groupHead.comparators[i].getLeafComparator(context);
      }
    }
  }

  /** Holds current head document for a single group.
   *
   * @lucene.experimental */
  public class FunctionGroupHead extends AllGroupHeadsCollector.GroupHead<MutableValue> {

    final FieldComparator<?>[] comparators;
    final LeafFieldComparator[] leafComparators;

    @SuppressWarnings({"unchecked","rawtypes"})
    private FunctionGroupHead(MutableValue groupValue, Sort sort, int doc) throws IOException {
      super(groupValue, doc + readerContext.docBase);
      final SortField[] sortFields = sort.getSort();
      comparators = new FieldComparator[sortFields.length];
      leafComparators = new LeafFieldComparator[sortFields.length];
      for (int i = 0; i < sortFields.length; i++) {
        comparators[i] = sortFields[i].getComparator(1, i);
        leafComparators[i] = comparators[i].getLeafComparator(readerContext);
        leafComparators[i].setScorer(scorer);
        leafComparators[i].copy(0, doc);
        leafComparators[i].setBottom(0);
      }
    }

    @Override
    public int compare(int compIDX, int doc) throws IOException {
      return leafComparators[compIDX].compareBottom(doc);
    }

    @Override
    public void updateDocHead(int doc) throws IOException {
      for (LeafFieldComparator comparator : leafComparators) {
        comparator.copy(0, doc);
        comparator.setBottom(0);
      }
      this.doc = doc + readerContext.docBase;
    }
  }

  @Override
  public boolean needsScores() {
    return sortWithinGroup.needsScores();
  }
}
