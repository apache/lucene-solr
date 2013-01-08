package org.apache.lucene.search.grouping.function;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.grouping.AbstractAllGroupHeadsCollector;
import org.apache.lucene.util.mutable.MutableValue;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link AbstractAllGroupHeadsCollector} for retrieving the most relevant groups when grouping
 * by {@link ValueSource}.
 *
 * @lucene.experimental
 */
public class FunctionAllGroupHeadsCollector extends AbstractAllGroupHeadsCollector<FunctionAllGroupHeadsCollector.GroupHead> {

  private final ValueSource groupBy;
  private final Map<?, ?> vsContext;
  private final Map<MutableValue, GroupHead> groups;
  private final Sort sortWithinGroup;

  private FunctionValues.ValueFiller filler;
  private MutableValue mval;
  private AtomicReaderContext readerContext;
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
    groups = new HashMap<MutableValue, GroupHead>();
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
    GroupHead groupHead = groups.get(mval);
    if (groupHead == null) {
      MutableValue groupValue = mval.duplicate();
      groupHead = new GroupHead(groupValue, sortWithinGroup, doc);
      groups.put(groupValue, groupHead);
      temporalResult.stop = true;
    } else {
      temporalResult.stop = false;
    }
    this.temporalResult.groupHead = groupHead;
  }

  @Override
  protected Collection<GroupHead> getCollectedGroupHeads() {
    return groups.values();
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
    for (GroupHead groupHead : groups.values()) {
      for (FieldComparator<?> comparator : groupHead.comparators) {
        comparator.setScorer(scorer);
      }
    }
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    this.readerContext = context;
    FunctionValues values = groupBy.getValues(vsContext, context);
    filler = values.getValueFiller();
    mval = filler.getValue();

    for (GroupHead groupHead : groups.values()) {
      for (int i = 0; i < groupHead.comparators.length; i++) {
        groupHead.comparators[i] = groupHead.comparators[i].setNextReader(context);
      }
    }
  }

  /** Holds current head document for a single group.
   *
   * @lucene.experimental */
  public class GroupHead extends AbstractAllGroupHeadsCollector.GroupHead<MutableValue> {

    final FieldComparator<?>[] comparators;

    @SuppressWarnings({"unchecked","rawtypes"})
    private GroupHead(MutableValue groupValue, Sort sort, int doc) throws IOException {
      super(groupValue, doc + readerContext.docBase);
      final SortField[] sortFields = sort.getSort();
      comparators = new FieldComparator[sortFields.length];
      for (int i = 0; i < sortFields.length; i++) {
        comparators[i] = sortFields[i].getComparator(1, i).setNextReader(readerContext);
        comparators[i].setScorer(scorer);
        comparators[i].copy(0, doc);
        comparators[i].setBottom(0);
      }
    }

    @Override
    public int compare(int compIDX, int doc) throws IOException {
      return comparators[compIDX].compareBottom(doc);
    }

    @Override
    public void updateDocHead(int doc) throws IOException {
      for (FieldComparator<?> comparator : comparators) {
        comparator.copy(0, doc);
        comparator.setBottom(0);
      }
      this.doc = doc + readerContext.docBase;
    }
  }
}
