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
import org.apache.lucene.search.grouping.AbstractAllGroupsCollector;
import org.apache.lucene.util.mutable.MutableValue;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A collector that collects all groups that match the
 * query. Only the group value is collected, and the order
 * is undefined.  This collector does not determine
 * the most relevant document of a group.
 *
 * <p/>
 * Implementation detail: Uses {@link ValueSource} and {@link FunctionValues} to retrieve the
 * field values to group by.
 *
 * @lucene.experimental
 */
public class FunctionAllGroupsCollector extends AbstractAllGroupsCollector<MutableValue> {

  private final Map<?, ?> vsContext;
  private final ValueSource groupBy;
  private final SortedSet<MutableValue> groups = new TreeSet<MutableValue>();

  private FunctionValues.ValueFiller filler;
  private MutableValue mval;

  /**
   * Constructs a {@link FunctionAllGroupsCollector} instance.
   *
   * @param groupBy The {@link ValueSource} to group by
   * @param vsContext The ValueSource context
   */
  public FunctionAllGroupsCollector(ValueSource groupBy, Map<?, ?> vsContext) {
    this.vsContext = vsContext;
    this.groupBy = groupBy;
  }

  @Override
  public Collection<MutableValue> getGroups() {
    return groups;
  }

  @Override
  public void collect(int doc) throws IOException {
    filler.fillValue(doc);
    if (!groups.contains(mval)) {
      groups.add(mval.duplicate());
    }
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    FunctionValues values = groupBy.getValues(vsContext, context);
    filler = values.getValueFiller();
    mval = filler.getValue();
  }
}
