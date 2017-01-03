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
import org.apache.lucene.search.grouping.FirstPassGroupingCollector;
import org.apache.lucene.util.mutable.MutableValue;

import java.io.IOException;
import java.util.Map;

/**
 * Concrete implementation of {@link FirstPassGroupingCollector} that groups based on
 * {@link ValueSource} instances.
 *
 * @lucene.experimental
 */
public class FunctionFirstPassGroupingCollector extends FirstPassGroupingCollector<MutableValue> {

  private final ValueSource groupByVS;
  private final Map<?, ?> vsContext;

  private FunctionValues.ValueFiller filler;
  private MutableValue mval;

  /**
   * Creates a first pass collector.
   *
   * @param groupByVS  The {@link ValueSource} instance to group by
   * @param vsContext  The ValueSource context
   * @param groupSort  The {@link Sort} used to sort the
   *                   groups.  The top sorted document within each group
   *                   according to groupSort, determines how that group
   *                   sorts against other groups.  This must be non-null,
   *                   ie, if you want to groupSort by relevance use
   *                   Sort.RELEVANCE.
   * @param topNGroups How many top groups to keep.
   * @throws IOException When I/O related errors occur
   */
  public FunctionFirstPassGroupingCollector(ValueSource groupByVS, Map<?, ?> vsContext, Sort groupSort, int topNGroups) throws IOException {
    super(groupSort, topNGroups);
    this.groupByVS = groupByVS;
    this.vsContext = vsContext;
  }

  @Override
  protected MutableValue getDocGroupValue(int doc) {
    filler.fillValue(doc);
    return mval;
  }

  @Override
  protected MutableValue copyDocGroupValue(MutableValue groupValue, MutableValue reuse) {
    if (reuse != null) {
      reuse.copy(groupValue);
      return reuse;
    }
    return groupValue.duplicate();
  }

  @Override
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    super.doSetNextReader(readerContext);
    FunctionValues values = groupByVS.getValues(vsContext, readerContext);
    filler = values.getValueFiller();
    mval = filler.getValue();
  }

}
