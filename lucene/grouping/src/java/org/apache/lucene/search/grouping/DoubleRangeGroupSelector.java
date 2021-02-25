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
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Scorable;

/** A GroupSelector implementation that groups documents by double values */
public class DoubleRangeGroupSelector extends GroupSelector<DoubleRange> {

  private final DoubleValuesSource source;
  private final DoubleRangeFactory rangeFactory;

  private Set<DoubleRange> inSecondPass;
  private boolean includeEmpty = true;
  private boolean positioned;
  private DoubleRange current;

  private LeafReaderContext context;
  private DoubleValues values;

  /**
   * Creates a new DoubleRangeGroupSelector
   *
   * @param source a DoubleValuesSource to retrieve double values per document
   * @param rangeFactory a DoubleRangeFactory that defines how to group the double values into range
   *     buckets
   */
  public DoubleRangeGroupSelector(DoubleValuesSource source, DoubleRangeFactory rangeFactory) {
    this.source = source;
    this.rangeFactory = rangeFactory;
  }

  @Override
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
    this.context = readerContext;
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    this.values = source.getValues(context, DoubleValuesSource.fromScorer(scorer));
  }

  @Override
  public State advanceTo(int doc) throws IOException {
    positioned = values.advanceExact(doc);
    if (positioned == false) {
      return includeEmpty ? State.ACCEPT : State.SKIP;
    }
    this.current = rangeFactory.getRange(values.doubleValue(), this.current);
    if (inSecondPass == null) {
      return State.ACCEPT;
    }
    return inSecondPass.contains(this.current) ? State.ACCEPT : State.SKIP;
  }

  @Override
  public DoubleRange currentValue() throws IOException {
    return positioned ? this.current : null;
  }

  @Override
  public DoubleRange copyValue() throws IOException {
    return positioned ? new DoubleRange(this.current.min, this.current.max) : null;
  }

  @Override
  public void setGroups(Collection<SearchGroup<DoubleRange>> searchGroups) {
    inSecondPass = new HashSet<>();
    includeEmpty = false;
    for (SearchGroup<DoubleRange> group : searchGroups) {
      if (group.groupValue == null) {
        includeEmpty = true;
      } else {
        inSecondPass.add(group.groupValue);
      }
    }
  }
}
