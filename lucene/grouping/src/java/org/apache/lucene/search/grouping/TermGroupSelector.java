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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * A GroupSelector implementation that groups via SortedDocValues
 */
public class TermGroupSelector extends GroupSelector<BytesRef> {

  private final String field;
  private final BytesRefHash values = new BytesRefHash();
  private final Map<Integer, Integer> ordsToGroupIds = new HashMap<>();

  private SortedDocValues docValues;
  private int groupId;

  private boolean secondPass;
  private boolean includeEmpty;

  /**
   * Create a new TermGroupSelector
   * @param field the SortedDocValues field to use for grouping
   */
  public TermGroupSelector(String field) {
    this.field = field;
  }

  @Override
  public void setNextReader(LeafReaderContext readerContext) throws IOException {
    this.docValues = DocValues.getSorted(readerContext.reader(), field);
    this.ordsToGroupIds.clear();
    BytesRef scratch = new BytesRef();
    for (int i = 0; i < values.size(); i++) {
      values.get(i, scratch);
      int ord = this.docValues.lookupTerm(scratch);
      if (ord >= 0)
        ordsToGroupIds.put(ord, i);
    }
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException { }

  @Override
  public State advanceTo(int doc) throws IOException {
    if (this.docValues.advanceExact(doc) == false) {
      groupId = -1;
      return includeEmpty ? State.ACCEPT : State.SKIP;
    }
    int ord = docValues.ordValue();
    if (ordsToGroupIds.containsKey(ord)) {
      groupId = ordsToGroupIds.get(ord);
      return State.ACCEPT;
    }
    if (secondPass)
      return State.SKIP;
    groupId = values.add(docValues.binaryValue());
    ordsToGroupIds.put(ord, groupId);
    return State.ACCEPT;
  }

  private BytesRef scratch = new BytesRef();

  @Override
  public BytesRef currentValue() {
    if (groupId == -1)
      return null;
    values.get(groupId, scratch);
    return scratch;
  }

  @Override
  public BytesRef copyValue() {
    if (groupId == -1)
      return null;
    return BytesRef.deepCopyOf(currentValue());
  }

  @Override
  public void setGroups(Collection<SearchGroup<BytesRef>> searchGroups) {
    this.values.clear();
    this.values.reinit();
    for (SearchGroup<BytesRef> sg : searchGroups) {
      if (sg.groupValue == null)
        includeEmpty = true;
      else
        this.values.add(sg.groupValue);
    }
    this.secondPass = true;
  }
}
