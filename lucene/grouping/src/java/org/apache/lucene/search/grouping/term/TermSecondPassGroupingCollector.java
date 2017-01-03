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
package org.apache.lucene.search.grouping.term;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.SecondPassGroupingCollector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SentinelIntSet;

/**
 * Concrete implementation of {@link SecondPassGroupingCollector} that groups based on
 * field values and more specifically uses {@link SortedDocValues}
 * to collect grouped docs.
 *
 * @lucene.experimental
 */
public class TermSecondPassGroupingCollector extends SecondPassGroupingCollector<BytesRef> {

  private final String groupField;
  private final SentinelIntSet ordSet;

  private SortedDocValues index;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public TermSecondPassGroupingCollector(String groupField, Collection<SearchGroup<BytesRef>> groups, Sort groupSort, Sort withinGroupSort,
                                         int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields)
      throws IOException {
    super(groups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
    this.groupField = groupField;
    this.ordSet = new SentinelIntSet(groupMap.size(), -2);
    super.groupDocs = (SearchGroupDocs<BytesRef>[]) new SearchGroupDocs[ordSet.keys.length];
  }

  @Override
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    super.doSetNextReader(readerContext);
    index = DocValues.getSorted(readerContext.reader(), groupField);

    // Rebuild ordSet
    ordSet.clear();
    for (SearchGroupDocs<BytesRef> group : groupMap.values()) {
//      System.out.println("  group=" + (group.groupValue == null ? "null" : group.groupValue.utf8ToString()));
      int ord = group.groupValue == null ? -1 : index.lookupTerm(group.groupValue);
      if (group.groupValue == null || ord >= 0) {
        groupDocs[ordSet.put(ord)] = group;
      }
    }
  }

  @Override
  protected SearchGroupDocs<BytesRef> retrieveGroup(int doc) throws IOException {
    int slot = ordSet.find(index.getOrd(doc));
    if (slot >= 0) {
      return groupDocs[slot];
    }
    return null;
  }

}
