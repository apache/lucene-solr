package org.apache.lucene.search.grouping;

/**
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

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Sort;

/**
 * Concrete implementation of {@link AbstractSecondPassGroupingCollector} that groups based on
 * field values and more specifically uses {@link org.apache.lucene.search.FieldCache.StringIndex}
 * to collect grouped docs.
 *
 * @lucene.experimental
 */
public class TermSecondPassGroupingCollector extends AbstractSecondPassGroupingCollector<String> {

  private final SentinelIntSet ordSet;
  private FieldCache.StringIndex index;
  private final String groupField;

  @SuppressWarnings("unchecked")
  public TermSecondPassGroupingCollector(String groupField, Collection<SearchGroup<String>> groups, Sort groupSort, Sort withinGroupSort,
                                         int maxDocsPerGroup, boolean getScores, boolean getMaxScores, boolean fillSortFields)
      throws IOException {
    super(groups, groupSort, withinGroupSort, maxDocsPerGroup, getScores, getMaxScores, fillSortFields);
    ordSet = new SentinelIntSet(groupMap.size(), -1);
    this.groupField = groupField;
    groupDocs = (SearchGroupDocs<String>[]) new SearchGroupDocs[ordSet.keys.length];
  }

  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    super.setNextReader(reader, docBase);
    index = FieldCache.DEFAULT.getStringIndex(reader, groupField);

    // Rebuild ordSet
    ordSet.clear();
    for (SearchGroupDocs<String> group : groupMap.values()) {
//      System.out.println("  group=" + (group.groupValue == null ? "null" : group.groupValue.utf8ToString()));
      int ord = group.groupValue == null ? 0 : index.binarySearchLookup(group.groupValue);
      if (ord >= 0) {
        groupDocs[ordSet.put(ord)] = group;
      }
    }
  }

  @Override
  protected SearchGroupDocs<String> retrieveGroup(int doc) throws IOException {
    int slot = ordSet.find(index.order[doc]);
    if (slot >= 0) {
      return groupDocs[slot];
    }
    return null;
  }
}