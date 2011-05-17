package org.apache.lucene.search.grouping;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Scorer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A collector that collects all groups that match the
 * query. Only the group value is collected, and the order
 * is undefined.  This collector does not determine
 * the most relevant document of a group.
 *
 * <p/>
 * Internally, {@link SentinelIntSet} is used to detect
 * if a group is already added to the total count.  For each
 * segment the {@link SentinelIntSet} is cleared and filled
 * with previous counted groups that occur in the new
 * segment.
 *
 * @lucene.experimental
 */
public class AllGroupsCollector extends Collector {

  private static final int DEFAULT_INITIAL_SIZE = 128;

  private final String groupField;
  private final SentinelIntSet ordSet;
  private final List<String> groups;

  private FieldCache.StringIndex index;

  /**
   * Expert: Constructs a {@link AllGroupsCollector}
   *
   * @param groupField  The field to group by
   * @param initialSize The initial size of the {@link SentinelIntSet}. The initial size should roughly match the total
   * number of expected unique groups. Be aware that the heap usage is 4 bytes * initialSize.
   */
  public AllGroupsCollector(String groupField, int initialSize) {
    this.groupField = groupField;
    ordSet = new SentinelIntSet(initialSize, -1);
    groups = new ArrayList<String>(initialSize);
  }

  /**
   * Constructs a {@link AllGroupsCollector}. This sets the
   * initialSize for the {@link SentinelIntSet} and group
   * list to 128 in the {@link #AllGroupsCollector(String,
   * int)} constructor.
   *
   * @param groupField The field to group by
   */
  public AllGroupsCollector(String groupField) {
    this(groupField, DEFAULT_INITIAL_SIZE);
  }

  public void setScorer(Scorer scorer) throws IOException {
  }

  public void collect(int doc) throws IOException {
    int key = index.order[doc];
    if (!ordSet.exists(key)) {
      ordSet.put(key);
      String term = key == 0 ? null : index.lookup[index.order[doc]];
      groups.add(term);
    }
  }

  /**
   * Returns the total number of groups for the executed search.
   * This is a convenience method. The following code snippet has the same effect: <pre>getGroups().size()</pre>
   *
   * @return The total number of groups for the executed search
   */
  public int getGroupCount() {
    return groups.size();
  }

  /**
   * Returns the group values
   * <p/>
   * This is an unordered collections of group values. For each group that matched the query there is a {@link String}
   * representing a group value.
   *
   * @return the group values
   */
  public Collection<String> getGroups() {
    return groups;
  }

  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    index = FieldCache.DEFAULT.getStringIndex(reader, groupField);

    // Clear ordSet and fill it with previous encountered groups that can occur in the current segment.
    ordSet.clear();
    for (String countedGroup : groups) {
      int ord = index.binarySearchLookup(countedGroup);
      if (ord >= 0) {
        ordSet.put(ord);
      }
    }
  }

  public boolean acceptsDocsOutOfOrder() {
    return true;
  }
}
