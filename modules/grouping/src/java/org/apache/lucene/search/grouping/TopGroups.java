package org.apache.lucene.search.grouping;

import org.apache.lucene.search.SortField;

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

/** Represents result returned by a grouping search.
 *
 * @lucene.experimental */
public class TopGroups<GROUP_VALUE_TYPE> {
  /** Number of documents matching the search */
  public final int totalHitCount;

  /** Number of documents grouped into the topN groups */
  public final int totalGroupedHitCount;

  /** The total number of unique groups. If <code>null</code> this value is not computed. */
  public final Integer totalGroupCount;

  /** Group results in groupSort order */
  public final GroupDocs<GROUP_VALUE_TYPE>[] groups;

  /** How groups are sorted against each other */
  public final SortField[] groupSort;

  /** How docs are sorted within each group */
  public final SortField[] withinGroupSort;

  public TopGroups(SortField[] groupSort, SortField[] withinGroupSort, int totalHitCount, int totalGroupedHitCount, GroupDocs<GROUP_VALUE_TYPE>[] groups) {
    this.groupSort = groupSort;
    this.withinGroupSort = withinGroupSort;
    this.totalHitCount = totalHitCount;
    this.totalGroupedHitCount = totalGroupedHitCount;
    this.groups = groups;
    this.totalGroupCount = null;
  }

  public TopGroups(TopGroups<GROUP_VALUE_TYPE> oldTopGroups, Integer totalGroupCount) {
    this.groupSort = oldTopGroups.groupSort;
    this.withinGroupSort = oldTopGroups.withinGroupSort;
    this.totalHitCount = oldTopGroups.totalHitCount;
    this.totalGroupedHitCount = oldTopGroups.totalGroupedHitCount;
    this.groups = oldTopGroups.groups;
    this.totalGroupCount = totalGroupCount;
  }
}
