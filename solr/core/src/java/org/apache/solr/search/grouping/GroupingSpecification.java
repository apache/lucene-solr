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
package org.apache.solr.search.grouping;

import org.apache.lucene.search.Sort;
import org.apache.solr.search.Grouping;
import org.apache.solr.search.SortSpec;

/**
 * Encapsulates the grouping options like fields group sort and more specified by clients.
 *
 * @lucene.experimental
 */
public class GroupingSpecification {

  private String[] fields = new String[]{};
  private String[] queries = new String[]{};
  private String[] functions = new String[]{};
  private SortSpec groupSortSpec;
  private SortSpec sortSpecWithinGroup;
  private boolean includeGroupCount;
  private boolean main;
  private Grouping.Format responseFormat;
  private boolean needScore;
  private boolean truncateGroups;

  public String[] getFields() {
    return fields;
  }

  public void setFields(String[] fields) {
    if (fields == null) {
      return;
    }

    this.fields = fields;
  }

  public String[] getQueries() {
    return queries;
  }

  public void setQueries(String[] queries) {
    if (queries == null) {
      return;
    }

    this.queries = queries;
  }

  public String[] getFunctions() {
    return functions;
  }

  public void setFunctions(String[] functions) {
    if (functions == null) {
      return;
    }

    this.functions = functions;
  }

  @Deprecated
  public int getWithinGroupOffset() {
    return sortSpecWithinGroup.getOffset();
  }
  @Deprecated
  public int getGroupOffset() {
    return getWithinGroupOffset();
  }


  @Deprecated
  public int getWithinGroupLimit() {
    return sortSpecWithinGroup.getCount();
  }
  @Deprecated
  public int getGroupLimit() {
    return getWithinGroupLimit();
  }


  @Deprecated
  public int getOffset() {
    return groupSortSpec.getOffset();
  }


  @Deprecated
  public int getLimit() {
    return groupSortSpec.getCount();
  }


  @Deprecated
  public Sort getGroupSort() {
    return groupSortSpec.getSort();
  }


  @Deprecated
  public Sort getSortWithinGroup() {
    return sortSpecWithinGroup.getSort();
  }


  public boolean isIncludeGroupCount() {
    return includeGroupCount;
  }

  public void setIncludeGroupCount(boolean includeGroupCount) {
    this.includeGroupCount = includeGroupCount;
  }

  public boolean isMain() {
    return main;
  }

  public void setMain(boolean main) {
    this.main = main;
  }

  public Grouping.Format getResponseFormat() {
    return responseFormat;
  }

  public void setResponseFormat(Grouping.Format responseFormat) {
    this.responseFormat = responseFormat;
  }

  public boolean isNeedScore() {
    return needScore;
  }

  public void setNeedScore(boolean needScore) {
    this.needScore = needScore;
  }

  public boolean isTruncateGroups() {
    return truncateGroups;
  }

  public void setTruncateGroups(boolean truncateGroups) {
    this.truncateGroups = truncateGroups;
  }

  public SortSpec getGroupSortSpec() {
    return groupSortSpec;
  }

  public void setGroupSortSpec(SortSpec groupSortSpec) {
    this.groupSortSpec = groupSortSpec;
  }

  public SortSpec getSortSpecWithinGroup() {
    return sortSpecWithinGroup;
  }

  public void setSortSpecWithinGroup(SortSpec sortSpecWithinGroup) {
    this.sortSpecWithinGroup = sortSpecWithinGroup;
  }

}
