package org.apache.solr.search.grouping.distributed.command;

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

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermFirstPassGroupingCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.grouping.Command;

import java.io.IOException;
import java.util.*;

/**
 * Creates all the collectors needed for the first phase and how to handle the results.
 */
public class SearchGroupsFieldCommand implements Command<Pair<Integer, Collection<SearchGroup<BytesRef>>>> {

  public static class Builder {

    private SchemaField field;
    private Sort groupSort;
    private Integer topNGroups;
    private boolean includeGroupCount = false;

    public Builder setField(SchemaField field) {
      this.field = field;
      return this;
    }

    public Builder setGroupSort(Sort groupSort) {
      this.groupSort = groupSort;
      return this;
    }

    public Builder setTopNGroups(int topNGroups) {
      this.topNGroups = topNGroups;
      return this;
    }

    public Builder setIncludeGroupCount(boolean includeGroupCount) {
      this.includeGroupCount = includeGroupCount;
      return this;
    }

    public SearchGroupsFieldCommand build() {
      if (field == null || groupSort == null || topNGroups == null) {
        throw new IllegalStateException("All fields must be set");
      }

      return new SearchGroupsFieldCommand(field, groupSort, topNGroups, includeGroupCount);
    }

  }

  private final SchemaField field;
  private final Sort groupSort;
  private final int topNGroups;
  private final boolean includeGroupCount;

  private TermFirstPassGroupingCollector firstPassGroupingCollector;
  private TermAllGroupsCollector allGroupsCollector;

  private SearchGroupsFieldCommand(SchemaField field, Sort groupSort, int topNGroups, boolean includeGroupCount) {
    this.field = field;
    this.groupSort = groupSort;
    this.topNGroups = topNGroups;
    this.includeGroupCount = includeGroupCount;
  }

  @Override
  public List<Collector> create() throws IOException {
    List<Collector> collectors = new ArrayList<>();
    if (topNGroups > 0) {
      firstPassGroupingCollector = new TermFirstPassGroupingCollector(field.getName(), groupSort, topNGroups);
      collectors.add(firstPassGroupingCollector);
    }
    if (includeGroupCount) {
      allGroupsCollector = new TermAllGroupsCollector(field.getName());
      collectors.add(allGroupsCollector);
    }
    return collectors;
  }

  @Override
  public Pair<Integer, Collection<SearchGroup<BytesRef>>> result() {
    final Collection<SearchGroup<BytesRef>> topGroups;
    if (topNGroups > 0) {
      topGroups = firstPassGroupingCollector.getTopGroups(0, true);
    } else {
      topGroups = Collections.emptyList();
    }
    final Integer groupCount;
    if (includeGroupCount) {
      groupCount = allGroupsCollector.getGroupCount();
    } else {
      groupCount = null;
    }
    return new Pair<>(groupCount, topGroups);
  }

  @Override
  public Sort getSortWithinGroup() {
    return null;
  }

  @Override
  public Sort getGroupSort() {
    return groupSort;
  }

  @Override
  public String getKey() {
    return field.getName();
  }
}
