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
package org.apache.solr.search.grouping.distributed.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.AllGroupsCollector;
import org.apache.lucene.search.grouping.FirstPassGroupingCollector;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TermGroupSelector;
import org.apache.lucene.search.grouping.ValueSourceGroupSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.grouping.Command;

/**
 * Creates all the collectors needed for the first phase and how to handle the results.
 */
public class SearchGroupsFieldCommand implements Command<SearchGroupsFieldCommandResult> {

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

  @SuppressWarnings({"rawtypes"})
  private FirstPassGroupingCollector firstPassGroupingCollector;
  @SuppressWarnings({"rawtypes"})
  private AllGroupsCollector allGroupsCollector;

  private SearchGroupsFieldCommand(SchemaField field, Sort groupSort, int topNGroups, boolean includeGroupCount) {
    this.field = field;
    this.groupSort = groupSort;
    this.topNGroups = topNGroups;
    this.includeGroupCount = includeGroupCount;
  }

  @Override
  public List<Collector> create() throws IOException {
    final List<Collector> collectors = new ArrayList<>(2);
    final FieldType fieldType = field.getType();
    if (topNGroups > 0) {
      if (fieldType.getNumberType() != null) {
        ValueSource vs = fieldType.getValueSource(field, null);
        firstPassGroupingCollector
            = new FirstPassGroupingCollector<>(new ValueSourceGroupSelector(vs, new HashMap<>()), groupSort, topNGroups);
      } else {
        firstPassGroupingCollector
            = new FirstPassGroupingCollector<>(new TermGroupSelector(field.getName()), groupSort, topNGroups);
      }
      collectors.add(firstPassGroupingCollector);
    }
    if (includeGroupCount) {
      if (fieldType.getNumberType() != null) {
        ValueSource vs = fieldType.getValueSource(field, null);
        allGroupsCollector = new AllGroupsCollector<>(new ValueSourceGroupSelector(vs, new HashMap<>()));
      } else {
        allGroupsCollector = new AllGroupsCollector<>(new TermGroupSelector(field.getName()));
      }
      collectors.add(allGroupsCollector);
    }
    return collectors;
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public SearchGroupsFieldCommandResult result() throws IOException {
    final Collection<SearchGroup<BytesRef>> topGroups;
    if (firstPassGroupingCollector != null) {
      if (field.getType().getNumberType() != null) {
        topGroups = GroupConverter.fromMutable(field, firstPassGroupingCollector.getTopGroups(0));
      } else {
        topGroups = firstPassGroupingCollector.getTopGroups(0);
      }
    } else {
      topGroups = Collections.emptyList();
    }
    final Integer groupCount;
    if (allGroupsCollector != null) {
      groupCount = allGroupsCollector.getGroupCount();
    } else {
      groupCount = null;
    }
    return new SearchGroupsFieldCommandResult(groupCount, topGroups);
  }

  @Override
  public Sort getWithinGroupSort() {
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
