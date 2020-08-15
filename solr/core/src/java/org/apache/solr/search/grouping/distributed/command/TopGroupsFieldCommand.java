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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TermGroupSelector;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.TopGroupsCollector;
import org.apache.lucene.search.grouping.ValueSourceGroupSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.grouping.Command;

/**
 * Defines all collectors for retrieving the second phase and how to handle the collector result.
 */
public class TopGroupsFieldCommand implements Command<TopGroups<BytesRef>> {

  public static class Builder {

    private Query query;
    private SchemaField field;
    private Sort groupSort;
    private Sort withinGroupSort;
    private Collection<SearchGroup<BytesRef>> firstPhaseGroups;
    private Integer maxDocPerGroup;
    private boolean needScores = false;
    private boolean needMaxScore = false;

    public Builder setQuery(Query query) {
      this.query = query;
      return this;
    }

    public Builder setField(SchemaField field) {
      this.field = field;
      return this;
    }

    public Builder setGroupSort(Sort groupSort) {
      this.groupSort = groupSort;
      return this;
    }

    public Builder setSortWithinGroup(Sort withinGroupSort) {
      this.withinGroupSort = withinGroupSort;
      return this;
    }

    public Builder setFirstPhaseGroups(Collection<SearchGroup<BytesRef>> firstPhaseGroups) {
      this.firstPhaseGroups = firstPhaseGroups;
      return this;
    }

    public Builder setMaxDocPerGroup(int maxDocPerGroup) {
      this.maxDocPerGroup = maxDocPerGroup;
      return this;
    }

    public Builder setNeedScores(Boolean needScores) {
      this.needScores = needScores;
      return this;
    }

    public Builder setNeedMaxScore(Boolean needMaxScore) {
      this.needMaxScore = needMaxScore;
      return this;
    }

    public TopGroupsFieldCommand build() {
      if (query == null || field == null || groupSort == null ||  withinGroupSort == null || firstPhaseGroups == null ||
          maxDocPerGroup == null) {
        throw new IllegalStateException("All required fields must be set");
      }

      return new TopGroupsFieldCommand(query, field, groupSort, withinGroupSort, firstPhaseGroups, maxDocPerGroup, needScores, needMaxScore);
    }

  }

  private final Query query;
  private final SchemaField field;
  private final Sort groupSort;
  private final Sort withinGroupSort;
  private final Collection<SearchGroup<BytesRef>> firstPhaseGroups;
  private final int maxDocPerGroup;
  private final boolean needScores;
  private final boolean needMaxScore;
  @SuppressWarnings({"rawtypes"})
  private TopGroupsCollector secondPassCollector;
  private TopGroups<BytesRef> topGroups;

  private TopGroupsFieldCommand(Query query,
                                SchemaField field,
                                Sort groupSort,
                                Sort withinGroupSort,
                                Collection<SearchGroup<BytesRef>> firstPhaseGroups,
                                int maxDocPerGroup,
                                boolean needScores,
                                boolean needMaxScore) {
    this.query = query;
    this.field = field;
    this.groupSort = groupSort;
    this.withinGroupSort = withinGroupSort;
    this.firstPhaseGroups = firstPhaseGroups;
    this.maxDocPerGroup = maxDocPerGroup;
    this.needScores = needScores;
    this.needMaxScore = needMaxScore;
  }

  @Override
  public List<Collector> create() throws IOException {
    if (firstPhaseGroups.isEmpty()) {
      return Collections.emptyList();
    }

    final List<Collector> collectors = new ArrayList<>(1);
    final FieldType fieldType = field.getType();
    if (fieldType.getNumberType() != null) {
      ValueSource vs = fieldType.getValueSource(field, null);
      Collection<SearchGroup<MutableValue>> v = GroupConverter.toMutable(field, firstPhaseGroups);
      secondPassCollector = new TopGroupsCollector<>(new ValueSourceGroupSelector(vs, new HashMap<>()),
          v, groupSort, withinGroupSort, maxDocPerGroup, needMaxScore
      );
    } else {
      secondPassCollector = new TopGroupsCollector<>(new TermGroupSelector(field.getName()),
          firstPhaseGroups, groupSort, withinGroupSort, maxDocPerGroup, needMaxScore
      );
    }
    collectors.add(secondPassCollector);
    return collectors;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void postCollect(IndexSearcher searcher) throws IOException {
    if (firstPhaseGroups.isEmpty()) {
      topGroups = new TopGroups<>(groupSort.getSort(), withinGroupSort.getSort(), 0, 0, new GroupDocs[0], Float.NaN);
      return;
    }

    FieldType fieldType = field.getType();
    if (fieldType.getNumberType() != null) {
      topGroups = GroupConverter.fromMutable(field, secondPassCollector.getTopGroups(0));
    } else {
      topGroups = secondPassCollector.getTopGroups(0);
    }
    if (needScores) {
      for (GroupDocs<?> group : topGroups.groups) {
        TopFieldCollector.populateScores(group.scoreDocs, searcher, query);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public TopGroups<BytesRef> result() throws IOException {
    return topGroups;
  }

  @Override
  public String getKey() {
    return field.getName();
  }

  @Override
  public Sort getGroupSort() {
    return groupSort;
  }

  @Override
  public Sort getWithinGroupSort() {
    return withinGroupSort;
  }
}
