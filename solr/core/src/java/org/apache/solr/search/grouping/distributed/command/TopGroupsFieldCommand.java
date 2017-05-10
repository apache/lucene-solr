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
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.grouping.Command;

/**
 * Defines all collectors for retrieving the second phase and how to handle the collector result.
 */
public class TopGroupsFieldCommand implements Command<TopGroups<BytesRef>> {

  public static class Builder {

    private SchemaField field;
    private SortSpec groupSortSpec;
    private SortSpec withinGroupSortSpec;
    private Collection<SearchGroup<BytesRef>> firstPhaseGroups;
    private Integer maxDocPerGroup;
    private boolean needScores = false;
    private boolean needMaxScore = false;

    public Builder setField(SchemaField field) {
      this.field = field;
      return this;
    }

    public Builder setGroupSortSpec(SortSpec groupSortSpec) {
      this.groupSortSpec = groupSortSpec;
      return this;
    }

    public Builder setWithinGroupSortSpec(SortSpec withinGroupSortSpec) {
      this.withinGroupSortSpec = withinGroupSortSpec;
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
      if (field == null || groupSortSpec == null || groupSortSpec.getSort() == null || withinGroupSortSpec == null ||  withinGroupSortSpec.getSort() == null || firstPhaseGroups == null ||
          maxDocPerGroup == null) {
        throw new IllegalStateException("All required fields must be set");
      }

      return new TopGroupsFieldCommand(field, groupSortSpec, withinGroupSortSpec, firstPhaseGroups, maxDocPerGroup, needScores, needMaxScore);
    }

  }

  private final SchemaField field;
  private final SortSpec groupSortSpec;
  private final SortSpec withinGroupSortSpec;
  private final Collection<SearchGroup<BytesRef>> firstPhaseGroups;
  private final int maxDocPerGroup;
  private final boolean needScores;
  private final boolean needMaxScore;
  private TopGroupsCollector secondPassCollector;

  private TopGroupsFieldCommand(SchemaField field,
                                SortSpec groupSortSpec,
                                SortSpec withinGroupSortSpec,
                                Collection<SearchGroup<BytesRef>> firstPhaseGroups,
                                int maxDocPerGroup,
                                boolean needScores,
                                boolean needMaxScore) {
    this.field = field;
    this.groupSortSpec = groupSortSpec;
    this.withinGroupSortSpec = withinGroupSortSpec;
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

    final Sort groupSort = groupSortSpec.getSort();
    final Sort withinGroupSort = withinGroupSortSpec.getSort();

    final List<Collector> collectors = new ArrayList<>(1);
    final FieldType fieldType = field.getType();
    if (fieldType.getNumberType() != null) {
      ValueSource vs = fieldType.getValueSource(field, null);
      Collection<SearchGroup<MutableValue>> v = GroupConverter.toMutable(field, firstPhaseGroups);
      secondPassCollector = new TopGroupsCollector<>(new ValueSourceGroupSelector(vs, new HashMap<>()),
          v, groupSort, withinGroupSort, maxDocPerGroup, needScores, needMaxScore, true
      );
    } else {
      secondPassCollector = new TopGroupsCollector<>(new TermGroupSelector(field.getName()),
          firstPhaseGroups, groupSort, withinGroupSort, maxDocPerGroup, needScores, needMaxScore, true
      );
    }
    collectors.add(secondPassCollector);
    return collectors;
  }

  @Override
  @SuppressWarnings("unchecked")
  public TopGroups<BytesRef> result() {
    if (firstPhaseGroups.isEmpty()) {
      return new TopGroups<>(groupSortSpec.getSort().getSort(), withinGroupSortSpec.getSort().getSort(), 0, 0, new GroupDocs[0], Float.NaN);
    }

    FieldType fieldType = field.getType();
    if (fieldType.getNumberType() != null) {
      return GroupConverter.fromMutable(field, secondPassCollector.getTopGroups(0));
    } else {
      return secondPassCollector.getTopGroups(0);
    }
  }

  @Override
  public String getKey() {
    return field.getName();
  }

  @Override
  public SortSpec getGroupSortSpec() {
    return groupSortSpec;
  }

  @Override
  public SortSpec getWithinGroupSortSpec() {
    return withinGroupSortSpec;
  }

}
