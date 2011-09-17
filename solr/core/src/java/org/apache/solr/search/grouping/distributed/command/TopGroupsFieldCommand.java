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
import org.apache.lucene.search.grouping.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.TermSecondPassGroupingCollector;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.grouping.Command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class TopGroupsFieldCommand implements Command<TopGroups<BytesRef>> {

  public static class Builder {

    private SchemaField field;
    private Sort groupSort;
    private Sort sortWithinGroup;
    private Collection<SearchGroup<BytesRef>> firstPhaseGroups;
    private Integer maxDocPerGroup;
    private boolean needScores = false;
    private boolean needMaxScore = false;
    private boolean needGroupCount = false;

    public Builder setField(SchemaField field) {
      this.field = field;
      return this;
    }

    public Builder setGroupSort(Sort groupSort) {
      this.groupSort = groupSort;
      return this;
    }

    public Builder setSortWithinGroup(Sort sortWithinGroup) {
      this.sortWithinGroup = sortWithinGroup;
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

    public Builder setNeedGroupCount(Boolean needGroupCount) {
      this.needGroupCount = needGroupCount;
      return this;
    }

    public TopGroupsFieldCommand build() {
      if (field == null || groupSort == null ||  sortWithinGroup == null || firstPhaseGroups == null ||
          maxDocPerGroup == null) {
        throw new IllegalStateException("All required fields must be set");
      }

      return new TopGroupsFieldCommand(field, groupSort, sortWithinGroup, firstPhaseGroups, maxDocPerGroup, needScores, needMaxScore, needGroupCount);
    }

  }

  private final SchemaField field;
  private final Sort groupSort;
  private final Sort sortWithinGroup;
  private final Collection<SearchGroup<BytesRef>> firstPhaseGroups;
  private final int maxDocPerGroup;
  private final boolean needScores;
  private final boolean needMaxScore;
  private final boolean needGroupCount;

  private TermSecondPassGroupingCollector secondPassCollector;
  private TermAllGroupsCollector allGroupsCollector;

  private TopGroupsFieldCommand(SchemaField field,
                                Sort groupSort,
                                Sort sortWithinGroup,
                                Collection<SearchGroup<BytesRef>> firstPhaseGroups,
                                int maxDocPerGroup,
                                boolean needScores,
                                boolean needMaxScore,
                                boolean needGroupCount) {
    this.field = field;
    this.groupSort = groupSort;
    this.sortWithinGroup = sortWithinGroup;
    this.firstPhaseGroups = firstPhaseGroups;
    this.maxDocPerGroup = maxDocPerGroup;
    this.needScores = needScores;
    this.needMaxScore = needMaxScore;
    this.needGroupCount = needGroupCount;
  }

  public List<Collector> create() throws IOException {
    List<Collector> collectors = new ArrayList<Collector>();
    secondPassCollector = new TermSecondPassGroupingCollector(
          field.getName(), firstPhaseGroups, groupSort, sortWithinGroup, maxDocPerGroup, needScores, needMaxScore, true
    );
    collectors.add(secondPassCollector);
    if (!needGroupCount) {
      return collectors;
    }
    allGroupsCollector = new TermAllGroupsCollector(field.getName());
    collectors.add(allGroupsCollector);
    return collectors;
  }

  public TopGroups<BytesRef> result() {
    TopGroups<BytesRef> result = secondPassCollector.getTopGroups(0);
    if (allGroupsCollector != null) {
      result = new TopGroups<BytesRef>(result, allGroupsCollector.getGroupCount());
    }
    return result;
  }

  public String getKey() {
    return field.getName();
  }

  public Sort getGroupSort() {
    return groupSort;
  }

  public Sort getSortWithinGroup() {
    return sortWithinGroup;
  }
}
