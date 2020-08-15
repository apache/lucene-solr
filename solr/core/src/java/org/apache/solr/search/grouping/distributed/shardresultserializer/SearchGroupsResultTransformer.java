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
package org.apache.solr.search.grouping.distributed.shardresultserializer;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.grouping.Command;
import org.apache.solr.search.grouping.distributed.command.SearchGroupsFieldCommand;
import org.apache.solr.search.grouping.distributed.command.SearchGroupsFieldCommandResult;

import java.io.IOException;
import java.util.*;

/**
 * Implementation for transforming {@link SearchGroup} into a {@link NamedList} structure and visa versa.
 */
@SuppressWarnings({"rawtypes"})
public class SearchGroupsResultTransformer implements ShardResultTransformer<List<Command>, Map<String, SearchGroupsFieldCommandResult>> {

  private static final String TOP_GROUPS = "topGroups";
  private static final String GROUP_COUNT = "groupCount";

  private final SolrIndexSearcher searcher;

  public SearchGroupsResultTransformer(SolrIndexSearcher searcher) {
    this.searcher = searcher;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public NamedList transform(List<Command> data) throws IOException {
    final NamedList<NamedList> result = new NamedList<>(data.size());
    for (Command command : data) {
      final NamedList<Object> commandResult = new NamedList<>(2);
      if (SearchGroupsFieldCommand.class.isInstance(command)) {
        SearchGroupsFieldCommand fieldCommand = (SearchGroupsFieldCommand) command;
        final SearchGroupsFieldCommandResult fieldCommandResult = fieldCommand.result();
        final Collection<SearchGroup<BytesRef>> searchGroups = fieldCommandResult.getSearchGroups();
        if (searchGroups != null) {
          commandResult.add(TOP_GROUPS, serializeSearchGroup(searchGroups, fieldCommand));
        }
        final Integer groupedCount = fieldCommandResult.getGroupCount();
        if (groupedCount != null) {
          commandResult.add(GROUP_COUNT, groupedCount);
        }
      } else {
        continue;
      }

      result.add(command.getKey(), commandResult);
    }
    return result;
  }

  @SuppressWarnings({"rawtypes"})
  private SearchGroup<BytesRef> deserializeOneSearchGroup(SchemaField groupField, String groupValue,
      SortField[] groupSortField, List<Comparable> rawSearchGroupData) {
    SearchGroup<BytesRef> searchGroup = new SearchGroup<>();
    searchGroup.groupValue = null;
    if (groupValue != null) {
      if (groupField != null) {
        BytesRefBuilder builder = new BytesRefBuilder();
        groupField.getType().readableToIndexed(groupValue, builder);
        searchGroup.groupValue = builder.get();
      } else {
        searchGroup.groupValue = new BytesRef(groupValue);
      }
    }
    searchGroup.sortValues = rawSearchGroupData.toArray(new Comparable[rawSearchGroupData.size()]);
    for (int i = 0; i < searchGroup.sortValues.length; i++) {
      SchemaField field = groupSortField[i].getField() != null ? searcher.getSchema().getFieldOrNull(groupSortField[i].getField()) : null;
      searchGroup.sortValues[i] = ShardResultTransformerUtils.unmarshalSortValue(searchGroup.sortValues[i], field);
    }
    return searchGroup;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  public Map<String, SearchGroupsFieldCommandResult> transformToNative(NamedList<NamedList> shardResponse, Sort groupSort, Sort withinGroupSort, String shard) {
    final Map<String, SearchGroupsFieldCommandResult> result = new HashMap<>(shardResponse.size());
    for (Map.Entry<String, NamedList> command : shardResponse) {
      List<SearchGroup<BytesRef>> searchGroups = new ArrayList<>();
      NamedList topGroupsAndGroupCount = command.getValue();
      @SuppressWarnings({"unchecked"})
      final NamedList<List<Comparable>> rawSearchGroups = (NamedList<List<Comparable>>) topGroupsAndGroupCount.get(TOP_GROUPS);
      if (rawSearchGroups != null) {
        final SchemaField groupField = searcher.getSchema().getFieldOrNull(command.getKey());
        final SortField[] groupSortField = groupSort.getSort();
        for (Map.Entry<String, List<Comparable>> rawSearchGroup : rawSearchGroups){
          SearchGroup<BytesRef> searchGroup = deserializeOneSearchGroup(
              groupField, rawSearchGroup.getKey(),
              groupSortField, rawSearchGroup.getValue());
          searchGroups.add(searchGroup);
        }
      }

      final Integer groupCount = (Integer) topGroupsAndGroupCount.get(GROUP_COUNT);
      result.put(command.getKey(), new SearchGroupsFieldCommandResult(groupCount, searchGroups));
    }
    return result;
  }

  private Object[] serializeOneSearchGroup(SortField[] groupSortField, SearchGroup<BytesRef> searchGroup) {
    Object[] convertedSortValues = new Object[searchGroup.sortValues.length];
    for (int i = 0; i < searchGroup.sortValues.length; i++) {
      Object sortValue = searchGroup.sortValues[i];
      SchemaField field = groupSortField[i].getField() != null ?
          searcher.getSchema().getFieldOrNull(groupSortField[i].getField()) : null;
      convertedSortValues[i] = ShardResultTransformerUtils.marshalSortValue(sortValue, field);
    }
    return convertedSortValues;
  }

  @SuppressWarnings({"rawtypes"})
  private NamedList serializeSearchGroup(Collection<SearchGroup<BytesRef>> data, SearchGroupsFieldCommand command) {
    final NamedList<Object[]> result = new NamedList<>(data.size());

    SortField[] groupSortField = command.getGroupSort().getSort();
    for (SearchGroup<BytesRef> searchGroup : data) {
      Object[] convertedSortValues = serializeOneSearchGroup(groupSortField, searchGroup);
      SchemaField field = searcher.getSchema().getFieldOrNull(command.getKey());
      String groupValue = searchGroup.groupValue != null ? field.getType().indexedToReadable(searchGroup.groupValue, new CharsRefBuilder()).toString() : null;
      result.add(groupValue, convertedSortValues);
    }

    return result;
  }

}
