package org.apache.solr.search.grouping.distributed.shardresultserializer;

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

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.FieldType;
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
public class SearchGroupsResultTransformer implements ShardResultTransformer<List<Command>, Map<String, SearchGroupsFieldCommandResult>> {

  private static final String TOP_GROUPS = "topGroups";
  private static final String GROUP_COUNT = "groupCount";

  private final SolrIndexSearcher searcher;

  public SearchGroupsResultTransformer(SolrIndexSearcher searcher) {
    this.searcher = searcher;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NamedList transform(List<Command> data) throws IOException {
    final NamedList<NamedList> result = new NamedList<>(data.size());
    for (Command command : data) {
      final NamedList<Object> commandResult = new NamedList<>(2);
      if (SearchGroupsFieldCommand.class.isInstance(command)) {
        SearchGroupsFieldCommand fieldCommand = (SearchGroupsFieldCommand) command;
        final SearchGroupsFieldCommandResult fieldCommandResult = fieldCommand.result();
        final Collection<SearchGroup<BytesRef>> searchGroups = fieldCommandResult.getSearchGroups();
        if (searchGroups != null) {
          commandResult.add(TOP_GROUPS, serializeSearchGroup(searchGroups, fieldCommand.getGroupSort()));
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

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, SearchGroupsFieldCommandResult> transformToNative(NamedList<NamedList> shardResponse, Sort groupSort, Sort sortWithinGroup, String shard) {
    final Map<String, SearchGroupsFieldCommandResult> result = new HashMap<>(shardResponse.size());
    for (Map.Entry<String, NamedList> command : shardResponse) {
      List<SearchGroup<BytesRef>> searchGroups = new ArrayList<>();
      NamedList topGroupsAndGroupCount = command.getValue();
      @SuppressWarnings("unchecked")
      final NamedList<List<Comparable>> rawSearchGroups = (NamedList<List<Comparable>>) topGroupsAndGroupCount.get(TOP_GROUPS);
      if (rawSearchGroups != null) {
        for (Map.Entry<String, List<Comparable>> rawSearchGroup : rawSearchGroups){
          SearchGroup<BytesRef> searchGroup = new SearchGroup<>();
          searchGroup.groupValue = rawSearchGroup.getKey() != null ? new BytesRef(rawSearchGroup.getKey()) : null;
          searchGroup.sortValues = rawSearchGroup.getValue().toArray(new Comparable[rawSearchGroup.getValue().size()]);
          for (int i = 0; i < searchGroup.sortValues.length; i++) {
            SchemaField field = groupSort.getSort()[i].getField() != null ? searcher.getSchema().getFieldOrNull(groupSort.getSort()[i].getField()) : null;
            if (field != null) {
              FieldType fieldType = field.getType();
              if (searchGroup.sortValues[i] != null) {
                searchGroup.sortValues[i] = fieldType.unmarshalSortValue(searchGroup.sortValues[i]);
              }
            }
          }
          searchGroups.add(searchGroup);
        }
      }

      final Integer groupCount = (Integer) topGroupsAndGroupCount.get(GROUP_COUNT);
      result.put(command.getKey(), new SearchGroupsFieldCommandResult(groupCount, searchGroups));
    }
    return result;
  }

  private NamedList serializeSearchGroup(Collection<SearchGroup<BytesRef>> data, Sort groupSort) {
    final NamedList<Object[]> result = new NamedList<>(data.size());

    for (SearchGroup<BytesRef> searchGroup : data) {
      Object[] convertedSortValues = new Object[searchGroup.sortValues.length];
      for (int i = 0; i < searchGroup.sortValues.length; i++) {
        Object sortValue = searchGroup.sortValues[i];
        SchemaField field = groupSort.getSort()[i].getField() != null ? searcher.getSchema().getFieldOrNull(groupSort.getSort()[i].getField()) : null;
        if (field != null) {
          FieldType fieldType = field.getType();
          if (sortValue != null) {
            sortValue = fieldType.marshalSortValue(sortValue);
          }
        }
        convertedSortValues[i] = sortValue;
      }
      String groupValue = searchGroup.groupValue != null ? searchGroup.groupValue.utf8ToString() : null;
      result.add(groupValue, convertedSortValues);
    }

    return result;
  }

}
