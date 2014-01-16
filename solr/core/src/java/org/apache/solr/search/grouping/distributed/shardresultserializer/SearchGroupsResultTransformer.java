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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.solr.search.grouping.distributed.command.Pair;
import org.apache.solr.search.grouping.distributed.command.SearchGroupsFieldCommand;

/**
 * Implementation for transforming {@link SearchGroup} into a {@link NamedList} structure and visa versa.
 */
public class SearchGroupsResultTransformer implements ShardResultTransformer<List<Command>, Map<String, Pair<Integer, Collection<SearchGroup<BytesRef>>>>> {

  private final SolrIndexSearcher searcher;

  public SearchGroupsResultTransformer(SolrIndexSearcher searcher) {
    this.searcher = searcher;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NamedList transform(List<Command> data) throws IOException {
    NamedList<NamedList> result = new NamedList<NamedList>();
    for (Command command : data) {
      final NamedList<Object> commandResult = new NamedList<Object>();
      if (SearchGroupsFieldCommand.class.isInstance(command)) {
        SearchGroupsFieldCommand fieldCommand = (SearchGroupsFieldCommand) command;
        Pair<Integer, Collection<SearchGroup<BytesRef>>> pair = fieldCommand.result();
        Integer groupedCount = pair.getA();
        Collection<SearchGroup<BytesRef>> searchGroups = pair.getB();
        if (searchGroups != null) {
          commandResult.add("topGroups", serializeSearchGroup(searchGroups, fieldCommand.getGroupSort()));
        }
        if (groupedCount != null) {
          commandResult.add("groupCount", groupedCount);
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
  public Map<String, Pair<Integer, Collection<SearchGroup<BytesRef>>>> transformToNative(NamedList<NamedList> shardResponse, Sort groupSort, Sort sortWithinGroup, String shard) {
    Map<String, Pair<Integer, Collection<SearchGroup<BytesRef>>>> result = new HashMap<String, Pair<Integer, Collection<SearchGroup<BytesRef>>>>();
    for (Map.Entry<String, NamedList> command : shardResponse) {
      List<SearchGroup<BytesRef>> searchGroups = new ArrayList<SearchGroup<BytesRef>>();
      NamedList topGroupsAndGroupCount = command.getValue();
      @SuppressWarnings("unchecked")
      NamedList<List<Comparable>> rawSearchGroups = (NamedList<List<Comparable>>) topGroupsAndGroupCount.get("topGroups");
      if (rawSearchGroups != null) {
        for (Map.Entry<String, List<Comparable>> rawSearchGroup : rawSearchGroups){
          SearchGroup<BytesRef> searchGroup = new SearchGroup<BytesRef>();
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

      Integer groupCount = (Integer) topGroupsAndGroupCount.get("groupCount");
      result.put(command.getKey(), new Pair<Integer, Collection<SearchGroup<BytesRef>>>(groupCount, searchGroups));
    }
    return result;
  }

  private NamedList serializeSearchGroup(Collection<SearchGroup<BytesRef>> data, Sort groupSort) {
    NamedList<Object[]> result = new NamedList<Object[]>();

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
