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

import java.io.IOException;
import java.util.*;

/**
 * Implementation for transforming {@link SearchGroup} into a {@link NamedList} structure and visa versa.
 */
public class SearchGroupsResultTransformer implements ShardResultTransformer<List<Command>, Map<String, Collection<SearchGroup<BytesRef>>>> {

  private final SolrIndexSearcher searcher;

  public SearchGroupsResultTransformer(SolrIndexSearcher searcher) {
    this.searcher = searcher;
  }

  /**
   * {@inheritDoc}
   */
  public NamedList transform(List<Command> data) throws IOException {
    NamedList<NamedList> result = new NamedList<NamedList>();
    for (Command command : data) {
      NamedList commandResult;
      if (SearchGroupsFieldCommand.class.isInstance(command)) {
        SearchGroupsFieldCommand fieldCommand = (SearchGroupsFieldCommand) command;
        Collection<SearchGroup<BytesRef>> searchGroups = fieldCommand.result();
        if (searchGroups == null) {
          continue;
        }

        commandResult = serializeSearchGroup(searchGroups, fieldCommand.getGroupSort());
      } else {
        commandResult = null;
      }

      result.add(command.getKey(), commandResult);
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  public Map<String, Collection<SearchGroup<BytesRef>>> transformToNative(NamedList<NamedList> shardResponse, Sort groupSort, Sort sortWithinGroup, String shard) throws IOException {
    Map<String, Collection<SearchGroup<BytesRef>>> result = new HashMap<String, Collection<SearchGroup<BytesRef>>>();
    for (Map.Entry<String, NamedList> command : shardResponse) {
      List<SearchGroup<BytesRef>> searchGroups = new ArrayList<SearchGroup<BytesRef>>();
      @SuppressWarnings("unchecked")
      NamedList<List<Comparable>> rawSearchGroups = command.getValue();
      for (Map.Entry<String, List<Comparable>> rawSearchGroup : rawSearchGroups){
        SearchGroup<BytesRef> searchGroup = new SearchGroup<BytesRef>();
        searchGroup.groupValue = rawSearchGroup.getKey() != null ? new BytesRef(rawSearchGroup.getKey()) : null;
        searchGroup.sortValues = rawSearchGroup.getValue().toArray(new Comparable[rawSearchGroup.getValue().size()]);
        searchGroups.add(searchGroup);
      }

      result.put(command.getKey(), searchGroups);
    }
    return result;
  }

  private NamedList serializeSearchGroup(Collection<SearchGroup<BytesRef>> data, Sort groupSort) {
    NamedList<Comparable[]> result = new NamedList<Comparable[]>();
    CharsRef spare = new CharsRef();

    for (SearchGroup<BytesRef> searchGroup : data) {
      Comparable[] convertedSortValues = new Comparable[searchGroup.sortValues.length];
      for (int i = 0; i < searchGroup.sortValues.length; i++) {
        Comparable sortValue = (Comparable) searchGroup.sortValues[i];
        SchemaField field = groupSort.getSort()[i].getField() != null ? searcher.getSchema().getFieldOrNull(groupSort.getSort()[i].getField()) : null;
        if (field != null) {
          FieldType fieldType = field.getType();
          if (sortValue instanceof BytesRef) {
            UnicodeUtil.UTF8toUTF16((BytesRef)sortValue, spare);
            String indexedValue = spare.toString();
            sortValue = (Comparable) fieldType.toObject(field.createField(fieldType.indexedToReadable(indexedValue), 0.0f));
          } else if (sortValue instanceof String) {
            sortValue = (Comparable) fieldType.toObject(field.createField(fieldType.indexedToReadable((String) sortValue), 0.0f));
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
