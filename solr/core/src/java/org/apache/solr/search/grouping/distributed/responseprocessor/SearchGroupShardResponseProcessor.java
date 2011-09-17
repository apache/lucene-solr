package org.apache.solr.search.grouping.distributed.responseprocessor;

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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.grouping.distributed.ShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.shardresultserializer.SearchGroupsResultTransformer;

import java.io.IOException;
import java.util.*;

/**
 * Concrete implementation for merging {@link SearchGroup} instances from shard responses.
 */
public class SearchGroupShardResponseProcessor implements ShardResponseProcessor {

  /**
   * {@inheritDoc}
   */
  public void process(ResponseBuilder rb, ShardRequest shardRequest) {
    SortSpec ss = rb.getSortSpec();
    Sort groupSort = rb.getGroupingSpec().getGroupSort();
    String[] fields = rb.getGroupingSpec().getFields();

    Map<String, List<Collection<SearchGroup<BytesRef>>>> commandSearchGroups = new HashMap<String, List<Collection<SearchGroup<BytesRef>>>>();
    Map<String, Map<SearchGroup<BytesRef>, String>> tempSearchGroupToShard = new HashMap<String, Map<SearchGroup<BytesRef>, String>>();
    for (String field : fields) {
      commandSearchGroups.put(field, new ArrayList<Collection<SearchGroup<BytesRef>>>(shardRequest.responses.size()));
      tempSearchGroupToShard.put(field, new HashMap<SearchGroup<BytesRef>, String>());
      if (!rb.searchGroupToShard.containsKey(field)) {
        rb.searchGroupToShard.put(field, new HashMap<SearchGroup<BytesRef>, String>());
      }
    }

    SearchGroupsResultTransformer serializer = new SearchGroupsResultTransformer(rb.req.getSearcher());
    try {
      for (ShardResponse srsp : shardRequest.responses) {
        @SuppressWarnings("unchecked")
        NamedList<NamedList> firstPhaseResult = (NamedList<NamedList>) srsp.getSolrResponse().getResponse().get("firstPhase");
        Map<String, Collection<SearchGroup<BytesRef>>> result = serializer.transformToNative(firstPhaseResult, groupSort, null, srsp.getShard());
        for (String field : commandSearchGroups.keySet()) {
          Collection<SearchGroup<BytesRef>> searchGroups = result.get(field);
          if (searchGroups == null) {
            continue;
          }

          commandSearchGroups.get(field).add(searchGroups);
          for (SearchGroup<BytesRef> searchGroup : searchGroups) {
            tempSearchGroupToShard.get(field).put(searchGroup, srsp.getShard());
          }
        }
      }
      for (String groupField : commandSearchGroups.keySet()) {
        List<Collection<SearchGroup<BytesRef>>> topGroups = commandSearchGroups.get(groupField);
        Collection<SearchGroup<BytesRef>> mergedTopGroups = SearchGroup.merge(topGroups, ss.getOffset(), ss.getCount(), groupSort);
        if (mergedTopGroups == null) {
          continue;
        }

        rb.mergedSearchGroups.put(groupField, mergedTopGroups);
        for (SearchGroup<BytesRef> mergedTopGroup : mergedTopGroups) {
          rb.searchGroupToShard.get(groupField).put(mergedTopGroup, tempSearchGroupToShard.get(groupField).get(mergedTopGroup));
        }
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

}