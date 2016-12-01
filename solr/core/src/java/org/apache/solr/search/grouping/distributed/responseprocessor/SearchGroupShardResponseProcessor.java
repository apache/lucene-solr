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
package org.apache.solr.search.grouping.distributed.responseprocessor;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.grouping.distributed.ShardResponseProcessor;
import org.apache.solr.search.grouping.distributed.command.SearchGroupsFieldCommandResult;
import org.apache.solr.search.grouping.distributed.shardresultserializer.SearchGroupsResultTransformer;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 * Concrete implementation for merging {@link SearchGroup} instances from shard responses.
 */
public class SearchGroupShardResponseProcessor implements ShardResponseProcessor {

  /**
   * {@inheritDoc}
   */
  @Override
  public void process(ResponseBuilder rb, ShardRequest shardRequest) {
//    SortSpec ss = rb.getSortSpec(); // used only for offset and count; might as well use groupSortSpec (?)
//    Good question. I would suggest to keep the 'ss' around for now for the following reasons:
//    (1) Not changing it now doesn't mean that it couldn't be changed later i.e. beyond the scope of SOLR-6203 work.
//    (2) Changing it now (as part of SOLR-6203) has limited benefit, as you say it's used in just two places.
//    (3) Before/When changing it, we would need to convince ourselves by code inspection that indeed ss.get(Count|Offset)()
//        is here always the same as groupSortSpec.get(Count|Offset)() within the master and branch_6x Apache code base.
//    (4) The code that is responsible for 'setting' what is behind ss.get(Count|Offset)() and groupSortSpec.get(Count|Offset)()
//        could be non-lucene.experimental and customised by folks outside the Apache code base. That would not be a blocker
//        as such but if such customisation is a possibility (though probably unlikely in this specific case) then we might wish
//        to communicate the potential change in behaviour as part of the CHANGES.txt entry somehow e.g. something along the lines of
//        "... now assumes that ... matches ... i.e. ... does not modify ..." which would not be impossible but could be a little
//        tricky, for (point (2)) relatively limited benefit.
//    More verbose than expected list of reasons there. Does that kind of make sense?
    SortSpec groupSortSpec = rb.getGroupingSpec().getGroupSortSpec();
    final String[] fields = rb.getGroupingSpec().getFields();

    SortSpec withinGroupSortSpec = rb.getGroupingSpec().getWithinGroupSortSpec();
    assert withinGroupSortSpec.getSort() != null;

    final Map<String, List<Collection<SearchGroup<BytesRef>>>> commandSearchGroups = new HashMap<>(fields.length, 1.0f);
    final Map<String, Map<SearchGroup<BytesRef>, Set<String>>> tempSearchGroupToShards = new HashMap<>(fields.length, 1.0f);
    for (String field : fields) {
      commandSearchGroups.put(field, new ArrayList<Collection<SearchGroup<BytesRef>>>(shardRequest.responses.size()));
      tempSearchGroupToShards.put(field, new HashMap<SearchGroup<BytesRef>, Set<String>>());
      if (!rb.searchGroupToShards.containsKey(field)) {
        rb.searchGroupToShards.put(field, new HashMap<SearchGroup<BytesRef>, Set<String>>());
      }
    }

    SearchGroupsResultTransformer serializer = new SearchGroupsResultTransformer(rb.req.getSearcher());
    try {
      int maxElapsedTime = 0;
      int hitCountDuringFirstPhase = 0;

      NamedList<Object> shardInfo = null;
      if (rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
        shardInfo = new SimpleOrderedMap<>(shardRequest.responses.size());
        rb.rsp.getValues().add(ShardParams.SHARDS_INFO + ".firstPhase", shardInfo);
      }

      for (ShardResponse srsp : shardRequest.responses) {
        if (shardInfo != null) {
          SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>(4);

          if (srsp.getException() != null) {
            Throwable t = srsp.getException();
            if (t instanceof SolrServerException) {
              t = ((SolrServerException) t).getCause();
            }
            nl.add("error", t.toString());
            StringWriter trace = new StringWriter();
            t.printStackTrace(new PrintWriter(trace));
            nl.add("trace", trace.toString());
          } else {
            nl.add("numFound", (Integer) srsp.getSolrResponse().getResponse().get("totalHitCount"));
          }
          if (srsp.getSolrResponse() != null) {
            nl.add("time", srsp.getSolrResponse().getElapsedTime());
          }
          if (srsp.getShardAddress() != null) {
            nl.add("shardAddress", srsp.getShardAddress());
          }
          shardInfo.add(srsp.getShard(), nl);
        }
        if (rb.req.getParams().getBool(ShardParams.SHARDS_TOLERANT, false) && srsp.getException() != null) {
          if(rb.rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY) == null) {
            rb.rsp.getResponseHeader().add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
          }
          continue; // continue if there was an error and we're tolerant.  
        }
        maxElapsedTime = (int) Math.max(maxElapsedTime, srsp.getSolrResponse().getElapsedTime());
        @SuppressWarnings("unchecked")
        NamedList<NamedList> firstPhaseResult = (NamedList<NamedList>) srsp.getSolrResponse().getResponse().get("firstPhase");
        final Map<String, SearchGroupsFieldCommandResult> result = serializer.transformToNative(firstPhaseResult, groupSortSpec, withinGroupSortSpec, srsp.getShard());
        for (String field : commandSearchGroups.keySet()) {
          final SearchGroupsFieldCommandResult firstPhaseCommandResult = result.get(field);

          final Integer groupCount = firstPhaseCommandResult.getGroupCount();
          if (groupCount != null) {
            Integer existingGroupCount = rb.mergedGroupCounts.get(field);
            // Assuming groups don't cross shard boundary...
            rb.mergedGroupCounts.put(field, existingGroupCount != null ? existingGroupCount + groupCount : groupCount);
          }

          final Collection<SearchGroup<BytesRef>> searchGroups = firstPhaseCommandResult.getSearchGroups();
          if (searchGroups == null) {
            continue;
          }

          commandSearchGroups.get(field).add(searchGroups);
          for (SearchGroup<BytesRef> searchGroup : searchGroups) {
            Map<SearchGroup<BytesRef>, java.util.Set<String>> map = tempSearchGroupToShards.get(field);
            Set<String> shards = map.get(searchGroup);
            if (shards == null) {
              shards = new HashSet<>();
              map.put(searchGroup, shards);
            }
            shards.add(srsp.getShard());
          }
        }
        hitCountDuringFirstPhase += (Integer) srsp.getSolrResponse().getResponse().get("totalHitCount");
      }
      rb.totalHitCount = hitCountDuringFirstPhase;
      rb.firstPhaseElapsedTime = maxElapsedTime;
      for (String groupField : commandSearchGroups.keySet()) {
        List<Collection<SearchGroup<BytesRef>>> topGroups = commandSearchGroups.get(groupField);
        Collection<SearchGroup<BytesRef>> mergedTopGroups = SearchGroup.merge(topGroups, groupSortSpec.getOffset(), groupSortSpec.getCount(), groupSortSpec.getSort());
        if (mergedTopGroups == null) {
          continue;
        }

        rb.mergedSearchGroups.put(groupField, mergedTopGroups);
        for (SearchGroup<BytesRef> mergedTopGroup : mergedTopGroups) {
          rb.searchGroupToShards.get(groupField).put(mergedTopGroup, tempSearchGroupToShards.get(groupField).get(mergedTopGroup));
        }
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

}
