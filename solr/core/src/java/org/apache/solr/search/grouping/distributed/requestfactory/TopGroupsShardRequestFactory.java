package org.apache.solr.search.grouping.distributed.requestfactory;

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

import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.Grouping;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.grouping.distributed.ShardRequestFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Concrete implementation of {@link ShardRequestFactory} that creates {@link ShardRequest} instances for getting the
 * top groups from all shards.
 */
public class TopGroupsShardRequestFactory implements ShardRequestFactory {

  /**
   * Represents a string value for
   */
  public static final String GROUP_NULL_VALUE = "" + ReverseStringFilter.START_OF_HEADING_MARKER;

  /**
   * {@inheritDoc}
   */
  @Override
  public ShardRequest[] constructRequest(ResponseBuilder rb) {
    // If we have a group.query we need to query all shards... Or we move this to the group first phase queries
    boolean containsGroupByQuery = rb.getGroupingSpec().getQueries().length > 0;
    // TODO: If groups.truncate=true we only have to query the specific shards even faceting and statistics are enabled
    if ((rb.getQueryCommand().getFlags() & SolrIndexSearcher.GET_DOCSET) != 0 || containsGroupByQuery) {
      // In case we need more results such as faceting and statistics we have to query all shards
      return createRequestForAllShards(rb);
    } else {
      // In case we only need top groups we only have to query the shards that contain these groups.
      return createRequestForSpecificShards(rb);
    }
  }

  private ShardRequest[] createRequestForSpecificShards(ResponseBuilder rb) {
    // Determine all unique shards to query for TopGroups
    Set<String> uniqueShards = new HashSet<>();
    for (String command : rb.searchGroupToShards.keySet()) {
      Map<SearchGroup<BytesRef>, Set<String>> groupsToShard = rb.searchGroupToShards.get(command);
      for (Set<String> shards : groupsToShard.values()) {
        uniqueShards.addAll(shards);
      }
    }

    return createRequest(rb, uniqueShards.toArray(new String[uniqueShards.size()]));
  }

  private ShardRequest[] createRequestForAllShards(ResponseBuilder rb) {
    return createRequest(rb, ShardRequest.ALL_SHARDS);
  }

  private ShardRequest[] createRequest(ResponseBuilder rb, String[] shards)
  {
    ShardRequest sreq = new ShardRequest();
    sreq.shards = shards;
    sreq.purpose = ShardRequest.PURPOSE_GET_TOP_IDS;
    sreq.params = new ModifiableSolrParams(rb.req.getParams());

    // If group.format=simple group.offset doesn't make sense
    Grouping.Format responseFormat = rb.getGroupingSpec().getResponseFormat();
    if (responseFormat == Grouping.Format.simple || rb.getGroupingSpec().isMain()) {
      sreq.params.remove(GroupParams.GROUP_OFFSET);
    }

    sreq.params.remove(ShardParams.SHARDS);

    // set the start (offset) to 0 for each shard request so we can properly merge
    // results from the start.
    if (rb.shards_start > -1) {
      // if the client set shards.start set this explicitly
      sreq.params.set(CommonParams.START, rb.shards_start);
    } else {
      sreq.params.set(CommonParams.START, "0");
    }
    if (rb.shards_rows > -1) {
      // if the client set shards.rows set this explicity
      sreq.params.set(CommonParams.ROWS, rb.shards_rows);
    } else {
      sreq.params.set(CommonParams.ROWS, rb.getSortSpec().getOffset() + rb.getSortSpec().getCount());
    }

    sreq.params.set(GroupParams.GROUP_DISTRIBUTED_SECOND, "true");
    final IndexSchema schema = rb.req.getSearcher().getSchema();
    for (Map.Entry<String, Collection<SearchGroup<BytesRef>>> entry : rb.mergedSearchGroups.entrySet()) {
      for (SearchGroup<BytesRef> searchGroup : entry.getValue()) {
        String groupValue;
        if (searchGroup.groupValue != null) {
          String rawGroupValue = searchGroup.groupValue.utf8ToString();
          FieldType fieldType = schema.getField(entry.getKey()).getType();
          groupValue = fieldType.indexedToReadable(rawGroupValue);
        } else {
          groupValue = GROUP_NULL_VALUE;
        }
        sreq.params.add(GroupParams.GROUP_DISTRIBUTED_TOPGROUPS_PREFIX + entry.getKey(), groupValue);
      }
    }

    if ((rb.getFieldFlags() & SolrIndexSearcher.GET_SCORES) != 0 || rb.getSortSpec().includesScore()) {
      sreq.params.set(CommonParams.FL, schema.getUniqueKeyField().getName() + ",score");
    } else {
      sreq.params.set(CommonParams.FL, schema.getUniqueKeyField().getName());
    }
    
    int origTimeAllowed = sreq.params.getInt(CommonParams.TIME_ALLOWED, -1);
    if (origTimeAllowed > 0) {
      sreq.params.set(CommonParams.TIME_ALLOWED, Math.max(1,origTimeAllowed - rb.firstPhaseElapsedTime));
    }

    return new ShardRequest[] {sreq};
  }

}
