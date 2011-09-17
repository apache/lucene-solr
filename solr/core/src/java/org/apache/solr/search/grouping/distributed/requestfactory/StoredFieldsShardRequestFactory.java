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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.GroupParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.grouping.distributed.ShardRequestFactory;
import org.apache.solr.search.grouping.distributed.command.QueryCommandResult;

import java.util.*;

/**
 *
 */
public class StoredFieldsShardRequestFactory implements ShardRequestFactory {

  @Override
  public ShardRequest[] constructRequest(ResponseBuilder rb) {
    HashMap<String, Set<ShardDoc>> shardMap = new HashMap<String,Set<ShardDoc>>();
    for (TopGroups<BytesRef> topGroups : rb.mergedTopGroups.values()) {
      for (GroupDocs<BytesRef> group : topGroups.groups) {
        mapShardToDocs(shardMap, group.scoreDocs);
      }
    }

    for (QueryCommandResult queryCommandResult : rb.mergedQueryCommandResults.values()) {
      mapShardToDocs(shardMap, queryCommandResult.getTopDocs().scoreDocs);
    }

    ShardRequest[] shardRequests = new ShardRequest[shardMap.size()];
    SchemaField uniqueField = rb.req.getSchema().getUniqueKeyField();
    int i = 0;
    for (Collection<ShardDoc> shardDocs : shardMap.values()) {
      ShardRequest sreq = new ShardRequest();
      sreq.purpose = ShardRequest.PURPOSE_GET_FIELDS;
      sreq.shards = new String[] {shardDocs.iterator().next().shard};
      sreq.params = new ModifiableSolrParams();
      sreq.params.add( rb.req.getParams());
      sreq.params.remove(GroupParams.GROUP);
      sreq.params.remove(CommonParams.SORT);
      sreq.params.remove(ResponseBuilder.FIELD_SORT_VALUES);
      String fl = sreq.params.get(CommonParams.FL);
      if (fl != null) {
         fl = fl.trim();
        // currently, "score" is synonymous with "*,score" so
        // don't add "id" if the fl is empty or "score" or it would change the meaning.
         if (fl.length()!=0 && !"score".equals(fl) && !"*".equals(fl)) {
           sreq.params.set(CommonParams.FL, fl+','+uniqueField.getName());
         }
      }

      List<String> ids = new ArrayList<String>(shardDocs.size());
      for (ShardDoc shardDoc : shardDocs) {
        ids.add(shardDoc.id.toString());
      }
      sreq.params.add(ShardParams.IDS, StrUtils.join(ids, ','));
      shardRequests[i++] = sreq;
    }

    return shardRequests;
  }

  private void mapShardToDocs(HashMap<String, Set<ShardDoc>> shardMap, ScoreDoc[] scoreDocs) {
    for (ScoreDoc scoreDoc : scoreDocs) {
      ShardDoc solrDoc = (ShardDoc) scoreDoc;
      Set<ShardDoc> shardDocs = shardMap.get(solrDoc.shard);
      if (shardDocs == null) {
        shardMap.put(solrDoc.shard, shardDocs = new HashSet<ShardDoc>());
      }
      shardDocs.add(solrDoc);
    }
  }

}