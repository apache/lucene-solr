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
package org.apache.solr.client.solrj.cloud.autoscaling;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.util.Pair;

/**
 * This suggester produces a SPLITSHARD request using provided {@link org.apache.solr.client.solrj.cloud.autoscaling.Suggester.Hint#COLL_SHARD} value.
 */
class SplitShardSuggester extends Suggester {

  @Override
  public CollectionParams.CollectionAction getAction() {
    return CollectionParams.CollectionAction.SPLITSHARD;
  }

  @Override
  SolrRequest init() {
    Set<Pair<String, String>> shards = (Set<Pair<String, String>>) hints.getOrDefault(Hint.COLL_SHARD, Collections.emptySet());
    if (shards.isEmpty()) {
      throw new RuntimeException("split-shard requires 'collection' and 'shard'");
    }
    if (shards.size() > 1) {
      throw new RuntimeException("split-shard requires exactly one pair of 'collection' and 'shard'");
    }
    Pair<String, String> collShard = shards.iterator().next();
    Map<String, Object> params = (Map<String, Object>)hints.getOrDefault(Hint.PARAMS, Collections.emptyMap());
    Float splitFuzz = (Float)params.get(CommonAdminParams.SPLIT_FUZZ);
    CollectionAdminRequest.SplitShard req = CollectionAdminRequest.splitShard(collShard.first()).setShardName(collShard.second());
    if (splitFuzz != null) {
      req.setSplitFuzz(splitFuzz);
    }
    String splitMethod = (String)params.get(CommonAdminParams.SPLIT_METHOD);
    if (splitMethod != null) {
      req.setSplitMethod(splitMethod);
    }
    Boolean splitByPrefix = (Boolean)params.get(CommonAdminParams.SPLIT_BY_PREFIX);
    if (splitByPrefix != null) {
      req.setSplitByPrefix(splitByPrefix);
    }
    return req;
  }
}
