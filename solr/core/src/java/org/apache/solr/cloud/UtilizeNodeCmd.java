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

package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.params.AutoScalingParams.NODE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

public class UtilizeNodeCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;

  public UtilizeNodeCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    ocmh.checkRequired(message, NODE);
    String nodeName = message.getStr(NODE);
    String async = message.getStr(ASYNC);
    AutoScalingConfig autoScalingConfig = ocmh.overseer.getSolrCloudManager().getDistribStateManager().getAutoScalingConfig();

    //first look for any violation that may use this replica
    List<ZkNodeProps> requests = new ArrayList<>();
    //first look for suggestions if any
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(autoScalingConfig, ocmh.overseer.getSolrCloudManager());
    for (Suggester.SuggestionInfo suggestionInfo : suggestions) {
      log.info("op: " + suggestionInfo.getOperation());
      String coll = null;
      List<String> pieces = StrUtils.splitSmart(suggestionInfo.getOperation().getPath(), '/');
      if (pieces.size() > 1) {
        coll = pieces.get(2);
      } else {
        continue;
      }
      log.info("coll: " + coll);
      state.getCollection(coll).forEachReplica((s, r) -> {
        if (Objects.equals(r.getName(), r.getName())) {
          log.info("replica to be moved " + r);
        }
      });
      if (suggestionInfo.getOperation() instanceof V2Request) {
        String targetNode = (String) Utils.getObjectByPath(suggestionInfo.getOperation(), true, "command/move-replica/targetNode");
        if (Objects.equals(targetNode, nodeName)) {
          String replica = (String) Utils.getObjectByPath(suggestionInfo.getOperation(), true, "command/move-replica/replica");
          requests.add(new ZkNodeProps(COLLECTION_PROP, coll,
              CollectionParams.TARGET_NODE, targetNode,
              ASYNC, async,
              REPLICA_PROP, replica));
        }
      }
    }
    executeAll(requests);
    Policy.Session session = autoScalingConfig.getPolicy().createSession(ocmh.overseer.getSolrCloudManager());
    for (; ; ) {
      Suggester suggester = session.getSuggester(MOVEREPLICA)
          .hint(Suggester.Hint.TARGET_NODE, nodeName);
      session = suggester.getSession();
      SolrRequest request = suggester.getSuggestion();
      if (request == null) break;
      requests.add(new ZkNodeProps(COLLECTION_PROP, request.getParams().get(COLLECTION_PROP),
          CollectionParams.TARGET_NODE, request.getParams().get(CollectionParams.TARGET_NODE),
          REPLICA_PROP, request.getParams().get(REPLICA_PROP),
          ASYNC, request.getParams().get(ASYNC)));
    }


    executeAll(requests);
  }

  private void executeAll(List<ZkNodeProps> requests) throws Exception {
    if (requests.isEmpty()) return;
    for (ZkNodeProps props : requests) {
      NamedList result = new NamedList();
      ocmh.commandMap.get(MOVEREPLICA)
          .call(ocmh.overseer.getSolrCloudManager().getClusterStateProvider().getClusterState(),
              props,
              result);
    }
    requests.clear();
  }

}
