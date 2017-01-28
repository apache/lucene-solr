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
package org.apache.solr.client.solrj.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.cloud.rule.SnitchContext;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;

public class SolrUrlListBuilder {

  Random rand = new Random();
  private ImplicitSnitch snitch;
  private SnitchContext snitchContext;

  public SolrUrlListBuilder(ImplicitSnitch snitch, SnitchContext snitchContext)
  {
    this.snitch = snitch;
    this.snitchContext = snitchContext;
  }

  public List<String> buildUrlList(Map<String,Slice> slices, Set<String> liveNodes, boolean sendToLeaders, SolrParams reqParams, String collection  )
  {

    List<String> replicas = new ArrayList<>();
    List<String> theUrlList = new ArrayList<>();
    List<String> leaderUrlList = null;
    List<String> urlList = null;
    List<String> replicasList = null;

    String []routingRules = reqParams.getParams(ShardParams.ROUTING_RULE);

    // build a map of unique nodes
    // TODO: allow filtering by group, role, etc
    Map<String,ZkNodeProps> nodes = new HashMap<>();
    List<String> urlList2 = new ArrayList<>();
    for (Slice slice : slices.values()) {
      for (ZkNodeProps nodeProps : slice.getReplicasMap().values()) {
        ZkCoreNodeProps coreNodeProps = new ZkCoreNodeProps(nodeProps);
        String node = coreNodeProps.getNodeName();
        if((routingRules!=null && routingRules.length!=0) && !nodeMatchRoutingRule(node,routingRules,snitch,snitchContext))
          continue;
        if (!liveNodes.contains(coreNodeProps.getNodeName())
            || Replica.State.getState(coreNodeProps.getState()) != Replica.State.ACTIVE) continue;
        if (nodes.put(node, nodeProps) == null) {
          if (!sendToLeaders || coreNodeProps.isLeader()) {
            String url;
            if (reqParams.get(UpdateParams.COLLECTION) == null) {
              url = ZkCoreNodeProps.getCoreUrl(nodeProps.getStr(ZkStateReader.BASE_URL_PROP), collection);
            } else {
              url = coreNodeProps.getCoreUrl();
            }
            urlList2.add(url);
          } else {
            String url;
            if (reqParams.get(UpdateParams.COLLECTION) == null) {
              url = ZkCoreNodeProps.getCoreUrl(nodeProps.getStr(ZkStateReader.BASE_URL_PROP), collection);
            } else {
              url = coreNodeProps.getCoreUrl();
            }
            replicas.add(url);
          }
        }
      }
    }

    if (sendToLeaders) {
      leaderUrlList = urlList2;
      replicasList = replicas;
    } else {
      urlList = urlList2;
    }

    if (sendToLeaders) {
      theUrlList = new ArrayList<>(leaderUrlList.size());
      theUrlList.addAll(leaderUrlList);
    } else {
      theUrlList = new ArrayList<>(urlList.size());
      theUrlList.addAll(urlList);
    }

    Collections.shuffle(theUrlList, rand);
    if (sendToLeaders) {
      ArrayList<String> theReplicas = new ArrayList<>(
          replicasList.size());
      theReplicas.addAll(replicasList);
      Collections.shuffle(theReplicas, rand);
      theUrlList.addAll(theReplicas);
    }

    return theUrlList;
  }

  private boolean nodeMatchRoutingRule(String node, String[] routingRules, ImplicitSnitch snitch,
      SnitchContext context) {
    HashMap<String,String> routingRulesMap = StringArrayToHashMap(routingRules);

    //get tags associated with this node
    snitch.getTags(node, routingRulesMap.keySet(), context);
    Map<String, Object> tags = context.getTags();

    for(String tag : tags.keySet())
    {
      String ip = routingRulesMap.get(tag);
      if(!ip.equals(tags.get(tag))) return false;
    }
    return true;
  }

  private HashMap<String,String> StringArrayToHashMap(String[] routingRules) {
    HashMap<String,String> routingRulesMap = new HashMap<>();
    for(String routingRule : routingRules)
    {
      String []rule = routingRule.split(":");
      routingRulesMap.put(rule[0], rule[1]);
    }
    return routingRulesMap;
  }
}
