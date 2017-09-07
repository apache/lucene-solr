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

package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.NoneSuggester;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientDataProvider;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for using the configured policy and preferences
 * with the hints provided by the trigger event to compute the required cluster operations.
 *
 * The cluster operations computed here are put into the {@link ActionContext}'s properties
 * with the key name "operations". The value is a List of SolrRequest objects.
 */
public class ComputePlanAction extends TriggerActionBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void process(TriggerEvent event, ActionContext context) {
    log.debug("-- processing event: {} with context properties: {}", event, context.getProperties());
    CoreContainer container = context.getCoreContainer();
    try {
      try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder()
          .withZkHost(container.getZkController().getZkServerAddress())
          .withHttpClient(container.getUpdateShardHandler().getHttpClient())
          .build()) {
        ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
        AutoScalingConfig autoScalingConf = zkStateReader.getAutoScalingConfig();
        if (autoScalingConf.isEmpty()) {
          log.error("Action: " + getName() + " executed but no policy is configured");
          return;
        }
        Policy policy = autoScalingConf.getPolicy();
        Policy.Session session = policy.createSession(new SolrClientDataProvider(cloudSolrClient));
        Policy.Suggester suggester = getSuggester(session, event, zkStateReader);
        while (true) {
          SolrRequest operation = suggester.getOperation();
          if (operation == null) break;
          log.info("Computed Plan: {}", operation.getParams());
          Map<String, Object> props = context.getProperties();
          props.compute("operations", (k, v) -> {
            List<SolrRequest> operations = (List<SolrRequest>) v;
            if (operations == null) operations = new ArrayList<>();
            operations.add(operation);
            return operations;
          });
          session = suggester.getSession();
          suggester = getSuggester(session, event, zkStateReader);
        }
      }
    } catch (KeeperException e) {
      log.error("ZooKeeperException while processing event: " + event, e);
    } catch (InterruptedException e) {
      log.error("Interrupted while processing event: " + event, e);
    } catch (IOException e) {
      log.error("IOException while processing event: " + event, e);
    } catch (Exception e) {
      log.error("Unexpected exception while processing event: " + event, e);
    }
  }

  protected Policy.Suggester getSuggester(Policy.Session session, TriggerEvent event, ZkStateReader zkStateReader) {
    Policy.Suggester suggester;
    switch (event.getEventType()) {
      case NODEADDED:
        suggester = session.getSuggester(CollectionParams.CollectionAction.MOVEREPLICA)
            .hint(Policy.Suggester.Hint.TARGET_NODE, event.getProperty(TriggerEvent.NODE_NAMES));
        log.debug("Created suggester with targetNode: {}", event.getProperty(TriggerEvent.NODE_NAMES));
        break;
      case NODELOST:
        suggester = session.getSuggester(CollectionParams.CollectionAction.MOVEREPLICA)
            .hint(Policy.Suggester.Hint.SRC_NODE, event.getProperty(TriggerEvent.NODE_NAMES));
        log.debug("Created suggester with srcNode: {}", event.getProperty(TriggerEvent.NODE_NAMES));
        break;
      case SEARCHRATE:
        Map<String, Map<String, Double>> hotShards = (Map<String, Map<String, Double>>)event.getProperty(AutoScalingParams.SHARD);
        Map<String, Double> hotCollections = (Map<String, Double>)event.getProperty(AutoScalingParams.COLLECTION);
        List<ReplicaInfo> hotReplicas = (List<ReplicaInfo>)event.getProperty(AutoScalingParams.REPLICA);
        Map<String, Double> hotNodes = (Map<String, Double>)event.getProperty(AutoScalingParams.NODE);

        if (hotShards.isEmpty() && hotCollections.isEmpty() && hotReplicas.isEmpty()) {
          // node -> MOVEREPLICA
          if (hotNodes.isEmpty()) {
            log.warn("Neither hot replicas / collection nor nodes are reported in event: " + event);
            return NoneSuggester.INSTANCE;
          }
          suggester = session.getSuggester(CollectionParams.CollectionAction.MOVEREPLICA);
          for (String node : hotNodes.keySet()) {
            suggester = suggester.hint(Policy.Suggester.Hint.SRC_NODE, node);
          }
        } else {
          // collection || shard || replica -> ADDREPLICA
          suggester = session.getSuggester(CollectionParams.CollectionAction.ADDREPLICA);
          Set<String> collections = new HashSet<>();
          // XXX improve this when AddReplicaSuggester supports coll_shard hint
          hotReplicas.forEach(r -> collections.add(r.getCollection()));
          hotShards.forEach((coll, shards) -> collections.add(coll));
          hotCollections.forEach((coll, rate) -> collections.add(coll));
          for (String coll : collections) {
            suggester = suggester.hint(Policy.Suggester.Hint.COLL, coll);
          }
        }
        break;
      default:
        throw new UnsupportedOperationException("No support for events other than nodeAdded and nodeLost, received: " + event.getEventType());
    }
    return suggester;
  }
}
