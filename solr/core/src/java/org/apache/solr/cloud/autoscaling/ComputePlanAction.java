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
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientDataProvider;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * todo nocommit
 */
public class ComputePlanAction implements TriggerAction {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private Map<String, String> initArgs;

  @Override
  public void close() throws IOException {

  }

  @Override
  public void init(Map<String, String> args) {
    this.initArgs = args;
  }

  @Override
  public String getName() {
    return initArgs.get("name");
  }

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
        Map<String, Object> autoScalingConf = Utils.getJson(zkStateReader.getZkClient(), ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, true);
        if (autoScalingConf.isEmpty()) {
          log.error("Action: " + getName() + " executed but no policy is configured");
          return;
        }
        AutoScalingConfig config = new AutoScalingConfig(autoScalingConf);
        Policy policy = config.getPolicy();
        Policy.Session session = policy.createSession(new SolrClientDataProvider(cloudSolrClient));
        Policy.Suggester suggester = getSuggester(session, event);
        while (true) {
          SolrRequest operation = suggester.getOperation();
          if (operation == null) break;
          log.info("Computed Plan: {}", operation);
          Map<String, Object> props = context.getProperties();
          props.compute("operations", (k, v) -> {
            List<SolrRequest> operations = (List<SolrRequest>) v;
            if (operations == null) operations = new ArrayList<>();
            operations.add(operation);
            return operations;
          });
          session = suggester.getSession();
          suggester = getSuggester(session, event);
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

  private Policy.Suggester getSuggester(Policy.Session session, TriggerEvent event) {
    Policy.Suggester suggester;
    switch (event.getEventType()) {
      case NODEADDED:
        NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) event;
        suggester = session.getSuggester(CollectionParams.CollectionAction.MOVEREPLICA)
            .hint(Policy.Suggester.Hint.TARGET_NODE, nodeAddedEvent.getProperty(TriggerEvent.NODE_NAME));
        log.debug("Created suggester with targetNode: {}", nodeAddedEvent.getProperty(TriggerEvent.NODE_NAME));
        break;
      case NODELOST:
        NodeLostTrigger.NodeLostEvent nodeLostEvent = (NodeLostTrigger.NodeLostEvent) event;
        suggester = session.getSuggester(CollectionParams.CollectionAction.MOVEREPLICA)
            .hint(Policy.Suggester.Hint.SRC_NODE, nodeLostEvent.getProperty(TriggerEvent.NODE_NAME));
        log.debug("Created suggester with srcNode: {}", nodeLostEvent.getProperty(TriggerEvent.NODE_NAME));
        break;
      default:
        throw new UnsupportedOperationException("No support for events other than nodeAdded and nodeLost, received: " + event.getEventType());
    }
    return suggester;
  }
}
