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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.NoneSuggester;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.UnsupportedSuggester;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.TriggerEvent.NODE_NAMES;

/**
 * This class is responsible for using the configured policy and preferences
 * with the hints provided by the trigger event to compute the required cluster operations.
 * <p>
 * The cluster operations computed here are put into the {@link ActionContext}'s properties
 * with the key name "operations". The value is a List of SolrRequest objects.
 */
public class ComputePlanAction extends TriggerActionBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  Set<String> collections = new HashSet<>();

  public ComputePlanAction() {
    super();
    TriggerUtils.validProperties(validProperties, "collections");
  }


  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {
    super.configure(loader, cloudManager, properties);
    String colString = (String) properties.get("collections");
    if (colString != null && !colString.isEmpty()) {
      collections.addAll(StrUtils.splitSmart(colString, ','));
    }
  }

  @Override
  public void process(TriggerEvent event, ActionContext context) throws Exception {
    log.debug("-- processing event: {} with context properties: {}", event, context.getProperties());
    SolrCloudManager cloudManager = context.getCloudManager();
    try {
      AutoScalingConfig autoScalingConf = cloudManager.getDistribStateManager().getAutoScalingConfig();
      if (autoScalingConf.isEmpty()) {
        throw new Exception("Action: " + getName() + " executed but no policy is configured");
      }
      PolicyHelper.SessionWrapper sessionWrapper = PolicyHelper.getSession(cloudManager);
      Policy.Session session = sessionWrapper.get();
      ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
      if (log.isTraceEnabled()) {
        log.trace("-- session: {}", session);
        log.trace("-- state: {}", clusterState);
      }
      try {
        Suggester suggester = getSuggester(session, event, context, cloudManager);
        int maxOperations = getMaxNumOps(event, autoScalingConf, clusterState);
        int requestedOperations = getRequestedNumOps(event);
        if (requestedOperations > maxOperations) {
          log.warn("Requested number of operations {} higher than maximum {}, adjusting...",
              requestedOperations, maxOperations);
        }
        int opCount = 0;
        int opLimit = maxOperations;
        if (requestedOperations > 0) {
          opLimit = requestedOperations;
        }
        do {
          // computing changes in large clusters may take a long time
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("stopping - thread was interrupted");
          }
          SolrRequest operation = suggester.getSuggestion();
          opCount++;
          // prepare suggester for the next iteration
          if (suggester.getSession() != null) {
            session = suggester.getSession();
          }
          suggester = getSuggester(session, event, context, cloudManager);

          // break on first null op
          // unless a specific number of ops was requested
          // uncomment the following to log too many operations
          /*if (opCount > 10) {
            PolicyHelper.logState(cloudManager, initialSuggester);
          }*/

          if (operation == null) {
            if (requestedOperations < 0) {
              //uncomment the following to log zero operations
//              PolicyHelper.logState(cloudManager, initialSuggester);
              break;
            } else {
              log.info("Computed plan empty, remained " + (opCount - opLimit) + " requested ops to try.");
              continue;
            }
          }
          log.debug("Computed Plan: {}", operation.getParams());
          if (!collections.isEmpty()) {
            String coll = operation.getParams().get(CoreAdminParams.COLLECTION);
            if (coll != null && !collections.contains(coll)) {
              // discard an op that doesn't affect our collections
              log.debug("-- discarding due to collection={} not in {}", coll, collections);
              continue;
            }
          }
          Map<String, Object> props = context.getProperties();
          props.compute("operations", (k, v) -> {
            List<SolrRequest> operations = (List<SolrRequest>) v;
            if (operations == null) operations = new ArrayList<>();
            operations.add(operation);
            return operations;
          });
        } while (opCount < opLimit);
      } finally {
        releasePolicySession(sessionWrapper, session);
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected exception while processing event: " + event, e);
    }
  }

  private void releasePolicySession(PolicyHelper.SessionWrapper sessionWrapper, Policy.Session session) {
    sessionWrapper.returnSession(session);
    sessionWrapper.release();

  }

  protected int getMaxNumOps(TriggerEvent event, AutoScalingConfig autoScalingConfig, ClusterState clusterState) {
    // estimate a maximum default limit that should be sufficient for most purposes:
    // number of nodes * total number of replicas * 3
    AtomicInteger totalRF = new AtomicInteger();
    clusterState.forEachCollection(coll -> {
      Integer rf = coll.getReplicationFactor();
      if (rf == null) {
        if (coll.getSlices().isEmpty()) {
          rf = 1; // ???
        } else {
          rf = coll.getReplicas().size() / coll.getSlices().size();
        }
      }
      totalRF.addAndGet(rf * coll.getSlices().size());
    });
    int totalMax = clusterState.getLiveNodes().size() * totalRF.get() * 3;
    int maxOp = (Integer) autoScalingConfig.getProperties().getOrDefault(AutoScalingParams.MAX_COMPUTE_OPERATIONS, totalMax);
    Object o = event.getProperty(AutoScalingParams.MAX_COMPUTE_OPERATIONS, maxOp);
    try {
      return Integer.parseInt(String.valueOf(o));
    } catch (Exception e) {
      log.warn("Invalid '" + AutoScalingParams.MAX_COMPUTE_OPERATIONS + "' event property: " + o + ", using default " + maxOp);
      return maxOp;
    }
  }

  protected int getRequestedNumOps(TriggerEvent event) {
    Collection<TriggerEvent.Op> ops = (Collection<TriggerEvent.Op>) event.getProperty(TriggerEvent.REQUESTED_OPS, Collections.emptyList());
    if (ops.isEmpty()) {
      return -1;
    } else {
      return ops.size();
    }
  }

  private static final String START = "__start__";

  protected Suggester getSuggester(Policy.Session session, TriggerEvent event, ActionContext context, SolrCloudManager cloudManager) throws IOException {
    Suggester suggester;
    switch (event.getEventType()) {
      case NODEADDED:
        suggester = getNodeAddedSuggester(cloudManager, session, event);
        break;
      case NODELOST:
        String preferredOp = (String) event.getProperty(AutoScalingParams.PREFERRED_OP, CollectionParams.CollectionAction.MOVEREPLICA.toLower());
        CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(preferredOp);
        switch (action) {
          case MOVEREPLICA:
            suggester = session.getSuggester(action)
                .hint(Suggester.Hint.SRC_NODE, event.getProperty(NODE_NAMES));
            break;
          case DELETENODE:
            int start = (Integer)event.getProperty(START, 0);
            List<String> srcNodes = (List<String>) event.getProperty(NODE_NAMES);
            if (srcNodes.isEmpty() || start >= srcNodes.size()) {
              return NoneSuggester.get(session);
            }
            String sourceNode = srcNodes.get(start);
            suggester = session.getSuggester(action)
                .hint(Suggester.Hint.SRC_NODE, Collections.singletonList(sourceNode));
            event.getProperties().put(START, ++start);
            break;
          case NONE:
            return NoneSuggester.get(session);
          default:
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported preferredOperation: " + action.toLower() + " specified for node lost trigger");
        }
        break;
      case SEARCHRATE:
      case METRIC:
      case INDEXSIZE:
        List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>)event.getProperty(TriggerEvent.REQUESTED_OPS, Collections.emptyList());
        int start = (Integer)event.getProperty(START, 0);
        if (ops.isEmpty() || start >= ops.size()) {
          return NoneSuggester.get(session);
        }
        TriggerEvent.Op op = ops.get(start);
        suggester = session.getSuggester(op.getAction());
        if (suggester instanceof UnsupportedSuggester) {
          List<TriggerEvent.Op> unsupportedOps = (List<TriggerEvent.Op>)context.getProperties().computeIfAbsent(TriggerEvent.UNSUPPORTED_OPS, k -> new ArrayList<TriggerEvent.Op>());
          unsupportedOps.add(op);
        }
        for (Map.Entry<Suggester.Hint, Object> e : op.getHints().entrySet()) {
          suggester = suggester.hint(e.getKey(), e.getValue());
        }
        suggester = suggester.forceOperation(true);
        event.getProperties().put(START, ++start);
        break;
      case SCHEDULED:
        preferredOp = (String) event.getProperty(AutoScalingParams.PREFERRED_OP, CollectionParams.CollectionAction.MOVEREPLICA.toLower());
        action = CollectionParams.CollectionAction.get(preferredOp);
        suggester = session.getSuggester(action);
        break;
      default:
        throw new UnsupportedOperationException("No support for events other than nodeAdded, nodeLost, searchRate, metric, scheduled and indexSize. Received: " + event.getEventType());
    }
    return suggester;
  }

  private Suggester getNodeAddedSuggester(SolrCloudManager cloudManager, Policy.Session session, TriggerEvent event) throws IOException {
    String preferredOp = (String) event.getProperty(AutoScalingParams.PREFERRED_OP, CollectionParams.CollectionAction.MOVEREPLICA.toLower());
    Replica.Type replicaType = (Replica.Type) event.getProperty(AutoScalingParams.REPLICA_TYPE, Replica.Type.NRT);
    CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(preferredOp);

    Suggester suggester = session.getSuggester(action)
        .hint(Suggester.Hint.TARGET_NODE, event.getProperty(NODE_NAMES));
    switch (action) {
      case ADDREPLICA:
        // add all collection/shard pairs and let policy engine figure out which one
        // to place on the target node
        // todo in future we can prune ineligible collection/shard pairs
        ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
        Set<Pair<String, String>> collShards = new HashSet<>();
        clusterState.getCollectionStates().forEach((collectionName, collectionRef) -> {
          DocCollection docCollection = collectionRef.get();
          if (docCollection != null)  {
            docCollection.getActiveSlices().stream()
                .map(slice -> new Pair<>(collectionName, slice.getName()))
                .forEach(collShards::add);
          }
        });
        suggester.hint(Suggester.Hint.COLL_SHARD, collShards);
        suggester.hint(Suggester.Hint.REPLICATYPE, replicaType);
        break;
      case MOVEREPLICA:
      case NONE:
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unsupported preferredOperation=" + preferredOp + " for node added event");
    }
    return suggester;
  }
}
