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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.solr.cloud.autoscaling.TriggerEvent.NODE_NAMES;

/**
 * This class is responsible for using the configured policy and preferences
 * with the hints provided by the trigger event to compute the required cluster operations.
 * <p>
 * The cluster operations computed here are put into the {@link ActionContext}'s properties
 * with the key name "operations". The value is a List of SolrRequest objects.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class ComputePlanAction extends TriggerActionBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String DIAGNOSTICS = "__compute_diag__";

  // accept all collections by default
  Predicate<String> collectionsPredicate = s -> true;

  public ComputePlanAction() {
    super();
    TriggerUtils.validProperties(validProperties, "collections");
  }


  @Override
  public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {
    super.configure(loader, cloudManager, properties);

    Object value = properties.get("collections");
    if (value instanceof String) {
      String colString = (String) value;
      if (!colString.isEmpty()) {
        List<String> whiteListedCollections = StrUtils.splitSmart(colString, ',');
        collectionsPredicate = whiteListedCollections::contains;
      }
    } else if (value instanceof Map) {
      @SuppressWarnings({"unchecked"})
      Map<String, String> matchConditions = (Map<String, String>) value;
      collectionsPredicate = collectionName -> {
        try {
          DocCollection collection = cloudManager.getClusterStateProvider().getCollection(collectionName);
          if (collection == null) {
            log.debug("Collection: {} was not found while evaluating conditions", collectionName);
            return false;
          }
          for (Map.Entry<String, String> entry : matchConditions.entrySet()) {
            if (!entry.getValue().equals(collection.get(entry.getKey()))) {
              if (log.isDebugEnabled()) {
                log.debug("Collection: {} does not match condition: {}:{}", collectionName, entry.getKey(), entry.getValue());
              }
              return false;
            }
          }
          return true;
        } catch (IOException e) {
          log.error("Exception fetching collection information for: {}", collectionName, e);
          return false;
        }
      };
    }
  }

  @Override
  public void process(TriggerEvent event, ActionContext context) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("-- processing event: {} with context properties: {}", event, context.getProperties());
    }
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
          log.debug("-- adjusting limit due to explicitly requested number of ops={}", requestedOperations);
          opLimit = requestedOperations;
        }
        addDiagnostics(event, "maxOperations", maxOperations);
        addDiagnostics(event, "requestedOperations", requestedOperations);
        addDiagnostics(event, "opLimit", opLimit);
        do {
          // computing changes in large clusters may take a long time
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("stopping - thread was interrupted");
          }
          @SuppressWarnings({"rawtypes"})
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
              log.debug("-- no more operations suggested, stopping after {} ops...", (opCount - 1));
              addDiagnostics(event, "noSuggestionsStopAfter", (opCount - 1));
              break;
            } else {
              log.info("Computed plan empty, remained {} requested ops to try.", opCount - opLimit);
              continue;
            }
          }
          if (log.isDebugEnabled()) {
            log.debug("Computed Plan: {}", operation.getParams());
          }
          Map<String, Object> props = context.getProperties();
          props.compute("operations", (k, v) -> {
            @SuppressWarnings({"unchecked", "rawtypes"})
            List<SolrRequest> operations = (List<SolrRequest>) v;
            if (operations == null) operations = new ArrayList<>();
            operations.add(operation);
            return operations;
          });
          if (opCount >= opLimit) {
            log.debug("-- reached limit of maxOps={}, stopping.", opLimit);
            addDiagnostics(event, "opLimitReached", true);
          }
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

  private void addDiagnostics(TriggerEvent event, String key, Object value) {
    if (log.isDebugEnabled()) {
      Map<String, Object> diag = (Map<String, Object>) event.getProperties()
          .computeIfAbsent(DIAGNOSTICS, n -> new HashMap<>());
      diag.put(key, value);
    }
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
    addDiagnostics(event, "estimatedMaxOps", totalMax);
    int maxOp = ((Number) autoScalingConfig.getProperties().getOrDefault(AutoScalingParams.MAX_COMPUTE_OPERATIONS, totalMax)).intValue();
    Object o = event.getProperty(AutoScalingParams.MAX_COMPUTE_OPERATIONS, maxOp);
    if (o != null) {
      try {
        maxOp = Integer.parseInt(String.valueOf(o));
      } catch (Exception e) {
        log.warn("Invalid '{}' event property: {}, using default {}", AutoScalingParams.MAX_COMPUTE_OPERATIONS, o, maxOp);
      }
    }
    if (maxOp < 0) {
      // unlimited
      maxOp = Integer.MAX_VALUE;
    } else if (maxOp < 1) {
      // try at least one operation
      log.debug("-- estimated maxOp={}, resetting to 1...", maxOp);
      maxOp = 1;
    }
    log.debug("-- estimated total max ops={}, effective maxOps={}", totalMax, maxOp);
    return maxOp;
  }

  protected int getRequestedNumOps(TriggerEvent event) {
    @SuppressWarnings({"unchecked"})
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
        suggester = getNodeLostSuggester(cloudManager, session, event);
        break;
      case SEARCHRATE:
      case METRIC:
      case INDEXSIZE:
        @SuppressWarnings({"unchecked"})
        List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>)event.getProperty(TriggerEvent.REQUESTED_OPS, Collections.emptyList());
        int start = (Integer)event.getProperty(START, 0);
        if (ops.isEmpty() || start >= ops.size()) {
          return NoneSuggester.get(session);
        }
        TriggerEvent.Op op = ops.get(start);
        suggester = session.getSuggester(op.getAction());
        if (suggester instanceof UnsupportedSuggester) {
          @SuppressWarnings({"unchecked"})
          List<TriggerEvent.Op> unsupportedOps = (List<TriggerEvent.Op>)context.getProperties().computeIfAbsent(TriggerEvent.UNSUPPORTED_OPS, k -> new ArrayList<TriggerEvent.Op>());
          unsupportedOps.add(op);
        }
        for (Map.Entry<Suggester.Hint, Object> e : op.getHints().entrySet()) {
          suggester = suggester.hint(e.getKey(), e.getValue());
        }
        if (applyCollectionHints(cloudManager, suggester) == 0) return NoneSuggester.get(session);
        suggester = suggester.forceOperation(true);
        event.getProperties().put(START, ++start);
        break;
      case SCHEDULED:
        String preferredOp = (String) event.getProperty(AutoScalingParams.PREFERRED_OP, CollectionParams.CollectionAction.MOVEREPLICA.toLower());
        CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(preferredOp);
        suggester = session.getSuggester(action);
        if (applyCollectionHints(cloudManager, suggester) == 0) return NoneSuggester.get(session);
        break;
      default:
        throw new UnsupportedOperationException("No support for events other than nodeAdded, nodeLost, searchRate, metric, scheduled and indexSize. Received: " + event.getEventType());
    }
    return suggester;
  }

  private Suggester getNodeLostSuggester(SolrCloudManager cloudManager, Policy.Session session, TriggerEvent event) throws IOException {
    String preferredOp = (String) event.getProperty(AutoScalingParams.PREFERRED_OP, CollectionParams.CollectionAction.MOVEREPLICA.toLower());
    CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(preferredOp);
    switch (action) {
      case MOVEREPLICA:
        Suggester s = session.getSuggester(action)
                .hint(Suggester.Hint.SRC_NODE, event.getProperty(NODE_NAMES));
        if (applyCollectionHints(cloudManager, s) == 0) {
          addDiagnostics(event, "noRelevantCollections", true);
          return NoneSuggester.get(session);
        }
        return s;
      case DELETENODE:
        int start = (Integer)event.getProperty(START, 0);
        @SuppressWarnings({"unchecked"})
        List<String> srcNodes = (List<String>) event.getProperty(NODE_NAMES);
        if (srcNodes.isEmpty() || start >= srcNodes.size()) {
          addDiagnostics(event, "noSourceNodes", true);
          return NoneSuggester.get(session);
        }
        String sourceNode = srcNodes.get(start);
        s = session.getSuggester(action)
                .hint(Suggester.Hint.SRC_NODE, event.getProperty(NODE_NAMES));
        if (applyCollectionHints(cloudManager, s) == 0) {
          log.debug("-- no relevant collections on {}, no operations computed.", srcNodes);
          addDiagnostics(event, "noRelevantCollections", true);
          return NoneSuggester.get(session);
        }
        s.hint(Suggester.Hint.SRC_NODE, Collections.singletonList(sourceNode));
        event.getProperties().put(START, ++start);
        return s;
      case NONE:
        return NoneSuggester.get(session);
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported preferredOperation: " + action.toLower() + " specified for node lost trigger");
    }
  }

  /**
   * Applies collection hints for all collections that match the {@link #collectionsPredicate}
   * and returns the number of collections that matched.
   * @return number of collections that match the {@link #collectionsPredicate}
   * @throws IOException if {@link org.apache.solr.client.solrj.impl.ClusterStateProvider} throws IOException
   */
  private int applyCollectionHints(SolrCloudManager cloudManager, Suggester s) throws IOException {
    ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
    Set<String> set = clusterState.getCollectionStates().keySet().stream()
            .filter(collectionRef -> collectionsPredicate.test(collectionRef))
            .collect(Collectors.toSet());
    if (set.size() < clusterState.getCollectionStates().size())  {
      // apply hints only if a subset of collections are selected
      set.forEach(c -> s.hint(Suggester.Hint.COLL, c));
    }
    return set.size();
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
        ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
        Set<Pair<String, String>> collShards = new HashSet<>();
        clusterState.getCollectionStates().entrySet().stream()
                .filter(e -> collectionsPredicate.test(e.getKey()))
                .forEach(entry -> {
                  DocCollection docCollection = entry.getValue().get();
                  if (docCollection != null) {
                    docCollection.getActiveSlices().stream()
                            .map(slice -> new Pair<>(entry.getKey(), slice.getName()))
                            .forEach(collShards::add);
                  }
                });
        log.debug("-- NODE_ADDED: ADDREPLICA suggester configured with {} collection/shard hints.", collShards.size());
        addDiagnostics(event, "relevantCollShard", collShards);
        suggester.hint(Suggester.Hint.COLL_SHARD, collShards);
        suggester.hint(Suggester.Hint.REPLICATYPE, replicaType);
        break;
      case MOVEREPLICA:
        log.debug("-- NODE_ADDED event specified MOVEREPLICA - no hints added.");
        break;
      case NONE:
        log.debug("-- NODE_ADDED event specified NONE - no operations suggested.");
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unsupported preferredOperation=" + preferredOp + " for node added event");
    }
    return suggester;
  }
}
