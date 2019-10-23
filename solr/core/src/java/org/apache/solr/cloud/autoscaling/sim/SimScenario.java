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

package org.apache.solr.cloud.autoscaling.sim;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Clause;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.cloud.autoscaling.Variable;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.AutoScaling;
import org.apache.solr.cloud.autoscaling.AutoScalingHandler;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.cloud.autoscaling.TriggerListener;
import org.apache.solr.cloud.autoscaling.TriggerListenerBase;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.CLIO;
import org.apache.solr.util.PropertiesUtil;
import org.apache.solr.util.RedactionUtils;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents an autoscaling scenario consisting of a series of autoscaling
 * operations on a simulated cluster.
 */
public class SimScenario implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Context variable: Random live node name. */
  public static final String RANDOM_NODE_CTX_PROP = "_random_node_";
  /** Context variable: Node name of the current Overseer leader. */
  public static final String OVERSEER_LEADER_CTX_PROP = "_overseer_leader_";
  /** Context variable: List of live nodes. */
  public static final String LIVE_NODES_CTX_PROP = "_live_nodes_";
  /** Context variable: List of collections. */
  public static final String COLLECTIONS_CTX_PROP = "_collections_";
  /** Context variable: List of calculated suggestions. */
  public static final String SUGGESTIONS_CTX_PROP = "_suggestions_";
  /** Context variable: List of SolrResponses of SOLR_REQUEST operations. */
  public static final String RESPONSES_CTX_PROP = "_responses_";
  /** Context variable: Current loop iteration or none if outside of loop. */
  public static final String LOOP_ITER_PROP = "_loop_iter_";
  /** Last trigger event captured by WAIT_EVENT. */
  public static final String TRIGGER_EVENT_PREFIX = "_trigger_event_";

  public SimCloudManager cluster;
  public AutoScalingConfig config;
  public List<SimOp> ops = new ArrayList<>();
  public Map<String, Object> context = new HashMap<>();
  public PrintStream console = CLIO.getErrStream();
  public boolean verbose;
  public boolean abortLoop;
  public boolean abortScenario;

  /** Base class for implementation of scenario DSL actions. */
  public static abstract class SimOp {
    ModifiableSolrParams initParams;
    ModifiableSolrParams params;

    public void init(SolrParams params) {
      this.initParams = new ModifiableSolrParams(params);
    }

    /**
     * This method prepares a copy of initial params (and sets the value of {@link #params}
     * with all property references resolved against the current {@link SimScenario#context}
     * and system properties. This method should always be called before invoking
     * {@link #execute(SimScenario)}.
     * @param scenario current scenario
     */
    public void prepareCurrentParams(SimScenario scenario) {
      Properties props = new Properties();
      scenario.context.forEach((k, v) -> {
        if (v instanceof String[]) {
          v = String.join(",", (String[]) v);
        } else if (v instanceof Collection) {
          StringBuilder sb = new StringBuilder();
          for (Object o : (Collection<Object>)v) {
            if (sb.length() > 0) {
              sb.append(',');
            }
            if ((o instanceof String) || (o instanceof Number)) {
              sb.append(o);
            } else {
              // skip all values
              return;
            }
          }
          v = sb.toString();
        } else if ((v instanceof String) || (v instanceof Number)) {
          // don't convert, put as is
        } else {
          // skip
          return;
        }
        props.put(k, v);
      });
      ModifiableSolrParams currentParams = new ModifiableSolrParams();
      initParams.forEach(e -> {
        String newKey = PropertiesUtil.substituteProperty(e.getKey(), props);
        if (newKey == null) {
          newKey = e.getKey();
        }
        String[] newValues;
        if (e.getValue() != null && e.getValue().length > 0) {
          String[] values = e.getValue();
          newValues = new String[values.length];
          for (int k = 0; k < values.length; k++) {
            String newVal = PropertiesUtil.substituteProperty(values[k], props);
            if (newVal == null) {
              newVal = values[k];
            }
            newValues[k] = newVal;
          }
        } else {
          newValues = e.getValue();
        }
        currentParams.add(newKey, newValues);
      });
      params = currentParams;
    }

    /**
     * Execute the operation.
     * @param scenario current scenario.
     */
    public abstract void execute (SimScenario scenario) throws Exception;
  }


  /**
   * Actions supported by the scenario.
   */
  public enum SimAction {
    /** Create a new simulated cluster. */
    CREATE_CLUSTER,
    /** Create a simulated cluster from autoscaling snapshot. */
    LOAD_SNAPSHOT,
    /** Save autoscaling snapshot of the current simulated cluster. */
    SAVE_SNAPSHOT,
    /** Calculate autoscaling suggestions and put them in the scenario's context. */
    CALCULATE_SUGGESTIONS,
    /** Apply previously calculated autoscaling suggestions. */
    APPLY_SUGGESTIONS,
    /** Kill specific nodes, or a number of randomly selected nodes. */
    KILL_NODES,
    /** Add new nodes. */
    ADD_NODES,
    /** Load autoscaling.json configuration from a file. */
    LOAD_AUTOSCALING,
    /** Start a loop. */
    LOOP_START,
    /** End a loop. */
    LOOP_END,
    /** Set operation delays to simulate long-running actions. */
    SET_OP_DELAYS,
    /** Execute a SolrRequest (must be supported by {@link SimCloudManager}). */
    SOLR_REQUEST,
    /** Wait for a collection to reach the indicated number of shards and replicas. */
    WAIT_COLLECTION,
    /** Prepare a listener to listen for an autoscaling event. */
    EVENT_LISTENER,
    /** Wait for an autoscaling event using previously prepared listener. */
    WAIT_EVENT,
    /** Run the simulation for a while, allowing background tasks to execute. */
    RUN,
    /** Dump the internal state of the simulator to console. */
    DUMP,
    /** Set a variable in context. */
    CTX_SET,
    /** Remove a variable from context. */
    CTX_REMOVE,
    /** Set metrics for a node. */
    SET_NODE_METRICS,
    /** Set metrics for each replica of a collection's shard(s). */
    SET_SHARD_METRICS,
    /** Bulk index a number of simulated documents. */
    INDEX_DOCS,
    /** Assert a condition. */
    ASSERT;

    public static SimAction get(String str) {
      if (str != null) {
        try {
          return SimAction.valueOf(str.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
          return null;
        }
      } else {
        return null;
      }
    }

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }
  }

  public static Map<SimAction, Class<? extends SimOp>> simOps = new HashMap<>();
  static {
    simOps.put(SimAction.CREATE_CLUSTER, CreateCluster.class);
    simOps.put(SimAction.LOAD_SNAPSHOT, LoadSnapshot.class);
    simOps.put(SimAction.SAVE_SNAPSHOT, SaveSnapshot.class);
    simOps.put(SimAction.LOAD_AUTOSCALING, LoadAutoscaling.class);
    simOps.put(SimAction.CALCULATE_SUGGESTIONS, CalculateSuggestions.class);
    simOps.put(SimAction.APPLY_SUGGESTIONS, ApplySuggestions.class);
    simOps.put(SimAction.KILL_NODES, KillNodes.class);
    simOps.put(SimAction.ADD_NODES, AddNodes.class);
    simOps.put(SimAction.LOOP_START, LoopOp.class);
    simOps.put(SimAction.LOOP_END, null);
    simOps.put(SimAction.SET_OP_DELAYS, SetOpDelays.class);
    simOps.put(SimAction.SOLR_REQUEST, RunSolrRequest.class);
    simOps.put(SimAction.RUN, RunSimulator.class);
    simOps.put(SimAction.WAIT_COLLECTION, WaitCollection.class);
    simOps.put(SimAction.EVENT_LISTENER, SetEventListener.class);
    simOps.put(SimAction.WAIT_EVENT, WaitEvent.class);
    simOps.put(SimAction.CTX_SET, CtxSet.class);
    simOps.put(SimAction.CTX_REMOVE, CtxRemove.class);
    simOps.put(SimAction.DUMP, Dump.class);
    simOps.put(SimAction.SET_NODE_METRICS, SetNodeMetrics.class);
    simOps.put(SimAction.SET_SHARD_METRICS, SetShardMetrics.class);
    simOps.put(SimAction.INDEX_DOCS, IndexDocs.class);
    simOps.put(SimAction.ASSERT, Assert.class);
  }

  /**
   * Loop action.
   */
  public static class LoopOp extends SimOp {
    // populated by the DSL parser
    List<SimOp> ops = new ArrayList<>();
    int iterations;

    @Override
    public void execute(SimScenario scenario) throws Exception {
      iterations = Integer.parseInt(params.get("iterations", "10"));
      for (int i = 0; i < iterations; i++) {
        if (scenario.abortLoop) {
          log.info("        -- abortLoop requested, aborting after " + i + " iterations.");
          return;
        }
        scenario.context.put(LOOP_ITER_PROP, i);
        log.info("   * iter " + (i + 1) + ":");
        for (SimOp op : ops) {
          op.prepareCurrentParams(scenario);
          log.info("     - " + op.getClass().getSimpleName() + "\t" + op.params.toString());
          op.execute(scenario);
          if (scenario.abortLoop) {
            log.info("        -- abortLoop requested, aborting after " + i + " iterations.");
            return;
          }
        }
      }
    }
  }

  /**
   * Set a context property.
   */
  public static class CtxSet extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      String key = params.required().get("key");
      String[] values = params.required().getParams("value");
      if (values != null) {
        scenario.context.put(key, Arrays.asList(values));
      } else {
        scenario.context.remove(key);
      }
    }
  }

  /**
   * Remove a context property.
   */
  public static class CtxRemove extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      String key = params.required().get("key");
      scenario.context.remove(key);
    }
  }

  /**
   * Create a simulated cluster.
   */
  public static class CreateCluster extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      int numNodes = Integer.parseInt(params.get("numNodes", "5"));
      boolean disableMetricsHistory = Boolean.parseBoolean(params.get("disableMetricsHistory", "false"));
      String timeSourceStr = params.get("timeSource", "simTime:50");
      if (scenario.cluster != null) { // close & reset
        IOUtils.closeQuietly(scenario.cluster);
        scenario.context.clear();
      }
      scenario.cluster = SimCloudManager.createCluster(numNodes, TimeSource.get(timeSourceStr));
      if (disableMetricsHistory) {
        scenario.cluster.disableMetricsHistory();
      }
      scenario.config = scenario.cluster.getDistribStateManager().getAutoScalingConfig();
    }
  }

  /**
   * Create a simulated cluster from an autoscaling snapshot.
   */
  public static class LoadSnapshot extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      String path = params.get("path");
      SnapshotCloudManager snapshotCloudManager;
      if (path == null) {
        String zkHost = params.get("zkHost");
        if (zkHost == null) {
          throw new IOException(SimAction.LOAD_SNAPSHOT + " must specify 'path' or 'zkHost'");
        } else {
          try (CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(zkHost), Optional.empty()).build()) {
            cloudSolrClient.connect();
            try (SolrClientCloudManager realCloudManager = new SolrClientCloudManager(NoopDistributedQueueFactory.INSTANCE, cloudSolrClient)) {
              snapshotCloudManager = new SnapshotCloudManager(realCloudManager, null);
            }
          }
        }
      } else {
        snapshotCloudManager = SnapshotCloudManager.readSnapshot(new File(path));
      }
      scenario.cluster = SimCloudManager.createCluster(snapshotCloudManager, null, snapshotCloudManager.getTimeSource());
      scenario.config = scenario.cluster.getDistribStateManager().getAutoScalingConfig();
    }
  }

  /**
   * Save an autoscaling snapshot.
   */
  public static class SaveSnapshot extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      String path = params.get("path");
      if (path == null) {
        throw new IOException(SimAction.SAVE_SNAPSHOT + " must specify 'path'");
      }
      boolean redact = Boolean.parseBoolean(params.get("redact", "false"));
      SnapshotCloudManager snapshotCloudManager = new SnapshotCloudManager(scenario.cluster, null);
      snapshotCloudManager.saveSnapshot(new File(path), true, redact);
    }
  }

  /**
   * Load autoscaling.json configuration.
   */
  public static class LoadAutoscaling extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      Map<String, Object> map;
      boolean addDefaults = Boolean.parseBoolean(params.get("withDefaultTriggers", "true"));
      int defaultWaitFor = Integer.parseInt(params.get("defaultWaitFor", "120"));
      String path = params.get("path");
      if (path == null) {
        String json = params.get("json");
        if (json == null) {
          throw new IOException(SimAction.LOAD_AUTOSCALING + " must specify either 'path' or 'json'");
        } else {
          map = (Map<String, Object>) Utils.fromJSONString(json);
        }
      } else {
        File f = new File(path);
        Reader r;
        if (f.exists()) {
          r = new InputStreamReader(new FileInputStream(f), Charset.forName("UTF-8"));
        } else {
          InputStream is = getClass().getResourceAsStream(path);
          if (is == null) {
            throw new IOException("path " + path + " does not exist and it's not a resource");
          }
          r = new InputStreamReader(is, Charset.forName("UTF-8"));
        }
        map = (Map<String, Object>) Utils.fromJSON(r);
      }
      AutoScalingConfig config = new AutoScalingConfig(map);
      if (addDefaults) {
        // add default triggers
        if (!config.getTriggerConfigs().containsKey(AutoScaling.AUTO_ADD_REPLICAS_TRIGGER_NAME)) {
          Map<String, Object> props = new HashMap<>(AutoScaling.AUTO_ADD_REPLICAS_TRIGGER_PROPS);
          props.put("waitFor", defaultWaitFor);
          AutoScalingConfig.TriggerConfig trigger = new AutoScalingConfig.TriggerConfig(AutoScaling.AUTO_ADD_REPLICAS_TRIGGER_NAME, props);
          config = config.withTriggerConfig(trigger);
          config = AutoScalingHandler.withSystemLogListener(config, AutoScaling.AUTO_ADD_REPLICAS_TRIGGER_NAME);
        }
        if (!config.getTriggerConfigs().containsKey(AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_NAME)) {
          AutoScalingConfig.TriggerConfig trigger = new AutoScalingConfig.TriggerConfig(AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_NAME, AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_PROPS);
          config = config.withTriggerConfig(trigger);
          config = AutoScalingHandler.withSystemLogListener(config, AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_NAME);
        }
      }
      scenario.config = config;
      // set this config on the simulator
      scenario.cluster.getSimDistribStateManager().simSetAutoScalingConfig(config);
      // wait until it finished processing the config
      (new TimeOut(30, TimeUnit.SECONDS, scenario.cluster.getTimeSource()))
          .waitFor("OverseerTriggerThread never caught up to the latest znodeVersion", () -> {
            try {
              AutoScalingConfig autoscalingConfig = scenario.cluster.getDistribStateManager().getAutoScalingConfig();
              return autoscalingConfig.getZkVersion() == scenario.cluster.getOverseerTriggerThread().getProcessedZnodeVersion();
            } catch (Exception e) {
              throw new RuntimeException("FAILED", e);
            }
          });

    }
  }

  /**
   * Kill one or more nodes.
   */
  public static class KillNodes extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      if (params.get("numNodes") != null) {
        int numNodes = Integer.parseInt(params.get("numNodes"));
        scenario.cluster.simRemoveRandomNodes(numNodes, false, scenario.cluster.getRandom());
      } else if (params.get("nodes") != null || params.get("node") != null) {
        Set<String> nodes = new HashSet<>();
        String[] nodesValues = params.getParams("nodes");
        if (nodesValues != null) {
          for (String nodesValue : nodesValues) {
            String[] vals = nodesValue.split(",");
            nodes.addAll(Arrays.asList(vals));
          }
        }
        nodesValues = params.getParams("node");
        if (nodesValues != null) {
          nodes.addAll(Arrays.asList(nodesValues));
        }
        for (String node : nodes) {
          scenario.cluster.simRemoveNode(node, false);
        }
      }
    }
  }

  /**
   * Add one or more nodes.
   */
  public static class AddNodes extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      int numNodes = Integer.parseInt(params.get("numNodes"));
      for (int i = 0; i < numNodes; i++) {
        scenario.cluster.simAddNode();
      }
    }
  }

  /**
   * Calculate autoscaling suggestions.
   */
  public static class CalculateSuggestions extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(scenario.config, scenario.cluster);
      scenario.context.put(SUGGESTIONS_CTX_PROP, suggestions);
      log.info("        - " + suggestions.size() + " suggestions");
      if (suggestions.isEmpty()) {
        scenario.abortLoop = true;
      }
    }
  }

  /**
   * Apply autoscaling suggestions.
   */
  public static class ApplySuggestions extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      List<Suggester.SuggestionInfo> suggestions = (List<Suggester.SuggestionInfo>) scenario.context.getOrDefault(SUGGESTIONS_CTX_PROP, Collections.emptyList());
      int unresolvedCount = 0;
      for (Suggester.SuggestionInfo suggestion : suggestions) {
        SolrRequest operation = suggestion.getOperation();
        if (operation == null) {
          unresolvedCount++;
          if (suggestion.getViolation() == null) {
            log.error("       -- ignoring suggestion without violation and without operation: " + suggestion);
          }
          continue;
        }
        SolrParams params = operation.getParams();
        if (operation instanceof V2Request) {
          params = SimUtils.v2AdminRequestToV1Params((V2Request)operation);
        }
        Map<String, Object> paramsMap = new LinkedHashMap<>();
        params.toMap(paramsMap);
        ReplicaInfo info = scenario.cluster.getSimClusterStateProvider().simGetReplicaInfo(
            params.get(CollectionAdminParams.COLLECTION), params.get("replica"));
        if (info == null) {
          log.error("Could not find ReplicaInfo for params: " + params);
        } else if (scenario.verbose) {
          paramsMap.put("replicaInfo", info);
        } else if (info.getVariable(Variable.Type.CORE_IDX.tagName) != null) {
          paramsMap.put(Variable.Type.CORE_IDX.tagName, info.getVariable(Variable.Type.CORE_IDX.tagName));
        }
        try {
          scenario.cluster.request(operation);
        } catch (Exception e) {
          log.error("Aborting - error executing suggestion " + suggestion, e);
          break;
        }
      }
      if (suggestions.size() > 0 && unresolvedCount == suggestions.size()) {
        log.info("        -- aborting simulation, only " + unresolvedCount + " unresolved violations remain");
        scenario.abortLoop = true;
      }
    }
  }

  /**
   * Execute a SolrRequest supported by {@link SimCloudManager}.
   */
  public static class RunSolrRequest extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      String path = params.get("path", "/");
      SolrRequest.METHOD m = SolrRequest.METHOD.valueOf(params.get("httpMethod", "GET"));
      params.remove("httpMethod");
      String streamBody = params.get("stream.body");
      params.remove("stream.body");
      GenericSolrRequest req = new GenericSolrRequest(m, path, params);
      if (streamBody != null) {
        req.setContentWriter(new RequestWriter.StringPayloadContentWriter(streamBody, "application/json"));
      }
      SolrResponse rsp = scenario.cluster.request(req);
      List<SolrResponse> responses = (List<SolrResponse>) scenario.context.computeIfAbsent(RESPONSES_CTX_PROP, Utils.NEW_ARRAYLIST_FUN);
      responses.add(rsp);
    }
  }

  /**
   * Set delays for specified collection operations in order to simulate slow execution.
   */
  public static class SetOpDelays extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      String[] collections = params.remove("collection");
      if (collections == null || collections.length == 0) {
        throw new IOException("'collection' param is required but missing: " + params);
      }
      Map<String, Long> delays = new HashMap<>();
      params.forEach(e -> {
        String key = e.getKey();
        CollectionParams.CollectionAction a = CollectionParams.CollectionAction.get(key);
        if (a == null) {
          log.warn("Invalid collection action " + key + ", skipping...");
          return;
        }
        String[] values = e.getValue();
        if (values == null || values[0].isBlank()) {
          delays.put(a.name(), null);
        } else {
          Long value = Long.parseLong(values[0]);
          delays.put(a.name(), value);
        }
      });
      for (String collection : collections) {
        scenario.cluster.getSimClusterStateProvider().simSetOpDelays(collection, delays);
      }
    }
  }

  /**
   * Run the simulator for a while.
   */
  public static class RunSimulator extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      int timeMs = Integer.parseInt(params.get("time", "60000"));
      scenario.cluster.getTimeSource().sleep(timeMs);
    }
  }

  /**
   * Wait for a specific collection shape.
   */
  public static class WaitCollection extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      String collection = params.required().get("collection");
      int shards = Integer.parseInt(params.required().get("shards"));
      int replicas = Integer.parseInt(params.required().get("replicas"));
      boolean withInactive = Boolean.parseBoolean(params.get("withInactive", "false"));
      boolean requireLeaders = Boolean.parseBoolean(params.get("requireLeaders", "true"));
      int waitSec = Integer.parseInt(params.required().get("wait", "" + CloudUtil.DEFAULT_TIMEOUT));
      CloudUtil.waitForState(scenario.cluster, collection, waitSec, TimeUnit.SECONDS,
          CloudUtil.clusterShape(shards, replicas, withInactive, requireLeaders));
    }
  }

  private static class SimWaitListener extends TriggerListenerBase {
    private final TimeSource timeSource;
    private final AutoScalingConfig.TriggerListenerConfig config;
    private CountDownLatch triggerFired = new CountDownLatch(1);
    private TriggerEvent event;

    SimWaitListener(TimeSource timeSource, AutoScalingConfig.TriggerListenerConfig config) {
      this.timeSource = timeSource;
      this.config = config;
    }

    @Override
    public AutoScalingConfig.TriggerListenerConfig getConfig() {
      return config;
    }

    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName, ActionContext context, Throwable error, String message) throws Exception {
      triggerFired.countDown();
      this.event = event;
    }

    public TriggerEvent getEvent() {
      return event;
    }

    public void wait(int waitSec) throws Exception {
      long waitTime = timeSource.convertDelay(TimeUnit.SECONDS, waitSec, TimeUnit.MILLISECONDS);
      boolean await =  triggerFired.await(waitTime, TimeUnit.MILLISECONDS);
      if (!await) {
        throw new IOException("Timed out waiting for trigger " + config.trigger + " to fire after simulated " +
            waitSec + "s (real " + waitTime + "ms).");
      }
    }
  }

  /**
   * Set a temporary listener to wait for a specific trigger event processing.
   */
  public static class SetEventListener extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      String trigger = params.required().get(AutoScalingParams.TRIGGER);
      Map<String, Object> cfgMap = new HashMap<>();
      String name = ".sim_wait_event_" + trigger;
      cfgMap.put(AutoScalingParams.NAME, name);
      cfgMap.put(AutoScalingParams.TRIGGER, trigger);

      String[] beforeActions = params.getParams(AutoScalingParams.BEFORE_ACTION);
      String[] afterActions = params.getParams(AutoScalingParams.AFTER_ACTION);
      if (beforeActions != null) {
        for (String beforeAction : beforeActions) {
          ((List<String>)cfgMap.computeIfAbsent(AutoScalingParams.BEFORE_ACTION, Utils.NEW_ARRAYLIST_FUN)).add(beforeAction);
        }
      }
      if (afterActions != null) {
        for (String afterAction : afterActions) {
          ((List<String>)cfgMap.computeIfAbsent(AutoScalingParams.AFTER_ACTION, Utils.NEW_ARRAYLIST_FUN)).add(afterAction);
        }
      }
      String[] stages = params.required().getParams(AutoScalingParams.STAGE);
      for (String stage : stages) {
        String[] lst = stage.split("[,\\s]+");
        for (String val : lst) {
          try {
            TriggerEventProcessorStage.valueOf(val);
            ((List<String>)cfgMap.computeIfAbsent(AutoScalingParams.STAGE, Utils.NEW_ARRAYLIST_FUN)).add(val);
          } catch (IllegalArgumentException e) {
            throw new IOException("Invalid stage name '" + val + "'");
          }
        }
      }
      final AutoScalingConfig.TriggerListenerConfig listenerConfig = new AutoScalingConfig.TriggerListenerConfig(name, cfgMap);
      TriggerListener listener = new SimWaitListener(scenario.cluster.getTimeSource(), listenerConfig);
      if (scenario.context.containsKey("_sim_waitListener_" + trigger)) {
        throw new IOException("currently only one listener can be set per trigger. Trigger name: " + trigger);
      }
      scenario.context.put("_sim_waitListener_" + trigger, listener);
      scenario.cluster.getOverseerTriggerThread().getScheduledTriggers().addAdditionalListener(listener);
    }
  }

  /**
   * Wait for the previously set listener to capture an event.
   */
  public static class WaitEvent extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      String trigger = params.required().get(AutoScalingParams.TRIGGER);
      int waitSec = Integer.parseInt(params.get("wait", "" + CloudUtil.DEFAULT_TIMEOUT));
      SimWaitListener listener = (SimWaitListener)scenario.context.remove("_sim_waitListener_" + trigger);
      if (listener == null) {
        throw new IOException(SimAction.WAIT_EVENT + " must be preceded by " + SimAction.EVENT_LISTENER + " for trigger " + trigger);
      }
      try {
        listener.wait(waitSec);
        scenario.context.remove(TRIGGER_EVENT_PREFIX + trigger);
        if (listener.getEvent() != null) {
          Map<String, Object> ev = listener.getEvent().toMap(new LinkedHashMap<>());
          scenario.context.put(TRIGGER_EVENT_PREFIX + trigger, ev);
        }
      } finally {
        scenario.cluster.getOverseerTriggerThread().getScheduledTriggers().removeAdditionalListener(listener);
      }
    }
  }

  public static class SetNodeMetrics extends SimOp {

    @Override
    public void execute(SimScenario scenario) throws Exception {
      String nodeset = params.required().get(Clause.NODESET);
      Set<String> nodes = new HashSet<>();
      if (nodeset.equals(Policy.ANY)) {
        nodes.addAll(scenario.cluster.getLiveNodesSet().get());
      } else {
        String[] list = nodeset.split("[,\\s]+");
        for (String node : list) {
          if (node.isBlank()) {
            continue;
          }
          nodes.add(node);
        }
      }
      Map<String, Object> values = new HashMap<>();
      params.remove(Clause.NODESET);
      for (String key : params.getParameterNames()) {
        values.put(key, params.get(key));
      }
      for (String node : nodes) {
        scenario.cluster.getSimNodeStateProvider().simSetNodeValues(node, values);
      }
    }
  }

  public static class SetShardMetrics extends SimOp {

    @Override
    public void execute(SimScenario scenario) throws Exception {
      String collection = params.required().get("collection");
      String shard = params.get("shard");
      boolean delta = params.getBool("delta", false);
      boolean divide = params.getBool("divide", false);
      params.remove("collection");
      params.remove("shard");
      params.remove("delta");
      params.remove("divide");
      Map<String, Object> values = new HashMap<>();
      for (String key : params.getParameterNames()) {
        // try guessing if it's a number
        try {
          Double d = Double.valueOf(params.get(key));
          values.put(key, d);
        } catch (NumberFormatException nfe) {
          // not a number
          values.put(key, params.get(key));
        }
      }
      values.forEach((k, v) -> {
        try {
          scenario.cluster.getSimClusterStateProvider().simSetShardValue(collection, shard, k, v, delta, divide);
        } catch (Exception e) {
          throw new RuntimeException("Error setting shard value", e);
        }
      });
    }
  }

  public static class IndexDocs extends SimOp {

    @Override
    public void execute(SimScenario scenario) throws Exception {
      String collection = params.required().get("collection");
      long numDocs = params.required().getLong("numDocs");
      long start = params.getLong("start", 0L);

      UpdateRequest ureq = new UpdateRequest();
      ureq.setParam("collection", collection);
      ureq.setDocIterator(new FakeDocIterator(start, numDocs));
      scenario.cluster.simGetSolrClient().request(ureq);
    }
  }

  public enum Condition {
    EQUALS,
    NOT_EQUALS,
    NULL,
    NOT_NULL;

    public static Condition get(String p) {
      if (p == null) {
        return null;
      } else {
        try {
          return Condition.valueOf(p.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
          return null;
        }
      }
    }
  }

  public static class Assert extends SimOp {

    @Override
    public void execute(SimScenario scenario) throws Exception {
      String key = params.get("key");
      Condition condition = Condition.get(params.required().get("condition"));
      if (condition == null) {
        throw new IOException("Invalid 'condition' in params: " + params);
      }
      String expected = params.get("expected");
      if (condition != Condition.NOT_NULL && condition != Condition.NULL && expected == null) {
        throw new IOException("'expected' param is required when condition is " + condition);
      }
      Object value;
      if (key != null) {
        if (key.contains("/")) {
          value = Utils.getObjectByPath(scenario.context, true, key);
        } else {
          value = scenario.context.get(key);
        }
      } else {
        value = params.required().get("value");
      }
      switch (condition) {
        case NULL:
          if (value != null) {
            throw new IOException("expected value should be null but was '" + value + "'");
          }
          break;
        case NOT_NULL:
          if (value == null) {
            throw new IOException("expected value should not be null");
          }
          break;
        case EQUALS:
          if (!expected.equals(String.valueOf(value))) {
            throw new IOException("expected value is '" + expected + "' but actual value is '" + value + "'");
          }
          break;
        case NOT_EQUALS:
          if (expected.equals(String.valueOf(value))) {
            throw new IOException("expected value is '" + expected + "' and actual value is the same while it should be different");
          }
          break;
      }
    }
  }


  /**
   * Dump the simulator state to the console.
   */
  public static class Dump extends SimOp {
    @Override
    public void execute(SimScenario scenario) throws Exception {
      boolean redact = Boolean.parseBoolean(params.get("redact", "false"));
      boolean withData = Boolean.parseBoolean(params.get("withData", "false"));
      boolean withStats = Boolean.parseBoolean(params.get("withStats", "false"));
      boolean withSuggestions = Boolean.parseBoolean(params.get("withSuggestions", "true"));
      boolean withDiagnostics = Boolean.parseBoolean(params.get("withDiagnostics", "false"));
      boolean withNodeState = Boolean.parseBoolean(params.get("withNodeState", "false"));
      boolean withClusterState = Boolean.parseBoolean(params.get("withClusterState", "false"));
      boolean withManagerState = Boolean.parseBoolean(params.get("withManagerState", "false"));
      SnapshotCloudManager snapshotCloudManager = new SnapshotCloudManager(scenario.cluster, null);
      Map<String, Object> snapshot = snapshotCloudManager.getSnapshot(true, redact);
      if (!withData) {
        snapshot.remove(SnapshotCloudManager.DISTRIB_STATE_KEY);
      }
      if (!withNodeState) {
        snapshot.remove(SnapshotCloudManager.NODE_STATE_KEY);
      }
      if (!withClusterState) {
        snapshot.remove(SnapshotCloudManager.CLUSTER_STATE_KEY);
      }
      if (!withStats) {
        snapshot.remove(SnapshotCloudManager.STATISTICS_STATE_KEY);
      }
      if (!withManagerState) {
        snapshot.remove(SnapshotCloudManager.MANAGER_STATE_KEY);
      }
      if (!withDiagnostics) {
        ((Map<String, Object>)snapshot.get(SnapshotCloudManager.AUTOSCALING_STATE_KEY)).remove("diagnostics");
      }
      if (!withSuggestions) {
        ((Map<String, Object>)snapshot.get(SnapshotCloudManager.AUTOSCALING_STATE_KEY)).remove("suggestions");
      }
      String data = Utils.toJSONString(snapshot);
      if (redact) {
        RedactionUtils.RedactionContext ctx = SimUtils.getRedactionContext(snapshotCloudManager.getClusterStateProvider().getClusterState());
        data = RedactionUtils.redactNames(ctx.getRedactions(), data);
      }
      scenario.console.println(data);
    }
  }

  /**
   * Parse a DSL string and create a scenario ready to run.
   * @param data DSL string with commands and parameters
   * @return configured scenario
   * @throws Exception on syntax errors
   */
  public static SimScenario load(String data) throws Exception {
    SimScenario scenario = new SimScenario();
    String[] lines = data.split("\\r?\\n");
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      line = line.trim();
      if (line.isBlank() || line.startsWith("#") || line.startsWith("//")) {
        continue;
      }
      // remove trailing / / comments
      String[] comments = line.split("//");
      String expr = comments[0];
      // split on blank
      String[] parts = expr.split("\\s+");
      if (parts.length > 2) {
        log.warn("Invalid line - wrong number of parts " + parts.length + ", skipping: " + line);
        continue;
      }
      SimAction action = SimAction.get(parts[0]);
      if (action == null) {
        log.warn("Invalid scenario action " + parts[0] + ", skipping...");
        continue;
      }
      if (action == SimAction.LOOP_END) {
        if (!scenario.context.containsKey("loop")) {
          throw new IOException("LOOP_END without start!");
        }
        scenario.context.remove("loop");
        continue;
      }
      Class<? extends SimOp> opClass = simOps.get(action);
      SimOp op = opClass.getConstructor().newInstance();
      ModifiableSolrParams params = new ModifiableSolrParams();
      if (parts.length > 1) {
        String paramsString = parts[1];
        if (parts[1].contains("?")) { // url-like with path?params...
          String[] urlParts = parts[1].split("\\?");
          params.set("path", urlParts[0]);
          paramsString = urlParts.length > 1 ? urlParts[1] : "";
        }
        String[] paramsParts = paramsString.split("&");
        for (String paramPair : paramsParts) {
          String[] paramKV = paramPair.split("=");
          String k = URLDecoder.decode(paramKV[0], "UTF-8");
          String v = paramKV.length > 1 ? URLDecoder.decode(paramKV[1], "UTF-8") : null;
          params.add(k, v);
        }
      }
      op.init(params);
      // loop handling
      if (action == SimAction.LOOP_START) {
        if (scenario.context.containsKey("loop")) {
          throw new IOException("only one loop level is allowed");
        }
        scenario.context.put("loop", op);
        scenario.ops.add(op);
        continue;
      }
      LoopOp currentLoop = (LoopOp) scenario.context.get("loop");
      if (currentLoop != null) {
        currentLoop.ops.add(op);
      } else {
        scenario.ops.add(op);
      }
    }
    if (scenario.context.containsKey("loop")) {
      throw new IOException("Unterminated loop statement");
    }
    // sanity check set_listener / wait_listener
    int numSets = 0, numWaits = 0;
    for (SimOp op : scenario.ops) {
      if (op instanceof SetEventListener) {
        numSets++;
      } else if (op instanceof WaitEvent) {
        numWaits++;
      }
      if (numWaits > numSets) {
        throw new Exception("Unexpected " + SimAction.WAIT_EVENT + " without previous " + SimAction.EVENT_LISTENER);
      }
    }
    if (numSets > numWaits) {
      throw new Exception(SimAction.EVENT_LISTENER + " count should be equal to " + SimAction.WAIT_EVENT + " count but was " +
          numSets + " > " + numWaits);
    }
    return scenario;
  }

  /**
   * Run the scenario.
   */
  public void run() throws Exception {
    for (int i = 0; i < ops.size(); i++) {
      if (abortScenario) {
        log.info("-- abortScenario requested, aborting after " + i + " ops.");
        return;
      }
      SimOp op = ops.get(i);
      log.info((i + 1) + ".\t" + op.getClass().getSimpleName() + "\t" + op.initParams.toString());
      // substitute parameters based on the current context
      if (cluster != null && cluster.getLiveNodesSet().size() > 0) {
        context.put(LIVE_NODES_CTX_PROP, new ArrayList<>(cluster.getLiveNodesSet().get()));
        context.put(RANDOM_NODE_CTX_PROP, cluster.getSimClusterStateProvider().simGetRandomNode());
        context.put(COLLECTIONS_CTX_PROP, cluster.getSimClusterStateProvider().simListCollections());
        context.put(OVERSEER_LEADER_CTX_PROP, cluster.getSimClusterStateProvider().simGetOverseerLeader());
      } else {
        context.remove(LIVE_NODES_CTX_PROP);
        context.remove(COLLECTIONS_CTX_PROP);
        context.remove(RANDOM_NODE_CTX_PROP);
        context.remove(SUGGESTIONS_CTX_PROP);
        context.remove(OVERSEER_LEADER_CTX_PROP);
      }
      op.prepareCurrentParams(this);
      log.info("\t\t" + op.getClass().getSimpleName() + "\t" + op.params.toString());
      op.execute(this);
    }
  }

  @Override
  public void close() throws Exception {
    if (cluster != null) {
      cluster.close();
      cluster = null;
    }
  }
}
