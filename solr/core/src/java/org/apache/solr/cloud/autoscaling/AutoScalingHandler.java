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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.client.solrj.cloud.autoscaling.Cell;
import org.apache.solr.client.solrj.cloud.autoscaling.Clause;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.Preference;
import org.apache.solr.client.solrj.cloud.autoscaling.Row;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientDataProvider;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;
import static org.apache.solr.common.params.CommonParams.JSON;

/**
 * Handler for /cluster/autoscaling
 */
public class AutoScalingHandler extends RequestHandlerBase implements PermissionNameProvider {
  public static final String HANDLER_PATH = "/admin/autoscaling";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static ImmutableSet<String> singletonCommands = ImmutableSet.of("set-cluster-preferences", "set-cluster-policy");
  protected final CoreContainer container;
  private final List<Map<String, String>> DEFAULT_ACTIONS = new ArrayList<>(3);

  public AutoScalingHandler(CoreContainer container) {
    this.container = container;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    try {
      String httpMethod = (String) req.getContext().get("httpMethod");
      RequestHandlerUtils.setWt(req, JSON);

      if ("GET".equals(httpMethod)) {
        String path = (String) req.getContext().get("path");
        if (path == null) path = "/cluster/autoscaling";
        List<String> parts = StrUtils.splitSmart(path, '/');
        if (parts.get(0).isEmpty()) parts.remove(0);

        if (parts.size() < 2 || parts.size() > 3) {
          // invalid
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown path: " + path);
        }

        Map<String, Object> map = zkReadAutoScalingConf(container.getZkController().getZkStateReader());
        if (parts.size() == 2) {
          rsp.getValues().addAll(map);
        } else if (parts.size() == 3 && "diagnostics".equals(parts.get(2))) {
          handleDiagnostics(rsp, map);
        }
      } else {
        if (req.getContentStreams() == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No commands specified for autoscaling");
        }
        List<CommandOperation> ops = CommandOperation.readCommands(req.getContentStreams(), rsp.getValues(), singletonCommands);
        if (ops == null) {
          // errors have already been added to the response so there's nothing left to do
          return;
        }
        for (CommandOperation op : ops) {
          switch (op.name) {
            case "set-policy":
              handleSetPolicies(req, rsp, op);
              break;
            case "remove-policy":
              handleRemovePolicy(req, rsp, op);
              break;
            case "set-cluster-preferences":
              handleSetClusterPreferences(req, rsp, op);
              break;
            case "set-cluster-policy":
              handleSetClusterPolicy(req, rsp, op);
              break;
            default:
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown command: " + op.name);
          }
        }
      }
    } catch (Exception e) {
      rsp.getValues().add("result", "failure");
      throw e;
    } finally {
      RequestHandlerUtils.addExperimentalFormatWarning(rsp);
    }
  }

  private void handleDiagnostics(SolrQueryResponse rsp, Map<String, Object> autoScalingConf) throws IOException {
    Policy policy = new Policy(autoScalingConf);
    try (CloudSolrClient build = new CloudSolrClient.Builder()
        .withHttpClient(container.getUpdateShardHandler().getHttpClient())
        .withZkHost(container.getZkController().getZkServerAddress()).build()) {
      Policy.Session session = policy.createSession(new SolrClientDataProvider(build));
      List<Row> sorted = session.getSorted();
      List<Clause.Violation> violations = session.getViolations();

      List<Preference> clusterPreferences = policy.getClusterPreferences();

      List<Map<String, Object>> sortedNodes = new ArrayList<>(sorted.size());
      for (Row row : sorted) {
        Map<String, Object> map = Utils.makeMap("node", row.node);
        for (Cell cell : row.getCells()) {
          for (Preference clusterPreference : clusterPreferences) {
            Policy.SortParam name = clusterPreference.getName();
            if (cell.getName().equalsIgnoreCase(name.name())) {
              map.put(name.name(), cell.getValue());
              break;
            }
          }
        }
        sortedNodes.add(map);
      }

      Map<String, Object> map = new HashMap<>(2);
      map.put("sortedNodes", sortedNodes);

      map.put("violations", violations);
      rsp.getValues().add("diagnostics", map);
    }
  }

  private void handleSetClusterPolicy(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op) throws KeeperException, InterruptedException, IOException {
    List clusterPolicy = (List) op.getCommandData();
    if (clusterPolicy == null || !(clusterPolicy instanceof List)) {
      op.addError("A list of cluster policies was not found");
      checkErr(op);
    }

    try {
      zkSetClusterPolicy(container.getZkController().getZkStateReader(), clusterPolicy);
    } catch (Exception e) {
      log.warn("error persisting policies");
      op.addError(e.getMessage());
      checkErr(op);

    }
    rsp.getValues().add("result", "success");
  }

  private void checkErr(CommandOperation op) {
    if (!op.hasError()) return;
    throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, "Error in command payload", CommandOperation.captureErrors(Collections.singletonList(op)));
  }

  private void handleSetClusterPreferences(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op) throws KeeperException, InterruptedException, IOException {
    List preferences = (List) op.getCommandData();
    if (preferences == null || !(preferences instanceof List)) {
      op.addError("A list of cluster preferences not found");
      checkErr(op);
    }
    zkSetPreferences(container.getZkController().getZkStateReader(), preferences);
    rsp.getValues().add("result", "success");
  }

  private void handleRemovePolicy(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op) throws KeeperException, InterruptedException, IOException {
    String policyName = (String) op.getCommandData();

    if (op.hasError()) checkErr(op);
    Map<String, Object> autoScalingConf = zkReadAutoScalingConf(container.getZkController().getZkStateReader());
    Map<String, Object> policies = (Map<String, Object>) autoScalingConf.get("policies");
    if (policies == null || !policies.containsKey(policyName)) {
      op.addError("No policy exists with name: " + policyName);
    }
    checkErr(op);
    zkSetPolicies(container.getZkController().getZkStateReader(), policyName, null);
    rsp.getValues().add("result", "success");
  }

  private void handleSetPolicies(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op) throws KeeperException, InterruptedException, IOException {
    Map<String, Object> policies = op.getDataMap();
    for (Map.Entry<String, Object> policy : policies.entrySet()) {
      String policyName = policy.getKey();
      if (policyName == null || policyName.trim().length() == 0) {
        op.addError("The policy name cannot be null or empty");
      }
    }
    checkErr(op);

    try {
      zkSetPolicies(container.getZkController().getZkStateReader(), null, policies);
    } catch (Exception e) {
      log.warn("error persisting policies", e);
      op.addError(e.getMessage());
      checkErr(op);
    }

    rsp.getValues().add("result", "success");
  }

  private void zkSetPolicies(ZkStateReader reader, String policyBeRemoved, Map<String, Object> newPolicies) throws KeeperException, InterruptedException, IOException {
    while (true) {
      Stat stat = new Stat();
      ZkNodeProps loaded = null;
      byte[] data = reader.getZkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, stat, true);
      loaded = ZkNodeProps.load(data);
      Map<String, Object> policies = (Map<String, Object>) loaded.get("policies");
      if (policies == null) policies = new HashMap<>(1);
      if (newPolicies != null) {
        policies.putAll(newPolicies);
      } else {
        policies.remove(policyBeRemoved);
      }
      loaded = loaded.plus("policies", policies);
      verifyAutoScalingConf(loaded.getProperties());
      try {
        reader.getZkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(loaded), stat.getVersion(), true);
      } catch (KeeperException.BadVersionException bve) {
        // somebody else has changed the configuration so we must retry
        continue;
      }
      break;
    }
  }

  private void zkSetPreferences(ZkStateReader reader, List preferences) throws KeeperException, InterruptedException, IOException {
    while (true) {
      Stat stat = new Stat();
      ZkNodeProps loaded = null;
      byte[] data = reader.getZkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, stat, true);
      loaded = ZkNodeProps.load(data);
      loaded = loaded.plus("cluster-preferences", preferences);
      verifyAutoScalingConf(loaded.getProperties());
      try {
        reader.getZkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(loaded), stat.getVersion(), true);
      } catch (KeeperException.BadVersionException bve) {
        // somebody else has changed the configuration so we must retry
        continue;
      }
      break;
    }
  }

  private void zkSetClusterPolicy(ZkStateReader reader, List clusterPolicy) throws KeeperException, InterruptedException, IOException {
    while (true) {
      Stat stat = new Stat();
      ZkNodeProps loaded = null;
      byte[] data = reader.getZkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, stat, true);
      loaded = ZkNodeProps.load(data);
      loaded = loaded.plus("cluster-policy", clusterPolicy);
      verifyAutoScalingConf(loaded.getProperties());
      try {
        reader.getZkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(loaded), stat.getVersion(), true);
      } catch (KeeperException.BadVersionException bve) {
        // somebody else has changed the configuration so we must retry
        continue;
      }
      break;
    }
  }

  private void verifyAutoScalingConf(Map<String, Object> autoScalingConf) throws IOException {
    try (CloudSolrClient build = new CloudSolrClient.Builder()
        .withHttpClient(container.getUpdateShardHandler().getHttpClient())
        .withZkHost(container.getZkController().getZkServerAddress()).build()) {
      Policy policy = new Policy(autoScalingConf);
      Policy.Session session = policy.createSession(new SolrClientDataProvider(build));
      log.debug("Verified autoscaling configuration");
    }
  }

  private Map<String, Object> zkReadAutoScalingConf(ZkStateReader reader) throws KeeperException, InterruptedException {
    byte[] data = reader.getZkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    ZkNodeProps loaded = ZkNodeProps.load(data);
    return loaded.getProperties();
  }

  @Override
  public String getDescription() {
    return "A handler for autoscaling configuration";
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    switch (request.getHttpMethod()) {
      case "GET":
        return Name.AUTOSCALING_READ_PERM;
      case "POST":
        return Name.AUTOSCALING_WRITE_PERM;
      default:
        return null;
    }
  }

  @Override
  public Collection<Api> getApis() {
    return ApiBag.wrapRequestHandlers(this, "autoscaling.Commands");
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public SolrRequestHandler getSubHandler(String path) {
    if (path.equals("/diagnostics")) return this;
    return null;
  }
}
