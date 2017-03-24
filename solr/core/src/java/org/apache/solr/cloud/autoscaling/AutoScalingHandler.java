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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.CommandOperation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

/**
 * Handler for /cluster/autoscaling
 */
public class AutoScalingHandler extends RequestHandlerBase implements PermissionNameProvider {
  public static final String HANDLER_PATH = "/admin/autoscaling";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final CoreContainer container;
  private final List<Map<String, String>> DEFAULT_ACTIONS = new ArrayList<>(3);

  public AutoScalingHandler(CoreContainer container) {
    this.container = container;
    Map<String, String> map = new HashMap<>(2);
    map.put("name", "compute_plan");
    map.put("class", "solr.ComputePlanAction");
    DEFAULT_ACTIONS.add(map);
    map = new HashMap<>(2);
    map.put("name", "execute_plan");
    map.put("class", "solr.ExecutePlanAction");
    DEFAULT_ACTIONS.add(map);
    map = new HashMap<>(2);
    map.put("name", "log_plan");
    map.put("class", "solr.LogPlanAction");
    DEFAULT_ACTIONS.add(map);
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    if (req.getContentStreams() == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No contentStream");
    }
    List<CommandOperation> ops = CommandOperation.readCommands(req.getContentStreams(), rsp);
    if (ops == null) {
      // errors have already been added to the response so there's nothing left to do
      return;
    }
    for (CommandOperation op : ops) {
      switch (op.name) {
        case "set-trigger":
          handleSetTrigger(req, rsp, op);
          break;
        case "remove-trigger":
          handleRemoveTrigger(req, rsp, op);
      }
    }
  }

  private void handleSetTrigger(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op) throws KeeperException, InterruptedException {
    String triggerName = op.getStr("name");

    if (triggerName == null || triggerName.trim().length() == 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The trigger name cannot be null or empty");
    }

    String eventTypeStr = op.getStr("event");
    if (eventTypeStr == null || eventTypeStr.trim().length() == 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The event type cannot be null or empty in trigger: " + triggerName);
    }
    AutoScaling.EventType eventType = AutoScaling.EventType.valueOf(eventTypeStr.trim().toUpperCase(Locale.ROOT));

    String waitForStr = op.getStr("waitFor", null);
    if (waitForStr != null) {
      char c = waitForStr.charAt(waitForStr.length() - 1);
      long waitForValue = Long.parseLong(waitForStr.substring(0, waitForStr.length() - 1));
      int seconds;
      switch (c) {
        case 'h':
          seconds = (int) TimeUnit.HOURS.toSeconds(waitForValue);
          break;
        case 'm':
          seconds = (int) TimeUnit.MINUTES.toSeconds(waitForValue);
          break;
        case 's':
          seconds = (int) waitForValue;
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid 'waitFor' value in trigger: " + triggerName);
      }
      op.getDataMap().put("waitFor", seconds);
    }

    Integer lowerBound = op.getInt("lowerBound", null);
    Integer upperBound = op.getInt("upperBound", null);

    List<Map<String, String>> actions = (List<Map<String, String>>) op.getVal("actions");
    if (actions == null) {
      actions = DEFAULT_ACTIONS;
    }

    // validate that we can load all the actions
    // todo nocommit -- what about MemClassLoader?
    for (Map<String, String> action : actions) {
      if (!action.containsKey("name") || !action.containsKey("class")) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No 'name' or 'class' specified for action: " + action);
      }
      String klass = action.get("class");
      try {
        container.getResourceLoader().findClass(klass, TriggerAction.class);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error trying to find Action: " + klass, e);
      }
    }

    zkSetTrigger(container.getZkController().getZkStateReader(), triggerName, op.getValuesExcluding("name"));
    rsp.getValues().add("result", "success");
  }

  private void handleRemoveTrigger(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op) throws KeeperException, InterruptedException {
    String triggerName = op.getStr("name");

    if (triggerName == null || triggerName.trim().length() == 0) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The trigger name cannot be null or empty");
    }
    Map<String, Object> autoScalingConf = zkReadAutoScalingConf(container.getZkController().getZkStateReader());
    Map<String, Object> triggers = (Map<String, Object>) autoScalingConf.get("triggers");
    if (triggers == null || !triggers.containsKey(triggerName)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No trigger exists with name: " + triggerName);
    }
    zkSetTrigger(container.getZkController().getZkStateReader(), triggerName, null);
    rsp.getValues().add("result", "success");
  }

  private void zkSetTrigger(ZkStateReader reader, String triggerName, Map<String, Object> triggerProperties) throws KeeperException, InterruptedException {
    while (true) {
      Stat stat = new Stat();
      ZkNodeProps loaded = null;
      byte[] data = reader.getZkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, stat, true);
      loaded = ZkNodeProps.load(data);
      Map<String, Object> triggers = (Map<String, Object>) loaded.get("triggers");
      if (triggers == null) triggers = new HashMap<>(1);
      if (triggerProperties != null) {
        triggers.put(triggerName, triggerProperties);
      } else {
        triggers.remove(triggerName);
      }
      loaded = loaded.plus("triggers", triggers);
      try {
        reader.getZkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(loaded), stat.getVersion(), true);
      } catch (KeeperException.BadVersionException bve) {
        // somebody else has changed the configuration so we must retry
        continue;
      }
      break;
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
    return ApiBag.wrapRequestHandlers(this, "scaling.Commands");
  }
}
