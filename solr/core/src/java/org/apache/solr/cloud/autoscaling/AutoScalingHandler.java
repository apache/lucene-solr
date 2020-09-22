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
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.autoscaling.Clause;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.Preference;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toSet;
import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;
import static org.apache.solr.common.params.AutoScalingParams.*;
import static org.apache.solr.common.params.CommonParams.JSON;

/**
 * Handler for /cluster/autoscaling.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
public class AutoScalingHandler extends RequestHandlerBase implements PermissionNameProvider {
  public static final String HANDLER_PATH = "/admin/autoscaling";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final SolrCloudManager cloudManager;
  protected final SolrResourceLoader loader;
  protected final AutoScaling.TriggerFactory triggerFactory;
  private final List<Map<String, String>> DEFAULT_ACTIONS = new ArrayList<>(3);
  private static Set<String> singletonCommands = Stream.of("set-cluster-preferences", "set-cluster-policy")
      .collect(collectingAndThen(toSet(), Collections::unmodifiableSet));

  private final TimeSource timeSource;

  public AutoScalingHandler(SolrCloudManager cloudManager, SolrResourceLoader loader) {
    this.cloudManager = cloudManager;
    this.loader = loader;
    this.triggerFactory = new AutoScaling.TriggerFactoryImpl(loader, cloudManager);
    this.timeSource = cloudManager.getTimeSource();
    Map<String, String> map = new HashMap<>(2);
    map.put(NAME, "compute_plan");
    map.put(CLASS, "solr.ComputePlanAction");
    DEFAULT_ACTIONS.add(map);
    map = new HashMap<>(2);
    map.put(NAME, "execute_plan");
    map.put(CLASS, "solr.ExecutePlanAction");
    DEFAULT_ACTIONS.add(map);
  }

  Optional<BiConsumer<SolrQueryResponse, AutoScalingConfig>> getSubpathExecutor(List<String> path, SolrQueryRequest request) {
    if (path.size() == 3) {
      if (DIAGNOSTICS.equals(path.get(2))) {
        return Optional.of((rsp, autoScalingConf) -> handleDiagnostics(rsp, autoScalingConf));
      } else if (SUGGESTIONS.equals(path.get(2))) {
        return Optional.of((rsp, autoScalingConf) -> handleSuggestions(rsp, autoScalingConf, request.getParams()));
      } else {
        return Optional.empty();
      }

    }
    return Optional.empty();
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    try {
      String httpMethod = (String) req.getContext().get("httpMethod");
      RequestHandlerUtils.setWt(req, JSON);

      if ("GET".equals(httpMethod)) {
        String path = (String) req.getContext().get("path");
        if (path == null) path = "/cluster/autoscaling";
        List<String> parts = StrUtils.splitSmart(path, '/', true);

        if (parts.size() < 2 || parts.size() > 3) {
          // invalid
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown path: " + path);
        }

        AutoScalingConfig autoScalingConf = cloudManager.getDistribStateManager().getAutoScalingConfig();
        if (parts.size() == 2) {
          autoScalingConf.writeMap(new MapWriter.EntryWriter() {

            @Override
            public MapWriter.EntryWriter put(CharSequence k, Object v) {
              rsp.getValues().add(k.toString(), v);
              return this;
            }
          });
        } else {
          getSubpathExecutor(parts, req).ifPresent(it -> it.accept(rsp, autoScalingConf));
        }
      } else {
        if (req.getContentStreams() == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No commands specified for autoscaling");
        }
        String path = (String) req.getContext().get("path");
        if (path != null) {
          List<String> parts = StrUtils.splitSmart(path, '/', true);
          if(parts.size() == 3){
            getSubpathExecutor(parts, req).ifPresent(it -> {
              Map map = null;
              try {
                map = (Map) Utils.fromJSON(req.getContentStreams().iterator().next().getStream());
              } catch (IOException e1) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "error parsing payload", e1);
              }
              it.accept(rsp, new AutoScalingConfig(map));
            });

            return;
          }

        }
        List<CommandOperation> ops = CommandOperation.readCommands(req.getContentStreams(), rsp.getValues(), singletonCommands);
        if (ops == null) {
          // errors have already been added to the response so there's nothing left to do
          return;
        }
        processOps(req, rsp, ops);
      }

    } catch (Exception e) {
      rsp.getValues().add("result", "failure");
      throw e;
    } finally {
      RequestHandlerUtils.addExperimentalFormatWarning(rsp);
    }
  }


  @SuppressWarnings({"unchecked"})
  private void handleSuggestions(SolrQueryResponse rsp, AutoScalingConfig autoScalingConf, SolrParams params) {
    rsp.getValues().add("suggestions",
        PolicyHelper.getSuggestions(autoScalingConf, cloudManager, params));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void processOps(SolrQueryRequest req, SolrQueryResponse rsp, List<CommandOperation> ops)
      throws KeeperException, InterruptedException, IOException {
    while (true) {
      AutoScalingConfig initialConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();
      AutoScalingConfig currentConfig = initialConfig;
      for (CommandOperation op : ops) {
        switch (op.name) {
          case CMD_SET_TRIGGER:
            currentConfig = handleSetTrigger(req, rsp, op, currentConfig);
            break;
          case CMD_REMOVE_TRIGGER:
            currentConfig = handleRemoveTrigger(req, rsp, op, currentConfig);
            break;
          case CMD_SET_LISTENER:
            currentConfig = handleSetListener(req, rsp, op, currentConfig);
            break;
          case CMD_REMOVE_LISTENER:
            currentConfig = handleRemoveListener(req, rsp, op, currentConfig);
            break;
          case CMD_SUSPEND_TRIGGER:
            currentConfig = handleSuspendTrigger(req, rsp, op, currentConfig);
            break;
          case CMD_RESUME_TRIGGER:
            currentConfig = handleResumeTrigger(req, rsp, op, currentConfig);
            break;
          case CMD_SET_POLICY:
            currentConfig = handleSetPolicies(req, rsp, op, currentConfig);
            break;
          case CMD_REMOVE_POLICY:
            currentConfig = handleRemovePolicy(req, rsp, op, currentConfig);
            break;
          case CMD_SET_CLUSTER_PREFERENCES:
            currentConfig = handleSetClusterPreferences(req, rsp, op, currentConfig);
            break;
          case CMD_SET_CLUSTER_POLICY:
            currentConfig = handleSetClusterPolicy(req, rsp, op, currentConfig);
            break;
          case CMD_SET_PROPERTIES:
            currentConfig = handleSetProperties(req, rsp, op, currentConfig);
            break;
          default:
            op.addError("Unknown command: " + op.name);
        }
      }
      List errs = CommandOperation.captureErrors(ops);
      if (!errs.isEmpty()) {
        throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, "Error in command payload", errs);
      }

      if (!currentConfig.equals(initialConfig)) {
        // update in ZK
        if (setAutoScalingConfig(currentConfig)) {
          break;
        } else {
          // someone else updated the config, get the latest one and re-apply our ops
          rsp.getValues().add("retry", "initialVersion=" + initialConfig.getZkVersion());
          continue;
        }
      } else {
        // no changes
        break;
      }
    }
    rsp.getValues().add("result", "success");
  }

  private AutoScalingConfig handleSetProperties(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op, AutoScalingConfig currentConfig) {
    Map<String, Object> map = op.getDataMap() == null ? Collections.emptyMap() : op.getDataMap();
    Map<String, Object> configProps = new HashMap<>(currentConfig.getProperties());
    configProps.putAll(map);
    // remove a key which is set to null
    map.forEach((k, v) -> {
      if (v == null)  configProps.remove(k);
    });
    return currentConfig.withProperties(configProps);
  }

  @SuppressWarnings({"unchecked"})
  private void handleDiagnostics(SolrQueryResponse rsp, AutoScalingConfig autoScalingConf) {
    Policy policy = autoScalingConf.getPolicy();
    rsp.getValues().add("diagnostics", PolicyHelper.getDiagnostics(policy, cloudManager));
  }

  @SuppressWarnings({"unchecked"})
  private AutoScalingConfig handleSetClusterPolicy(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op,
                                                   AutoScalingConfig currentConfig) throws KeeperException, InterruptedException, IOException {
    List<Map<String, Object>> clusterPolicy = (List<Map<String, Object>>) op.getCommandData();
    if (clusterPolicy == null || !(clusterPolicy instanceof List)) {
      op.addError("set-cluster-policy expects an array of objects");
      return currentConfig;
    }
    List<Clause> cp = null;
    try {
      cp = clusterPolicy.stream().map(Clause::create).collect(Collectors.toList());
    } catch (Exception e) {
      op.addError(e.getMessage());
      return currentConfig;
    }
    Policy p = currentConfig.getPolicy().withClusterPolicy(cp);
    currentConfig = currentConfig.withPolicy(p);
    return currentConfig;
  }

  @SuppressWarnings({"unchecked"})
  private AutoScalingConfig handleSetClusterPreferences(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op,
                                                        AutoScalingConfig currentConfig) throws KeeperException, InterruptedException, IOException {
    List<Map<String, Object>> preferences = (List<Map<String, Object>>) op.getCommandData();
    if (preferences == null || !(preferences instanceof List)) {
      op.addError("A list of cluster preferences not found");
      return currentConfig;
    }
    List<Preference> prefs = null;
    try {
      prefs = preferences.stream().map(Preference::new).collect(Collectors.toList());
    } catch (Exception e) {
      op.addError(e.getMessage());
      return currentConfig;
    }
    Policy p = currentConfig.getPolicy().withClusterPreferences(prefs);
    currentConfig = currentConfig.withPolicy(p);
    return currentConfig;
  }

  private AutoScalingConfig handleRemovePolicy(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op,
                                               AutoScalingConfig currentConfig) throws KeeperException, InterruptedException, IOException {
    String policyName = (String) op.getVal("");

    if (op.hasError()) return currentConfig;

    Map<String, List<Clause>> policies = currentConfig.getPolicy().getPolicies();
    if (policies == null || !policies.containsKey(policyName)) {
      op.addError("No policy exists with name: " + policyName);
      return currentConfig;
    }

    cloudManager.getClusterStateProvider().getClusterState().forEachCollection(coll -> {
      if (policyName.equals(coll.getPolicyName()))
        op.addError(StrUtils.formatString("policy : {0} is being used by collection {1}", policyName, coll.getName()));
    });
    if (op.hasError()) return currentConfig;
    policies = new HashMap<>(policies);
    policies.remove(policyName);
    Policy p = currentConfig.getPolicy().withPolicies(policies);
    currentConfig = currentConfig.withPolicy(p);
    return currentConfig;
  }

  @SuppressWarnings({"unchecked"})
  private AutoScalingConfig handleSetPolicies(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op,
                                              AutoScalingConfig currentConfig) throws KeeperException, InterruptedException, IOException {
    Map<String, Object> policiesMap = op.getDataMap();
    for (Map.Entry<String, Object> policy : policiesMap.entrySet()) {
      String policyName = policy.getKey();
      if (policyName == null || policyName.trim().length() == 0) {
        op.addError("The policy name cannot be null or empty");
        return currentConfig;
      }
    }
    Map<String, List<Clause>> currentClauses = new HashMap<>(currentConfig.getPolicy().getPolicies());
    Map<String, List<Clause>> newClauses = null;
    try {
      newClauses = Policy.clausesFromMap((Map<String, List<Map<String, Object>>>) op.getCommandData(),
          new ArrayList<>() );
    } catch (Exception e) {
      op.addError(e.getMessage());
      return currentConfig;
    }
    currentClauses.putAll(newClauses);
    Policy p = currentConfig.getPolicy().withPolicies(currentClauses);
    currentConfig = currentConfig.withPolicy(p);
    return currentConfig;
  }

  @SuppressWarnings({"unchecked"})
  private AutoScalingConfig handleResumeTrigger(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op,
                                                AutoScalingConfig currentConfig) throws KeeperException, InterruptedException {
    String triggerName = op.getStr(NAME);
    if (op.hasError()) return currentConfig;
    Map<String, AutoScalingConfig.TriggerConfig> triggers = currentConfig.getTriggerConfigs();
    Set<String> changed = new HashSet<>();
    if (!Policy.EACH.equals(triggerName) && !triggers.containsKey(triggerName)) {
      op.addError("No trigger exists with name: " + triggerName);
      return currentConfig;
    }
    Map<String, AutoScalingConfig.TriggerConfig> newTriggers = new HashMap<>();
    for (Map.Entry<String, AutoScalingConfig.TriggerConfig> entry : triggers.entrySet()) {
      if (Policy.EACH.equals(triggerName) || triggerName.equals(entry.getKey())) {
        AutoScalingConfig.TriggerConfig trigger = entry.getValue();
        if (!trigger.enabled) {
          trigger = trigger.withEnabled(true);
          newTriggers.put(entry.getKey(), trigger);
          changed.add(entry.getKey());
        } else {
          newTriggers.put(entry.getKey(), entry.getValue());
        }
      } else {
        newTriggers.put(entry.getKey(), entry.getValue());
      }
    }
    rsp.getValues().add("changed", changed);
    if (!changed.isEmpty()) {
      currentConfig = currentConfig.withTriggerConfigs(newTriggers);
    }
    return currentConfig;
  }

  @SuppressWarnings({"unchecked"})
  private AutoScalingConfig handleSuspendTrigger(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op,
                                                 AutoScalingConfig currentConfig) throws KeeperException, InterruptedException {
    String triggerName = op.getStr(NAME);
    if (op.hasError()) return currentConfig;
    String timeout = op.getStr(TIMEOUT, null);
    Date resumeTime = null;
    if (timeout != null) {
      try {
        int timeoutSeconds = parseHumanTime(timeout);
        resumeTime = new Date(TimeUnit.MILLISECONDS.convert(timeSource.getTimeNs(), TimeUnit.NANOSECONDS)
            + TimeUnit.MILLISECONDS.convert(timeoutSeconds, TimeUnit.SECONDS));
      } catch (IllegalArgumentException e) {
        op.addError("Invalid 'timeout' value for suspend trigger: " + triggerName);
        return currentConfig;
      }
    }

    Map<String, AutoScalingConfig.TriggerConfig> triggers = currentConfig.getTriggerConfigs();
    Set<String> changed = new HashSet<>();

    if (!Policy.EACH.equals(triggerName) && !triggers.containsKey(triggerName)) {
      op.addError("No trigger exists with name: " + triggerName);
      return currentConfig;
    }
    Map<String, AutoScalingConfig.TriggerConfig> newTriggers = new HashMap<>();
    for (Map.Entry<String, AutoScalingConfig.TriggerConfig> entry : triggers.entrySet()) {
      if (Policy.EACH.equals(triggerName) || triggerName.equals(entry.getKey())) {
        AutoScalingConfig.TriggerConfig trigger = entry.getValue();
        if (trigger.enabled) {
          trigger = trigger.withEnabled(false);
          if (resumeTime != null) {
            trigger = trigger.withProperty(RESUME_AT, resumeTime.getTime());
          }
          newTriggers.put(entry.getKey(), trigger);
          changed.add(trigger.name);
        } else {
          newTriggers.put(entry.getKey(), entry.getValue());
        }
      } else {
        newTriggers.put(entry.getKey(), entry.getValue());
      }
    }
    rsp.getValues().add("changed", changed);
    if (!changed.isEmpty()) {
      currentConfig = currentConfig.withTriggerConfigs(newTriggers);
    }
    return currentConfig;
  }

  private AutoScalingConfig handleRemoveListener(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op,
                                    AutoScalingConfig currentConfig) throws KeeperException, InterruptedException {
    String listenerName = op.getStr(NAME);

    if (op.hasError()) return currentConfig;
    Map<String, AutoScalingConfig.TriggerListenerConfig> listeners = currentConfig.getTriggerListenerConfigs();
    if (listeners == null || !listeners.containsKey(listenerName)) {
      op.addError("No listener exists with name: " + listenerName);
      return currentConfig;
    }
    currentConfig = currentConfig.withoutTriggerListenerConfig(listenerName);
    return currentConfig;
  }

  private AutoScalingConfig handleSetListener(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op,
                                 AutoScalingConfig currentConfig) throws KeeperException, InterruptedException {
    String listenerName = op.getStr(NAME);
    String triggerName = op.getStr(TRIGGER);
    List<String> stageNames = op.getStrs(STAGE, Collections.emptyList());
    String listenerClass = op.getStr(CLASS);
    List<String> beforeActions = op.getStrs(BEFORE_ACTION, Collections.emptyList());
    List<String> afterActions = op.getStrs(AFTER_ACTION, Collections.emptyList());

    if (op.hasError()) return currentConfig;

    Map<String, AutoScalingConfig.TriggerConfig> triggers = currentConfig.getTriggerConfigs();
    if (triggers == null || !triggers.containsKey(triggerName)) {
      op.addError("A trigger with the name " + triggerName + " does not exist");
      return currentConfig;
    }
    AutoScalingConfig.TriggerConfig triggerConfig = triggers.get(triggerName);

    if (stageNames.isEmpty() && beforeActions.isEmpty() && afterActions.isEmpty()) {
      op.addError("Either 'stage' or 'beforeAction' or 'afterAction' must be specified");
      return currentConfig;
    }

    for (String stage : stageNames) {
      try {
        TriggerEventProcessorStage.valueOf(stage);
      } catch (IllegalArgumentException e) {
        op.addError("Invalid stage name: " + stage);
      }
    }
    if (op.hasError()) return currentConfig;

    AutoScalingConfig.TriggerListenerConfig listenerConfig = new AutoScalingConfig.TriggerListenerConfig(listenerName, op.getValuesExcluding("name"));

    // validate that we can load the listener class
    // todo allow creation from blobstore
    TriggerListener listener = null;
    try {
      listener = loader.newInstance(listenerClass, TriggerListener.class);
      listener.configure(loader, cloudManager, listenerConfig);
    } catch (TriggerValidationException e) {
      log.warn("invalid listener configuration", e);
      op.addError("invalid listener configuration: " + e.toString());
      return currentConfig;
    } catch (Exception e) {
      log.warn("error loading listener class ", e);
      op.addError("Listener not found: " + listenerClass + ". error message:" + e.getMessage());
      return currentConfig;
    } finally {
      if (listener != null) {
        IOUtils.closeQuietly(listener);
      }
    }

    Set<String> actionNames = new HashSet<>();
    actionNames.addAll(beforeActions);
    actionNames.addAll(afterActions);
    for (AutoScalingConfig.ActionConfig action : triggerConfig.actions) {
      actionNames.remove(action.name);
    }
    if (!actionNames.isEmpty()) {
      op.addError("The trigger '" + triggerName + "' does not have actions named: " + actionNames);
      return currentConfig;
    }
    // todo - handle races between competing set-trigger and set-listener invocations
    currentConfig = currentConfig.withTriggerListenerConfig(listenerConfig);
    return currentConfig;
  }

  @SuppressWarnings({"unchecked"})
  private AutoScalingConfig handleSetTrigger(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op,
                                             AutoScalingConfig currentConfig) throws KeeperException, InterruptedException {
    // we're going to modify the op - use a copy
    String triggerName = op.getStr(NAME);
    String eventTypeStr = op.getStr(EVENT);

    if (op.hasError()) return currentConfig;
    TriggerEventType.valueOf(eventTypeStr.trim().toUpperCase(Locale.ROOT));

    String waitForStr = op.getStr(WAIT_FOR, null);

    CommandOperation opCopy = new CommandOperation(op.name, Utils.getDeepCopy((Map) op.getCommandData(), 10));

    if (waitForStr != null) {
      int seconds = 0;
      try {
        seconds = parseHumanTime(waitForStr);
      } catch (IllegalArgumentException e) {
        op.addError("Invalid 'waitFor' value '" + waitForStr + "' in trigger: " + triggerName);
        return currentConfig;
      }
      opCopy.getDataMap().put(WAIT_FOR, seconds);
    }

    List<Map<String, String>> actions = (List<Map<String, String>>) op.getVal(ACTIONS);
    if (actions == null) {
      actions = DEFAULT_ACTIONS;
      opCopy.getDataMap().put(ACTIONS, actions);
    }

    // validate that we can load all the actions
    // todo allow creation from blobstore
    for (Map<String, String> action : actions) {
      if (!action.containsKey(NAME) || !action.containsKey(CLASS)) {
        op.addError("No 'name' or 'class' specified for action: " + action);
        return currentConfig;
      }
      String klass = action.get(CLASS);
      try {
        loader.findClass(klass, TriggerAction.class);
      } catch (Exception e) {
        log.warn("Could not load class : ", e);
        op.addError("Action not found: " + klass + " " + e.getMessage());
        return currentConfig;
      }
    }
    AutoScalingConfig.TriggerConfig trigger = new AutoScalingConfig.TriggerConfig(triggerName, opCopy.getValuesExcluding("name"));
    // validate trigger config
    AutoScaling.Trigger t = null;
    try {
      t = triggerFactory.create(trigger.event, trigger.name, trigger.properties);
    } catch (Exception e) {
      op.addError("Error validating trigger config " + trigger.name + ": " + e.toString());
      return currentConfig;
    } finally {
      if (t != null) {
        IOUtils.closeQuietly(t);
      }
    }
    currentConfig = currentConfig.withTriggerConfig(trigger);
    // check that there's a default SystemLogListener, unless user specified another one
    return withSystemLogListener(currentConfig, triggerName);
  }

  public static AutoScalingConfig withSystemLogListener(AutoScalingConfig autoScalingConfig, String triggerName) {
    Map<String, AutoScalingConfig.TriggerListenerConfig> configs = autoScalingConfig.getTriggerListenerConfigs();
    for (AutoScalingConfig.TriggerListenerConfig cfg : configs.values()) {
      if (triggerName.equals(cfg.trigger)) {
        // already has some listener config
        return autoScalingConfig;
      }
    }
    // need to add
    Map<String, Object> properties = new HashMap<>();
    properties.put(AutoScalingParams.CLASS, SystemLogListener.class.getName());
    properties.put(AutoScalingParams.TRIGGER, triggerName);
    properties.put(AutoScalingParams.STAGE, EnumSet.allOf(TriggerEventProcessorStage.class));
    AutoScalingConfig.TriggerListenerConfig listener =
        new AutoScalingConfig.TriggerListenerConfig(triggerName + CollectionAdminParams.SYSTEM_COLL, properties);
    autoScalingConfig = autoScalingConfig.withTriggerListenerConfig(listener);
    return autoScalingConfig;
  }

  private int parseHumanTime(String timeStr) {
    char c = timeStr.charAt(timeStr.length() - 1);
    long timeValue = Long.parseLong(timeStr.substring(0, timeStr.length() - 1));
    int seconds;
    switch (c) {
      case 'h':
        seconds = (int) TimeUnit.HOURS.toSeconds(timeValue);
        break;
      case 'm':
        seconds = (int) TimeUnit.MINUTES.toSeconds(timeValue);
        break;
      case 's':
        seconds = (int) timeValue;
        break;
      default:
        throw new IllegalArgumentException("Invalid time value");
    }
    return seconds;
  }

  private AutoScalingConfig handleRemoveTrigger(SolrQueryRequest req, SolrQueryResponse rsp, CommandOperation op,
                                   AutoScalingConfig currentConfig) throws KeeperException, InterruptedException {
    String triggerName = op.getStr(NAME);
    boolean removeListeners = op.getBoolean(REMOVE_LISTENERS, false);

    if (op.hasError()) return currentConfig;
    Map<String, AutoScalingConfig.TriggerConfig> triggerConfigs = currentConfig.getTriggerConfigs();
    if (!triggerConfigs.containsKey(triggerName)) {
      op.addError("No trigger exists with name: " + triggerName);
      return currentConfig;
    }
    triggerConfigs = new HashMap<>(triggerConfigs);
    Set<String> activeListeners = new HashSet<>();
    Map<String, AutoScalingConfig.TriggerListenerConfig> listeners = currentConfig.getTriggerListenerConfigs();
    for (AutoScalingConfig.TriggerListenerConfig listener : listeners.values()) {
      if (triggerName.equals(listener.trigger)) {
        activeListeners.add(listener.name);
      }
    }
    if (!activeListeners.isEmpty()) {
      boolean onlySystemLog = false;
      if (activeListeners.size() == 1) {
        AutoScalingConfig.TriggerListenerConfig cfg = listeners.get(activeListeners.iterator().next());
        if (SystemLogListener.class.getName().equals(cfg.listenerClass) ||
            ("solr." + SystemLogListener.class.getSimpleName()).equals(cfg.listenerClass)) {
          onlySystemLog = true;
        }
      }
      if (removeListeners || onlySystemLog) {
        listeners = new HashMap<>(listeners);
        listeners.keySet().removeAll(activeListeners);
      } else {
        op.addError("Cannot remove trigger: " + triggerName + " because it has active listeners: " + activeListeners);
        return currentConfig;
      }
    }
    triggerConfigs.remove(triggerName);
    currentConfig = currentConfig.withTriggerConfigs(triggerConfigs).withTriggerListenerConfigs(listeners);
    return currentConfig;
  }


  private boolean setAutoScalingConfig(AutoScalingConfig currentConfig) throws KeeperException, InterruptedException, IOException {
    verifyAutoScalingConf(currentConfig);
    try {
      cloudManager.getDistribStateManager().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(currentConfig), currentConfig.getZkVersion());
    } catch (BadVersionException bve) {
      // somebody else has changed the configuration so we must retry
      return false;
    }
    //log.debug("-- saved version " + currentConfig.getZkVersion() + ": " + currentConfig);
    return true;
  }

  private void verifyAutoScalingConf(AutoScalingConfig autoScalingConf) throws IOException {
    autoScalingConf.getPolicy().createSession(cloudManager);
    log.debug("Verified autoscaling configuration");
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
      case "POST": {
        return StrUtils.splitSmart(request.getResource(), '/', true).size() == 3 ?
            Name.AUTOSCALING_READ_PERM :
            Name.AUTOSCALING_WRITE_PERM;
      }
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
    if (path.equals("/diagnostics") || path.equals("/suggestions")) return this;
    return null;
  }
}
