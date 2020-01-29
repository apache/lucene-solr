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

package org.apache.solr.managed;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.request.beans.ResourcePoolConfig;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ResourceManagerAPI implements SolrInfoBean {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static class ResourcePoolConfigs implements ReflectMapWriter {
    // not persisted
    public int znodeVersion = -1;

    @JsonProperty
    public Map<String, ResourcePoolConfig> configs = new LinkedHashMap<>();

    @Override
    public String toString() {
      return "ResourcePoolConfigs{" +
          "znodeVersion=" + znodeVersion +
          ", configs=" + configs +
          '}';
    }

    public void setPoolConfig(String name, ResourcePoolConfig config) {
      if (!(configs instanceof LinkedHashMap)) {
        configs = new LinkedHashMap<>(configs);
      }
      configs.put(name, config);
    }

    public ResourcePoolConfig removePoolConfig(String name) {
      if (configs.containsKey(name)) {
        if (!(configs instanceof LinkedHashMap)) {
          configs = new LinkedHashMap<>(configs);
        }
        return configs.remove(name);
      } else {
        return null;
      }
    }
  }

  private final ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();

  private final ResourceManager resourceManager;
  private final SolrCloudManager solrCloudManager;
  private final DistribStateManager stateManager;
  private final ReentrantLock updateLock = new ReentrantLock();

  public final ReadPoolAPI readPoolApi = new ReadPoolAPI();
  public final EditPoolAPI editPoolApi = new EditPoolAPI();
  public final ReadComponentAPI readComponentApi = new ReadComponentAPI();
  public final EditComponentAPI editComponentApi = new EditComponentAPI();

  private volatile boolean isClosed = false;

  private SolrMetricsContext solrMetricsContext;

  private ResourcePoolConfigs resourcePoolConfigs = new ResourcePoolConfigs();

  // this config file may be frequently updated, changes are applied on live nodes
  public final String POOL_CONFIGS_PATH = ZkStateReader.RESOURCE_MANAGER_ZNODE + ZkStateReader.RESOURCE_MANAGER_POOL_CONF_PATH;
  // this config file change requires a node restart, it's not watched
  public final String MANAGER_CONFIG_PATH = ZkStateReader.RESOURCE_MANAGER_ZNODE + ZkStateReader.RESOURCE_MANAGER_CONF_PATH;

  public ResourceManagerAPI(CoreContainer coreContainer) {
    if (coreContainer.isZooKeeperAware()) {
      solrCloudManager = coreContainer.getZkController().getSolrCloudManager();
      stateManager = solrCloudManager.getDistribStateManager();
      // default config
      Map<String, Object> resManConfig = new HashMap<>();
      resManConfig.put(FieldType.CLASS_NAME, DefaultResourceManager.class.getName());
      Map<String, Object> configProps;
      try {
        configProps = Utils.getJson(stateManager, MANAGER_CONFIG_PATH);
      } catch (Exception e) {
        log.warn("Exception retrieving {}, using defaults: {}", MANAGER_CONFIG_PATH, e);
        configProps = Collections.emptyMap();
      }
      // apply overrides
      resManConfig.putAll(configProps);

      ResourceManager resMan;
      try {
        resMan = ResourceManager.load(coreContainer.getResourceLoader(), coreContainer.getMetricManager(),
            solrCloudManager.getTimeSource(), new PluginInfo("resourceManager", resManConfig));
      } catch (Exception e) {
        log.warn("Resource manager initialization error - disabling!", e);
        resMan = NoOpResourceManager.INSTANCE;
      }
      resourceManager = resMan;
      updateResourcePoolConfigsFromZk();
    } else {
      resourceManager = NoOpResourceManager.INSTANCE;
      solrCloudManager = null;
      stateManager = null;
    }
  }

  public ResourceManager getResourceManager() {
    return resourceManager;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String childScope) {
    this.solrMetricsContext = parentContext.getChildContext(this, childScope);
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public String getDescription() {
    return "Resource management handler API v2";
  }

  @Override
  public Category getCategory() {
    return Category.RESOURCE;
  }

  @Override
  public void close() {
    updateLock.lock();
    try {
      SolrInfoBean.super.close();
      if (resourceManager != null) {
        resourceManager.close();
      }
      isClosed = true;
    } catch (Exception e) {
      log.warn("Error closing, ignoring...", e);
    } finally {
      updateLock.unlock();
    }
  }

  private boolean checkEnabled(CommandOperation payload) {
    if (!resourceManager.isEnabled()) {
      payload.addError("Resource manager is not enabled.");
      return false;
    }
    return true;
  }

  // ========= ZK config monitoring & refresh & save ===========

  private final Watcher resourceManagerConfigWatcher = event -> {
    // session events are not change events, and do not remove the watcher
    if (Watcher.Event.EventType.None.equals(event.getType())) {
      return;
    }
    updateResourcePoolConfigsFromZk();
  };

  private void updateResourcePoolConfigsFromZk() {
    if (isClosed) {
      return;
    }
    updateLock.lock();
    try {
      while (!isClosed) {
        try {
          if (Thread.currentThread().isInterrupted()) {
            log.warn("Interrupted");
            break;
          }
          if (!stateManager.hasData(POOL_CONFIGS_PATH)) {
            resourcePoolConfigs = new ResourcePoolConfigs();
            resourcePoolConfigs.configs = resourceManager.getDefaultPoolConfigs();
            resourcePoolConfigs.znodeVersion = -1;
            return;
          }
          VersionedData data = stateManager.getData(POOL_CONFIGS_PATH, resourceManagerConfigWatcher);
          if (data.getVersion() == resourcePoolConfigs.znodeVersion) {
            // spurious wake-up
            return;
          }
          ResourcePoolConfigs newResourcePoolConfigs = mapper.readValue(data.getData(), ResourcePoolConfigs.class);
          newResourcePoolConfigs.znodeVersion = data.getVersion();
          log.debug("Loaded new resource pool configs: {}", newResourcePoolConfigs);
          resourceManager.setPoolConfigs(newResourcePoolConfigs.configs);
          // update our version only after the changes have been processed
          resourcePoolConfigs = newResourcePoolConfigs;
          return;
        } catch (IOException | KeeperException e) {
          if (e instanceof KeeperException.SessionExpiredException ||
              (e.getCause() != null && e.getCause() instanceof KeeperException.SessionExpiredException)) {
            log.warn("Solr cannot talk to ZK, exiting " +
                getClass().getSimpleName() + " pool", e);
            return;
          } else {
            log.error("A ZK error has occurred, retrying", e);
            solrCloudManager.getTimeSource().sleep(100);
          }
        }
      }
    } catch (InterruptedException e) {
      log.error("Error reading resource pool configuration from zookeeper", SolrZkClient.checkInterrupted(e));
    } finally {
      updateLock.unlock();
    }
  }

  // this has to be called under updateLock
  private void persistResourcePoolConfigsToZk() throws Exception {
    if (isClosed) {
      return;
    }
    if (stateManager.hasData(POOL_CONFIGS_PATH)) {
      stateManager.setData(POOL_CONFIGS_PATH, Utils.toJSON(resourcePoolConfigs), resourcePoolConfigs.znodeVersion);
    } else {
      stateManager.makePath(POOL_CONFIGS_PATH, Utils.toJSON(resourcePoolConfigs), CreateMode.PERSISTENT, false);
    }
  }

  // ============= helper methods ==============

  private List<ManagedComponent> getComponents(SolrQueryRequest req, SolrQueryResponse rsp, AtomicReference<ResourceManagerPool> poolRef) {
    String poolName = req.getPathTemplateValues().get("poolName");
    if (poolName == null || poolName.isBlank()) {
      rsp.setException(new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required pool name."));
      return null;
    }
    ResourceManagerPool<ManagedComponent> pool = resourceManager.getPool(poolName);
    if (pool == null) {
      rsp.setException(new SolrException(SolrException.ErrorCode.NOT_FOUND, "Pool '" + poolName + "' not found."));
      return null;
    }
    poolRef.set(pool);
    String componentName = req.getPathTemplateValues().get("componentName");
    NamedList<Object> result = new SimpleOrderedMap<>();
    // we support a prefix of component names because eg. searcher caches will have a quickly
    // changing unique suffix
    List<ManagedComponent> components = componentName == null ? new ArrayList<>(pool.getComponents().values()) : pool.getComponents().values().stream()
        .sorted(Comparator.comparing(c -> c.getManagedComponentId().toString()))
        .filter(c -> c.getManagedComponentId().toString().startsWith(componentName)).collect(Collectors.toList());
    return components;
  }

  // =========== API END POINTS ==========

  @EndPoint(
      method = SolrRequest.METHOD.GET,
      path = {
          "/cluster/resources",
          "/cluster/resources/{poolName}"
      },
      permission = PermissionNameProvider.Name.RESOURCE_READ_PERM
  )
  public class ReadPoolAPI {
    @Command()
    public void list(SolrQueryRequest req, SolrQueryResponse rsp) {
      SolrParams params = req.getParams();
      String name = req.getPathTemplateValues().get("poolName");
      NamedList<Object> result = new SimpleOrderedMap<>();
      Set<String> poolNames = new TreeSet<String>(
          name != null ?
              resourceManager.listPools().stream()
                  .filter(n -> n.startsWith(name))
                  .collect(Collectors.toSet())
              :
              resourceManager.listPools());
      boolean withValues = req.getParams().getBool("values", false);
      boolean withComponents = req.getParams().getBool("components", false);
      boolean withLimits = req.getParams().getBool("limits", false);
      boolean withParams = req.getParams().getBool("params", false);
      poolNames.forEach(p -> {
        ResourceManagerPool pool = resourceManager.getPool(p);
        if (pool == null) {
          return;
        }
        SimpleOrderedMap<Object> perPool = new SimpleOrderedMap<>();
        result.add(p, perPool);
        perPool.add("type", pool.getType());
        perPool.add("size", pool.getComponents().size());
        if (withLimits) {
          perPool.add("poolLimits", pool.getPoolLimits());
        }
        if (withParams) {
          perPool.add("poolParams", pool.getPoolParams());
        }
        if (withComponents) {
          perPool.add("components", new TreeSet<>(pool.getComponents().keySet()));
        }
        if (withValues) {
          try {
            Map<String, Map<String, Object>> values = pool.getCurrentValues();
            perPool.add("totalValues", new TreeMap<>(pool.aggregateTotalValues(values)));
          } catch (Exception e) {
            log.warn("Error getting current values from pool " + name, e);
            perPool.add("error", "Error getting current values: " + e.toString());
          }
        }
      });

      rsp.add("result", result);
    }
  }

  @EndPoint(
      method = SolrRequest.METHOD.POST,
      path = {
          "/cluster/resources",
          "/cluster/resources/{poolName}"
      },
      permission = PermissionNameProvider.Name.RESOURCE_WRITE_PERM
  )
  public class EditPoolAPI {
    @Command(name = "create")
    public void create(SolrQueryRequest req, SolrQueryResponse rsp, PayloadObj<ResourcePoolConfig> payload) {
      if (!checkEnabled(payload)) {
        return;
      }
      ResourcePoolConfig config = payload.get();
      if (config == null || !config.isValid()) {
        rsp.setException(new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid / incomplete pool config: " + config));
        return;
      }
      if (resourceManager.hasPool(config.name)) {
        rsp.setException(new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Pool " + config.name + " already exists."));
        return;
      }
      NamedList<Object> result = new SimpleOrderedMap<>();
      rsp.add("result", result);
      updateLock.lock();
      try {
        resourceManager.createPool(config.name, config.type, config.poolLimits, config.poolParams);
        resourcePoolConfigs.setPoolConfig(config.name, config);
        persistResourcePoolConfigsToZk();
        result.add("success", "pool " + config.name + " has been created.");
      } catch (Exception e) {
        rsp.setException(new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Pool '" + config.name + "' creation failed: " + e.toString(), e));
      } finally {
        updateLock.unlock();
      }
    }

    @Command(name = "delete")
    public void delete(SolrQueryRequest req, SolrQueryResponse rsp, PayloadObj<String> payload) {
      if (!checkEnabled(payload)) {
        return;
      }
      NamedList<Object> result = new SimpleOrderedMap<>();
      rsp.add("result", result);
      Set<String> toDelete = new TreeSet<>();
      if (payload.get() != null && !payload.get().isBlank()) {
        toDelete.add(payload.get());
      }
      if (req.getPathTemplateValues().get("poolName") != null) {
        toDelete.add(req.getPathTemplateValues().get("poolName"));
      }
      AtomicBoolean needsSave = new AtomicBoolean(false);
      updateLock.lock();
      try {
        toDelete.forEach(p -> {
          if (resourceManager.getDefaultPoolConfigs().containsKey(p)) {
            result.add(p, "ignored - cannot delete default pool");
            return;
          }
          try {
            resourceManager.removePool(p);
            resourcePoolConfigs.removePoolConfig(p);
            needsSave.set(true);
            result.add(p, "success");
          } catch (Exception e) {
            result.add(p, "error: " + e.toString());
          }
        });
        if (needsSave.get()) {
          persistResourcePoolConfigsToZk();
        }
      } catch (Exception e) {
        payload.addError("Error persisting changed configuration: " + e.toString());
      } finally {
        updateLock.unlock();
      }
    }

    @Command(name = "setlimits")
    public void setLimits(SolrQueryRequest req, SolrQueryResponse rsp, PayloadObj<Map<String, Object>> payload) {
      if (!checkEnabled(payload)) {
        return;
      }
      String poolName = req.getPathTemplateValues().get("poolName");
      if (poolName == null || poolName.isBlank()) {
        payload.addError("poolName cannot be empty");
        return;
      }
      Map<String, Object> newLimits = payload.get();
      ResourceManagerPool pool = resourceManager.getPool(poolName);
      if (pool == null) {
        return;
      }
      Map<String, Object> currentLimits = new HashMap<>(pool.getPoolLimits());
      newLimits.forEach((k, v) -> {
        if (v == null) {
          currentLimits.remove(k);
        } else {
          currentLimits.put(k, v);
        }
      });
      updateLock.lock();
      try {
        pool.setPoolLimits(currentLimits);
        resourcePoolConfigs.configs.get(poolName).poolLimits = currentLimits;
        persistResourcePoolConfigsToZk();
      } catch (Exception e) {
        payload.addError("Error persisting changed configuration: " + e.toString());
        return;
      } finally {
        updateLock.unlock();
      }
      NamedList<Object> result = new SimpleOrderedMap<>();
      result.add(poolName, currentLimits);
      rsp.add("result", result);
    }

    @Command(name = "setparams")
    public void setParams(SolrQueryRequest req, SolrQueryResponse rsp, PayloadObj<Map<String, Object>> payload) {
      if (!checkEnabled(payload)) {
        return;
      }
      String poolName = req.getPathTemplateValues().get("poolName");
      if (poolName == null || poolName.isBlank()) {
        payload.addError("poolName cannot be empty");
        return;
      }
      ResourceManagerPool pool = resourceManager.getPool(poolName);
      if (pool == null) {
        return;
      }
      Map<String, Object> newParams = payload.get();
      Map<String, Object> currentParams = new HashMap<>(pool.getPoolParams());
      newParams.forEach((k, v) -> {
        if (v == null) {
          currentParams.remove(k);
        } else {
          currentParams.put(k, v);
        }
      });
      updateLock.lock();
      try {
        pool.setPoolLimits(currentParams);
        resourcePoolConfigs.configs.get(poolName).poolParams = currentParams;
        persistResourcePoolConfigsToZk();
      } catch (Exception e) {
        payload.addError("Error persisting changed configuration: " + e.toString());
        return;
      } finally {
        updateLock.unlock();
      }
      NamedList<Object> result = new SimpleOrderedMap<>();
      result.add(poolName, currentParams);
      rsp.add("result", result);
    }
  }

  @EndPoint(
      method = SolrRequest.METHOD.GET,
      path = {
          "/cluster/resources/{poolName}/components",
          "/cluster/resources/{poolName}/components/{componentName}"
      },
      permission = PermissionNameProvider.Name.RESOURCE_READ_PERM
  )
  public class ReadComponentAPI {

    @Command()
    public void list(SolrQueryRequest req, SolrQueryResponse rsp) {
      AtomicReference<ResourceManagerPool> poolRef = new AtomicReference<>();
      List<ManagedComponent> components = getComponents(req, rsp, poolRef);
      if (components == null) {
        return;
      }
      ResourceManagerPool pool = poolRef.get();
      boolean withValues = req.getParams().getBool("values", false);
      boolean withLimits = req.getParams().getBool("limits", false);
      NamedList<Object> result = new SimpleOrderedMap<>();
      rsp.add("result", result);
      components.forEach(component -> {
        NamedList<Object> perComponent = new SimpleOrderedMap<>();
        result.add(component.getManagedComponentId().toString(), perComponent);
        perComponent.add("class", component.getClass().getName());
        if (withLimits) {
          try {
            perComponent.add("resourceLimits", new TreeMap<>(pool.getResourceLimits(component)));
          } catch (Exception e) {
            log.warn("Error getting resource limits of " + component.getManagedComponentId(), e);
            result.add("error", "Error getting resource limits of " + component.getManagedComponentId() + ": " + e.toString());
          }
        }
        if (withValues) {
          try {
            perComponent.add("monitoredValues", new TreeMap<>(pool.getMonitoredValues(component)));
          } catch (Exception e) {
            log.warn("Error getting monitored values of " + component.getManagedComponentId() + "/" + pool.getName() + " : " + e.toString(), e);
            perComponent.add("error", "Error getting monitored values of " + component.getManagedComponentId() + ": " + e.toString());
          }
        }
      });
    }
  }

  // NOTE: changes to component limits and pool registration are NOT persisted!
  @EndPoint(
      method = SolrRequest.METHOD.POST,
      path = {
          "/cluster/resources/{poolName}/components",
          "/cluster/resources/{poolName}/components/{componentName}"
      },
      permission = PermissionNameProvider.Name.RESOURCE_WRITE_PERM
  )
  public class EditComponentAPI {

    @Command(name = "setlimits")
    public void setLimits(SolrQueryRequest req, SolrQueryResponse rsp, PayloadObj<String> payload) {
      if (!checkEnabled(payload)) return;
      AtomicReference<ResourceManagerPool> poolRef = new AtomicReference<>();
      List<ManagedComponent> components = getComponents(req, rsp, poolRef);
      if (components == null) {
        return;
      }
      NamedList<Object> result = new SimpleOrderedMap<>();
      rsp.add("result", result);
      ResourceManagerPool pool = poolRef.get();
      Map<String, Object> newLimits = (Map<String, Object>)Utils.fromJSONString(payload.get());
      for (ManagedComponent cmp : components) {
        try {
          Map<String, Object> currentLimits = new HashMap<>(pool.getResourceLimits(cmp));
          newLimits.forEach((k, v) -> {
            if (v == null) {
              currentLimits.remove(k);
            } else {
              currentLimits.put(k, v);
            }
          });
          try {
            pool.setResourceLimits(cmp, newLimits, ChangeListener.Reason.USER);
            result.add("success", newLimits);
          } catch (Exception e) {
            log.warn("Error setting resource limits of " + cmp.getManagedComponentId() + "/" + pool.getName() + " : " + e.toString(), e);
            result.add("error", "Error setting resource limits of " + cmp.getManagedComponentId() + ": " + e.toString());
          }
        } catch (Exception e) {
          log.warn("Error getting resource limits of " + cmp.getManagedComponentId() + "/" + pool.getName() + " : " + e.toString(), e);
          result.add("error", "Error getting resource limits of " + cmp.getManagedComponentId() + ": " + e.toString());
        }
      }
    }

    @Command(name = "delete")
    public void delete(SolrQueryRequest req, SolrQueryResponse rsp, PayloadObj<String> payload) {
      if (!checkEnabled(payload)) {
        return;
      }
      NamedList<Object> result = new SimpleOrderedMap<>();
      rsp.add("result", result);
      AtomicReference<ResourceManagerPool> poolRef = new AtomicReference<>();
      List<ManagedComponent> components = getComponents(req, rsp, poolRef);
      if (components == null) {
        return;
      }
      ResourceManagerPool pool = poolRef.get();
      components.forEach(component ->
        result.add(component.getManagedComponentId().toString(),
            pool.unregisterComponent(component.getManagedComponentId().toString()) ? "removed" : "not found")
      );
    }
  }
}
