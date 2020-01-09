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
package org.apache.solr.handler.admin;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.managed.ChangeListener;
import org.apache.solr.managed.ManagedComponent;
import org.apache.solr.managed.ResourceManager;
import org.apache.solr.managed.ResourceManagerPool;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request handler to access and modify pools and their resource limits.
 */
public class ResourceManagerHandler extends RequestHandlerBase implements PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String POOL_PARAM = "pool";
  public static final String LIMIT_PREFIX_PARAM = "limit.";
  public static final String ARG_PREFIX_PARAM = "arg.";
  public static final String POOL_ACTION_PARAM = "poolAction";
  public static final String RES_ACTION_PARAM = "resAction";

  public enum PoolOp {
    LIST,
    STATUS,
    CREATE,
    DELETE,
    SETLIMITS;

    public static PoolOp get(String p) {
      if (p != null) {
        try {
          return PoolOp.valueOf(p.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
          return null;
        }
      }
      return null;
    }
  }

  public enum ResOp {
    LIST,
    STATUS,
    DELETE,
    GETLIMITS,
    SETLIMITS;

    public static ResOp get(String p) {
      if (p != null) {
        try {
          return ResOp.valueOf(p.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
          return null;
        }
      }
      return null;
    }
  }

  private final ResourceManager resourceManager;

  public ResourceManagerHandler(ResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    if (resourceManager == null) {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "ResourceManager instance not initialized.");
    }
    String poolAction = req.getParams().get(POOL_ACTION_PARAM);
    String resAction = req.getParams().get(RES_ACTION_PARAM);
    if (poolAction != null && resAction != null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Only one of " + POOL_ACTION_PARAM + " or " + RES_ACTION_PARAM + " can be specified: " + req.getParams());
    }
    if (poolAction != null) {
      handlePoolRequest(req, rsp);
    } else if (resAction != null) {
      handleResourceRequest(req, rsp);
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing " + POOL_ACTION_PARAM + " and " + RES_ACTION_PARAM + ": " + req.getParams());
    }
  }

  private void handlePoolRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    SolrParams params = req.getParams();
    PoolOp op = PoolOp.get(params.get(POOL_ACTION_PARAM));
    if (op == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unsupported pool operation: " + params.get(POOL_ACTION_PARAM));
    }
    final String name = params.get(CommonParams.NAME);
    if (op == PoolOp.CREATE) {
      if (name == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Required parameter " + CommonParams.NAME + " missing: " + params);
      }
    }
    NamedList<Object> result = new SimpleOrderedMap<>();
    Set<String> poolNames = new TreeSet<String>(
        name != null ?
            resourceManager.listPools().stream()
                .filter(n -> n.startsWith(name))
                .collect(Collectors.toSet())
        :
        resourceManager.listPools());
    switch (op) {
      case LIST:
        poolNames.forEach(p -> {
          ResourceManagerPool pool = resourceManager.getPool(p);
          if (pool == null) {
            return;
          }
          SimpleOrderedMap<Object> perPool = new SimpleOrderedMap<>();
          result.add(p, perPool);
          perPool.add("type", pool.getType());
          perPool.add("size", pool.getComponents().size());
          perPool.add("poolLimits", pool.getPoolLimits());
          perPool.add("poolParams", pool.getParams());
        });
        break;
      case STATUS:
        poolNames.forEach(p -> {
          ResourceManagerPool pool = resourceManager.getPool(p);
          if (pool == null) {
            return;
          }
          SimpleOrderedMap<Object> perPool = new SimpleOrderedMap<>();
          result.add(p, perPool);
          perPool.add("type", pool.getType());
          perPool.add("size", pool.getComponents().size());
          perPool.add("poolLimits", pool.getPoolLimits());
          perPool.add("poolParams", pool.getParams());
          perPool.add("resources", new TreeSet<>(pool.getComponents().keySet()));
          try {
            Map<String, Map<String, Object>> values = pool.getCurrentValues();
            perPool.add("totalValues", new TreeMap<>(pool.aggregateTotalValues(values)));
          } catch (Exception e) {
            log.warn("Error getting current values from pool " + name, e);
            perPool.add("error", "Error getting current values: " + e.toString());
          }
        });
        break;
      case CREATE:
        String type = params.get(CommonParams.TYPE);
        if (type == null || type.isBlank()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Required parameter " + CommonParams.TYPE + " missing or empty: " + params);
        }
        Map<String, Object> limits = getMap(params, LIMIT_PREFIX_PARAM);
        Map<String, Object> args = getMap(params, ARG_PREFIX_PARAM);
        try {
          resourceManager.createPool(name, type, limits, args);
          result.add("success", "created");
        } catch (Exception e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Pool '" + name + "' creation failed: " + e.toString(), e);
        }
        break;
      case DELETE:
        poolNames.forEach(p -> {
          try {
            resourceManager.removePool(p);
            result.add(p, "success");
          } catch (Exception e) {
            result.add(p, "error: " + e.toString());
          }
        });
        break;
      case SETLIMITS:
        poolNames.forEach(p -> {
          ResourceManagerPool pool = resourceManager.getPool(p);
          if (pool == null) {
            return;
          }
          Map<String, Object> currentLimits = new HashMap<>(pool.getPoolLimits());
          Map<String, Object> newLimits = getMap(params, LIMIT_PREFIX_PARAM);
          newLimits.forEach((k, v) -> {
            if (v == null) {
              currentLimits.remove(k);
            } else {
              currentLimits.put(k, v);
            }
          });
          pool.setPoolLimits(newLimits);
          result.add(p, newLimits);
        });
        break;
    }
    rsp.getValues().add("result", result);
  }

  private Map<String, Object> getMap(SolrParams params, String prefix) {
    Map<String, Object> result = new HashMap<>();
    params.forEach(entry -> {
      if (!entry.getKey().startsWith(prefix)) {
        return;
      }
      String val = params.get(entry.getKey());
      if (val.isBlank()) {
        val = null;
      }
      // convert into a number if possible
      Object newVal;
      try {
        newVal = Long.parseLong(val);
      } catch (NumberFormatException nfe) {
        try {
          newVal = Double.parseDouble(val);
        } catch (NumberFormatException nfe2) {
          newVal = val;
        }
      }
      result.put(entry.getKey().substring(prefix.length()), newVal);
    });
    return result;
  }

  private void handleResourceRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    SolrParams params = req.getParams();
    ResOp op = ResOp.get(params.get(RES_ACTION_PARAM));
    if (op == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unsupported resource operation: " + params.get(RES_ACTION_PARAM));
    }
    String poolName = params.get(POOL_PARAM);
    if (poolName == null || poolName.isBlank()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Resource operation requires '" + POOL_PARAM + "' parameter.");
    }
    ResourceManagerPool<ManagedComponent> pool = resourceManager.getPool(poolName);
    if (pool == null) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Pool '" + poolName + "' not found.");
    }
    String resName = params.get(CommonParams.NAME);
    NamedList<Object> result = new SimpleOrderedMap<>();
    // we support a prefix of resource names because eg. searcher caches will have a quickly
    // changing unique suffix
    List<ManagedComponent> components = resName == null ? new ArrayList<>(pool.getComponents().values()) : pool.getComponents().values().stream()
        .sorted(Comparator.comparing(c -> c.getManagedComponentId().toString()))
        .filter(c -> c.getManagedComponentId().toString().startsWith(resName)).collect(Collectors.toList());
    if (op != ResOp.LIST && components.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Component(s) '" + resName + " not found in pool '" + poolName + "'.");
    }
    switch (op) {
      case LIST:
        Set<String> componentNames = new TreeSet<>(pool.getComponents().keySet());
        componentNames.forEach(n -> {
          ManagedComponent component = pool.getComponents().get(n);
          if (component == null) {
            // removed in the meantime
            return;
          }
          NamedList<Object> perRes = new SimpleOrderedMap<>();
          result.add(n, perRes);
          perRes.add("class", component.getClass().getName());
          try {
            perRes.add("resourceLimits", new TreeMap<>(pool.getResourceLimits(component)));
          } catch (Exception e) {
            log.warn("Error getting resourceLimits of " + component.getManagedComponentId(), e);
            result.add("error", "Error getting resource limits of " + resName + ": " + e.toString());
          }
        });
        break;
      case STATUS:
        for (ManagedComponent component : components) {
          SimpleOrderedMap perComponent = new SimpleOrderedMap();
          result.add(component.getManagedComponentId().toString(), perComponent);
          perComponent.add("class", component.getClass().getName());
          try {
            perComponent.add("resourceLimits", new TreeMap<>(pool.getResourceLimits(component)));
          } catch (Exception e) {
            log.warn("Error getting resource limits of " + resName + "/" + poolName + " : " + e.toString(), e);
            perComponent.add("error", "Error getting resource limits of " + resName + ": " + e.toString());
          }
          try {
            perComponent.add("monitoredValues", new TreeMap<>(pool.getMonitoredValues(component)));
          } catch (Exception e) {
            log.warn("Error getting monitored values of " + resName + "/" + poolName + " : " + e.toString(), e);
            perComponent.add("error", "Error getting monitored values of " + resName + ": " + e.toString());
          }
        }
        break;
      case GETLIMITS:
        for (ManagedComponent component : components) {
          SimpleOrderedMap perComponent = new SimpleOrderedMap();
          result.add(component.getManagedComponentId().toString(), perComponent);
          try {
            perComponent.add("resourceLimits", new TreeMap<>(pool.getResourceLimits(component)));
          } catch (Exception e) {
            log.warn("Error getting resource limits of " + resName + "/" + poolName + " : " + e.toString(), e);
            perComponent.add("error", "Error getting resource limits of " + resName + ": " + e.toString());
          }
        }
        break;
      case SETLIMITS:
        for (ManagedComponent cmp : components) {
          try {
            Map<String, Object> currentLimits = new HashMap<>(pool.getResourceLimits(cmp));
            Map<String, Object> newLimits = getMap(params, LIMIT_PREFIX_PARAM);
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
              log.warn("Error setting resource limits of " + resName + "/" + poolName + " : " + e.toString(), e);
              result.add("error", "Error setting resource limits of " + resName + ": " + e.toString());
            }
          } catch (Exception e) {
            log.warn("Error getting resource limits of " + resName + "/" + poolName + " : " + e.toString(), e);
            result.add("error", "Error getting resource limits of " + resName + ": " + e.toString());
          }
        }
        break;
      case DELETE:
        for (ManagedComponent component : components) {
          result.add(component.getManagedComponentId().toString(), pool.unregisterComponent(component.getManagedComponentId().toString()) ? "removed" : "not found");
        }
    }
    rsp.getValues().add("result", result);
  }

  @Override
  public String getDescription() {
    return "ResourceManager handler";
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    if ("GET".equals(request.getHttpMethod())) {
      return Name.RESOURCE_READ_PERM;
    } else {
      return Name.RESOURCE_WRITE_PERM;
    }
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }
}