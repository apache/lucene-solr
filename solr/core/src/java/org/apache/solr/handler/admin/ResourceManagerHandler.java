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
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.managed.ManagedResource;
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
  public static final String RESOURCE_PARAM = "resource";
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
    String name = null;
    if (op != PoolOp.LIST) {
      name = params.get(CommonParams.NAME);
      if (name == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Required parameter " + CommonParams.NAME + " missing: " + params);
      }
    }
    NamedList<Object> result = new SimpleOrderedMap<>();
    switch (op) {
      case LIST:
        resourceManager.listPools().forEach(p -> {
          ResourceManagerPool pool = resourceManager.getPool(p);
          if (pool == null) {
            return;
          }
          NamedList<Object> perPool = new SimpleOrderedMap<>();
          result.add(p, perPool);
          perPool.add("type", pool.getType());
          perPool.add("size", pool.getResources().size());
          perPool.add("limits", pool.getPoolLimits());
          perPool.add("args", pool.getArgs());
        });
        break;
      case STATUS:
        ResourceManagerPool pool = resourceManager.getPool(name);
        if (pool == null) {
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Pool '" + name + "' not found.");
        }
        result.add("type", pool.getType());
        result.add("size", pool.getResources().size());
        result.add("limits", pool.getPoolLimits());
        result.add("args", pool.getArgs());
        result.add("resources", pool.getResources().keySet());
        try {
          result.add("currentValues", pool.getCurrentValues());
          result.add("totalValues", pool.getTotalValues());
        } catch (Exception e) {
          log.warn("Error getting current values from pool " + name, e);
          result.add("error", "Error getting current values: " + e.toString());
        }
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
        try {
          resourceManager.removePool(name);
          result.add("success", "removed");
        } catch (Exception e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Pool '" + name + "' deletion failed: " + e.toString(), e);
        }
        break;
      case SETLIMITS:
        ResourceManagerPool pool1 = resourceManager.getPool(name);
        if (pool1 == null) {
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Pool '" + name + "' not found.");
        }
        Map<String, Object> currentLimits = new HashMap<>(pool1.getPoolLimits());
        Map<String, Object> newLimits = getMap(params, LIMIT_PREFIX_PARAM);
        newLimits.forEach((k, v) -> {
          if (v == null) {
            currentLimits.remove(k);
          } else {
            currentLimits.put(k, v);
          }
        });
        pool1.setPoolLimits(newLimits);
        result.add("success", newLimits);
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
      result.put(entry.getKey().substring(prefix.length()), val);
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
    ResourceManagerPool pool = resourceManager.getPool(poolName);
    if (pool == null) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Pool '" + poolName + "' not found.");
    }
    String resName = params.get(RESOURCE_PARAM);
    if ((resName == null || resName.isBlank()) && op != ResOp.LIST) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing '" + RESOURCE_PARAM + "' parameter.");
    }
    NamedList<Object> result = new SimpleOrderedMap<>();
    switch (op) {
      case LIST:
        pool.getResources().forEach((n, resource) -> {
          NamedList<Object> perRes = new SimpleOrderedMap<>();
          result.add(n, perRes);
          perRes.add("class", resource.getClass().getName());
          perRes.add("types", resource.getManagedResourceTypes());
          perRes.add("managedLimits", resource.getManagedLimits());
        });
        break;
      case STATUS:
        ManagedResource resource = pool.getResources().get(resName);
        if (resource == null) {
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Resource '" + resName + " not found in pool '" + poolName + "'.");
        }
        result.add("class", resource.getClass().getName());
        result.add("types", resource.getManagedResourceTypes());
        result.add("managedLimits", resource.getManagedLimits());
        try {
          result.add("monitoredValues", resource.getMonitoredValues(Collections.emptySet()));
        } catch (Exception e) {
          log.warn("Error getting monitored values of " + resName + "/" + poolName + " : " + e.toString(), e);
          result.add("error", "Error getting monitored values of " + resName + ": " + e.toString());
        }
        break;
      case GETLIMITS:
        ManagedResource resource1 = pool.getResources().get(resName);
        if (resource1 == null) {
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Resource '" + resName + " not found in pool '" + poolName + "'.");
        }
        result.add("managedLimits", resource1.getManagedLimits());
        break;
      case SETLIMITS:
        ManagedResource resource2 = pool.getResources().get(resName);
        if (resource2 == null) {
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Resource '" + resName + " not found in pool '" + poolName + "'.");
        }
        Map<String, Object> currentLimits = new HashMap<>(resource2.getManagedLimits());
        Map<String, Object> newLimits = getMap(params, LIMIT_PREFIX_PARAM);
        newLimits.forEach((k, v) -> {
          if (v == null) {
            currentLimits.remove(k);
          } else {
            currentLimits.put(k, v);
          }
        });
        resource2.setManagedLimits(newLimits);
        result.add("success", newLimits);
        break;
      case DELETE:
        result.add("success", pool.removeResource(resName) ? "removed" : "not found");
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
