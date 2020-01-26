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
package org.apache.solr.core;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;
import static org.apache.solr.common.util.Utils.fromJSON;
import static org.apache.solr.common.util.Utils.getDeepCopy;

/**
 * The class encapsulates the request time parameters . This is immutable and any changes performed
 * returns a copy of the Object with the changed values
 */
public class RequestParams implements MapSerializable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map data;
  private final Map<String, ParamSet> paramsets = new LinkedHashMap<>();
  private final int znodeVersion;

  public RequestParams(Map data, int znodeVersion) {
    if (data == null) data = Collections.EMPTY_MAP;
    this.data = data;
    Map paramsets = (Map) data.get(NAME);
    if (paramsets != null) {
      for (Object o : paramsets.entrySet()) {
        Map.Entry e = (Map.Entry) o;
        if (e.getValue() instanceof Map) {
          Map value = (Map) e.getValue();
          this.paramsets.put((String) e.getKey(), createParamSet(value, 0l));
        }
      }
    }
    this.znodeVersion = znodeVersion;
  }

  public static ParamSet createParamSet(Map map, Long version) {
    Map copy = getDeepCopy(map, 3);
    Map meta = (Map) copy.remove("");
    if (meta == null && version != null) {
      meta = Collections.singletonMap("v", version);
    }
    Map invariants = (Map) copy.remove(INVARIANTS);
    Map appends = (Map) copy.remove(APPENDS);
    return new ParamSet(copy, invariants, appends, meta);
  }

  /**
   * This converts Lists to arrays of strings. Because Solr expects
   * params to be String[]
   */
  private static Map getMapCopy(Map value) {
    Map copy = new LinkedHashMap<>();
    for (Object o1 : value.entrySet()) {
      Map.Entry entry = (Map.Entry) o1;
      if ("".equals(entry.getKey())) {
        copy.put(entry.getKey(), entry.getValue());
        continue;
      }
      if (entry.getValue() != null) {
        if (entry.getValue() instanceof List) {
          List l = (List) entry.getValue();
          String[] sarr = new String[l.size()];
          for (int i = 0; i < l.size(); i++) {
            if (l.get(i) != null) sarr[i] = String.valueOf(l.get(i));
          }
          copy.put(entry.getKey(), sarr);
        } else {
          copy.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
      } else {
        copy.put(entry.getKey(), entry.getValue());
      }
    }
    return copy;
  }

  public ParamSet getParams(String name) {
    return paramsets.get(name);
  }

  public VersionedParams getParams(String name, String type) {
    ParamSet paramSet = paramsets.get(name);
    return paramSet == null ? null : paramSet.getParams(type);
  }

  public int getZnodeVersion() {
    return znodeVersion;
  }

  @Override
  public Map<String, Object> toMap(Map<String, Object> map) {
    return getMapWithVersion(data, znodeVersion);
  }

  public static Map<String, Object> getMapWithVersion(Map<String, Object> data, int znodeVersion) {
    Map result = new LinkedHashMap();
    result.put(ConfigOverlay.ZNODEVER, znodeVersion);
    result.putAll(data);
    return result;
  }

  public RequestParams setParams(String name, ParamSet paramSet) {
    Map deepCopy = getDeepCopy(data, 3);
    Map p = (Map) deepCopy.get(NAME);
    if (p == null) deepCopy.put(NAME, p = new LinkedHashMap());
    if (paramSet == null) p.remove(name);
    else p.put(name, paramSet.toMap(new LinkedHashMap<>()));
    return new RequestParams(deepCopy, znodeVersion);
  }

  public static RequestParams getFreshRequestParams(SolrResourceLoader loader, RequestParams requestParams) {
    if (loader instanceof ZkSolrResourceLoader) {
      ZkSolrResourceLoader resourceLoader = (ZkSolrResourceLoader) loader;
      try {
        Stat stat = resourceLoader.getZkController().getZkClient().exists(resourceLoader.getConfigSetZkPath() + "/" + RequestParams.RESOURCE, null, true);
        log.debug("latest version of {} in ZK  is : {}", resourceLoader.getConfigSetZkPath() + "/" + RequestParams.RESOURCE, stat == null ? "" : stat.getVersion());
        if (stat == null) {
          requestParams = new RequestParams(Collections.EMPTY_MAP, -1);
        } else if (requestParams == null || stat.getVersion() > requestParams.getZnodeVersion()) {
          Object[] o = getMapAndVersion(loader, RequestParams.RESOURCE);
          requestParams = new RequestParams((Map) o[0], (Integer) o[1]);
          log.info("request params refreshed to version {}", requestParams.getZnodeVersion());
        }
      } catch (KeeperException | InterruptedException e) {
        SolrZkClient.checkInterrupted(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }

    } else {
      Object[] o = getMapAndVersion(loader, RequestParams.RESOURCE);
      requestParams = new RequestParams((Map) o[0], (Integer) o[1]);
    }

    return requestParams;

  }


  private static Object[] getMapAndVersion(SolrResourceLoader loader, String name) {
    try (InputStream in = loader.openResource(name)) {
      int version = 0; //will be always 0 for file based resourceloader
      if (in instanceof ZkSolrResourceLoader.ZkByteArrayInputStream) {
        version = ((ZkSolrResourceLoader.ZkByteArrayInputStream) in).getStat().getVersion();
        log.info("conf resource {} loaded . version : {} ", name, version);
      }
      try {
        Map m = (Map) fromJSON (in);
        return new Object[]{m, version};
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error parsing conf resource " + name, e);
      }

    } catch (IOException e) {
      //no problem no overlay.json file
      return new Object[]{Collections.EMPTY_MAP, -1};
    }
  }


  public byte[] toByteArray() {
    return Utils.toJSON(data);
  }

  public static final String USEPARAM = "useParams";
  public static final String NAME = "params";
  public static final String RESOURCE = "params.json";
  public static final String APPENDS = "_appends_";
  public static final String INVARIANTS = "_invariants_";

  public static class ParamSet implements MapSerializable {
    private final Map defaults, appends, invariants;
    Map<String, VersionedParams> paramsMap;
    public final Map meta;

    ParamSet(Map defaults, Map invariants, Map appends, Map meta) {
      this.defaults = defaults;
      this.invariants = invariants;
      this.appends = appends;
      ImmutableMap.Builder<String, VersionedParams> builder = ImmutableMap.<String, VersionedParams>builder().put(PluginInfo.DEFAULTS,
          new VersionedParams(defaults, this));
      if (appends != null) builder.put(PluginInfo.APPENDS, new VersionedParams(appends, this));
      if (invariants != null) builder.put(PluginInfo.INVARIANTS, new VersionedParams(invariants, this));
      paramsMap = builder.build();
      this.meta = meta;
    }

    public Long getVersion() {
      return meta == null ? Long.valueOf(0l) : (Long) meta.get("v");
    }

    @Override
    public Map<String, Object> toMap(Map<String, Object> result) {
      result.putAll(defaults);
      if (appends != null) result.put(APPENDS, appends);
      if (invariants != null) result.put(INVARIANTS, invariants);
      if(meta != null) result.put("", meta);
      return result;
    }


    public ParamSet update(Map map) {
      ParamSet p = createParamSet(map, null);
      return new ParamSet(
          mergeMaps(getDeepCopy(defaults, 2), p.defaults),
          mergeMaps(getDeepCopy(invariants, 2), p.invariants),
          mergeMaps(getDeepCopy(appends, 2), p.appends),
          mergeMaps(getDeepCopy(meta, 2), singletonMap("v", getVersion() + 1))
      );
    }

    private static Map mergeMaps(Map m1, Map m2) {
      if (m1 == null && m2 == null) return null;
      if (m1 == null) return m2;
      if (m2 == null) return m1;
      m1.putAll(m2);
      return m1;
    }

    /**
     * @param type one of defaults, appends, invariants
     */
    public VersionedParams getParams(String type) {
      return paramsMap.get(type);
    }

    /**get the raw map
     */
    public Map<String, Object> get() {
      return defaults;
    }
  }

  public static class VersionedParams extends MapSolrParams {
    final ParamSet paramSet;

    public VersionedParams(Map map, ParamSet paramSet) {
      super(getMapCopy(map));
      this.paramSet = paramSet;
    }
  }
}
