package org.apache.solr.core;

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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class encapsulates the request time parameters . This is immutable and any changes performed
 * returns a copy of the Object with the changed values
 */
public class RequestParams implements MapSerializable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map data;
  private final Map<String, VersionedParams> paramsets = new LinkedHashMap<>();
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
          Map copy = getMapCopy(value);
          Map meta = (Map) copy.remove("");
          this.paramsets.put((String) e.getKey(), new VersionedParams(Collections.unmodifiableMap(copy), meta));
        }
      }
    }
    this.znodeVersion = znodeVersion;
  }

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

  public VersionedParams getParams(String name) {
    return paramsets.get(name);
  }

  public int getZnodeVersion() {
    return znodeVersion;
  }

  @Override
  public Map<String, Object> toMap() {
    return getMapWithVersion(data, znodeVersion);
  }

  public static Map<String, Object> getMapWithVersion(Map<String, Object> data, int znodeVersion) {
    Map result = new LinkedHashMap();
    result.put(ConfigOverlay.ZNODEVER, znodeVersion);
    result.putAll(data);
    return result;
  }

  public RequestParams setParams(String name, Map values) {
    Map deepCopy = Utils.getDeepCopy(data, 3);
    Map p = (Map) deepCopy.get(NAME);
    if (p == null) deepCopy.put(NAME, p = new LinkedHashMap());
    if (values == null) {
      p.remove(name);
    } else {
      Map old = (Map) p.get(name);
      long version = 0;
      Map meta = null;
      if (old != null) {
        meta = (Map) old.get("");
        if (meta != null) {
          Long oldVersion = (Long) old.get("v");
          if (oldVersion != null) version = oldVersion.longValue() + 1;
        }
        meta = new LinkedHashMap<>(meta);
      } else {
        meta = new LinkedHashMap<>();
      }

      meta.put("v", version);
      values = new LinkedHashMap<>(values);
      values.put("", meta);
      p.put(name, values);
    }
    return new RequestParams(deepCopy, znodeVersion);
  }

  public static RequestParams getFreshRequestParams(SolrResourceLoader loader, RequestParams requestParams) {
    if (loader instanceof ZkSolrResourceLoader) {
      ZkSolrResourceLoader resourceLoader = (ZkSolrResourceLoader) loader;
      try {
        Stat stat = resourceLoader.getZkController().getZkClient().exists(resourceLoader.getConfigSetZkPath() + "/" + RequestParams.RESOURCE, null, true);
        log.debug("latest version of {} in ZK  is : {}", resourceLoader.getConfigSetZkPath() + "/" + RequestParams.RESOURCE, stat == null ? "": stat.getVersion());
        if (stat == null) {
          requestParams = new RequestParams(Collections.EMPTY_MAP, -1);
        } else if (requestParams == null || stat.getVersion() > requestParams.getZnodeVersion()) {
          Object[] o = getMapAndVersion(loader, RequestParams.RESOURCE);
          requestParams = new RequestParams((Map) o[0], (Integer) o[1]);
          log.info("request params refreshed to version {}", requestParams.getZnodeVersion());
        }
      } catch (KeeperException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
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
        Map m = (Map) ObjectBuilder.getVal(new JSONParser(new InputStreamReader(in, StandardCharsets.UTF_8)));
        return new Object[]{m, version};
      } catch (IOException e) {
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

  public static class VersionedParams extends MapSolrParams {
    Map meta;

    public VersionedParams(Map<String, String> map, Map meta) {
      super(map);
      this.meta = meta;
    }

    public Map getRawMap() {
      return meta;
    }


    public Long getVersion() {
      return meta == null ? 0l : (Long) meta.get("v");
    }
  }
}
