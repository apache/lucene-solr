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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.api.Api;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.VERSION;
import static org.apache.solr.core.RuntimeLib.SHA512;

public class LibListener implements ClusterPropertiesListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;

  Map<String, RuntimeLib> runtimeLibs = new HashMap<>();
  MemClassLoader memClassLoader;
  final ExtHandler extHandler;
  private int myversion = -1;

  LibListener(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    extHandler = new ExtHandler(this);
  }


  public <T> T newInstance(String cName, Class<T> expectedType) {
    try {
      return coreContainer.getResourceLoader().newInstance(cName, expectedType,
          null, new Class[]{CoreContainer.class}, new Object[]{coreContainer});
    } catch (SolrException e) {
      if (memClassLoader != null) {
        try {
          Class<? extends T> klas = memClassLoader.findClass(cName, expectedType);
          try {
            return klas.getConstructor(CoreContainer.class).newInstance(coreContainer);
          } catch (NoSuchMethodException ex) {
            return klas.getConstructor().newInstance();
          }
        } catch (Exception ex) {
          if (!memClassLoader.getErrors().isEmpty()) {
            //some libraries were no loaded due to some errors. May the class was there in those libraries
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "There were errors loading some libraries: " + StrUtils.join(memClassLoader.getErrors(), ','), ex);
          }
          //there were no errors in loading any libraries. The class was probably not suppoed to be there in those libraries
          // so throw the original exception
          throw e;
        }
      } else {
        throw e;
      }
    }
  }

  @Override
  public boolean onChange(Map<String, Object> properties) {
    log.info("clusterprops.json changed , version {}", coreContainer.getZkController().getZkStateReader().getClusterPropsVersion());
    boolean forceReload = updateRuntimeLibs(properties);
    extHandler.updateReqHandlers(properties, forceReload);
    for (SolrCore core : coreContainer.solrCores.getCores()) {
      core.globalClassLoaderChanged();
    }
    myversion = coreContainer.getZkController().getZkStateReader().getClusterPropsVersion();
    return false;
  }


  private boolean updateRuntimeLibs(Map<String, Object> properties) {
    Map m = (Map) properties.getOrDefault(RuntimeLib.TYPE, Collections.emptyMap());
    if (runtimeLibs.isEmpty() && m.isEmpty()) return false;
    boolean needsReload[] = new boolean[1];
    if (m.size() == runtimeLibs.size()) {
      m.forEach((k, v) -> {
        if (v instanceof Map) {
          if (!runtimeLibs.containsKey(k)) needsReload[0] = true;
          RuntimeLib rtl = runtimeLibs.get(k);
          if (rtl == null || !Objects.equals(rtl.getSha512(), ((Map) v).get(SHA512))) {
            needsReload[0] = true;
          }
        }

      });
    } else {
      needsReload[0] = true;
    }
    if (needsReload[0]) {
      createNewClassLoader(m);
    }
    return needsReload[0];
  }
  public ResourceLoader getResourceLoader() {
    return memClassLoader == null ? coreContainer.getResourceLoader() : memClassLoader;
  }
  void createNewClassLoader(Map m) {
    boolean[] loadedAll = new boolean[1];
    loadedAll[0] = true;
    Map<String, RuntimeLib> libMap = new LinkedHashMap<>();
    m.forEach((k, v) -> {
      if (v instanceof Map) {
        Map map = new HashMap((Map) v);
        map.put(CoreAdminParams.NAME, String.valueOf(k));
        RuntimeLib lib = new RuntimeLib(coreContainer);
        try {
          lib.init(new PluginInfo(null, map));
          if (lib.getUrl() == null) {
            log.error("Unable to initialize runtimeLib : " + Utils.toJSONString(v));
            loadedAll[0] = false;
          }
          lib.loadJar();
          libMap.put(lib.getName(), lib);
        } catch (Exception e) {
          log.error("error loading a runtimeLib " + Utils.toJSONString(v), e);
          loadedAll[0] = false;

        }
      }
    });

    if (loadedAll[0]) {
      log.info("Libraries changed. New memclassloader created with jars {}", libMap.values().stream().map(runtimeLib -> runtimeLib.getUrl()).collect(Collectors.toList()));
      this.memClassLoader = new MemClassLoader(new ArrayList<>(libMap.values()), coreContainer.getResourceLoader());
      this.runtimeLibs = libMap;

    }
  }

  static class ExtHandler extends RequestHandlerBase implements PermissionNameProvider {
    final LibListener libListener;

    private Map<String, SolrRequestHandler> customHandlers = new HashMap<>();

    ExtHandler(LibListener libListener) {
      this.libListener = libListener;
    }


    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {
      int v = req.getParams().getInt(ConfigOverlay.ZNODEVER, -1);
      if (v >= 0) {
        log.debug("expected version : {} , my version {}", v, libListener.myversion );
        ZkStateReader zkStateReader = libListener.coreContainer.getZkController().getZkStateReader();
        zkStateReader.forceRefreshClusterProps(v);
      }
      rsp.add("metadata", (MapWriter) ew -> ew.putIfNotNull(VERSION,
          libListener.coreContainer.getZkController().zkStateReader.getClusterPropsVersion()));
      rsp.add(RuntimeLib.TYPE, libListener.runtimeLibs.values());
      rsp.add(SolrRequestHandler.TYPE,
          (IteratorWriter) iw -> customHandlers.forEach((s, h) -> iw.addNoEx(ImmutableMap.of(s, h.getClass().getName()))));

    }

    @Override
    public Collection<Api> getApis() {
      return Collections.singleton(new Api(Utils.getSpec("node.ext")) {
        @Override
        public void call(SolrQueryRequest req, SolrQueryResponse rsp) {
          String name = ((V2HttpCall) req.getHttpSolrCall()).getUrlParts().get("handlerName");
          if (name == null) {
            handleRequestBody(req, rsp);
            return;
          }
          SolrRequestHandler handler = customHandlers.get(name);
          if (handler == null) {
            String err = StrUtils.formatString(" No such handler: {0}, available handlers : {1}" , name, customHandlers.keySet());
            log.error(err);
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND, err);
          }
          handler.handleRequest(req, rsp);
        }
      });
    }

    private void updateReqHandlers(Map<String, Object> properties, boolean forceReload) {
      Map m = (Map) properties.getOrDefault(SolrRequestHandler.TYPE, Collections.emptyMap());
      if (m.isEmpty() && customHandlers.isEmpty()) return;
      boolean hasChanged = true;
      if (customHandlers.size() == m.size() && customHandlers.keySet().containsAll(m.keySet())) hasChanged = false;
      if (forceReload || hasChanged) {
        log.debug("RequestHandlers being reloaded : {}", m.keySet());
        Map<String, SolrRequestHandler> newCustomHandlers = new HashMap<>();
        m.forEach((k, v) -> {
          if (v instanceof Map) {
            String klas = (String) ((Map) v).get(FieldType.CLASS_NAME);
            if (klas != null) {
              SolrRequestHandler inst = libListener.newInstance(klas, SolrRequestHandler.class);
              if (inst instanceof PluginInfoInitialized) {
                ((PluginInfoInitialized) inst).init(new PluginInfo(SolrRequestHandler.TYPE, (Map) v));
              }
              newCustomHandlers.put((String) k, inst);
            }
          } else {
            log.error("Invalid data for requestHandler : {} , {}", k, v);
          }
        });

        log.debug("Registering request handlers {} ", newCustomHandlers.keySet());
        Map<String, SolrRequestHandler> old = customHandlers;
        customHandlers = newCustomHandlers;
        old.forEach((s, h) -> PluginBag.closeQuietly(h));
      }
    }

    @Override
    public String getDescription() {
      return "Custom Handlers";
    }


    @Override
    public Boolean registerV1() {
      return Boolean.FALSE;
    }

    @Override
    public Boolean registerV2() {
      return Boolean.TRUE;
    }

    @Override
    public Name getPermissionName(AuthorizationContext request) {
      if(request.getResource().endsWith("/node/ext")) return Name.COLL_READ_PERM;
      return Name.CUSTOM_PERM;
    }
  }

}
