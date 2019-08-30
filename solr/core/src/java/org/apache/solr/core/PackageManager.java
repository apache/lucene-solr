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
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.api.Api;
import org.apache.solr.api.V2HttpCall;
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

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.PACKAGE;
import static org.apache.solr.common.params.CommonParams.VERSION;
import static org.apache.solr.core.RuntimeLib.SHA256;

public class PackageManager implements ClusterPropertiesListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;

  private Map<String, Package> pkgs = new HashMap<>();

  final ExtHandler extHandler;
  private int myversion = -1;

  public int getZNodeVersion(String pkg) {
    Package p = pkgs.get(pkg);
    return p == null ? -1 : p.lib.getZnodeVersion();
  }
  public RuntimeLib getLib(String name){
    Package p = pkgs.get(name);
    return p == null? null: p.lib;
  }

  static class Package implements MapWriter {
    final RuntimeLib lib;
    final MemClassLoader loader;
    final String name;

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      lib.writeMap(ew);
    }

    Package(RuntimeLib lib, MemClassLoader loader, int zkVersion, String name) {
      this.lib = lib;
      this.loader = loader;
      this.name = name;
    }

    public String getName() {
      return name;
    }


    public boolean isModified(Map map) {
      return (!Objects.equals(lib.getSha256(), (map).get(SHA256)) ||
          !Objects.equals(lib.getSig(), (map).get(SHA256)));
    }
  }

  PackageManager(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    extHandler = new ExtHandler(this);
  }


  public <T> T newInstance(String cName, Class<T> expectedType, String pkg) {
    try {
      return coreContainer.getResourceLoader().newInstance(cName, expectedType,
          null, new Class[]{CoreContainer.class}, new Object[]{coreContainer});
    } catch (SolrException e) {
      Package p = pkgs.get(pkg);

      if (p != null) {
        try {
          Class<? extends T> klas = p.loader.findClass(cName, expectedType);
          try {
            return klas.getConstructor(CoreContainer.class).newInstance(coreContainer);
          } catch (NoSuchMethodException ex) {
            return klas.getConstructor().newInstance();
          }
        } catch (Exception ex) {
          if (!p.loader.getErrors().isEmpty()) {
            //some libraries were no loaded due to some errors. May the class was there in those libraries
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "There were errors loading some libraries: " + StrUtils.join(p.loader.getErrors(), ','), ex);
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
    int v = coreContainer.getZkController().getZkStateReader().getClusterPropsVersion();
    boolean modified = updatePackages(properties, v);
    extHandler.updateReqHandlers(properties, modified);
    for (SolrCore core : coreContainer.solrCores.getCores()) {
      pkgs.forEach((s, pkg) -> core.packageUpdated(pkg.lib));
    }
    myversion = v;
    return false;
  }


  private boolean updatePackages(Map<String, Object> properties, int ver) {
    Map m = (Map) properties.getOrDefault(PACKAGE, Collections.emptyMap());
    if (pkgs.isEmpty() && m.isEmpty()) return false;
    boolean[] needsReload = new boolean[1];
    if (m.size() == pkgs.size()) {
      m.forEach((k, v) -> {
        if (v instanceof Map) {
          Package pkg = pkgs.get(k);
          if (pkg == null || pkg.isModified((Map) v)) {
            needsReload[0] = true;
          }
        }
      });
    } else {
      needsReload[0] = true;
    }
    if (needsReload[0]) {
      createNewClassLoaders(m, ver);
    }
    return needsReload[0];
  }

  public ResourceLoader getResourceLoader(String pkg) {
    Package p = pkgs.get(pkg);
    return p == null ? coreContainer.getResourceLoader() : p.loader;
  }

  void createNewClassLoaders(Map m, int ver) {
    boolean[] loadedAll = new boolean[1];
    loadedAll[0] = true;
    Map<String, Package> newPkgs = new LinkedHashMap<>();
    m.forEach((k, v) -> {
      if (v instanceof Map) {
        Map map = new HashMap((Map) v);
        map.put(CoreAdminParams.NAME, String.valueOf(k));
        String name = (String) k;
        Package existing = pkgs.get(name);
        if (existing != null && !existing.isModified(map)) {
          //this package has not changed
          newPkgs.put(name, existing);
        }

        RuntimeLib lib = new RuntimeLib(coreContainer);
        lib.znodeVersion = ver;
        try {
          lib.init(new PluginInfo(RuntimeLib.TYPE, map));
          if (lib.getUrl() == null) {
            log.error("Unable to initialize runtimeLib : " + Utils.toJSONString(v));
            loadedAll[0] = false;
          }
          lib.loadJar();

          newPkgs.put(name, new Package(lib,
              new MemClassLoader(Collections.singletonList(lib), coreContainer.getResourceLoader()),
              ver, name));
        } catch (Exception e) {
          log.error("error loading a runtimeLib " + Utils.toJSONString(v), e);
          loadedAll[0] = false;

        }
      }
    });

    if (loadedAll[0]) {
      log.info("Libraries changed. New memclassloader created with jars {}",
          newPkgs.values().stream().map(it -> it.lib.getUrl()).collect(Collectors.toList()));
      this.pkgs = newPkgs;

    }
  }

  static class ExtHandler extends RequestHandlerBase implements PermissionNameProvider {
    final PackageManager packageManager;

    private Map<String, Handler> customHandlers = new HashMap<>();

    ExtHandler(PackageManager packageManager) {
      this.packageManager = packageManager;
    }


    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {
      int v = req.getParams().getInt(ConfigOverlay.ZNODEVER, -1);
      if (v >= 0) {
        log.debug("expected version : {} , my version {}", v, packageManager.myversion);
        ZkStateReader zkStateReader = packageManager.coreContainer.getZkController().getZkStateReader();
        try {
          zkStateReader.forceRefreshClusterProps(v);
        } catch (SolrException e) {
          log.error("Error refreshing state ", e);
          throw e;
        }
      }
      rsp.add("metadata", (MapWriter) ew -> ew.putIfNotNull(VERSION,
          packageManager.coreContainer.getZkController().zkStateReader.getClusterPropsVersion()));
      rsp.add(RuntimeLib.TYPE, packageManager.pkgs.values());
      rsp.add(SolrRequestHandler.TYPE, customHandlers.values());

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
          Handler handler = customHandlers.get(name);
          if (handler == null) {
            String err = StrUtils.formatString(" No such handler: {0}, available handlers : {1}", name, customHandlers.keySet());
            log.error(err);
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND, err);
          }
          handler.handler.handleRequest(req, rsp);
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
        Map<String, Handler> newCustomHandlers = new HashMap<>();
        m.forEach((k, v) -> {
          if (v instanceof Map) {
            Map metaData = (Map) v;
            Handler existing = customHandlers.get(k);
            String name = (String) k;
            if (existing == null || existing.shouldReload(metaData, packageManager.pkgs)) {
              String klas = (String) metaData.get(FieldType.CLASS_NAME);
              if (klas != null) {
                String pkg = (String) metaData.get(PACKAGE);
                SolrRequestHandler inst = packageManager.newInstance(klas, SolrRequestHandler.class, pkg);
                if (inst instanceof PluginInfoInitialized) {
                  ((PluginInfoInitialized) inst).init(new PluginInfo(SolrRequestHandler.TYPE, metaData));
                }
                Package p = packageManager.pkgs.get(pkg);
                newCustomHandlers.put(name, new Handler(inst, pkg, p == null ? -1 : p.lib.getZnodeVersion(), metaData, name));
              } else {
                log.error("Invalid requestHandler {}", Utils.toJSONString(v));
              }

            } else {
              newCustomHandlers.put(name, existing);
            }

          } else {
            log.error("Invalid data for requestHandler : {} , {}", k, v);
          }
        });

        log.debug("Registering request handlers {} ", newCustomHandlers.keySet());
        Map<String, Handler> old = customHandlers;
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
      if (request.getResource().endsWith("/node/ext")) return Name.COLL_READ_PERM;
      return Name.CUSTOM_PERM;
    }

    static class Handler implements MapWriter {
      final SolrRequestHandler handler;
      final String pkg;
      final int zkversion;
      final Map meta;
      final String name;

      @Override
      public void writeMap(EntryWriter ew) throws IOException {
        ew.put(NAME, name);
        ew.put(ConfigOverlay.ZNODEVER, zkversion);
        meta.forEach(ew.getBiConsumer());
      }

      Handler(SolrRequestHandler handler, String pkg, int version, Map meta, String name) {
        this.handler = handler;
        this.pkg = pkg;
        this.zkversion = version;
        this.meta = Utils.getDeepCopy(meta, 3);
        this.name = name;
      }

      public boolean shouldReload(Map metaData, Map<String, Package> pkgs) {
        Package p = pkgs.get(pkg);
        //the metadata is same and the package has not changed since we last loaded
        return !meta.equals(metaData) || p == null || p.lib.getZnodeVersion() > zkversion;
      }
    }
  }

}
