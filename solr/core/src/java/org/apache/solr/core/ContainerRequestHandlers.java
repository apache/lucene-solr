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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.api.CallInfo;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.PACKAGE;
import static org.apache.solr.common.params.CommonParams.VERSION;
import static org.apache.solr.core.PluginBag.closeQuietly;

@EndPoint(spec = "node.ext", permission = PermissionNameProvider.Name.CUSTOM_PERM)
public class ContainerRequestHandlers {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final CoreContainer coreContainer;

  private Map<String, Handler> customHandlers = new HashMap<>();

  ContainerRequestHandlers(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {
    int v = req.getParams().getInt(ConfigOverlay.ZNODEVER, -1);
    if (v >= 0) {
      log.debug("expected version : {} , my version {}", v, coreContainer.getPackageBag().myVersion);
      ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
      try {
        zkStateReader.forceRefreshClusterProps(v);
      } catch (SolrException e) {
        log.error("Error refreshing state ", e);
        throw e;
      }
    }
    rsp.add("metadata", (MapWriter) ew -> ew.put(VERSION,
        coreContainer.getZkController().zkStateReader.getClusterPropsVersion()));
    rsp.add(SolrRequestHandler.TYPE, customHandlers.values());

  }

  @Command
  public void call(CallInfo info) {
    String name = ((V2HttpCall) info.req.getHttpSolrCall()).getUrlParts().get("handlerName");
    if (name == null) {
      handleRequestBody(info.req, info.rsp);
      return;
    }
    Handler wrapper = customHandlers.get(name);
    if (wrapper == null) {
      String err = StrUtils.formatString(" No such handler: {0}, available handlers : {1}", name, customHandlers.keySet());
      log.error(err);
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, err);
    }
    wrapper.handler.handleRequest(info.req, info.rsp);

  }


  void updateReqHandlers(Map<String, Object> properties) {
    Map m = (Map) properties.getOrDefault(SolrRequestHandler.TYPE, Collections.emptyMap());
    if (m.isEmpty() && customHandlers.isEmpty()) return;
    if (customHandlers.size() == m.size() && customHandlers.keySet().containsAll(m.keySet())) return;
    log.debug("RequestHandlers being reloaded : {}", m.keySet());
    Map<String, Handler> newCustomHandlers = new HashMap<>();
    List<Handler> toBeClosed = new ArrayList<>();
    for (Object o : m.entrySet()) {
      Object v = ((Map.Entry) o).getValue();
      String name = (String) ((Map.Entry) o).getKey();
      if (v instanceof Map) {
        Map metaData = (Map) v;
        Handler existing = customHandlers.get(name);
        if (existing == null || !existing.meta.equals(metaData)) {
          String klas = (String) metaData.get(FieldType.CLASS_NAME);
          if (klas != null) {
            newCustomHandlers.put(name, new Handler(metaData));
          } else {
            log.error("Invalid requestHandler {}", Utils.toJSONString(v));
          }
          if (existing != null) {
            toBeClosed.add(existing);
          }

        } else {
          newCustomHandlers.put(name, existing);
        }

      } else {
        log.error("Invalid data for requestHandler : {} , {}", name, v);
      }
    }

    log.debug("Registering request handlers {} ", newCustomHandlers.keySet());
    Map<String, Handler> old = customHandlers;
    for (Map.Entry<String, Handler> e : old.entrySet()) {
      if (!newCustomHandlers.containsKey(e.getKey())) {
        toBeClosed.add(e.getValue());
      }
    }
    customHandlers = newCustomHandlers;
    for (Handler wrapper : toBeClosed) {
      closeQuietly(wrapper);
    }
  }

  private SolrRequestHandler createHandler(Map metaData) {
    String pkg = (String) metaData.get(PACKAGE);
    SolrRequestHandler inst = coreContainer.getPackageBag().newInstance((String) metaData.get(FieldType.CLASS_NAME),
        SolrRequestHandler.class, pkg);
    if (inst instanceof PluginInfoInitialized) {
      ((PluginInfoInitialized) inst).init(new PluginInfo(SolrRequestHandler.TYPE, metaData));
    }
    return inst;
  }


  class Handler implements MapWriter, PackageListeners.Listener, AutoCloseable {
    SolrRequestHandler handler;
    final String pkg;
    int zkversion;
    PluginInfo meta;
    PackageBag.PackageInfo packageInfo;
    String name;

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.put(NAME, name);
      ew.put(ConfigOverlay.ZNODEVER, zkversion);
      meta.attributes.forEach(ew.getBiConsumer());
    }

    Handler(Map meta) {
      this.meta = new PluginInfo(SolrRequestHandler.TYPE, meta);
      pkg = (String) meta.get("package");
      this.handler = createHandler(meta);
      if (pkg != null) {
        this.packageInfo = coreContainer.getPackageBag().getPackageInfo(pkg);
        coreContainer.getPackageBag().listenerRegistry.addListener(this);
      }


    }

    @Override
    public String packageName() {
      return pkg;
    }

    @Override
    public PluginInfo pluginInfo() {
      return meta;
    }

    @Override
    public void changed(PackageBag.PackageInfo info) {
      if (this.packageInfo.znodeVersion < info.znodeVersion) {
        this.handler = createHandler(meta.attributes);
        this.packageInfo = info;
      }

    }

    @Override
    public void close() throws Exception {
      closeQuietly(handler);
    }

    @Override
    public PackageBag.PackageInfo packageInfo() {
      return packageInfo;
    }
  }
}
