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
package org.apache.solr.handler;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.RequestParams;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.pkg.PackageAPI;
import org.apache.solr.pkg.PackageListeners;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaManager;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Collections.singletonList;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.apache.solr.common.util.StrUtils.formatString;
import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.core.ConfigOverlay.NOT_EDITABLE;
import static org.apache.solr.core.ConfigOverlay.ZNODEVER;
import static org.apache.solr.core.ConfigSetProperties.IMMUTABLE_CONFIGSET_ARG;
import static org.apache.solr.core.PluginInfo.APPENDS;
import static org.apache.solr.core.PluginInfo.DEFAULTS;
import static org.apache.solr.core.PluginInfo.INVARIANTS;
import static org.apache.solr.core.RequestParams.USEPARAM;
import static org.apache.solr.core.SolrConfig.PluginOpts.REQUIRE_CLASS;
import static org.apache.solr.core.SolrConfig.PluginOpts.REQUIRE_NAME;
import static org.apache.solr.core.SolrConfig.PluginOpts.REQUIRE_NAME_IN_OVERLAY;
import static org.apache.solr.schema.FieldType.CLASS_NAME;

public class SolrConfigHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String CONFIGSET_EDITING_DISABLED_ARG = "disable.configEdit";
  public static final boolean configEditing_disabled = Boolean.getBoolean(CONFIGSET_EDITING_DISABLED_ARG);
  private static final Map<String, SolrConfig.SolrPluginInfo> namedPlugins;
  private Lock reloadLock = new ReentrantLock(true);

  public Lock getReloadLock() {
    return reloadLock;
  }

  private boolean isImmutableConfigSet = false;

  static {
    Map<String, SolrConfig.SolrPluginInfo> map = new HashMap<>();
    for (SolrConfig.SolrPluginInfo plugin : SolrConfig.plugins) {
      if (plugin.options.contains(REQUIRE_NAME) || plugin.options.contains(REQUIRE_NAME_IN_OVERLAY)) {
        map.put(plugin.getCleanTag().toLowerCase(Locale.ROOT), plugin);
      }
    }
    namedPlugins = Collections.unmodifiableMap(map);
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {

    RequestHandlerUtils.setWt(req, CommonParams.JSON);
    String httpMethod = (String) req.getContext().get("httpMethod");
    Command command = new Command(req, rsp, httpMethod);
    if ("POST".equals(httpMethod)) {
      if (configEditing_disabled || isImmutableConfigSet) {
        final String reason = configEditing_disabled ? "due to " + CONFIGSET_EDITING_DISABLED_ARG : "because ConfigSet is immutable";
        throw new SolrException(SolrException.ErrorCode.FORBIDDEN, " solrconfig editing is not enabled " + reason);
      }
      try {
        command.handlePOST();
      } finally {
        RequestHandlerUtils.addExperimentalFormatWarning(rsp);
      }
    } else {
      command.handleGET();
    }
  }

  @Override
  public void inform(SolrCore core) {
    isImmutableConfigSet = getImmutable(core);
  }

  public static boolean getImmutable(SolrCore core) {
    @SuppressWarnings({"rawtypes"})
    NamedList configSetProperties = core.getConfigSetProperties();
    if (configSetProperties == null) return false;
    Object immutable = configSetProperties.get(IMMUTABLE_CONFIGSET_ARG);
    return immutable != null && Boolean.parseBoolean(immutable.toString());
  }


  private class Command {
    private final SolrQueryRequest req;
    private final SolrQueryResponse resp;
    private final String method;
    private String path;
    List<String> parts;

    private Command(SolrQueryRequest req, SolrQueryResponse resp, String httpMethod) {
      this.req = req;
      this.resp = resp;
      this.method = httpMethod;
      path = (String) req.getContext().get("path");
      if (path == null) path = getDefaultPath();
      parts = StrUtils.splitSmart(path, '/', true);
    }

    private String getDefaultPath() {
      return "/config";
    }

    @SuppressWarnings({"unchecked"})
    private void handleGET() {
      if (parts.size() == 1) {
        //this is the whole config. sent out the whole payload
        resp.add("config", getConfigDetails(null, req));
      } else {
        if (ConfigOverlay.NAME.equals(parts.get(1))) {
          resp.add(ConfigOverlay.NAME, req.getCore().getSolrConfig().getOverlay());
        } else if (RequestParams.NAME.equals(parts.get(1))) {
          if (parts.size() == 3) {
            RequestParams params = req.getCore().getSolrConfig().getRequestParams();
            RequestParams.ParamSet p = params.getParams(parts.get(2));
            @SuppressWarnings({"rawtypes"})
            Map m = new LinkedHashMap<>();
            m.put(ZNODEVER, params.getZnodeVersion());
            if (p != null) {
              m.put(RequestParams.NAME, makeMap(parts.get(2), p.toMap(new LinkedHashMap<>())));
            }
            resp.add(SolrQueryResponse.NAME, m);
          } else {
            resp.add(SolrQueryResponse.NAME, req.getCore().getSolrConfig().getRequestParams());
          }

        } else {
          if (ZNODEVER.equals(parts.get(1))) {
            resp.add(ZNODEVER, Utils.makeMap(
                ConfigOverlay.NAME, req.getCore().getSolrConfig().getOverlay().getZnodeVersion(),
                RequestParams.NAME, req.getCore().getSolrConfig().getRequestParams().getZnodeVersion()));
            boolean isStale = false;
            int expectedVersion = req.getParams().getInt(ConfigOverlay.NAME, -1);
            int actualVersion = req.getCore().getSolrConfig().getOverlay().getZnodeVersion();
            if (expectedVersion > actualVersion) {
              log.info("expecting overlay version {} but my version is {}", expectedVersion, actualVersion);
              isStale = true;
            } else if (expectedVersion != -1) {
              log.info("I already have the expected version {} of config", expectedVersion);
            }
            expectedVersion = req.getParams().getInt(RequestParams.NAME, -1);
            actualVersion = req.getCore().getSolrConfig().getRequestParams().getZnodeVersion();
            if (expectedVersion > actualVersion) {
              log.info("expecting params version {} but my version is {}", expectedVersion, actualVersion);
              isStale = true;
            } else if (expectedVersion != -1) {
              log.info("I already have the expected version {} of params", expectedVersion);
            }
            if (isStale && req.getCore().getResourceLoader() instanceof ZkSolrResourceLoader) {
              new Thread(() -> {
                if (!reloadLock.tryLock()) {
                  log.info("Another reload is in progress . Not doing anything");
                  return;
                }
                try {
                  log.info("Trying to update my configs");
                  SolrCore.getConfListener(req.getCore(), (ZkSolrResourceLoader) req.getCore().getResourceLoader()).run();
                } catch (Exception e) {
                  log.error("Unable to refresh conf ", e);
                } finally {
                  reloadLock.unlock();
                }
              }, SolrConfigHandler.class.getSimpleName() + "-refreshconf").start();
            } else {
              if (log.isInfoEnabled()) {
                log.info("isStale {} , resourceloader {}", isStale, req.getCore().getResourceLoader().getClass().getName());
              }
            }

          } else {
            Map<String, Object> m = getConfigDetails(parts.get(1), req);
            Map<String, Object> val = makeMap(parts.get(1), m.get(parts.get(1)));
            String componentName = req.getParams().get("componentName");
            if (componentName != null) {
              @SuppressWarnings({"rawtypes"})
              Map pluginNameVsPluginInfo = (Map) val.get(parts.get(1));
              if (pluginNameVsPluginInfo != null) {
                @SuppressWarnings({"rawtypes"})
                Object o = pluginNameVsPluginInfo instanceof MapSerializable ?
                       pluginNameVsPluginInfo:
                        pluginNameVsPluginInfo.get(componentName);
                @SuppressWarnings({"rawtypes"})
                Map pluginInfo = o instanceof  MapSerializable? ((MapSerializable) o).toMap(new LinkedHashMap<>()): (Map) o;
                val.put(parts.get(1),pluginNameVsPluginInfo instanceof PluginInfo? pluginInfo :  makeMap(componentName, pluginInfo));
                if (req.getParams().getBool("meta", false)) {
                  // meta=true is asking for the package info of the plugin
                  // We go through all the listeners and see if there is one registered for this plugin
                  List<PackageListeners.Listener> listeners = req.getCore().getPackageListeners().getListeners();
                  for (PackageListeners.Listener listener :
                      listeners) {
                    Map<String, PackageAPI.PkgVersion> infos = listener.packageDetails();
                    if(infos == null || infos.isEmpty()) continue;
                    infos.forEach((s, mapWriter) -> {
                      if(s.equals(pluginInfo.get("class"))) {
                        (pluginInfo).put("_packageinfo_", mapWriter);
                      }
                    });
                  }
                }
              }
            }
            resp.add("config", val);
          }
        }
      }
    }

    @SuppressWarnings({"unchecked"})
    private Map<String, Object> getConfigDetails(String componentType, SolrQueryRequest req) {
      String componentName = componentType == null ? null : req.getParams().get("componentName");
      boolean showParams = req.getParams().getBool("expandParams", false);
      Map<String, Object> map = this.req.getCore().getSolrConfig().toMap(new LinkedHashMap<>());
      if (componentType != null && !SolrRequestHandler.TYPE.equals(componentType)) return map;
      @SuppressWarnings({"rawtypes"})
      Map reqHandlers = (Map) map.get(SolrRequestHandler.TYPE);
      if (reqHandlers == null) map.put(SolrRequestHandler.TYPE, reqHandlers = new LinkedHashMap<>());
      List<PluginInfo> plugins = this.req.getCore().getImplicitHandlers();
      for (PluginInfo plugin : plugins) {
        if (SolrRequestHandler.TYPE.equals(plugin.type)) {
          if (!reqHandlers.containsKey(plugin.name)) {
            reqHandlers.put(plugin.name, plugin);
          }
        }
      }
      if (!showParams) return map;
      for (Object o : reqHandlers.entrySet()) {
        @SuppressWarnings({"rawtypes"})
        Map.Entry e = (Map.Entry) o;
        if (componentName == null || e.getKey().equals(componentName)) {
          Map<String, Object> m = expandUseParams(req, e.getValue());
          e.setValue(m);
        }
      }

      return map;
    }

    @SuppressWarnings({"unchecked"})
    private Map<String, Object> expandUseParams(SolrQueryRequest req,
                                                Object plugin) {

      Map<String, Object> pluginInfo = null;
      if (plugin instanceof Map) {
        pluginInfo = (Map) plugin;
      } else if (plugin instanceof PluginInfo) {
        pluginInfo = ((PluginInfo) plugin).toMap(new LinkedHashMap<>());
      }
      @SuppressWarnings({"rawtypes"})
      String useParams = (String) pluginInfo.get(USEPARAM);
      String useParamsInReq = req.getOriginalParams().get(USEPARAM);
      if (useParams != null || useParamsInReq != null) {
        @SuppressWarnings({"rawtypes"})
        Map m = new LinkedHashMap<>();
        pluginInfo.put("_useParamsExpanded_", m);
        List<String> params = new ArrayList<>();
        if (useParams != null) params.addAll(StrUtils.splitSmart(useParams, ','));
        if (useParamsInReq != null) params.addAll(StrUtils.splitSmart(useParamsInReq, ','));
        for (String param : params) {
          RequestParams.ParamSet p = this.req.getCore().getSolrConfig().getRequestParams().getParams(param);
          if (p != null) {
            m.put(param, p);
          } else {
            m.put(param, "[NOT AVAILABLE]");
          }
        }


        LocalSolrQueryRequest r = new LocalSolrQueryRequest(req.getCore(), req.getOriginalParams());
        r.getContext().put(USEPARAM, useParams);
        @SuppressWarnings({"rawtypes"})
        NamedList nl = new PluginInfo(SolrRequestHandler.TYPE, pluginInfo).initArgs;
        SolrPluginUtils.setDefaults(r,
            getSolrParamsFromNamedList(nl, DEFAULTS),
            getSolrParamsFromNamedList(nl, APPENDS),
            getSolrParamsFromNamedList(nl, INVARIANTS));
        //SolrParams.wrapDefaults(maskUseParams, req.getParams())

        MapSolrParams mask = new MapSolrParams(ImmutableMap.<String, String>builder()
            .put("componentName", "")
            .put("expandParams", "")
            .build());
        pluginInfo.put("_effectiveParams_",
            SolrParams.wrapDefaults(mask, r.getParams()));
      }
      return pluginInfo;
    }


    private void handlePOST() throws IOException {
      List<CommandOperation> ops = CommandOperation.readCommands(req.getContentStreams(), resp.getValues());
      if (ops == null) return;
      try {
        for (; ; ) {
          ArrayList<CommandOperation> opsCopy = new ArrayList<>(ops.size());
          for (CommandOperation op : ops) opsCopy.add(op.getCopy());
          try {
            if (parts.size() > 1 && RequestParams.NAME.equals(parts.get(1))) {
              RequestParams params = RequestParams.getFreshRequestParams(req.getCore().getResourceLoader(), req.getCore().getSolrConfig().getRequestParams());
              handleParams(opsCopy, params);
            } else {
              ConfigOverlay overlay = SolrConfig.getConfigOverlay(req.getCore().getResourceLoader());
              handleCommands(opsCopy, overlay);
            }
            break;//succeeded . so no need to go over the loop again
          } catch (ZkController.ResourceModifiedInZkException e) {
            //retry
            if (log.isInfoEnabled()) {
              log.info("Race condition, the node is modified in ZK by someone else", e);
            }
          }
        }
      } catch (Exception e) {
        resp.setException(e);
        resp.add(CommandOperation.ERR_MSGS, singletonList(SchemaManager.getErrorStr(e)));
      }

    }


    @SuppressWarnings({"unchecked"})
    private void handleParams(ArrayList<CommandOperation> ops, RequestParams params) {
      for (CommandOperation op : ops) {
        switch (op.name) {
          case SET:
          case UPDATE: {
            Map<String, Object> map = op.getDataMap();
            if (op.hasError()) break;

            for (Map.Entry<String, Object> entry : map.entrySet()) {

              @SuppressWarnings({"rawtypes"})
              Map val;
              String key = entry.getKey();
              if (isNullOrEmpty(key)) {
                op.addError("null key ");
                continue;
              }
              key = key.trim();
              String err = validateName(key);
              if (err != null) {
                op.addError(err);
                continue;
              }

              try {
                val = (Map) entry.getValue();
              } catch (Exception e1) {
                op.addError("invalid params for key : " + key);
                continue;
              }

              if (val.containsKey("")) {
                op.addError("Empty keys are not allowed in params");
                continue;
              }

              RequestParams.ParamSet old = params.getParams(key);
              if (op.name.equals(UPDATE)) {
                if (old == null) {
                  op.addError(formatString("unknown paramset {0} cannot update ", key));
                  continue;
                }
                params = params.setParams(key, old.update(val));
              } else {
                Long version = old == null ? 0 : old.getVersion() + 1;
                params = params.setParams(key, RequestParams.createParamSet(val, version));
              }

            }
            break;

          }
          case "delete": {
            List<String> name = op.getStrs(CommandOperation.ROOT_OBJ);
            if (op.hasError()) break;
            for (String s : name) {
              if (params.getParams(s) == null) {
                op.addError(formatString("Could not delete. No such params ''{0}'' exist", s));
              }
              params = params.setParams(s, null);
            }
            break;
          }
          default: {
            op.unknownOperation();
          }
        }
      }


      @SuppressWarnings({"rawtypes"})
      List errs = CommandOperation.captureErrors(ops);
      if (!errs.isEmpty()) {
        throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, "error processing params", errs);
      }

      SolrResourceLoader loader = req.getCore().getResourceLoader();
      if (loader instanceof ZkSolrResourceLoader) {
        ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader) loader;
        if (ops.isEmpty()) {
          ZkController.touchConfDir(zkLoader);
        } else {
          if (log.isDebugEnabled()) {
            log.debug("persisting params data : {}", Utils.toJSONString(params.toMap(new LinkedHashMap<>())));
          }
          int latestVersion = ZkController.persistConfigResourceToZooKeeper(zkLoader,
              params.getZnodeVersion(), RequestParams.RESOURCE, params.toByteArray(), true);

          log.debug("persisted to version : {} ", latestVersion);
          waitForAllReplicasState(req.getCore().getCoreDescriptor().getCloudDescriptor().getCollectionName(),
              req.getCore().getCoreContainer().getZkController(), RequestParams.NAME, latestVersion, 30);
        }

      } else {
        SolrResourceLoader.persistConfLocally(loader, RequestParams.RESOURCE, params.toByteArray());
        req.getCore().getSolrConfig().refreshRequestParams();
      }

    }

    @SuppressWarnings({"unchecked"})
    private void handleCommands(List<CommandOperation> ops, ConfigOverlay overlay) throws IOException {
      for (CommandOperation op : ops) {
        switch (op.name) {
          case SET_PROPERTY:
            overlay = applySetProp(op, overlay);
            break;
          case UNSET_PROPERTY:
            overlay = applyUnset(op, overlay);
            break;
          case SET_USER_PROPERTY:
            overlay = applySetUserProp(op, overlay);
            break;
          case UNSET_USER_PROPERTY:
            overlay = applyUnsetUserProp(op, overlay);
            break;
          default: {
            List<String> pcs = StrUtils.splitSmart(op.name.toLowerCase(Locale.ROOT), '-');
            if (pcs.size() != 2) {
              op.addError(formatString("Unknown operation ''{0}'' ", op.name));
            } else {
              String prefix = pcs.get(0);
              String name = pcs.get(1);
              if (cmdPrefixes.contains(prefix) && namedPlugins.containsKey(name)) {
                SolrConfig.SolrPluginInfo info = namedPlugins.get(name);
                if ("delete".equals(prefix)) {
                  overlay = deleteNamedComponent(op, overlay, info.getCleanTag());
                } else {
                  overlay = updateNamedPlugin(info, op, overlay, prefix.equals("create") || prefix.equals("add"));
                }
              } else {
                op.unknownOperation();
              }
            }
          }
        }
      }
      @SuppressWarnings({"rawtypes"})
      List errs = CommandOperation.captureErrors(ops);
      if (!errs.isEmpty()) {
        log.error("ERROR:{}", Utils.toJSONString(errs));
        throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST, "error processing commands", errs);
      }

      SolrResourceLoader loader = req.getCore().getResourceLoader();
      if (loader instanceof ZkSolrResourceLoader) {
        int latestVersion = ZkController.persistConfigResourceToZooKeeper((ZkSolrResourceLoader) loader, overlay.getZnodeVersion(),
            ConfigOverlay.RESOURCE_NAME, overlay.toByteArray(), true);
        log.debug("Executed config commands successfully and persisted to ZK {}", ops);
        waitForAllReplicasState(req.getCore().getCoreDescriptor().getCloudDescriptor().getCollectionName(),
            req.getCore().getCoreContainer().getZkController(),
            ConfigOverlay.NAME,
            latestVersion, 30);
      } else {
        SolrResourceLoader.persistConfLocally(loader, ConfigOverlay.RESOURCE_NAME, overlay.toByteArray());
        req.getCore().getCoreContainer().reload(req.getCore().getName(), req.getCore().uniqueId);
        log.info("Executed config commands successfully and persisted to File System {}", ops);
      }

    }

    private ConfigOverlay deleteNamedComponent(CommandOperation op, ConfigOverlay overlay, String typ) {
      String name = op.getStr(CommandOperation.ROOT_OBJ);
      if (op.hasError()) return overlay;
      if (overlay.getNamedPlugins(typ).containsKey(name)) {
        return overlay.deleteNamedPlugin(name, typ);
      } else {
        op.addError(formatString("NO such {0} ''{1}'' ", typ, name));
        return overlay;
      }
    }

    private ConfigOverlay updateNamedPlugin(SolrConfig.SolrPluginInfo info, CommandOperation op, ConfigOverlay overlay, boolean isCeate) {
      String name = op.getStr(NAME);
      String clz = info.options.contains(REQUIRE_CLASS) ? op.getStr(CLASS_NAME) : op.getStr(CLASS_NAME, null);
      op.getMap(DEFAULTS, null);
      op.getMap(PluginInfo.INVARIANTS, null);
      op.getMap(PluginInfo.APPENDS, null);
      if (op.hasError()) return overlay;
      if (info.clazz == PluginBag.RuntimeLib.class) {
        if (!PluginBag.RuntimeLib.isEnabled()) {
          op.addError("Solr not started with -Denable.runtime.lib=true");
          return overlay;
        }
        try {
          try (PluginBag.RuntimeLib rtl = new PluginBag.RuntimeLib(req.getCore())) {
            rtl.init(new PluginInfo(info.tag, op.getDataMap()));
          }
        } catch (Exception e) {
          op.addError(e.getMessage());
          log.error("can't load this plugin ", e);
          return overlay;
        }
      }
      if (!verifyClass(op, clz, info.clazz)) return overlay;
      if (pluginExists(info, overlay, name)) {
        if (isCeate) {
          op.addError(formatString(" ''{0}'' already exists . Do an ''{1}'' , if you want to change it ", name, "update-" + info.getTagCleanLower()));
          return overlay;
        } else {
          return overlay.addNamedPlugin(op.getDataMap(), info.getCleanTag());
        }
      } else {
        if (isCeate) {
          return overlay.addNamedPlugin(op.getDataMap(), info.getCleanTag());
        } else {
          op.addError(formatString(" ''{0}'' does not exist . Do an ''{1}'' , if you want to create it ", name, "create-" + info.getTagCleanLower()));
          return overlay;
        }
      }
    }

    private boolean pluginExists(SolrConfig.SolrPluginInfo info, ConfigOverlay overlay, String name) {
      List<PluginInfo> l = req.getCore().getSolrConfig().getPluginInfos(info.clazz.getName());
      for (PluginInfo pluginInfo : l) if (name.equals(pluginInfo.name)) return true;
      return overlay.getNamedPlugins(info.getCleanTag()).containsKey(name);
    }

    @SuppressWarnings({"unchecked"})
    private boolean verifyClass(CommandOperation op, String clz, @SuppressWarnings({"rawtypes"})Class expected) {
      if (clz == null) return true;
      if (!"true".equals(String.valueOf(op.getStr("runtimeLib", null)))) {
        PluginInfo info = new PluginInfo(SolrRequestHandler.TYPE, op.getDataMap());
        //this is not dynamically loaded so we can verify the class right away
        try {
          if(expected == Expressible.class) {
            @SuppressWarnings("resource")
            SolrResourceLoader resourceLoader = info.pkgName == null ?
                req.getCore().getResourceLoader() :
                req.getCore().getResourceLoader(info.pkgName);
            resourceLoader.findClass(info.className, expected);
          } else {
            req.getCore().createInitInstance(info, expected, clz, "");
          }
        } catch (Exception e) {
          log.error("Error checking plugin : ", e);
          op.addError(e.getMessage());
          return false;
        }

      }
      return true;
    }

    private ConfigOverlay applySetUserProp(CommandOperation op, ConfigOverlay overlay) {
      Map<String, Object> m = op.getDataMap();
      if (op.hasError()) return overlay;
      for (Map.Entry<String, Object> e : m.entrySet()) {
        String name = e.getKey();
        Object val = e.getValue();
        overlay = overlay.setUserProperty(name, val);
      }
      return overlay;
    }

    private ConfigOverlay applyUnsetUserProp(CommandOperation op, ConfigOverlay overlay) {
      List<String> name = op.getStrs(CommandOperation.ROOT_OBJ);
      if (op.hasError()) return overlay;
      for (String o : name) {
        if (!overlay.getUserProps().containsKey(o)) {
          op.addError(formatString("No such property ''{0}''", name));
        } else {
          overlay = overlay.unsetUserProperty(o);
        }
      }
      return overlay;
    }


    private ConfigOverlay applyUnset(CommandOperation op, ConfigOverlay overlay) {
      List<String> name = op.getStrs(CommandOperation.ROOT_OBJ);
      if (op.hasError()) return overlay;

      for (String o : name) {
        if (!ConfigOverlay.isEditableProp(o, false, null)) {
          op.addError(formatString(NOT_EDITABLE, name));
        } else {
          overlay = overlay.unsetProperty(o);
        }
      }
      return overlay;
    }

    private ConfigOverlay applySetProp(CommandOperation op, ConfigOverlay overlay) {
      Map<String, Object> m = op.getDataMap();
      if (op.hasError()) return overlay;
      for (Map.Entry<String, Object> e : m.entrySet()) {
        String name = e.getKey();
        Object val = e.getValue();
        @SuppressWarnings({"rawtypes"})
        Class typ = ConfigOverlay.checkEditable(name, false, null);
        if (typ == null) {
          op.addError(formatString(NOT_EDITABLE, name));
          continue;
        }

        if (val != null) {
          if (typ == String.class) val = val.toString();
          String typeErr = "Property {0} must be of {1} type ";
          if (typ == Boolean.class) {
            try {
              val = Boolean.parseBoolean(val.toString());
            } catch (Exception exp) {
              op.addError(formatString(typeErr, name, typ.getSimpleName()));
              continue;
            }
          } else if (typ == Integer.class) {
            try {
              val = Integer.parseInt(val.toString());
            } catch (Exception exp) {
              op.addError(formatString(typeErr, name, typ.getSimpleName()));
              continue;
            }

          } else if (typ == Float.class) {
            try {
              val = Float.parseFloat(val.toString());
            } catch (Exception exp) {
              op.addError(formatString(typeErr, name, typ.getSimpleName()));
              continue;
            }

          }
        }


        overlay = overlay.setProperty(name, val);
      }
      return overlay;
    }

  }

  public static String validateName(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if ((c >= 'A' && c <= 'Z') ||
          (c >= 'a' && c <= 'z') ||
          (c >= '0' && c <= '9') ||
          c == '_' ||
          c == '-' ||
          c == '.'
      ) continue;
      else {
        return formatString("''{0}'' name should only have chars [a-zA-Z_-.0-9] ", s);
      }
    }
    return null;
  }

  @Override
  public SolrRequestHandler getSubHandler(String path) {
    if (subPaths.contains(path)) return this;
    if (path.startsWith("/params/")) return this;
    return null;
  }


  private static Set<String> subPaths = new HashSet<>(Arrays.asList("/overlay", "/params", "/updateHandler",
      "/query", "/jmx", "/requestDispatcher", "/znodeVersion"));

  static {
    for (SolrConfig.SolrPluginInfo solrPluginInfo : SolrConfig.plugins)
      subPaths.add("/" + solrPluginInfo.getCleanTag());

  }

  //////////////////////// SolrInfoMBeans methods //////////////////////


  @Override
  public String getDescription() {
    return "Edit solrconfig.xml";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }


  public static final String SET_PROPERTY = "set-property";
  public static final String UNSET_PROPERTY = "unset-property";
  public static final String SET_USER_PROPERTY = "set-user-property";
  public static final String UNSET_USER_PROPERTY = "unset-user-property";
  public static final String SET = "set";
  public static final String UPDATE = "update";
  public static final String CREATE = "create";
  private static Set<String> cmdPrefixes = ImmutableSet.of(CREATE, UPDATE, "delete", "add");

  /**
   * Block up to a specified maximum time until we see agreement on the schema
   * version in ZooKeeper across all replicas for a collection.
   */
  private static void waitForAllReplicasState(String collection,
                                              ZkController zkController,
                                              String prop,
                                              int expectedVersion,
                                              int maxWaitSecs) {
    final RTimer timer = new RTimer();
    // get a list of active replica cores to query for the schema zk version (skipping this core of course)
    List<PerReplicaCallable> concurrentTasks = new ArrayList<>();

    for (String coreUrl : getActiveReplicaCoreUrls(zkController, collection)) {
      PerReplicaCallable e = new PerReplicaCallable(coreUrl, prop, expectedVersion, maxWaitSecs);
      concurrentTasks.add(e);
    }
    if (concurrentTasks.isEmpty()) return; // nothing to wait for ...

    if (log.isInfoEnabled()) {
      log.info(formatString("Waiting up to {0} secs for {1} replicas to set the property {2} to be of version {3} for collection {4}",
          maxWaitSecs, concurrentTasks.size(), prop, expectedVersion, collection));
    }

    // use an executor service to invoke schema zk version requests in parallel with a max wait time
    int poolSize = Math.min(concurrentTasks.size(), 10);
    ExecutorService parallelExecutor =
        ExecutorUtil.newMDCAwareFixedThreadPool(poolSize, new SolrNamedThreadFactory("solrHandlerExecutor"));
    try {
      List<Future<Boolean>> results =
          parallelExecutor.invokeAll(concurrentTasks, maxWaitSecs, TimeUnit.SECONDS);

      // determine whether all replicas have the update
      List<String> failedList = null; // lazily init'd
      for (int f = 0; f < results.size(); f++) {
        Boolean success = false;
        Future<Boolean> next = results.get(f);
        if (next.isDone() && !next.isCancelled()) {
          // looks to have finished, but need to check if it succeeded
          try {
            success = next.get();
          } catch (ExecutionException e) {
            // shouldn't happen since we checked isCancelled
          }
        }

        if (!success) {
          String coreUrl = concurrentTasks.get(f).coreUrl;
          log.warn("Core {} could not get the expected version {}", coreUrl, expectedVersion);
          if (failedList == null) failedList = new ArrayList<>();
          failedList.add(coreUrl);
        }
      }

      // if any tasks haven't completed within the specified timeout, it's an error
      if (failedList != null)
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            formatString("{0} out of {1} the property {2} to be of version {3} within {4} seconds! Failed cores: {5}",
                failedList.size(), concurrentTasks.size() + 1, prop, expectedVersion, maxWaitSecs, failedList));

    } catch (InterruptedException ie) {
      log.warn(formatString(
          "Core  was interrupted . trying to set the property {1} to version {2} to propagate to {3} replicas for collection {4}",
          prop, expectedVersion, concurrentTasks.size(), collection));
      Thread.currentThread().interrupt();
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(parallelExecutor);
    }

    if (log.isInfoEnabled()) {
      log.info("Took {}ms to set the property {} to be of version {} for collection {}",
          timer.getTime(), prop, expectedVersion, collection);
    }
  }

  public static List<String> getActiveReplicaCoreUrls(ZkController zkController,
                                                      String collection) {
    List<String> activeReplicaCoreUrls = new ArrayList<>();
    ClusterState clusterState = zkController.getZkStateReader().getClusterState();
    Set<String> liveNodes = clusterState.getLiveNodes();
    final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (docCollection != null && docCollection.getActiveSlices() != null && docCollection.getActiveSlices().size() > 0) {
      final Collection<Slice> activeSlices = docCollection.getActiveSlices();
      for (Slice next : activeSlices) {
        Map<String, Replica> replicasMap = next.getReplicasMap();
        if (replicasMap != null) {
          for (Map.Entry<String, Replica> entry : replicasMap.entrySet()) {
            Replica replica = entry.getValue();
            if (replica.getState() == Replica.State.ACTIVE && liveNodes.contains(replica.getNodeName())) {
              activeReplicaCoreUrls.add(replica.getCoreUrl());
            }
          }
        }
      }
    }
    return activeReplicaCoreUrls;
  }

  @Override
  public Name getPermissionName(AuthorizationContext ctx) {
    switch (ctx.getHttpMethod()) {
      case "GET":
        return Name.CONFIG_READ_PERM;
      case "POST":
        return Name.CONFIG_EDIT_PERM;
      default:
        return null;
    }
  }

  @SuppressWarnings({"rawtypes"})
  private static class PerReplicaCallable extends SolrRequest implements Callable<Boolean> {
    String coreUrl;
    String prop;
    int expectedZkVersion;
    Number remoteVersion = null;
    int maxWait;

    PerReplicaCallable(String coreUrl, String prop, int expectedZkVersion, int maxWait) {
      super(METHOD.GET, "/config/" + ZNODEVER);
      this.coreUrl = coreUrl;
      this.expectedZkVersion = expectedZkVersion;
      this.prop = prop;
      this.maxWait = maxWait;
    }

    @Override
    public SolrParams getParams() {
      return new ModifiableSolrParams()
          .set(prop, expectedZkVersion)
          .set(CommonParams.WT, CommonParams.JAVABIN);
    }

    @Override
    public Boolean call() throws Exception {
      final RTimer timer = new RTimer();
      int attempts = 0;
      try (HttpSolrClient solr = new HttpSolrClient.Builder(coreUrl).build()) {
        // eventually, this loop will get killed by the ExecutorService's timeout
        while (true) {
          try {
            long timeElapsed = (long) timer.getTime() / 1000;
            if (timeElapsed >= maxWait) {
              return false;
            }
            log.info("Time elapsed : {} secs, maxWait {}", timeElapsed, maxWait);
            Thread.sleep(100);
            NamedList<Object> resp = solr.httpUriRequest(this).future.get();
            if (resp != null) {
              @SuppressWarnings({"rawtypes"})
              Map m = (Map) resp.get(ZNODEVER);
              if (m != null) {
                remoteVersion = (Number) m.get(prop);
                if (remoteVersion != null && remoteVersion.intValue() >= expectedZkVersion) break;
              }
            }

            attempts++;
            if (log.isInfoEnabled()) {
              log.info(formatString("Could not get expectedVersion {0} from {1} for prop {2}   after {3} attempts", expectedZkVersion, coreUrl, prop, attempts));
            }
          } catch (Exception e) {
            if (e instanceof InterruptedException) {
              break; // stop looping
            } else {
              log.warn("Failed to get /schema/zkversion from {} due to: ", coreUrl, e);
            }
          }
        }
      }
      return true;
    }


    @Override
    protected SolrResponse createResponse(SolrClient client) {
      return null;
    }
  }

  @Override
  public Collection<Api> getApis() {
    return ApiBag.wrapRequestHandlers(this,
        "core.config",
        "core.config.Commands",
        "core.config.Params",
        "core.config.Params.Commands");
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }
}
