package org.apache.solr.handler;

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
import java.net.URL;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaManager;
import org.apache.solr.util.CommandOperation;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.text.MessageFormat.format;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;
import static org.apache.solr.core.ConfigOverlay.NOT_EDITABLE;
import static org.apache.solr.core.PluginInfo.DEFAULTS;

public class SolrConfigHandler extends RequestHandlerBase implements SolrCoreAware{
  public static final Logger log = LoggerFactory.getLogger(SolrConfigHandler.class);
  public static final boolean configEditing_disabled = Boolean.getBoolean("disable.configEdit");

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {

    setWt(req, "json");
    String httpMethod = (String) req.getContext().get("httpMethod");
    Command command = new Command(req, rsp, httpMethod);
    if("POST".equals(httpMethod)){
      if(configEditing_disabled) throw new SolrException(SolrException.ErrorCode.FORBIDDEN," solrconfig editing is not enabled");
      command.handlePOST();
    }  else {
      command.handleGET();
    }
  }



  @Override
  public void inform(final SolrCore core) {
    if( ! (core.getResourceLoader() instanceof  ZkSolrResourceLoader)) return;
    final ZkSolrResourceLoader zkSolrResourceLoader = (ZkSolrResourceLoader) core.getResourceLoader();
    if(zkSolrResourceLoader != null){
      Runnable listener = new Runnable() {
        @Override
        public void run() {
          try {
            if(core.isClosed()) return;
            Stat stat = zkSolrResourceLoader.getZkController().getZkClient().exists((zkSolrResourceLoader).getCollectionZkPath() + "/" + ConfigOverlay.RESOURCE_NAME, null, true);
            if(stat == null) return;
            if (stat.getVersion() >  core.getSolrConfig().getOverlay().getZnodeVersion()) {
              core.getCoreDescriptor().getCoreContainer().reload(core.getName());
            }
          } catch (KeeperException.NoNodeException nne){
            //no problem
          } catch (KeeperException e) {
            log.error("error refreshing solrconfig ", e);
          } catch (InterruptedException e) {
            Thread.currentThread().isInterrupted();
          }
        }
      };

      zkSolrResourceLoader.getZkController().registerConfListenerForCore(zkSolrResourceLoader.getCollectionZkPath(), core,listener);
    }

  }


  private static class Command{
    private final SolrQueryRequest req;
    private final SolrQueryResponse resp;
    private final String method;

    private Command(SolrQueryRequest req, SolrQueryResponse resp, String httpMethod) {
      this.req = req;
      this.resp = resp;
      this.method = httpMethod;
    }

    private void handleGET() {
      String path = (String) req.getContext().get("path");
      if(path == null) path="/config";
      if("/config/overlay".equals(path)){
        resp.add("overlay", req.getCore().getSolrConfig().getOverlay().toOutputFormat());
        return;
      } else {
        List<String> parts =StrUtils.splitSmart(path, '/');
        if(parts.get(0).isEmpty()) parts.remove(0);
        if(parts.size() == 1) {
          resp.add("solrConfig", req.getCore().getSolrConfig().toMap());
        } else{
          Map<String, Object> m = req.getCore().getSolrConfig().toMap();
          resp.add("solrConfig", ZkNodeProps.makeMap(parts.get(1),m.get(parts.get(1))));
        }
      }
    }


    private void handlePOST() throws IOException {
    Iterable<ContentStream> streams = req.getContentStreams();
    if(streams == null ){
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "missing content stream");
    }
      try {
        for (ContentStream stream : streams) {
          runCommandsTillSuccess(stream);
        }
      } catch (Exception e) {
        resp.setException(e);
        resp.add(CommandOperation.ERR_MSGS, singletonList(SchemaManager.getErrorStr(e)));
      }

    }

    private void runCommandsTillSuccess(ContentStream stream) throws IOException {
      for (;;) {
        try {
          handleCommands(stream);
          break;
        } catch (ZkController.ResourceModifiedInZkException e) {
          log.info(e.getMessage());

        }
      }
    }

    private void handleCommands( ContentStream stream) throws IOException {
    ConfigOverlay overlay = req.getCore().getSolrConfig().getOverlay();
    List<CommandOperation> ops = CommandOperation.parse(stream.getReader());
    for (CommandOperation op : ops) {
      if(SET_PROPERTY.equals( op.name) ){
        overlay = applySetProp(op, overlay);
      }else if(UNSET_PROPERTY.equals(op.name)){
        overlay = applyUnset(op,overlay);
      }else if(SET_USER_PROPERTY.equals(op.name)){
        overlay = applySetUserProp(op ,overlay);
      }else if(UNSET_USER_PROPERTY.equals(op.name)){
        overlay = applyUnsetUserProp(op, overlay);
      }
    }
    List errs = CommandOperation.captureErrors(ops);
    if (!errs.isEmpty()) {
      resp.add(CommandOperation.ERR_MSGS,errs);
      return;
    }

    SolrResourceLoader loader = req.getCore().getResourceLoader();
    if (loader instanceof ZkSolrResourceLoader) {
      ZkController.persistConfigResourceToZooKeeper(loader,overlay.getZnodeVersion(),
          ConfigOverlay.RESOURCE_NAME,overlay.toByteArray(),true);

      String collectionName = req.getCore().getCoreDescriptor().getCloudDescriptor().getCollectionName();
      Map map = ZkNodeProps.makeMap(CoreAdminParams.ACTION, CollectionParams.CollectionAction.RELOAD.toString() ,
          CollectionParams.NAME, collectionName);

      SolrQueryRequest  solrQueryRequest = new LocalSolrQueryRequest(req.getCore(), new MapSolrParams(map));
      SolrQueryResponse tmpResp = new SolrQueryResponse();
      try {
        //doing a collection reload
        req.getCore().getCoreDescriptor().getCoreContainer().getCollectionsHandler().handleRequestBody(solrQueryRequest,tmpResp);
      } catch (Exception e) {
        String msg = MessageFormat.format("Unable to reload collection {0}", collectionName);
        log.error(msg);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
      }

    } else {
      SolrResourceLoader.persistConfLocally(loader, ConfigOverlay.RESOURCE_NAME, overlay.toByteArray());
      req.getCore().getCoreDescriptor().getCoreContainer().reload(req.getCore().getName());
    }

  }

    private ConfigOverlay applySetUserProp(CommandOperation op, ConfigOverlay overlay) {
      Map<String, Object> m = op.getDataMap();
      if(op.hasError()) return overlay;
      for (Map.Entry<String, Object> e : m.entrySet()) {
        String name = e.getKey();
        Object val = e.getValue();
        overlay = overlay.setUserProperty(name, val);
      }
      return overlay;
    }

    private ConfigOverlay applyUnsetUserProp(CommandOperation op, ConfigOverlay overlay) {
      List<String> name = op.getStrs(CommandOperation.ROOT_OBJ);
      if(op.hasError()) return overlay;
      for (String o : name) {
        if(!overlay.getUserProps().containsKey(o)) {
          op.addError(format("No such property ''{0}''", name));
        } else {
          overlay = overlay.unsetUserProperty(o);
        }
      }
      return overlay;
    }



    private ConfigOverlay applyUnset(CommandOperation op, ConfigOverlay overlay) {
      List<String> name = op.getStrs(CommandOperation.ROOT_OBJ);
      if(op.hasError()) return overlay;

      for (String o : name) {
        if(!ConfigOverlay.isEditableProp(o, false, null)) {
          op.addError(format(NOT_EDITABLE, name));
        } else {
          overlay = overlay.unsetProperty(o);
        }
      }
      return overlay;
    }

    private ConfigOverlay applySetProp(CommandOperation op, ConfigOverlay overlay) {
      Map<String, Object> m = op.getDataMap();
      if(op.hasError()) return overlay;
      for (Map.Entry<String, Object> e : m.entrySet()) {
        String name = e.getKey();
        Object val = e.getValue();
        if(!ConfigOverlay.isEditableProp(name, false, null)) {
          op.addError(format(NOT_EDITABLE, name));
          continue;
        }
        overlay = overlay.setProperty(name, val);
      }
      return overlay;
    }

  }

  static void setWt(SolrQueryRequest req, String wt){
    SolrParams params = req.getParams();
    if( params.get(CommonParams.WT) != null ) return;//wt is set by user
    Map<String,String> map = new HashMap<>(1);
    map.put(CommonParams.WT, wt);
    map.put("indent", "true");
    req.setParams(SolrParams.wrapDefaults(params, new MapSolrParams(map)));
  }


  public static void addImplicits(List<PluginInfo> infoList){
    Map m = makeMap("name", "/config", "class", SolrConfigHandler.class.getName());
    infoList.add(new PluginInfo(SolrRequestHandler.TYPE, m, new NamedList<>(singletonMap(DEFAULTS, new NamedList())), null));
  }



  @Override
  public SolrRequestHandler getSubHandler(String path) {
    if(subPaths.contains(path)) return this;
    return null;
  }


  private static Set<String> subPaths =  new HashSet<>(Arrays.asList("/overlay",
      "/query","/jmx","/requestDispatcher"));
  static {
    for (SolrConfig.SolrPluginInfo solrPluginInfo : SolrConfig.plugins) subPaths.add("/"+solrPluginInfo.tag.replaceAll("/",""));

  }

  //////////////////////// SolrInfoMBeans methods //////////////////////


  @Override
  public String getDescription() {
    return "Edit solrconfig.xml";
  }


  @Override
  public String getVersion() {
    return getClass().getPackage().getSpecificationVersion();
  }

  @Override
  public Category getCategory() {
    return Category.OTHER;
  }



  public static final String SET_PROPERTY = "set-property";
  public static final String UNSET_PROPERTY = "unset-property";
  public static final String SET_USER_PROPERTY = "set-user-property";
  public static final String UNSET_USER_PROPERTY = "unset-user-property";



}
