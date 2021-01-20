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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public final class RequestHandlers {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final SolrCore core;

  final PluginBag<SolrRequestHandler> handlers;

  /**
   * Trim the trailing '/' if it's there, and convert null to empty string.
   * 
   * we want:
   *  /update/csv   and
   *  /update/csv/
   * to map to the same handler 
   * 
   */
  public static String normalize( String p )
  {
    if(p == null) return "";
    if( p.endsWith( "/" ) && p.length() > 1 )
      return p.substring( 0, p.length()-1 );
    
    return p;
  }
  
  public RequestHandlers(SolrCore core) {
      this.core = core;
    // we need a thread safe registry since methods like register are currently documented to be thread safe.
    handlers =  new PluginBag<>(SolrRequestHandler.class, core, true);
  }

  /**
   * @return the RequestHandler registered at the given name 
   */
  public SolrRequestHandler get(String handlerName) {
    return handlers.get(normalize(handlerName));
  }

  /**
   * Handlers must be initialized before calling this function.  As soon as this is
   * called, the handler can immediately accept requests.
   * 
   * This call is thread safe.
   * 
   * @return the previous handler at the given path or null
   */
  public SolrRequestHandler register( String handlerName, SolrRequestHandler handler ) {
    String norm = normalize(handlerName);
    if (handler == null) {
      return handlers.remove(norm);
    }
    return handlers.put(norm, handler);
//    return register(handlerName, new PluginRegistry.PluginHolder<>(null, handler));
  }


  /**
   * Returns an unmodifiable Map containing the registered handlers
   */
  public PluginBag<SolrRequestHandler> getRequestHandlers() {
    return handlers;
  }


  /**
   * Read solrconfig.xml and register the appropriate handlers
   * 
   * This function should <b>only</b> be called from the SolrCore constructor.  It is
   * not intended as a public API.
   * 
   * While the normal runtime registration contract is that handlers MUST be initialized
   * before they are registered, this function does not do that exactly.
   *
   * This function registers all handlers first and then calls init() for each one.
   *
   * This is OK because this function is only called at startup and there is no chance that
   * a handler could be asked to handle a request before it is initialized.
   * 
   * The advantage to this approach is that handlers can know what path they are registered
   * to and what other handlers are available at startup.
   * 
   * Handlers will be registered and initialized in the order they appear in solrconfig.xml
   */

  void initHandlersFromConfig(SolrConfig config) {
    List<PluginInfo> implicits = core.getImplicitHandlers();
    // use link map so we iterate in the same order
    Map<String, PluginInfo> infoMap= new LinkedHashMap<>();
    //deduping implicit and explicit requesthandlers
    for (PluginInfo info : implicits) infoMap.put(info.name,info);
    for (PluginInfo info : config.getPluginInfos(SolrRequestHandler.class.getName())) infoMap.put(info.name, info);
    ArrayList<PluginInfo> infos = new ArrayList<>(infoMap.values());

    List<PluginInfo> modifiedInfos = new ArrayList<>();
    for (PluginInfo info : infos) {
      modifiedInfos.add(applyInitParams(config, info));
    }
    handlers.init(Collections.emptyMap(),core, modifiedInfos);
    handlers.alias(handlers.getDefault(), "");
    if (log.isDebugEnabled()) {
      log.debug("Registered paths: {}", StrUtils.join(new ArrayList<>(handlers.keySet()), ','));
    }
    if (handlers.get("") == null && !handlers.alias("/select", "")) {
      if (handlers.get("") == null && !handlers.alias("standard", "")) {
        log.warn("no default request handler is registered (either '/select' or 'standard')");
      }
    }
  }

  private PluginInfo applyInitParams(SolrConfig config, PluginInfo info) {
    List<InitParams> ags = new ArrayList<>();
    String p = info.attributes.get(InitParams.TYPE);
    if(p!=null) {
      for (String arg : StrUtils.splitSmart(p, ',')) {
        if(config.getInitParams().containsKey(arg)) ags.add(config.getInitParams().get(arg));
        else log.warn("INVALID paramSet {} in requestHandler {}", arg, info);
      }
    }
    for (InitParams args : config.getInitParams().values())
      if(args.matchPath(info.name)) ags.add(args);
    if(!ags.isEmpty()){
      info = info.copy();
      for (InitParams initParam : ags) {
        initParam.apply(info);
      }
    }
    return info;
  }

  public void close() {
    handlers.close();
  }
}







