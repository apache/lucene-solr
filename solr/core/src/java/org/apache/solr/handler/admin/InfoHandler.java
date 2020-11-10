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

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.api.ApiBag.ReqHandlerToApi;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.api.Api;

import static java.util.Collections.singletonList;
import static org.apache.solr.common.util.Utils.getSpec;
import static org.apache.solr.common.params.CommonParams.PATH;

public class InfoHandler extends RequestHandlerBase  {

  protected final CoreContainer coreContainer;

  /**
   * Overloaded ctor to inject CoreContainer into the handler.
   *
   * @param coreContainer Core Container of the solr webapp installed.
   */
  public InfoHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    handlers.put("threads", new ThreadDumpHandler());
    handlers.put("properties", new PropertiesRequestHandler());
    handlers.put("logging", new LoggingHandler(coreContainer));
    handlers.put("system", new SystemInfoHandler(coreContainer));
    if (coreContainer.getHealthCheckHandler() == null) {
      throw new IllegalStateException("HealthCheckHandler needs to be initialized before creating InfoHandler");
    }
    handlers.put("health", coreContainer.getHealthCheckHandler());

  }


  @Override
  final public void init(@SuppressWarnings({"rawtypes"})NamedList args) {

  }

  /**
   * The instance of CoreContainer this handler handles. This should be the CoreContainer instance that created this
   * handler.
   *
   * @return a CoreContainer instance
   */
  public CoreContainer getCoreContainer() {
    return this.coreContainer;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Make sure the cores is enabled
    CoreContainer cores = getCoreContainer();
    if (cores == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Core container instance missing");
    }

    String path = (String) req.getContext().get(PATH);
    handle(req, rsp, path);
  }

  private void handle(SolrQueryRequest req, SolrQueryResponse rsp, String path) {
    int i = path.lastIndexOf('/');
    String name = path.substring(i + 1, path.length());
    RequestHandlerBase handler = handlers.get(name.toLowerCase(Locale.ROOT));
    if(handler == null) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No handler by name "+name + " available names are "+ handlers.keySet());
    }
    handler.handleRequest(req, rsp);
    rsp.setHttpCaching(false);
  }


  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "System Information";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  protected PropertiesRequestHandler getPropertiesHandler() {
    return (PropertiesRequestHandler) handlers.get("properties");

  }

  protected ThreadDumpHandler getThreadDumpHandler() {
    return (ThreadDumpHandler) handlers.get("threads");
  }

  protected LoggingHandler getLoggingHandler() {
    return (LoggingHandler) handlers.get("logging");
  }

  protected SystemInfoHandler getSystemInfoHandler() {
    return (SystemInfoHandler) handlers.get("system");
  }

  protected void setPropertiesHandler(PropertiesRequestHandler propertiesHandler) {
    handlers.put("properties", propertiesHandler);
  }

  protected void setThreadDumpHandler(ThreadDumpHandler threadDumpHandler) {
    handlers.put("threads", threadDumpHandler);
  }

  protected void setLoggingHandler(LoggingHandler loggingHandler) {
    handlers.put("logging", loggingHandler);
  }

  protected void setSystemInfoHandler(SystemInfoHandler systemInfoHandler) {
    handlers.put("system", systemInfoHandler);
  }

  @Override
  public SolrRequestHandler getSubHandler(String subPath) {
    return this;
  }

  private Map<String, RequestHandlerBase> handlers = new ConcurrentHashMap<>();

  @Override
  public Collection<Api> getApis() {
    return singletonList(new ReqHandlerToApi(this, getSpec("node.Info")));
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }
}
