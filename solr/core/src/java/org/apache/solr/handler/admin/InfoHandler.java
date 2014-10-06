package org.apache.solr.handler.admin;

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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InfoHandler extends RequestHandlerBase {
  protected static Logger log = LoggerFactory.getLogger(InfoHandler.class);
  protected final CoreContainer coreContainer;
  
  private ThreadDumpHandler threadDumpHandler = new ThreadDumpHandler();
  private PropertiesRequestHandler propertiesHandler = new PropertiesRequestHandler();
  private LoggingHandler loggingHandler;
  private SystemInfoHandler systemInfoHandler;

  /**
   * Overloaded ctor to inject CoreContainer into the handler.
   *
   * @param coreContainer Core Container of the solr webapp installed.
   */
  public InfoHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
    systemInfoHandler = new SystemInfoHandler(coreContainer);
    loggingHandler = new LoggingHandler(coreContainer);
    
  }


  @Override
  final public void init(NamedList args) {

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

    String path = (String) req.getContext().get("path");
    int i = path.lastIndexOf('/');
    String name = path.substring(i + 1, path.length());
    
    if (name.equalsIgnoreCase("properties")) {
      propertiesHandler.handleRequest(req, rsp);
    } else if (name.equalsIgnoreCase("threads")) {
      threadDumpHandler.handleRequest(req, rsp);
    } else if (name.equalsIgnoreCase("logging")) {
      loggingHandler.handleRequest(req, rsp);
    }  else if (name.equalsIgnoreCase("system")) {
      systemInfoHandler.handleRequest(req, rsp);
    } else {
      if (name.equalsIgnoreCase("info")) name = "";
      throw new SolrException(ErrorCode.NOT_FOUND, "Info Handler not found: " + name);
    }
    
    rsp.setHttpCaching(false);
  }
  
  



  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "System Information";
  }

  protected PropertiesRequestHandler getPropertiesHandler() {
    return propertiesHandler;
  }

  protected ThreadDumpHandler getThreadDumpHandler() {
    return threadDumpHandler;
  }

  protected LoggingHandler getLoggingHandler() {
    return loggingHandler;
  }

  protected SystemInfoHandler getSystemInfoHandler() {
    return systemInfoHandler;
  }

  protected void setPropertiesHandler(PropertiesRequestHandler propertiesHandler) {
    this.propertiesHandler = propertiesHandler;
  }

  protected void setThreadDumpHandler(ThreadDumpHandler threadDumpHandler) {
    this.threadDumpHandler = threadDumpHandler;
  }

  protected void setLoggingHandler(LoggingHandler loggingHandler) {
    this.loggingHandler = loggingHandler;
  }

  protected void setSystemInfoHandler(SystemInfoHandler systemInfoHandler) {
    this.systemInfoHandler = systemInfoHandler;
  }

  @Override
  public SolrRequestHandler getSubHandler(String path) {
    return this;
  }
}
