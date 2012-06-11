/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.admin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.LoggerInfo;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.LoggerFactory;


/**
 * A request handler to show which loggers are registered and allows you to set them
 *
 * @since 4.0
 */
public class LoggingHandler extends RequestHandlerBase implements SolrCoreAware {
  static final org.slf4j.Logger log = LoggerFactory.getLogger(LoggingHandler.class);
  
  LogWatcher watcher = null;
  
  @Override
  public void inform(SolrCore core) {
    watcher = core.getCoreDescriptor().getCoreContainer().getLogging();
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Don't do anything if the framework is unknown
    if(watcher==null) {
      rsp.add("error", "Logging Not Initalized");
      return;
    }
    rsp.add("watcher", watcher.getName());
    
    SolrParams params = req.getParams();
    if(params.get("threshold")!=null) {
      watcher.setThreshold(params.get("threshold"));
    }
    
    // Write something at each level
    if(params.get("test")!=null) {
      log.trace("trace message");
      log.debug( "debug message");
      log.info("info (with exception)", new RuntimeException("test") );
      log.warn("warn (with exception)", new RuntimeException("test") );
      log.error("error (with exception)", new RuntimeException("test") );
    }
    
    String[] set = params.getParams("set");
    if (set != null) {
      for (String pair : set) {
        String[] split = pair.split(":");
        if (split.length != 2) {
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              "Invalid format, expected level:value, got " + pair);
        }
        String category = split[0];
        String level = split[1];

        watcher.setLogLevel(category, level);
      }
    }
    
    String since = req.getParams().get("since");
    if(since != null) {
      long time = -1;
      try {
        time = Long.parseLong(since);
      }
      catch(Exception ex) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "invalid timestamp: "+since);
      }
      AtomicBoolean found = new AtomicBoolean(false);
      SolrDocumentList docs = watcher.getHistory(time, found);
      if(docs==null) {
        rsp.add("error", "History not enabled");
        return;
      }
      else {
        SimpleOrderedMap<Object> info = new SimpleOrderedMap<Object>();
        if(time>0) {
          info.add("since", time);
          info.add("found", found);
        }
        else {
          info.add("levels", watcher.getAllLevels()); // show for the first request
        }
        info.add("last", watcher.getLastEvent());
        info.add("buffer", watcher.getHistorySize());
        info.add("threshold", watcher.getThreshold());
        
        rsp.add("info", info);
        rsp.add("history", docs);
      }
    }
    else {
      rsp.add("levels", watcher.getAllLevels());
  
      List<LoggerInfo> loggers = new ArrayList<LoggerInfo>(watcher.getAllLoggers());
      Collections.sort(loggers);
  
      List<SimpleOrderedMap<?>> info = new ArrayList<SimpleOrderedMap<?>>();
      for(LoggerInfo wrap:loggers) {
        info.add(wrap.getInfo());
      }
      rsp.add("loggers", info);
    }
    rsp.setHttpCaching(false);
  }

  // ////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Logging Handler";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}