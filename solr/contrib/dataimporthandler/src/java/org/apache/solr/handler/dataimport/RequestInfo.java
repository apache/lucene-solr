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
package org.apache.solr.handler.dataimport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;

public class RequestInfo {
  private final String command;
  private final boolean debug;  
  private final boolean syncMode;
  private final boolean commit; 
  private final boolean optimize;
  private final int start;
  private final long rows; 
  private final boolean clean; 
  private final List<String> entitiesToRun;
  private final Map<String,Object> rawParams;
  private final String configFile;
  private final String dataConfig;
  private final SolrQueryRequest request;
  
  //TODO:  find a different home for these two...
  private final ContentStream contentStream;  
  private final DebugInfo debugInfo;
  
  public RequestInfo(SolrQueryRequest request, Map<String,Object> requestParams, ContentStream stream) {
    this.request = request;
    this.contentStream = stream;    
    if (requestParams.containsKey("command")) { 
      command = (String) requestParams.get("command");
    } else {
      command = null;
    }    
    boolean debugMode = StrUtils.parseBool((String) requestParams.get("debug"), false);    
    if (debugMode) {
      debug = true;
      debugInfo = new DebugInfo(requestParams);
    } else {
      debug = false;
      debugInfo = null;
    }       
    if (requestParams.containsKey("clean")) {
      clean = StrUtils.parseBool( (String) requestParams.get("clean"), true);
    } else if (DataImporter.DELTA_IMPORT_CMD.equals(command) || DataImporter.IMPORT_CMD.equals(command)) {
      clean = false;
    } else  {
      clean = debug ? false : true;
    }    
    optimize = StrUtils.parseBool((String) requestParams.get("optimize"), false);
    if(optimize) {
      commit = true;
    } else {
      commit = StrUtils.parseBool( (String) requestParams.get("commit"), (debug ? false : true));
    }      
    if (requestParams.containsKey("rows")) {
      rows = Integer.parseInt((String) requestParams.get("rows"));
    } else {
      rows = debug ? 10 : Long.MAX_VALUE;
    }      
    
    if (requestParams.containsKey("start")) {
      start = Integer.parseInt((String) requestParams.get("start"));
    } else {
      start = 0;
    }
    syncMode = StrUtils.parseBool((String) requestParams.get("synchronous"), false);    
    
    Object o = requestParams.get("entity");     
    List<String> modifiableEntities = null;
    if(o != null) {
      if (o instanceof String) {
        modifiableEntities = new ArrayList<>();
        modifiableEntities.add((String) o);
      } else if (o instanceof List<?>) {
        @SuppressWarnings("unchecked")
        List<String> modifiableEntities1 = new ArrayList<>((List<String>) o);
        modifiableEntities = modifiableEntities1;
      } 
      entitiesToRun = Collections.unmodifiableList(modifiableEntities);
    } else {
      entitiesToRun = null;
    }
    String configFileParam = (String) requestParams.get("config");
    configFile = configFileParam;
    String dataConfigParam = (String) requestParams.get("dataConfig");
    if (dataConfigParam != null && dataConfigParam.trim().length() == 0) {
      // Empty data-config param is not valid, change it to null
      dataConfigParam = null;
    }
    dataConfig = dataConfigParam;
    this.rawParams = Collections.unmodifiableMap(new HashMap<>(requestParams));
  }

  public String getCommand() {
    return command;
  }

  public boolean isDebug() {
    return debug;
  }

  public boolean isSyncMode() {
    return syncMode;
  }

  public boolean isCommit() {
    return commit;
  }

  public boolean isOptimize() {
    return optimize;
  }

  public int getStart() {
    return start;
  }

  public long getRows() {
    return rows;
  }

  public boolean isClean() {
    return clean;
  }
  /**
   * Returns null if we are to run all entities, otherwise just run the entities named in the list.
   */
  public List<String> getEntitiesToRun() {
    return entitiesToRun;
  }

   public String getDataConfig() {
    return dataConfig;
  }

  public Map<String,Object> getRawParams() {
    return rawParams;
  }

  public ContentStream getContentStream() {
    return contentStream;
  }

  public DebugInfo getDebugInfo() {
    return debugInfo;
  }

  public String getConfigFile() {
    return configFile;
  }

  public SolrQueryRequest getRequest() {
    return request;
  }
}