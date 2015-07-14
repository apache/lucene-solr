package org.apache.solr.core;

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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.PingRequestHandler;
import org.apache.solr.handler.RealTimeGetHandler;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.handler.SchemaHandler;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.admin.LoggingHandler;
import org.apache.solr.handler.admin.LukeRequestHandler;
import org.apache.solr.handler.admin.PluginInfoHandler;
import org.apache.solr.handler.admin.PropertiesRequestHandler;
import org.apache.solr.handler.admin.SegmentsInfoRequestHandler;
import org.apache.solr.handler.admin.ShowFileRequestHandler;
import org.apache.solr.handler.admin.SolrInfoMBeanHandler;
import org.apache.solr.handler.admin.SystemInfoHandler;
import org.apache.solr.handler.admin.ThreadDumpHandler;
import org.apache.solr.request.SolrRequestHandler;

import static java.util.Collections.singletonMap;
import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.common.params.CommonParams.JSON;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.WT;
import static org.apache.solr.core.PluginInfo.DEFAULTS;
import static org.apache.solr.core.PluginInfo.INVARIANTS;

public class ImplicitPlugins {

  public static List<PluginInfo> getHandlers(SolrCore solrCore){
    List<PluginInfo> implicits = new ArrayList<>();

    //update handle implicits
    implicits.add(getReqHandlerInfo("/update", UpdateRequestHandler.class, null));
    implicits.add(getReqHandlerInfo(UpdateRequestHandler.JSON_PATH, UpdateRequestHandler.class, singletonMap("update.contentType", "application/json")));
    implicits.add(getReqHandlerInfo(UpdateRequestHandler.CSV_PATH, UpdateRequestHandler.class, singletonMap("update.contentType", "application/csv")));
    implicits.add(getReqHandlerInfo(UpdateRequestHandler.DOC_PATH, UpdateRequestHandler.class, makeMap("update.contentType", "application/json", "json.command", "false")));

    //solrconfighandler
    PluginInfo config = getReqHandlerInfo("/config", SolrConfigHandler.class, null);
    if (solrCore.getConfigSetProperties() != null) {
      config.initArgs.addAll(solrCore.getConfigSetProperties());
    }
    implicits.add(config);
    //schemahandler
    PluginInfo schema = getReqHandlerInfo("/schema", SchemaHandler.class, null);
    if (solrCore.getConfigSetProperties() != null) {
      schema.initArgs.addAll(solrCore.getConfigSetProperties());
    }
    implicits.add(schema);
    //register replicationhandler always for SolrCloud
    implicits.add(getReqHandlerInfo("/replication", ReplicationHandler.class,null));

    implicits.add(getReqHandlerInfo("/get", RealTimeGetHandler.class,
        makeMap(
            "omitHeader", "true",
            WT, JSON,
            "indent", "true")));
    //register adminHandlers
    implicits.add(getReqHandlerInfo("/admin/luke", LukeRequestHandler.class, null));
    implicits.add(getReqHandlerInfo("/admin/system", SystemInfoHandler.class, null));
    implicits.add(getReqHandlerInfo("/admin/mbeans", SolrInfoMBeanHandler.class, null));
    implicits.add(getReqHandlerInfo("/admin/plugins", PluginInfoHandler.class, null));
    implicits.add(getReqHandlerInfo("/admin/threads", ThreadDumpHandler.class, null));
    implicits.add(getReqHandlerInfo("/admin/properties", PropertiesRequestHandler.class, null));
    implicits.add(getReqHandlerInfo("/admin/logging", LoggingHandler.class, null));
    implicits.add(getReqHandlerInfo("/admin/file", ShowFileRequestHandler.class, null));
    PluginInfo ping = getReqHandlerInfo("/admin/ping", PingRequestHandler.class, null);
    ping.initArgs.add(INVARIANTS, new NamedList<>(makeMap("echoParams", "all", "q", "{!lucene}*:*")));
    implicits.add(ping);
    implicits.add(getReqHandlerInfo("/admin/segments", SegmentsInfoRequestHandler.class, null));
    return implicits;
  }

  public static PluginInfo getReqHandlerInfo(String name, Class clz, Map defaults){
    if(defaults == null) defaults= Collections.emptyMap();
    Map m = makeMap(NAME, name, "class", clz.getName());
    return new PluginInfo(SolrRequestHandler.TYPE, m, new NamedList<>(singletonMap(DEFAULTS, new NamedList(defaults))),null);
  }
}
