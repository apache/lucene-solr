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


import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.solr.client.solrj.io.ModelCache;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.handler.export.ExportWriter;
import org.apache.solr.handler.export.ExportWriterStream;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.JSON;

public class ExportHandler extends SearchHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private ModelCache modelCache = null;
  @SuppressWarnings({"rawtypes"})
  private ConcurrentMap objectCache = new ConcurrentHashMap();
  private SolrDefaultStreamFactory streamFactory = new ExportHandlerStreamFactory();
  private String coreName;
  private SolrClientCache solrClientCache;
  private StreamContext initialStreamContext;
  private String writerMetricsPath;

  public static class ExportHandlerStreamFactory extends SolrDefaultStreamFactory {
    static final String[] forbiddenStreams = new String[] {
        // source streams
        "search", "facet", "facet2D", "update", "delete", "jdbc", "topic",
        "commit", "random", "knnSearch",
        // execution streams
        "parallel", "executor", "daemon"
        // other streams?
    };

    public ExportHandlerStreamFactory() {
      super();
      for (String function : forbiddenStreams) {
        this.withoutFunctionName(function);
      }
      this.withFunctionName("input", ExportWriterStream.class);
    }
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    super.initializeMetrics(parentContext, scope);
    this.writerMetricsPath = SolrMetricManager.mkName("writer", getCategory().toString(), scope);
  }

  @Override
  public void inform(SolrCore core) {
    super.inform(core);
    String defaultCollection;
    String defaultZkhost;
    CoreContainer coreContainer = core.getCoreContainer();
    this.solrClientCache = coreContainer.getSolrClientCache();
    this.coreName = core.getName();

    if (coreContainer.isZooKeeperAware()) {
      defaultCollection = core.getCoreDescriptor().getCollectionName();
      defaultZkhost = core.getCoreContainer().getZkController().getZkServerAddress();
      streamFactory.withCollectionZkHost(defaultCollection, defaultZkhost);
      streamFactory.withDefaultZkHost(defaultZkhost);
      modelCache = new ModelCache(250,
          defaultZkhost,
          solrClientCache);
    }
    streamFactory.withSolrResourceLoader(core.getResourceLoader());
    StreamHandler.addExpressiblePlugins(streamFactory, core);
    initialStreamContext = new StreamContext();
    initialStreamContext.setStreamFactory(streamFactory);
    initialStreamContext.setSolrClientCache(solrClientCache);
    initialStreamContext.setModelCache(modelCache);
    initialStreamContext.setObjectCache(objectCache);
    initialStreamContext.put("core", this.coreName);
    initialStreamContext.put("solr-core", core);
    initialStreamContext.put("exportHandler", this);
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    try {
      super.handleRequestBody(req, rsp);
    } catch (Exception e) {
      rsp.setException(e);
    }
    String wt = req.getParams().get(CommonParams.WT, JSON);
    if("xsort".equals(wt)) wt = JSON;
    Map<String, String> map = new HashMap<>(1);
    map.put(CommonParams.WT, ReplicationHandler.FILE_STREAM);
    req.setParams(SolrParams.wrapDefaults(new MapSolrParams(map),req.getParams()));
    rsp.add(ReplicationHandler.FILE_STREAM, new ExportWriter(req, rsp, wt, initialStreamContext, solrMetricsContext,
        writerMetricsPath, this));
  }
}
