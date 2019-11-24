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

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.dataimport.DataImporter.IMPORT_CMD;

/**
 * <p>
 * Solr Request Handler for data import from databases and REST data sources.
 * </p>
 * <p>
 * It is configured in solrconfig.xml
 * </p>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DataImportHandler extends RequestHandlerBase implements
        SolrCoreAware {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private DataImporter importer;

  private boolean debugEnabled = true;

  private String myName = "dataimport";

  private MetricsMap metrics;

  private static final String PARAM_WRITER_IMPL = "writerImpl";
  private static final String DEFAULT_WRITER_NAME = "SolrWriter";
  static final String ENABLE_DIH_DATA_CONFIG_PARAM = "enable.dih.dataConfigParam";

  final boolean dataConfigParam_enabled = Boolean.getBoolean(ENABLE_DIH_DATA_CONFIG_PARAM);

  public DataImporter getImporter() {
    return this.importer;
  }

  @Override
  public void init(NamedList args) {
    super.init(args);
    Map<String,String> macro = new HashMap<>();
    macro.put("expandMacros", "false");
    defaults = SolrParams.wrapDefaults(defaults, new MapSolrParams(macro));
  }

  @Override
  public void inform(SolrCore core) {
    try {
      String name = getPluginInfo().name;
      if (name.startsWith("/")) {
        myName = name.substring(1);
      }
      // some users may have '/' in the handler name. replace with '_'
      myName = myName.replaceAll("/", "_");
      debugEnabled = StrUtils.parseBool((String)initArgs.get(ENABLE_DEBUG), true);
      importer = new DataImporter(core, myName);         
    } catch (Exception e) {
      log.error( DataImporter.MSG.LOAD_EXP, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, DataImporter.MSG.LOAD_EXP, e);
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
          throws Exception {
    rsp.setHttpCaching(false);
    
    //TODO: figure out why just the first one is OK...
    ContentStream contentStream = null;
    Iterable<ContentStream> streams = req.getContentStreams();
    if(streams != null){
      for (ContentStream stream : streams) {
          contentStream = stream;
          break;
      }
    }
    SolrParams params = req.getParams();
    NamedList defaultParams = (NamedList) initArgs.get("defaults");
    RequestInfo requestParams = new RequestInfo(req, getParamsMap(params), contentStream);
    String command = requestParams.getCommand();
    
    if (DataImporter.SHOW_CONF_CMD.equals(command)) {    
      String dataConfigFile = params.get("config");
      String dataConfig = params.get("dataConfig"); // needn't check dataConfigParam_enabled; we don't execute it
      if(dataConfigFile != null) {
        dataConfig = SolrWriter.getResourceAsString(req.getCore().getResourceLoader().openResource(dataConfigFile));
      }
      if(dataConfig==null)  {
        rsp.add("status", DataImporter.MSG.NO_CONFIG_FOUND);
      } else {
        // Modify incoming request params to add wt=raw
        ModifiableSolrParams rawParams = new ModifiableSolrParams(req.getParams());
        rawParams.set(CommonParams.WT, "raw");
        req.setParams(rawParams);
        ContentStreamBase content = new ContentStreamBase.StringStream(dataConfig);
        rsp.add(RawResponseWriter.CONTENT, content);
      }
      return;
    }

    if (params.get("dataConfig") != null && dataConfigParam_enabled == false) {
      throw new SolrException(SolrException.ErrorCode.FORBIDDEN,
          "Use of the dataConfig param (DIH debug mode) requires the system property " +
              ENABLE_DIH_DATA_CONFIG_PARAM + " because it's a security risk.");
    }

    rsp.add("initArgs", initArgs);
    String message = "";

    if (command != null) {
      rsp.add("command", command);
    }
    // If importer is still null
    if (importer == null) {
      rsp.add("status", DataImporter.MSG.NO_INIT);
      return;
    }

    if (command != null && DataImporter.ABORT_CMD.equals(command)) {
      importer.runCmd(requestParams, null);
    } else if (importer.isBusy()) {
      message = DataImporter.MSG.CMD_RUNNING;
    } else if (command != null) {
      if (DataImporter.FULL_IMPORT_CMD.equals(command)
              || DataImporter.DELTA_IMPORT_CMD.equals(command) ||
              IMPORT_CMD.equals(command)) {
        importer.maybeReloadConfiguration(requestParams, defaultParams);
        UpdateRequestProcessorChain processorChain =
                req.getCore().getUpdateProcessorChain(params);
        UpdateRequestProcessor processor = processorChain.createProcessor(req, rsp);
        SolrResourceLoader loader = req.getCore().getResourceLoader();
        DIHWriter sw = getSolrWriter(processor, loader, requestParams, req);
        
        if (requestParams.isDebug()) {
          if (debugEnabled) {
            // Synchronous request for the debug mode
            importer.runCmd(requestParams, sw);
            rsp.add("mode", "debug");
            rsp.add("documents", requestParams.getDebugInfo().debugDocuments);
            if (requestParams.getDebugInfo().debugVerboseOutput != null) {
              rsp.add("verbose-output", requestParams.getDebugInfo().debugVerboseOutput);
            }
          } else {
            message = DataImporter.MSG.DEBUG_NOT_ENABLED;
          }
        } else {
          // Asynchronous request for normal mode
          if(requestParams.getContentStream() == null && !requestParams.isSyncMode()){
            importer.runAsync(requestParams, sw);
          } else {
            importer.runCmd(requestParams, sw);
          }
        }
      } else if (DataImporter.RELOAD_CONF_CMD.equals(command)) { 
        if(importer.maybeReloadConfiguration(requestParams, defaultParams)) {
          message = DataImporter.MSG.CONFIG_RELOADED;
        } else {
          message = DataImporter.MSG.CONFIG_NOT_RELOADED;
        }
      }
    }
    rsp.add("status", importer.isBusy() ? "busy" : "idle");
    rsp.add("importResponse", message);
    rsp.add("statusMessages", importer.getStatusMessages());
  }

  /** The value is converted to a String or {@code List<String>} if multi-valued. */
  private Map<String, Object> getParamsMap(SolrParams params) {
    Map<String, Object> result = new HashMap<>();
    for (Map.Entry<String, String[]> pair : params){
        String s = pair.getKey();
        String[] val = pair.getValue();
        if (val == null || val.length < 1)
          continue;
        if (val.length == 1)
          result.put(s, val[0]);
        else
          result.put(s, Arrays.asList(val));
    }
    return result;
  }

  private DIHWriter getSolrWriter(final UpdateRequestProcessor processor,
      final SolrResourceLoader loader, final RequestInfo requestParams,
      SolrQueryRequest req) {
    SolrParams reqParams = req.getParams();
    String writerClassStr = null;
    if (reqParams != null && reqParams.get(PARAM_WRITER_IMPL) != null) {
      writerClassStr = (String) reqParams.get(PARAM_WRITER_IMPL);
    }
    DIHWriter writer;
    if (writerClassStr != null
        && !writerClassStr.equals(DEFAULT_WRITER_NAME)
        && !writerClassStr.equals(DocBuilder.class.getPackage().getName() + "."
            + DEFAULT_WRITER_NAME)) {
      try {
        Class<DIHWriter> writerClass = DocBuilder.loadClass(writerClassStr, req.getCore());
        Constructor<DIHWriter> cnstr = writerClass.getConstructor(new Class[] {
            UpdateRequestProcessor.class, SolrQueryRequest.class});
        return cnstr.newInstance((Object) processor, (Object) req);
      } catch (Exception e) {
        throw new DataImportHandlerException(DataImportHandlerException.SEVERE,
            "Unable to load Writer implementation:" + writerClassStr, e);
      }
    } else {
      return new SolrWriter(processor, req) {
        @Override
        public boolean upload(SolrInputDocument document) {
          try {
            return super.upload(document);
          } catch (RuntimeException e) {
            log.error("Exception while adding: " + document, e);
            return false;
          }
        }
      };
    }
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    super.initializeMetrics(parentContext, scope);
    metrics = new MetricsMap((detailed, map) -> {
      if (importer != null) {
        DocBuilder.Statistics cumulative = importer.cumulativeStatistics;

        map.put("Status", importer.getStatus().toString());

        if (importer.docBuilder != null) {
          DocBuilder.Statistics running = importer.docBuilder.importStatistics;
          map.put("Documents Processed", running.docCount);
          map.put("Requests made to DataSource", running.queryCount);
          map.put("Rows Fetched", running.rowsCount);
          map.put("Documents Deleted", running.deletedDocCount);
          map.put("Documents Skipped", running.skipDocCount);
        }

        map.put(DataImporter.MSG.TOTAL_DOC_PROCESSED, cumulative.docCount);
        map.put(DataImporter.MSG.TOTAL_QUERIES_EXECUTED, cumulative.queryCount);
        map.put(DataImporter.MSG.TOTAL_ROWS_EXECUTED, cumulative.rowsCount);
        map.put(DataImporter.MSG.TOTAL_DOCS_DELETED, cumulative.deletedDocCount);
        map.put(DataImporter.MSG.TOTAL_DOCS_SKIPPED, cumulative.skipDocCount);
      }
    });
    solrMetricsContext.gauge(metrics, true, "importer", getCategory().toString(), scope);
  }

  // //////////////////////SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return DataImporter.MSG.JMX_DESC;
  }

  public static final String ENABLE_DEBUG = "enableDebug";
}
