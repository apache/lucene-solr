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

import static org.apache.solr.handler.dataimport.DataImporter.IMPORT_CMD;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.util.SystemIdResolver;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.util.*;
import java.io.StringReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

/**
 * <p>
 * Solr Request Handler for data import from databases and REST data sources.
 * </p>
 * <p>
 * It is configured in solrconfig.xml
 * </p>
 * <p/>
 * <p>
 * Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a>
 * for more details.
 * </p>
 * <p/>
 * <b>This API is experimental and subject to change</b>
 *
 * @since solr 1.3
 */
public class DataImportHandler extends RequestHandlerBase implements
        SolrCoreAware {

  private static final Logger LOG = LoggerFactory.getLogger(DataImportHandler.class);

  private DataImporter importer;

  private boolean debugEnabled = true;

  private String myName = "dataimport";

  @Override
  @SuppressWarnings("unchecked")
  public void init(NamedList args) {
    super.init(args);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void inform(SolrCore core) {
    try {
      //hack to get the name of this handler
      for (Map.Entry<String, SolrRequestHandler> e : core.getRequestHandlers().entrySet()) {
        SolrRequestHandler handler = e.getValue();
        //this will not work if startup=lazy is set
        if( this == handler) {
          String name= e.getKey();
          if(name.startsWith("/")){
            myName = name.substring(1);
          }
          // some users may have '/' in the handler name. replace with '_'
          myName = myName.replaceAll("/","_") ;
        }
      }
      debugEnabled = StrUtils.parseBool((String)initArgs.get(ENABLE_DEBUG), true);
      importer = new DataImporter(core, myName);         
    } catch (Throwable e) {
      LOG.error( DataImporter.MSG.LOAD_EXP, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, DataImporter.MSG.LOAD_EXP, e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
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
      String dataConfig = params.get("dataConfig");
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
                req.getCore().getUpdateProcessingChain(params.get(UpdateParams.UPDATE_CHAIN));
        UpdateRequestProcessor processor = processorChain.createProcessor(req, rsp);
        SolrResourceLoader loader = req.getCore().getResourceLoader();
        SolrWriter sw = getSolrWriter(processor, loader, requestParams, req);
        
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

    RequestHandlerUtils.addExperimentalFormatWarning(rsp);
  }

  private Map<String, Object> getParamsMap(SolrParams params) {
    Iterator<String> names = params.getParameterNamesIterator();
    Map<String, Object> result = new HashMap<String, Object>();
    while (names.hasNext()) {
      String s = names.next();
      String[] val = params.getParams(s);
      if (val == null || val.length < 1)
        continue;
      if (val.length == 1)
        result.put(s, val[0]);
      else
        result.put(s, Arrays.asList(val));
    }
    return result;
  }

  private SolrWriter getSolrWriter(final UpdateRequestProcessor processor,
                                   final SolrResourceLoader loader, final RequestInfo requestParams, SolrQueryRequest req) {

    return new SolrWriter(processor, req) {

      @Override
      public boolean upload(SolrInputDocument document) {
        try {
          return super.upload(document);
        } catch (RuntimeException e) {
          LOG.error( "Exception while adding: " + document, e);
          return false;
        }
      }
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public NamedList getStatistics() {
    if (importer == null)
      return super.getStatistics();

    DocBuilder.Statistics cumulative = importer.cumulativeStatistics;
    SimpleOrderedMap result = new SimpleOrderedMap();

    result.add("Status", importer.getStatus().toString());

    if (importer.docBuilder != null) {
      DocBuilder.Statistics running = importer.docBuilder.importStatistics;
      result.add("Documents Processed", running.docCount);
      result.add("Requests made to DataSource", running.queryCount);
      result.add("Rows Fetched", running.rowsCount);
      result.add("Documents Deleted", running.deletedDocCount);
      result.add("Documents Skipped", running.skipDocCount);
    }

    result.add(DataImporter.MSG.TOTAL_DOC_PROCESSED, cumulative.docCount);
    result.add(DataImporter.MSG.TOTAL_QUERIES_EXECUTED, cumulative.queryCount);
    result.add(DataImporter.MSG.TOTAL_ROWS_EXECUTED, cumulative.rowsCount);
    result.add(DataImporter.MSG.TOTAL_DOCS_DELETED, cumulative.deletedDocCount);
    result.add(DataImporter.MSG.TOTAL_DOCS_SKIPPED, cumulative.skipDocCount);

    NamedList requestStatistics = super.getStatistics();
    if (requestStatistics != null) {
      for (int i = 0; i < requestStatistics.size(); i++) {
        result.add(requestStatistics.getName(i), requestStatistics.getVal(i));
      }
    }

    return result;
  }

  // //////////////////////SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return DataImporter.MSG.JMX_DESC;
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

  public static final String ENABLE_DEBUG = "enableDebug";
}
