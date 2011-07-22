/**
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
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SystemIdResolver;
import org.apache.solr.core.SolrConfig;
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

  private Map<String, Properties> dataSources = new HashMap<String, Properties>();

  private List<SolrInputDocument> debugDocuments;

  private boolean debugEnabled = true;

  private String myName = "dataimport";

  private Map<String , Object> coreScopeSession = new HashMap<String, Object>();

  @Override
  @SuppressWarnings("unchecked")
  public void init(NamedList args) {
    super.init(args);
  }

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
      NamedList defaults = (NamedList) initArgs.get("defaults");
      if (defaults != null) {
        String configLoc = (String) defaults.get("config");
        if (configLoc != null && configLoc.length() != 0) {
          processConfiguration(defaults);
          final InputSource is = new InputSource(core.getResourceLoader().openConfig(configLoc));
          is.setSystemId(SystemIdResolver.createSystemIdFromResourceName(configLoc));
          importer = new DataImporter(is, core,
                  dataSources, coreScopeSession, myName);
        }
      }
    } catch (Throwable e) {
      SolrConfig.severeErrors.add(e);
      LOG.error( DataImporter.MSG.LOAD_EXP, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              DataImporter.MSG.INVALID_CONFIG, e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
          throws Exception {
    rsp.setHttpCaching(false);
    SolrParams params = req.getParams();
    DataImporter.RequestParams requestParams = new DataImporter.RequestParams(getParamsMap(params));
    String command = requestParams.command;
    Iterable<ContentStream> streams = req.getContentStreams();
    if(streams != null){
      for (ContentStream stream : streams) {
          requestParams.contentStream = stream;
          break;
      }
    }
    if (DataImporter.SHOW_CONF_CMD.equals(command)) {
      // Modify incoming request params to add wt=raw
      ModifiableSolrParams rawParams = new ModifiableSolrParams(req.getParams());
      rawParams.set(CommonParams.WT, "raw");
      req.setParams(rawParams);
      String dataConfigFile = defaults.get("config");
      ContentStreamBase content = new ContentStreamBase.StringStream(SolrWriter
              .getResourceAsString(req.getCore().getResourceLoader().openResource(
              dataConfigFile)));
      rsp.add(RawResponseWriter.CONTENT, content);
      return;
    }

    rsp.add("initArgs", initArgs);
    String message = "";

    if (command != null)
      rsp.add("command", command);

    if (requestParams.debug && (importer == null || !importer.isBusy())) {
      // Reload the data-config.xml
      importer = null;
      if (requestParams.dataConfig != null) {
        try {
          processConfiguration((NamedList) initArgs.get("defaults"));
          importer = new DataImporter(new InputSource(new StringReader(requestParams.dataConfig)), req.getCore()
                  , dataSources, coreScopeSession, myName);
        } catch (RuntimeException e) {
          rsp.add("exception", DebugLogger.getStacktraceString(e));
          importer = null;
          return;
        }
      } else {
        inform(req.getCore());
      }
      message = DataImporter.MSG.CONFIG_RELOADED;
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

        UpdateRequestProcessorChain processorChain =
                req.getCore().getUpdateProcessingChain(params.get(UpdateParams.UPDATE_CHAIN));
        UpdateRequestProcessor processor = processorChain.createProcessor(req, rsp);
        SolrResourceLoader loader = req.getCore().getResourceLoader();
        SolrWriter sw = getSolrWriter(processor, loader, requestParams, req);

        if (requestParams.debug) {
          if (debugEnabled) {
            // Synchronous request for the debug mode
            importer.runCmd(requestParams, sw);
            rsp.add("mode", "debug");
            rsp.add("documents", debugDocuments);
            if (sw.debugLogger != null)
              rsp.add("verbose-output", sw.debugLogger.output);
            debugDocuments = null;
          } else {
            message = DataImporter.MSG.DEBUG_NOT_ENABLED;
          }
        } else {
          // Asynchronous request for normal mode
          if(requestParams.contentStream == null && !requestParams.syncMode){
            importer.runAsync(requestParams, sw);
          } else {
              importer.runCmd(requestParams, sw);
          }
        }
      } else if (DataImporter.RELOAD_CONF_CMD.equals(command)) {
        importer = null;
        inform(req.getCore());
        message = DataImporter.MSG.CONFIG_RELOADED;
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

  @SuppressWarnings("unchecked")
  private void processConfiguration(NamedList defaults) {
    if (defaults == null) {
      LOG.info("No configuration specified in solrconfig.xml for DataImportHandler");
      return;
    }

    LOG.info("Processing configuration from solrconfig.xml: " + defaults);

    dataSources = new HashMap<String, Properties>();

    int position = 0;

    while (position < defaults.size()) {
      if (defaults.getName(position) == null)
        break;

      String name = defaults.getName(position);
      if (name.equals("datasource")) {
        NamedList dsConfig = (NamedList) defaults.getVal(position);
        Properties props = new Properties();
        for (int i = 0; i < dsConfig.size(); i++)
          props.put(dsConfig.getName(i), dsConfig.getVal(i));
        LOG.info("Adding properties to datasource: " + props);
        dataSources.put((String) dsConfig.get("name"), props);
      }
      position++;
    }
  }

  private SolrWriter getSolrWriter(final UpdateRequestProcessor processor,
                                   final SolrResourceLoader loader, final DataImporter.RequestParams requestParams, SolrQueryRequest req) {

    return new SolrWriter(processor, req) {

      @Override
      public boolean upload(SolrInputDocument document) {
        try {
          if (requestParams.debug) {
            if (debugDocuments == null)
              debugDocuments = new ArrayList<SolrInputDocument>();
            debugDocuments.add(document);
          }
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
    NamedList result = new NamedList();

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
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getVersion() {
    return "1.0";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

  public static final String ENABLE_DEBUG = "enableDebug";
}
