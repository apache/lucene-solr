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
package org.apache.solr.rest;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.servlet.ResponseUtils;
import org.slf4j.Logger;
import static org.apache.solr.common.params.CommonParams.JSON;

/**
 * Base class for delegating REST-oriented requests to ManagedResources. ManagedResources are heavy-weight and
 * should not be created for every request, so this class serves as a gateway between a REST call and the resource.
 */
public abstract class BaseSolrResource {
  protected static final String SHOW_DEFAULTS = "showDefaults";
  public static final String UPDATE_TIMEOUT_SECS = "updateTimeoutSecs";

  private SolrCore solrCore;
  private IndexSchema schema;
  private SolrQueryRequest solrRequest;
  private SolrQueryResponse solrResponse;
  private QueryResponseWriter responseWriter;
  private String contentType;
  private int updateTimeoutSecs = -1;
  private int statusCode = -1;

  public SolrCore getSolrCore() { return solrCore; }
  public IndexSchema getSchema() { return schema; }
  public SolrQueryRequest getSolrRequest() { return solrRequest; }
  public SolrQueryResponse getSolrResponse() { return solrResponse; }
  public String getContentType() { return contentType; }
  protected int getUpdateTimeoutSecs() { return updateTimeoutSecs; }

  protected BaseSolrResource() {
    super();
  }

  /**
   * Pulls the SolrQueryRequest constructed in SolrDispatchFilter
   * from the SolrRequestInfo thread local, then gets the SolrCore
   * and IndexSchema and sets up the response.
   * writer.
   */
  public void doInit(SolrQueryRequest solrRequest, SolrQueryResponse solrResponse) {
    try {
      this.solrRequest = solrRequest;
      this.solrResponse = solrResponse;
      solrCore = solrRequest.getCore();
      schema = solrRequest.getSchema();
      String responseWriterName = solrRequest.getParams().get(CommonParams.WT, JSON);
      responseWriter = solrCore.getQueryResponseWriter(responseWriterName);
      contentType = responseWriter.getContentType(solrRequest, solrResponse);
      final String path = solrRequest.getPath();
      if ( ! RestManager.SCHEMA_BASE_PATH.equals(path)) {
        // don't set webapp property on the request when context and core/collection are excluded
        final int cutoffPoint = path.indexOf("/", 1);
        final String firstPathElement = -1 == cutoffPoint ? path : path.substring(0, cutoffPoint);
        solrRequest.getContext().put("webapp", firstPathElement); // Context path
      }

      // client application can set a timeout for update requests
      String updateTimeoutSecsParam = solrRequest.getParams().get(UPDATE_TIMEOUT_SECS);
      if (updateTimeoutSecsParam != null)
        updateTimeoutSecs = Integer.parseInt(updateTimeoutSecsParam);
    } catch (Throwable t) {
      if (t instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) t;
      }
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, t);
    }
  }

  /**
   * Deal with an exception on the SolrResponse, fill in response header info,
   * and log the accumulated messages on the SolrResponse.
   */
  protected void handlePostExecution(Logger log) {
    
    handleException(log);

    addDeprecatedWarning();

    if (log.isInfoEnabled() && solrResponse.getToLog().size() > 0) {
      log.info(solrResponse.getToLogAsString(solrCore.getLogId()));
    }
  }

  protected void addDeprecatedWarning(){
    solrResponse.add("warn","This API is deprecated");
  }

  /**
   * If there is an exception on the SolrResponse:
   * <ul>
   *   <li>error info is added to the SolrResponse;</li>
   *   <li>the response status code is set to the error code from the exception; and</li>
   *   <li>the exception message is added to the list of things to be logged.</li>
   * </ul>
   */
  protected void handleException(Logger log) {
    Exception exception = getSolrResponse().getException();
    if (null != exception) {
      @SuppressWarnings({"rawtypes"})
      NamedList info = new SimpleOrderedMap();
      this.statusCode = ResponseUtils.getErrorInfo(exception, info, log);
      getSolrResponse().add("error", info);
      String message = (String)info.get("msg");
      if (null != message && ! message.trim().isEmpty()) {
        getSolrResponse().getToLog().add("msg", "{" + message.trim() + "}");
      }
    }
  }

  /** Decode URL-encoded strings as UTF-8, and avoid converting "+" to space */
  protected static String urlDecode(String str) throws UnsupportedEncodingException {
    return URLDecoder.decode(str.replace("+", "%2B"), "UTF-8");
  }
}
