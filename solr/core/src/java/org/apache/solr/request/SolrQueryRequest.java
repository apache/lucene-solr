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
package org.apache.solr.request;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;
import org.apache.solr.servlet.HttpSolrCall;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.util.RTimerTree;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <p>Container for a request to execute a query.</p>
 * <p><code>SolrQueryRequest</code> is not thread safe.</p>
 *
 *
 */
public interface SolrQueryRequest extends AutoCloseable {

  /** returns the current request parameters */
  SolrParams getParams();

  /** Change the parameters for this request.  This does not affect
   *  the original parameters returned by getOriginalParams()
   */
  void setParams(SolrParams params);

  /** A Collection of ContentStreams passed to the request
   */
  Iterable<ContentStream> getContentStreams();

  /** Returns the original request parameters.  As this
   * does not normally include configured defaults
   * it's more suitable for logging.
   */
  SolrParams getOriginalParams();

  /**
   * Generic information associated with this request that may be both read and updated.
   */
  Map<Object,Object> getContext();

  /**
   * This method should be called when all uses of this request are
   * finished, so that resources can be freed.
   */
  void close();

  /** The start time of this request in milliseconds.
   * Use this only if you need the absolute system time at the start of the request,
   * getRequestTimer() provides a more accurate mechanism for timing purposes.
   */
  long getStartTime();

  /** The timer for this request, created when the request started being processed */
  RTimerTree getRequestTimer();

  /** The index searcher associated with this request */
  SolrIndexSearcher getSearcher();

  /** The solr core (coordinator, etc) associated with this request */
  SolrCore getCore();

  /** The schema snapshot from core.getLatestSchema() at request creation. */
  public IndexSchema getSchema();

  /** Replaces the current schema snapshot with the latest from the core. */
  public void updateSchemaToLatest();

  /**
   * Returns a string representing all the important parameters.
   * Suitable for logging.
   */
  public String getParamString();

  /** Returns any associated JSON (or null if none) in deserialized generic form.
   * Java classes used to represent the JSON are as follows: Map, List, String, Long, Double, Boolean
   */
  Map<String,Object> getJSON();

  void setJSON(Map<String, Object> json);

  Principal getUserPrincipal();

  default String getPath() {
    return (String) getContext().get("path");
  }

  /** Only for V2 API.
   * Returns a map of path segments and their values . For example ,
   * if the path is configured as /path/{segment1}/{segment2} and a reguest is made
   * as /path/x/y the returned map would contain {segment1:x ,segment2:y}
   */
  default Map<String, String> getPathTemplateValues() {
    return Collections.emptyMap();
  }

  /** Only for v2 API
   * if the  request contains a command payload, it's parsed and returned as a list of
   * CommandOperation objects
   * @param validateInput , If true it is validated against the json schema spec
   */
  default List<CommandOperation> getCommands(boolean validateInput) {
    return Collections.emptyList();
  }

  default String getHttpMethod() {
    return (String) getContext().get("httpMethod");
  }

  default HttpSolrCall getHttpSolrCall() {
    return null;
  }
}




