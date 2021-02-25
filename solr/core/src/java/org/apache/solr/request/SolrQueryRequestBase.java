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

import java.io.Closeable;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.api.ApiBag;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.JsonSchemaValidator;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RTimerTree;
import org.apache.solr.util.RefCounted;


/**
 * Base implementation of <code>SolrQueryRequest</code> that provides some
 * convenience methods for accessing parameters, and manages an IndexSearcher
 * reference.
 *
 * <p>
 * The <code>close()</code> method must be called on any instance of this
 * class once it is no longer in use.
 * </p>
 *
 *
 *
 */
public abstract class SolrQueryRequestBase implements SolrQueryRequest, Closeable {
  protected final SolrCore core;
  protected final SolrParams origParams;
  protected volatile IndexSchema schema;
  protected SolrParams params;
  protected Map<Object,Object> context;
  protected Iterable<ContentStream> streams;
  protected Map<String,Object> json;

  private final RTimerTree requestTimer;
  protected final long startTime;

  @SuppressForbidden(reason = "Need currentTimeMillis to get start time for request (to be used for stats/debugging)")
  public SolrQueryRequestBase(SolrCore core, SolrParams params, RTimerTree requestTimer) {
    this.core = core;
    this.schema = null == core ? null : core.getLatestSchema();
    this.params = this.origParams = params;
    this.requestTimer = requestTimer;
    this.startTime = System.currentTimeMillis();
  }

  public SolrQueryRequestBase(SolrCore core, SolrParams params) {
    this(core, params, new RTimerTree());
  }

  @Override
  public Map<Object,Object> getContext() {
    // SolrQueryRequest as a whole isn't thread safe, and this isn't either.
    if (context==null) context = new HashMap<>();
    return context;
  }

  @Override
  public SolrParams getParams() {
    return params;
  }

  @Override
  public SolrParams getOriginalParams() {
    return origParams;
  }

  @Override
  public void setParams(SolrParams params) {
    this.params = params;
  }


  // Get the start time of this request in milliseconds
  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public RTimerTree getRequestTimer () {
    return requestTimer;
  }

  // The index searcher associated with this request
  protected RefCounted<SolrIndexSearcher> searcherHolder;
  @Override
  public SolrIndexSearcher getSearcher() {
    if(core == null) return null;//a request for a core admin will not have a core
    // should this reach out and get a searcher from the core singleton, or
    // should the core populate one in a factory method to create requests?
    // or there could be a setSearcher() method that Solr calls

    if (searcherHolder==null) {
      searcherHolder = core.getSearcher();
    }

    return searcherHolder.get();
  }

  // The solr core (coordinator, etc) associated with this request
  @Override
  public SolrCore getCore() {
    return core;
  }

  // The index schema associated with this request
  @Override
  public IndexSchema getSchema() {
    //a request for a core admin will no have a core
    return schema;
  }

  @Override
  public void updateSchemaToLatest() {
    schema = core.getLatestSchema();
  }

  /**
   * Frees resources associated with this request, this method <b>must</b>
   * be called when the object is no longer in use.
   */
  @Override
  public void close() {
    if (searcherHolder!=null) {
      searcherHolder.decref();
      searcherHolder = null;
    }
  }

  /** A Collection of ContentStreams passed to the request
   */
  @Override
  public Iterable<ContentStream> getContentStreams() {
    return streams; 
  }
  
  public void setContentStreams( Iterable<ContentStream> s ) {
    streams = s; 
  }

  @Override
  public String getParamString() {
    return origParams.toString();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + '{' + params + '}';
  }

  @Override
  public Map<String, Object> getJSON() {
    return json;
  }

  @Override
  public void setJSON(Map<String, Object> json) {
    this.json = json;
  }

  @Override
  public Principal getUserPrincipal() {
    return null;
  }

  List<CommandOperation> parsedCommands;

  public List<CommandOperation> getCommands(boolean validateInput) {
    if (parsedCommands == null) {
      Iterable<ContentStream> contentStreams = getContentStreams();
      if (contentStreams == null) throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No content stream");
      for (ContentStream contentStream : contentStreams) {
        parsedCommands = ApiBag.getCommandOperations(contentStream, getValidators(), validateInput);
      }

    }
    return CommandOperation.clone(parsedCommands);

  }

  protected ValidatingJsonMap getSpec() {
    return null;
  }

  @SuppressWarnings({"unchecked"})
  protected Map<String, JsonSchemaValidator> getValidators(){
    return Collections.EMPTY_MAP;
  }
}
