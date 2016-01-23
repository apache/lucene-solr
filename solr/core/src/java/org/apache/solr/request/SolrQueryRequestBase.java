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

package org.apache.solr.request;

import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;

import java.util.Map;
import java.util.HashMap;

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
public abstract class SolrQueryRequestBase implements SolrQueryRequest {
  protected final SolrCore core;
  protected final SolrParams origParams;
  protected SolrParams params;
  protected Map<Object,Object> context;
  protected Iterable<ContentStream> streams;

  public SolrQueryRequestBase(SolrCore core, SolrParams params) {
    this.core = core;
    this.params = this.origParams = params;
  }

  public Map<Object,Object> getContext() {
    // SolrQueryRequest as a whole isn't thread safe, and this isn't either.
    if (context==null) context = new HashMap<Object,Object>();
    return context;
  }

  public SolrParams getParams() {
    return params;
  }

  public SolrParams getOriginalParams() {
    return origParams;
  }

  public void setParams(SolrParams params) {
    this.params = params;
  }

  protected final long startTime=System.currentTimeMillis();
  // Get the start time of this request in milliseconds
  public long getStartTime() {
    return startTime;
  }

  // The index searcher associated with this request
  protected RefCounted<SolrIndexSearcher> searcherHolder;
  public SolrIndexSearcher getSearcher() {
    if(core == null) return null;//a request for a core admin will no have a core
    // should this reach out and get a searcher from the core singleton, or
    // should the core populate one in a factory method to create requests?
    // or there could be a setSearcher() method that Solr calls

    if (searcherHolder==null) {
      searcherHolder = core.getSearcher();
    }

    return searcherHolder.get();
  }

  // The solr core (coordinator, etc) associated with this request
  public SolrCore getCore() {
    return core;
  }

  // The index schema associated with this request
  public IndexSchema getSchema() {
    //a request for a core admin will no have a core
    return core == null? null: core.getSchema();
  }

  /**
   * Frees resources associated with this request, this method <b>must</b>
   * be called when the object is no longer in use.
   */
  public void close() {
    if (searcherHolder!=null) {
      searcherHolder.decref();
      searcherHolder = null;
    }
  }

  /** A Collection of ContentStreams passed to the request
   */
  public Iterable<ContentStream> getContentStreams() {
    return streams; 
  }
  
  public void setContentStreams( Iterable<ContentStream> s ) {
    streams = s; 
  }

  public String getParamString() {
    return origParams.toString();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + '{' + params + '}';
  }

}
