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
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;

import java.util.Map;

/**
 * <p>Container for a request to execute a query.</p>
 * <p><code>SolrQueryRequest</code> is not thread safe.</p>
 * 
 *
 */
public interface SolrQueryRequest {

  /** returns the current request parameters */
  public SolrParams getParams();

  /** Change the parameters for this request.  This does not affect
   *  the original parameters returned by getOriginalParams()
   */
  public void setParams(SolrParams params);
  
  /** A Collection of ContentStreams passed to the request
   */
  public Iterable<ContentStream> getContentStreams();

  /** Returns the original request parameters.  As this
   * does not normally include configured defaults
   * it's more suitable for logging.
   */
  public SolrParams getOriginalParams();

  /**
   * Generic information associated with this request that may be both read and updated.
   */
  public Map<Object,Object> getContext();

  /**
   * This method should be called when all uses of this request are
   * finished, so that resources can be freed.
   */
  public void close();

  /** The start time of this request in milliseconds */
  public long getStartTime();

  /** The index searcher associated with this request */
  public SolrIndexSearcher getSearcher();

  /** The solr core (coordinator, etc) associated with this request */
  public SolrCore getCore();

  /** The index schema associated with this request */
  public IndexSchema getSchema();

  /**
   * Returns a string representing all the important parameters.
   * Suitable for logging.
   */
  public String getParamString();
}




