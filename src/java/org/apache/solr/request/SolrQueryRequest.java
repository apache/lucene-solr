/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.solr.core.SolrCore;

/**
 * @author yonik
 * @version $Id: SolrQueryRequest.java,v 1.3 2005/05/10 19:40:12 yonik Exp $
 */
public interface SolrQueryRequest {
  public String getParam(String name);

  public String getQueryString();

  // signifies the syntax and the handler that should be used
  // to execute this query.
  public String getQueryType();

  // starting position in matches to return to client
  public int getStart();

  // number of matching documents to return
  public int getLimit();

  // Get the start time of this request in milliseconds
  public long getStartTime();

  // The index searcher associated with this request
  public SolrIndexSearcher getSearcher();

  // The solr core (coordinator, etc) associated with this request
  public SolrCore getCore();

  // The index schema associated with this request
  public IndexSchema getSchema();

  /**
   * Returns a string representing all the important parameters.
   * Suitable for logging.
   */
  public String getParamString();

  /******
  // Get the current elapsed time in milliseconds
  public long getElapsedTime();
  ******/
}

