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

package org.apache.solr.client.solrj.response;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.util.NamedList;

/**
 * 
 * @version $Id$
 * @since solr 1.3
 */
public abstract class SolrResponseBase implements SolrResponse
{
  private long elapsedTime = -1;
  private NamedList<Object> response = null;
  private String requestUrl = null;
  
  public SolrResponseBase( NamedList<Object> res )
  {
    response = res;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }

  public void setElapsedTime(long elapsedTime) {
    this.elapsedTime = elapsedTime;
  }

  public NamedList<Object> getResponse() {
    return response;
  }

  public void setResponse(NamedList<Object> response) {
    this.response = response;
  }

  @Override
  public String toString() {
    return response.toString();
  }
  
  public NamedList getResponseHeader() {
    return (NamedList) response.get("responseHeader");
  }
  
  // these two methods are based on the logic in SolrCore.setResponseHeaderValues(...)
  public int getStatus() {
    return (Integer) getResponseHeader().get("status");
  }
  
  public int getQTime() {
    return (Integer) getResponseHeader().get("QTime");
  }

  public String getRequestUrl() {
    return requestUrl;
  }

  public void setRequestUrl(String requestUrl) {
    this.requestUrl = requestUrl;
  }
  
}
