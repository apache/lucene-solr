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

import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.response.SolrQueryResponse;

import java.util.Date;


public class SolrRequestInfo {
  protected final static ThreadLocal<SolrRequestInfo> threadLocal = new ThreadLocal<SolrRequestInfo>();

  protected SolrQueryRequest req;
  protected SolrQueryResponse rsp;
  protected Date now;
  protected ResponseBuilder rb;

  public static SolrRequestInfo getRequestInfo() {
    return threadLocal.get();
  }

  public static void setRequestInfo(SolrRequestInfo info) {
    // TODO: temporary sanity check... this can be changed to just an assert in the future
    SolrRequestInfo prev = threadLocal.get();
    if (prev != null) {
      SolrCore.log.error("Previous SolrRequestInfo was not closed!  req=" + prev.req.getOriginalParams().toString());  
    }
    assert prev == null;

    threadLocal.set(info);
  }

  public static void clearRequestInfo() {
    threadLocal.remove();
  }

  public SolrRequestInfo(SolrQueryRequest req, SolrQueryResponse rsp) {
    this.req = req;
    this.rsp = rsp;    
  }

  public Date getNOW() {    
    if (now != null) return now;

    long ms = 0;
    String nowStr = req.getParams().get("NOW");

    if (nowStr != null) {
      ms = Long.parseLong(nowStr);
    } else {
      ms = req.getStartTime();
    }

    now = new Date(ms);
    return now;
  }

  public SolrQueryRequest getReq() {
    return req;
  }

  public SolrQueryResponse getRsp() {
    return rsp;
  }

  /** May return null if the request handler is not based on SearchHandler */
  public ResponseBuilder getResponseBuilder() {
    return rb;
  }

  public void setResponseBuilder(ResponseBuilder rb) {
    this.rb = rb;
  }
}
