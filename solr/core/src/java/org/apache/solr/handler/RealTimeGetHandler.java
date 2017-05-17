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
package org.apache.solr.handler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.handler.component.HttpShardHandler;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;


public class RealTimeGetHandler extends SearchHandler {
  @Override
  protected List<String> getDefaultComponents()
  {
    List<String> names = new ArrayList<>(1);
    names.add(RealTimeGetComponent.COMPONENT_NAME);
    return names;
  }
  
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Tell HttpShardHandlerthat this request should only be distributed to NRT replicas
    req.getContext().put(HttpShardHandler.ONLY_NRT_REPLICAS, Boolean.TRUE);
    super.handleRequestBody(req, rsp);
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "The realtime get handler";
  }

  @Override
  public Collection<Api> getApis() {
    return ApiBag.wrapRequestHandlers(this, "core.RealtimeGet");
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }
}







