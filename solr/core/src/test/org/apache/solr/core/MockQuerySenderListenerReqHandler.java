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
package org.apache.solr.core;

import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.common.util.NamedList;

import java.util.concurrent.atomic.AtomicInteger;


/**
 *
 *
 **/
public class MockQuerySenderListenerReqHandler extends RequestHandlerBase {
  public SolrQueryRequest req;
  public SolrQueryResponse rsp;

  AtomicInteger initCounter = new AtomicInteger(0);

  @Override
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    initCounter.incrementAndGet();
    super.init(args);
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registryName, String tag, String scope) {
    super.initializeMetrics(manager, registryName, tag, scope);
    manager.registerGauge(this, registryName, () -> initCounter.intValue(), tag, true, "initCount", getCategory().toString(), scope);
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    this.req = req;
    this.rsp = rsp;
  }

  @Override
  public String getDescription() {
    String result = null;
    return result;
  }
}
