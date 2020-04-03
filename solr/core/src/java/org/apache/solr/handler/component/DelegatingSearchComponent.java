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
package org.apache.solr.handler.component;

import java.io.IOException;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.metrics.SolrMetricsContext;

/**
 * TODO
 */
public class DelegatingSearchComponent extends SearchComponent {

  public static final String COMPONENT_NAME = "delegate";

  private NamedList mappings;

  @Override
  public void init( NamedList args ) {
    super.init(args);
    this.mappings = (NamedList)args.remove("mappings");
  }

  private SearchComponent getDelegate(ResponseBuilder rb) {
    for (int ii = 0; ii < mappings.size(); ++ii) {
      if (rb.req.getParams().getBool(mappings.getName(ii), false)) {
        return rb.req.getCore().getSearchComponent((String)mappings.getVal(ii));
      }
    }
    return null;
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    getDelegate(rb).prepare(rb);
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    getDelegate(rb).process(rb);
  }

  public int distributedProcess(ResponseBuilder rb) throws IOException {
    return getDelegate(rb).distributedProcess(rb);
  }

  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    getDelegate(rb).modifyRequest(rb, who, sreq);
  }

  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    getDelegate(rb).handleResponses(rb, sreq);
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    getDelegate(rb).finishStage(rb);
  }

  @Override
  public String getDescription() {
    return "delegate";
  }

  @Override
  public Category getCategory() {
    return super.getCategory(); // TODO
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return super.getSolrMetricsContext(); // TODO
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    super.initializeMetrics(parentContext, scope); // TODO
  }

}
