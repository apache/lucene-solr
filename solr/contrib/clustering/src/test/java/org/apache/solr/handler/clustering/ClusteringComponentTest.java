package org.apache.solr.handler.clustering;
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

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;
import org.junit.Test;

/**
 *
 *
 **/
public class ClusteringComponentTest extends AbstractClusteringTestCase {

  @Test
  public void testComponent() throws Exception {
    SolrCore core = h.getCore();

    SearchComponent sc = core.getSearchComponent("clustering");
    assertTrue("sc is null and it shouldn't be", sc != null);
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.add(ClusteringComponent.COMPONENT_NAME, "true");
    params.add(CommonParams.Q, "*:*");

    params.add(ClusteringParams.USE_SEARCH_RESULTS, "true");


    SolrRequestHandler handler = core.getRequestHandler("standard");
    SolrQueryResponse rsp;
    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
    handler.handleRequest(req, rsp);
    NamedList values = rsp.getValues();
    Object clusters = values.get("clusters");
    //System.out.println("Clusters: " + clusters);
    assertTrue("clusters is null and it shouldn't be", clusters != null);
    req.close();

    params = new ModifiableSolrParams();
    params.add(ClusteringComponent.COMPONENT_NAME, "true");
    params.add(ClusteringParams.ENGINE_NAME, "mock");
    params.add(ClusteringParams.USE_COLLECTION, "true");
    params.add(QueryComponent.COMPONENT_NAME, "false");

    handler = core.getRequestHandler("docClustering");

    rsp = new SolrQueryResponse();
    rsp.add("responseHeader", new SimpleOrderedMap());
    req = new LocalSolrQueryRequest(core, params);
    handler.handleRequest(req, rsp);
    values = rsp.getValues();
    clusters = values.get("clusters");
    //System.out.println("Clusters: " + clusters);
    assertTrue("clusters is null and it shouldn't be", clusters != null);
    req.close();
  }

}
