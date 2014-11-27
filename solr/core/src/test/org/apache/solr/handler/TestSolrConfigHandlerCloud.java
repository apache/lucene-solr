package org.apache.solr.handler;

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


import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.TestSolrConfigHandler;
import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestHarness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSolrConfigHandlerCloud extends AbstractFullDistribZkTestBase {
  static final Logger log =  LoggerFactory.getLogger(TestSolrConfigHandlerCloud.class);
  private List<RestTestHarness> restTestHarnesses = new ArrayList<>();

  private void setupHarnesses() {
    for (final SolrServer client : clients) {
      RestTestHarness harness = new RestTestHarness(new RESTfulServerProvider() {
        @Override
        public String getBaseURL() {
          return ((HttpSolrServer)client).getBaseURL();
        }
      });
      restTestHarnesses.add(harness);
    }
  }

  @Override
  public void doTest() throws Exception {
    setupHarnesses();
    testReqHandlerAPIs();

  }

  private void testReqHandlerAPIs() throws Exception {
    DocCollection coll = cloudClient.getZkStateReader().getClusterState().getCollection("collection1");
    List<String> urls = new ArrayList<>();
    for (Slice slice : coll.getSlices()) {
      for (Replica replica : slice.getReplicas())
        urls.add(""+replica.get(ZkStateReader.BASE_URL_PROP) + "/"+replica.get(ZkStateReader.CORE_NAME_PROP));
    }

    RestTestHarness writeHarness = restTestHarnesses.get(random().nextInt(restTestHarnesses.size()));
    String testServerBaseUrl = urls.get(random().nextInt(urls.size()));
    TestSolrConfigHandler.reqhandlertests(writeHarness, testServerBaseUrl , cloudClient);
  }
}
