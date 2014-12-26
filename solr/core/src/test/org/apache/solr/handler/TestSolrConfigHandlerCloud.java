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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.ConfigOverlay;
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
    testReqParams();

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

  private void testReqParams() throws Exception{
    DocCollection coll = cloudClient.getZkStateReader().getClusterState().getCollection("collection1");
    List<String> urls = new ArrayList<>();
    for (Slice slice : coll.getSlices()) {
      for (Replica replica : slice.getReplicas())
        urls.add(""+replica.get(ZkStateReader.BASE_URL_PROP) + "/"+replica.get(ZkStateReader.CORE_NAME_PROP));
    }

    RestTestHarness writeHarness = restTestHarnesses.get(random().nextInt(restTestHarnesses.size()));
    String payload = " {\n" +
        "  'set' : {'x': {" +
        "                    'a':'A val',\n" +
        "                    'b': 'B val'}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params?wt=json", payload);

    TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        Arrays.asList("response", "params", "x", "a"),
        "A val",
        10);

    TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        Arrays.asList("response", "params", "x", "b"),
        "B val",
        10);

    payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/dump', 'class': 'org.apache.solr.handler.DumpRequestHandler' }\n" +
        "}";

    TestSolrConfigHandler.runConfigCommand(writeHarness, "/config?wt=json", payload);

    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/config/overlay?wt=json",
        cloudClient,
        Arrays.asList("overlay", "requestHandler", "/dump", "name"),
        "/dump",
        10);

    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump?wt=json&useParams=x",
        cloudClient,
        Arrays.asList("params", "a"),
        "A val",
        5);
    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump?wt=json&useParams=x&a=fomrequest",
        cloudClient,
        Arrays.asList("params", "a"),
        "fomrequest",
        5);

    payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/dump1', 'class': 'org.apache.solr.handler.DumpRequestHandler', 'useParams':'x' }\n" +
        "}";

    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config?wt=json", payload);

    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/config/overlay?wt=json",
        cloudClient,
        Arrays.asList("overlay", "requestHandler", "/dump1", "name"),
        "/dump1",
        10);

    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump1?wt=json",
        cloudClient,
        Arrays.asList("params", "a"),
        "A val",
        5);



    writeHarness = restTestHarnesses.get(random().nextInt(restTestHarnesses.size()));
    payload = " {\n" +
        "  'set' : {'y':{\n" +
        "                'c':'CY val',\n" +
        "                'b': 'BY val'}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params?wt=json", payload);

    TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        Arrays.asList("response", "params", "y", "c"),
        "CY val",
        10);

    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump?wt=json&useParams=y",
        cloudClient,
        Arrays.asList("params", "c"),
        "CY val",
        5);


    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump1?wt=json&useParams=y",
        cloudClient,
        Arrays.asList("params", "b"),
        "BY val",
        5);

    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump1?wt=json&useParams=y",
        cloudClient,
        Arrays.asList("params", "a"),
        null,
        5);

    payload = " {\n" +
        "  'update' : {'y': {\n" +
        "                'c':'CY val modified',\n" +
        "                'e':'EY val',\n" +
        "                'b': 'BY val'" +
        "}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params?wt=json", payload);

    TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        Arrays.asList("response", "params", "y", "c"),
        "CY val modified",
        10);

    TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        Arrays.asList("response", "params", "y", "e"),
        "EY val",
        10);

    payload = " {\n" +
        "  'set' : {'y': {\n" +
        "                'p':'P val',\n" +
        "                'q': 'Q val'" +
        "}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params?wt=json", payload);
    TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        Arrays.asList("response", "params", "y", "p"),
        "P val",
        10);

    TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        Arrays.asList("response", "params", "y", "c"),
        null,
        10);
    payload = " {'delete' : 'y'}";
    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params?wt=json", payload);
    TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        Arrays.asList("response", "params", "y", "p"),
        null,
        10);


  }

}
