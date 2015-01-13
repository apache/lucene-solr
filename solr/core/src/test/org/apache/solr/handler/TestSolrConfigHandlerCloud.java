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


import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.core.TestSolrConfigHandler;
import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestHarness;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static org.apache.solr.handler.TestBlobHandler.getAsString;

public class TestSolrConfigHandlerCloud extends AbstractFullDistribZkTestBase {
  static final Logger log =  LoggerFactory.getLogger(TestSolrConfigHandlerCloud.class);
  private List<RestTestHarness> restTestHarnesses = new ArrayList<>();

  private void setupHarnesses() {
    for (final SolrClient client : clients) {
      RestTestHarness harness = new RestTestHarness(new RESTfulServerProvider() {
        @Override
        public String getBaseURL() {
          return ((HttpSolrClient)client).getBaseURL();
        }
      });
      restTestHarnesses.add(harness);
    }
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    for (RestTestHarness r : restTestHarnesses) {
      r.close();
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

    Map result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        asList("response", "params", "x", "a"),
        "A val",
        10);
    compareValues(result, "B val", asList("response", "params", "x", "b"));

    payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/dump', 'class': 'org.apache.solr.handler.DumpRequestHandler' }\n" +
        "}";

    TestSolrConfigHandler.runConfigCommand(writeHarness, "/config?wt=json", payload);

    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/config/overlay?wt=json",
        cloudClient,
        asList("overlay", "requestHandler", "/dump", "name"),
        "/dump",
        10);

    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump?wt=json&useParams=x",
        cloudClient,
        asList("params", "a"),
        "A val",
        5);
    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump?wt=json&useParams=x&a=fomrequest",
        cloudClient,
        asList("params", "a"),
        "fomrequest",
        5);

    payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/dump1', 'class': 'org.apache.solr.handler.DumpRequestHandler', 'useParams':'x' }\n" +
        "}";

    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config?wt=json", payload);

    result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/config/overlay?wt=json",
        cloudClient,
        asList("overlay", "requestHandler", "/dump1", "name"),
        "/dump1",
        10);

    result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump1?wt=json",
        cloudClient,
        asList("params", "a"),
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

   result =  TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        asList("response", "params", "y", "c"),
        "CY val",
        10);

    result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump?wt=json&useParams=y",
        cloudClient,
        asList("params", "c"),
        "CY val",
        5);
    compareValues(result, "BY val", asList("params", "b"));
    compareValues(result, null, asList("params", "a"));
    payload = " {\n" +
        "  'update' : {'y': {\n" +
        "                'c':'CY val modified',\n" +
        "                'e':'EY val',\n" +
        "                'b': 'BY val'" +
        "}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params?wt=json", payload);

    result = TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        asList("response", "params", "y", "c"),
        "CY val modified",
        10);
    compareValues(result, "EY val", asList("response", "params", "y", "e"));


    payload = " {\n" +
        "  'set' : {'y': {\n" +
        "                'p':'P val',\n" +
        "                'q': 'Q val'" +
        "}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params?wt=json", payload);
    result = TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        asList("response", "params", "y", "p"),
        "P val",
        10);
    compareValues(result, null, asList("response", "params", "y", "c"));

    payload = " {'delete' : 'y'}";
    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params?wt=json", payload);
    TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        asList("response", "params", "y", "p"),
        null,
        10);


  }

  public static void compareValues(Map result, String expected, List<String> jsonPath) {
    assertTrue(MessageFormat.format("Could not get expected value  {0} for path {1} full output {2}", expected, jsonPath, getAsString(result)),
        Objects.equals(expected, ConfigOverlay.getObjectByPath(result, false, jsonPath)));
  }

}
