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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.RequestParams;
import org.apache.solr.core.TestSolrConfigHandler;
import org.apache.solr.util.RestTestHarness;
import org.junit.Test;

import static java.util.Arrays.asList;

public class TestSolrConfigHandlerCloud extends AbstractFullDistribZkTestBase {

  private static final long TIMEOUT_S = 10;

  @Test
  public void test() throws Exception {
    setupRestTestHarnesses();
    testReqHandlerAPIs();
    testReqParams();
    testAdminPath();
  }

  private void testAdminPath() throws Exception{
    String testServerBaseUrl = getRandomServer(cloudClient,"collection1");
    RestTestHarness writeHarness = randomRestTestHarness();
    String payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/admin/luke', " +
        "'class': 'org.apache.solr.handler.DumpRequestHandler'}}";

    TestSolrConfigHandler.runConfigCommand(writeHarness, "/config", payload);


    TestSolrConfigHandler.testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config/overlay",
        cloudClient,
        Arrays.asList("overlay", "requestHandler", "/admin/luke", "class"),
        "org.apache.solr.handler.DumpRequestHandler",
        TIMEOUT_S);

   NamedList<Object> rsp = cloudClient.request(new LukeRequest());
   System.out.println(rsp);
  }

  private void testReqHandlerAPIs() throws Exception {
    String testServerBaseUrl = getRandomServer(cloudClient,"collection1");
    RestTestHarness writeHarness = randomRestTestHarness();
    TestSolrConfigHandler.reqhandlertests(writeHarness, testServerBaseUrl , cloudClient);
  }

  public static String getRandomServer(CloudSolrClient cloudClient, String collName) {
    DocCollection coll = cloudClient.getZkStateReader().getClusterState().getCollection(collName);
    List<String> urls = new ArrayList<>();
    for (Slice slice : coll.getSlices()) {
      for (Replica replica : slice.getReplicas())
        urls.add(""+replica.getBaseUrl() + "/" + replica.get(ZkStateReader.CORE_NAME_PROP));
    }
    return urls.get(random().nextInt(urls.size()));
  }

  private void testReqParams() throws Exception{
    DocCollection coll = cloudClient.getZkStateReader().getClusterState().getCollection("collection1");
    List<String> urls = new ArrayList<>();
    for (Slice slice : coll.getSlices()) {
      for (Replica replica : slice.getReplicas())
        urls.add(""+replica.getBaseUrl() + "/" + replica.get(ZkStateReader.CORE_NAME_PROP));
    }

    RestTestHarness writeHarness = randomRestTestHarness();
    String payload = " {\n" +
        "  'set' : {'x': {" +
        "                    'a':'A val',\n" +
        "                    'b': 'B val'}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params", payload);

    @SuppressWarnings({"rawtypes"})
    Map result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/config/params",
        cloudClient,
        asList("response", "params", "x", "a"),
        "A val",
        TIMEOUT_S);
    compareValues(result, "B val", asList("response", "params", "x", "b"));

    payload = "{\n" +
        "'update-requesthandler' : { 'name' : '/dump', 'class': 'org.apache.solr.handler.DumpRequestHandler' }\n" +
        "}";

    TestSolrConfigHandler.runConfigCommand(writeHarness, "/config", payload);

    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/config/overlay",
        cloudClient,
        asList("overlay", "requestHandler", "/dump", "name"),
        "/dump",
        TIMEOUT_S);

    result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump?useParams=x",
        cloudClient,
        asList("params", "a"),
        "A val",
        TIMEOUT_S);
    compareValues(result, "", asList( "params", RequestParams.USEPARAM));

    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump?useParams=x&a=fomrequest",
        cloudClient,
        asList("params", "a"),
        "fomrequest",
        TIMEOUT_S);

    payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/dump1', 'class': 'org.apache.solr.handler.DumpRequestHandler', 'useParams':'x' }\n" +
        "}";

    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config", payload);

    result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/config/overlay",
        cloudClient,
        asList("overlay", "requestHandler", "/dump1", "name"),
        "/dump1",
        TIMEOUT_S);

    result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump1",
        cloudClient,
        asList("params", "a"),
        "A val",
        TIMEOUT_S);



    writeHarness = randomRestTestHarness();
    payload = " {\n" +
        "  'set' : {'y':{\n" +
        "                'c':'CY val',\n" +
        "                'b': 'BY val', " +
        "                'i': 20, " +
        "                'd': ['val 1', 'val 2']}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params", payload);

   result =  TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params",
        cloudClient,
        asList("response", "params", "y", "c"),
        "CY val",
        TIMEOUT_S);
    compareValues(result, 20l, asList("response", "params", "y", "i"));


    result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump?useParams=y",
        cloudClient,
        asList("params", "c"),
        "CY val",
        TIMEOUT_S);
    compareValues(result, "BY val", asList("params", "b"));
    compareValues(result, null, asList("params", "a"));
    compareValues(result, Arrays.asList("val 1", "val 2")  , asList("params", "d"));
    compareValues(result, "20"  , asList("params", "i"));
    payload = " {\n" +
        "  'update' : {'y': {\n" +
        "                'c':'CY val modified',\n" +
        "                'e':'EY val',\n" +
        "                'b': 'BY val'" +
        "}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params", payload);

    result = TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params",
        cloudClient,
        asList("response", "params", "y", "c"),
        "CY val modified",
        TIMEOUT_S);
    compareValues(result, "EY val", asList("response", "params", "y", "e"));


    payload = " {\n" +
        "  'set' : {'y': {\n" +
        "                'p':'P val',\n" +
        "                'q': 'Q val'" +
        "}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params", payload);
    result = TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params",
        cloudClient,
        asList("response", "params", "y", "p"),
        "P val",
        TIMEOUT_S);
    compareValues(result, null, asList("response", "params", "y", "c"));

    payload = " {'delete' : 'y'}";
    TestSolrConfigHandler.runConfigCommand(writeHarness,"/config/params", payload);
    TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params",
        cloudClient,
        asList("response", "params", "y", "p"),
        null,
        TIMEOUT_S);

    payload = " {'unset' : 'y'}";
    TestSolrConfigHandler.runConfigCommandExpectFailure(
        writeHarness,"/config/params", payload, "Unknown operation 'unset'");

    // deleting already deleted one should fail
    // error message should contain parameter set name
    payload = " {'delete' : 'y'}";
    TestSolrConfigHandler.runConfigCommandExpectFailure(
        writeHarness,"/config/params", payload, "Could not delete. No such params 'y' exist");

  }

  @SuppressWarnings({"unchecked"})
  public static void compareValues(@SuppressWarnings({"rawtypes"})Map result, Object expected, List<String> jsonPath) {
    Object val = Utils.getObjectByPath(result, false, jsonPath);
    assertTrue(StrUtils.formatString("Could not get expected value  {0} for path {1} full output {2}", expected, jsonPath, result.toString()),
        expected instanceof Predicate ?
            ((Predicate)expected ).test(val) :
            Objects.equals(expected, val)
        );
  }

}
