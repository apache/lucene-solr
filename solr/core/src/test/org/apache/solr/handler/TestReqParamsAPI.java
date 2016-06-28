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
import java.util.function.Predicate;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.RequestParams;
import org.apache.solr.core.TestSolrConfigHandler;
import org.apache.solr.util.RestTestHarness;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.solr.handler.TestSolrConfigHandlerCloud.compareValues;

public class TestReqParamsAPI extends AbstractFullDistribZkTestBase {
  private List<RestTestHarness> restTestHarnesses = new ArrayList<>();

  private void setupHarnesses() {
    for (final SolrClient client : clients) {
      RestTestHarness harness = new RestTestHarness(() -> ((HttpSolrClient) client).getBaseURL());
      restTestHarnesses.add(harness);
    }
  }

  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    for (RestTestHarness r : restTestHarnesses) {
      r.close();
    }
  }

  @Test
  public void test() throws Exception {
    setupHarnesses();
    testReqParams();
  }

  private void testReqParams() throws Exception {
    DocCollection coll = cloudClient.getZkStateReader().getClusterState().getCollection("collection1");
    List<String> urls = new ArrayList<>();
    for (Slice slice : coll.getSlices()) {
      for (Replica replica : slice.getReplicas())
        urls.add("" + replica.get(ZkStateReader.BASE_URL_PROP) + "/" + replica.get(ZkStateReader.CORE_NAME_PROP));
    }

    RestTestHarness writeHarness = restTestHarnesses.get(random().nextInt(restTestHarnesses.size()));
    String payload = " {\n" +
        "  'set' : {'x': {" +
        "                    'a':'A val',\n" +
        "                    'b': 'B val'}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness, "/config/params?wt=json", payload);

    Map result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        asList("response", "params", "x", "a"),
        "A val",
        10);
    compareValues(result, "B val", asList("response", "params", "x", "b"));

    payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/dump0', 'class': 'org.apache.solr.handler.DumpRequestHandler' }\n" +
        "}";

    TestSolrConfigHandler.runConfigCommand(writeHarness, "/config?wt=json", payload);

    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/config/overlay?wt=json",
        cloudClient,
        asList("overlay", "requestHandler", "/dump0", "name"),
        "/dump0",
        10);

    result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump0?wt=json&useParams=x",
        cloudClient,
        asList("params", "a"),
        "A val",
        5);
    compareValues(result, "", asList("params", RequestParams.USEPARAM));

    TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump0?wt=json&useParams=x&a=fomrequest",
        cloudClient,
        asList("params", "a"),
        "fomrequest",
        5);

    payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/dump1', 'class': 'org.apache.solr.handler.DumpRequestHandler', 'useParams':'x' }\n" +
        "}";

    TestSolrConfigHandler.runConfigCommand(writeHarness, "/config?wt=json", payload);

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
        "                'b': 'BY val', " +
        "                'i': 20, " +
        "                'd': ['val 1', 'val 2']}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness, "/config/params?wt=json", payload);

    result = TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        asList("response", "params", "y", "c"),
        "CY val",
        10);
    compareValues(result, 20l, asList("response", "params", "y", "i"));
    compareValues(result, null, asList("response", "params", "y", "a"));


    result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump1?wt=json&useParams=y",
        cloudClient,
        asList("params", "c"),
        "CY val",
        5);
    compareValues(result, "BY val", asList("params", "b"));
    compareValues(result, "A val", asList("params", "a"));
    compareValues(result, Arrays.asList("val 1", "val 2"), asList("params", "d"));
    compareValues(result, "20", asList("params", "i"));
    payload = " {\n" +
        "  'update' : {'y': {\n" +
        "                'c':'CY val modified',\n" +
        "                'e':'EY val',\n" +
        "                'b': 'BY val'" +
        "}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(writeHarness, "/config/params?wt=json", payload);

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


    TestSolrConfigHandler.runConfigCommand(writeHarness, "/config/params?wt=json", payload);
    result = TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        asList("response", "params", "y", "p"),
        "P val",
        10);
    compareValues(result, null, asList("response", "params", "y", "c"));
    compareValues(result, 2l, asList("response", "params", "y", "","v"));
    compareValues(result, 0l, asList("response", "params", "x", "","v"));

    payload = "{update :{x : {_appends_ :{ add : 'first' },  _invariants_ : {fixed: f }}}}";
    TestSolrConfigHandler.runConfigCommand(writeHarness, "/config/params?wt=json", payload);

    result = TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        asList("response", "params", "x", "_appends_", "add"),
        "first",
        10);
    compareValues(result, "f", asList("response", "params", "x", "_invariants_", "fixed"));


    result = TestSolrConfigHandler.testForResponseElement(null,
        urls.get(random().nextInt(urls.size())),
        "/dump1?wt=json&fixed=changeit&add=second",
        cloudClient,
        asList("params", "fixed"),
        "f",
        5);
    compareValues(result, new Predicate() {
      @Override
      public boolean test(Object o) {
        List l = (List) o;
        return l.contains("first") && l.contains("second");
      }
    }, asList("params", "add"));

    payload = " {'delete' : 'y'}";
    TestSolrConfigHandler.runConfigCommand(writeHarness, "/config/params?wt=json", payload);
    TestSolrConfigHandler.testForResponseElement(
        null,
        urls.get(random().nextInt(urls.size())),
        "/config/params?wt=json",
        cloudClient,
        asList("response", "params", "y", "p"),
        null,
        10);


  }


}
