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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.junit.Test;

public class TestConfigSetSchemaHandler extends AbstractFullDistribZkTestBase {
  //curl "localhost:8983/api/cluster/configset/gettingstarted/schema?path=/schema/fields"
  @Test
  public void testGetSchema() throws Exception {
    String name = UUID.randomUUID().toString();
    createConfigSet(name);
    String url = "/cluster/configset/" + name + "/schema";
    V2Response rsp = new V2Request.Builder(url)
        .withMethod(SolrRequest.METHOD.GET)
        .build()
        .process(cloudClient);
    Map map = (Map) Utils.fromJSONString(rsp.jsonStr());
    assertNull(rsp.jsonStr(), map.get("errorMessages"));
    assertNull(rsp.jsonStr(), map.get("errors"));
  }

  @Test
  public void testGetFields() throws Exception {
    String name = UUID.randomUUID().toString();
    createConfigSet(name);
    String url = "/cluster/configset/" + name + "/schema";
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("query", "/schema/fields");
    V2Response rsp = new V2Request.Builder(url)
        .withParams(params)
        .withMethod(SolrRequest.METHOD.GET)
        .build()
        .process(cloudClient);
    Map map = (Map) Utils.fromJSONString(rsp.jsonStr());
    assertNull(rsp.jsonStr(), map.get("errorMessages"));
    assertNull(rsp.jsonStr(), map.get("errors"));
  }

  @Test
  public void testAddField() throws Exception {
    String name = UUID.randomUUID().toString();
    createConfigSet(name);
    //add
    String url = "/cluster/configset/" + name + "/schema";
    String payload = "{\n" +
        "  \"add-field\":{\n" +
        "     \"name\":\"sell-by4\",\n" +
        "     \"type\":\"string\",\n" +
        "     \"stored\":true }\n" +
        "}";
    V2Response rsp = new V2Request.Builder(url)
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload(payload)
        .build()
        .process(cloudClient);
    assertEquals(0, rsp.getStatus());
    //get
    url = "/cluster/configset/" + name + "/schema";
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("query", "/schema/fields");
    rsp = new V2Request.Builder(url)
        .withParams(params)
        .withMethod(SolrRequest.METHOD.GET)
        .build()
        .process(cloudClient);
    Map map = (Map) Utils.fromJSONString(rsp.jsonStr());
    ArrayList<LinkedHashMap> fields = (ArrayList) map.get("fields");
    boolean value = false;
    for (LinkedHashMap field : fields) {
      String fieldName = field.get("name").toString();
      value = (fieldName.equals("sell-by4"));
    }
    assertEquals(true, value);
  }

  private V2Response createConfigSet(String name) throws org.apache.solr.client.solrj.SolrServerException, java.io.IOException {
    String url = "/cluster/configs";
    String payload = "{\n" +
        "  \"create\":{\n" +
        "    \"name\": \"" + name + "\",\n" +
        "    \"baseConfigSet\": \"_default\"}}";
    return new V2Request.Builder(url)
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload(payload)
        .build()
        .process(cloudClient);
  }
}
