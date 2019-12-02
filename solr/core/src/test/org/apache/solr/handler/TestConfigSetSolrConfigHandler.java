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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.util.Utils;
import org.junit.Before;
import org.junit.Test;

public class TestConfigSetSolrConfigHandler extends AbstractFullDistribZkTestBase {

  @Test
  public void testCreateConfigSet() throws Exception {
    V2Response rsp = createConfigSet("newConfigSet");
    Map map = (Map) Utils.fromJSONString(rsp.jsonStr());
    assertNull(rsp.jsonStr(), map.get("errorMessages"));
    assertNull(rsp.jsonStr(), map.get("errors"));
  }

  @Test
  public void testGetConfig() throws Exception {
    String name = UUID.randomUUID().toString();
    //create
    V2Response rsp = createConfigSet(name);
    Map map = (Map) Utils.fromJSONString(rsp.jsonStr());
    assertNull(rsp.jsonStr(), map.get("errorMessages"));
    assertNull(rsp.jsonStr(), map.get("errors"));
    //get
    String url = "/cluster/configset/" + name + "/config";
    rsp = new V2Request.Builder(url)
        .withMethod(SolrRequest.METHOD.GET)
        .build()
        .process(cloudClient);
    assertEquals(0, rsp.getStatus());
  }

  //test add a field and do a get to check if it is added
  @Test
  public void testAddConfig() throws Exception {
    String name = UUID.randomUUID().toString();
    //create
    V2Response rsp = createConfigSet(name);
    Map map = (Map) Utils.fromJSONString(rsp.jsonStr());
    assertNull(rsp.jsonStr(), map.get("errorMessages"));
    assertNull(rsp.jsonStr(), map.get("errors"));
    //add
    String url = "/cluster/configset/" + name + "/config";
    String payload = "{\n" +
        "    \"set-property\" : {\"query.filterCache.autowarmCount\":2000},\n" +
        "    \"unset-property\" :\"query.filterCache.size\"}";
    rsp = new V2Request.Builder(url)
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload(payload)
        .build()
        .process(cloudClient);
    assertEquals(0, rsp.getStatus());
    //get
    url = "/cluster/configset/" + name + "/config/overlay";
    rsp = new V2Request.Builder(url)
        .withMethod(SolrRequest.METHOD.GET)
        .build()
        .process(cloudClient);
    assertEquals(0, rsp.getStatus());
    Map overlayMap = (LinkedHashMap) rsp.getResponse().get("overlay");
    Long value = (Long) ((LinkedHashMap) ((LinkedHashMap) ((LinkedHashMap) overlayMap.get("props")).get("query")).get("filterCache")).get("autowarmCount");
    assertEquals(2000, value.longValue());
  }

  @Test
  public void testAddParams() throws Exception {
    String name = UUID.randomUUID().toString();
    //create
    V2Response rsp = createConfigSet(name);
    Map map = (Map) Utils.fromJSONString(rsp.jsonStr());
    assertNull(rsp.jsonStr(), map.get("errorMessages"));
    assertNull(rsp.jsonStr(), map.get("errors"));
    //add
    String url = "/cluster/configset/" + name + "/config/params";
    String payload = "{\n" +
        "  \"set\":{\n" +
        "    \"myFacets\":{\n" +
        "      \"facet\":\"true\",\n" +
        "      \"facet.limit\":50}},\n" +
        "  \"set\":{\n" +
        "    \"myQueries\":{\n" +
        "      \"defType\":\"edismax\",\n" +
        "      \"rows\":\"50\",\n" +
        "      \"df\":\"text_all\"}}\n" +
        "}";
    rsp = new V2Request.Builder(url)
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload(payload)
        .build()
        .process(cloudClient);
    assertEquals(0, rsp.getStatus());
    //get
    url = "/cluster/configset/" + name + "/config/params";
    rsp = new V2Request.Builder(url)
        .withMethod(SolrRequest.METHOD.GET)
        .build()
        .process(cloudClient);
    assertEquals(0, rsp.getStatus());
    Map paramsMap = (LinkedHashMap) rsp.getResponse().get("response");
    Long value = (Long) (((LinkedHashMap) ((LinkedHashMap) paramsMap.get("params")).get("myFacets")).get("facet.limit"));
    assertEquals(50, value.longValue());
  }

  @Test
  public void testAddUpdateProcessor() throws Exception {
    String name = UUID.randomUUID().toString();
    //create
    V2Response rsp = createConfigSet(name);
    Map map = (Map) Utils.fromJSONString(rsp.jsonStr());
    assertNull(rsp.jsonStr(), map.get("errorMessages"));
    assertNull(rsp.jsonStr(), map.get("errors"));
    //add
    String url = "/cluster/configset/" + name + "/config";
    String payload = "{\n" +
        "  \"add-requesthandler\":{\n" +
        "    \"name\":\"/query22\",\n" +
        "    \"class\":\"solr.SearchHandler\",\n" +
        "    \"defaults\":{\n" +
        "      \"echoParams\":\"explicit\",\n" +
        "      \"wt\":\"json\",\n" +
        "      \"indent\":true\n" +
        "    }\n" +
        "  }\n" +
        "}";
    rsp = new V2Request.Builder(url)
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload(payload)
        .build()
        .process(cloudClient);
    assertEquals(0, rsp.getStatus());
    //get
    url = "/cluster/configset/" + name + "/config";
    rsp = new V2Request.Builder(url)
        .withMethod(SolrRequest.METHOD.GET)
        .build()
        .process(cloudClient);
    assertEquals(0, rsp.getStatus());
    Map configMap = (LinkedHashMap) rsp.getResponse().get("config");
    boolean value = ((LinkedHashMap) configMap.get("requestHandler")).containsKey("/query22");
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
