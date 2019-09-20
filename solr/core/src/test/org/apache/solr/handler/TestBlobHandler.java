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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.SimplePostTool;
import org.junit.Test;
import org.noggit.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static org.apache.solr.common.util.Utils.fromJSONString;

public class TestBlobHandler extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void doBlobHandlerTest() throws Exception {

    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      CollectionAdminResponse response1;
      CollectionAdminRequest.Create createCollectionRequest = CollectionAdminRequest.createCollection(".system",1,2);
      response1 = createCollectionRequest.process(client);
      assertEquals(0, response1.getStatus());
      assertTrue(response1.isSuccess());
      DocCollection sysColl = cloudClient.getZkStateReader().getClusterState().getCollection(".system");
      Replica replica = sysColl.getActiveSlicesMap().values().iterator().next().getLeader();

      String baseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
      String url = baseUrl + "/.system/config/requestHandler";
      MapWriter map = TestSolrConfigHandlerConcurrent.getAsMap(url, cloudClient);
      assertNotNull(map);
      assertEquals("solr.BlobHandler", map._get(asList(
          "config",
          "requestHandler",
          "/blob",
          "class"),null));
      map = TestSolrConfigHandlerConcurrent.getAsMap(baseUrl + "/.system/schema/fields/blob", cloudClient);
      assertNotNull(map);
      assertEquals("blob", map._get(asList(
          "field",
          "name"),null));
      assertEquals("bytes", map._get( asList(
          "field",
          "type"),null));

      checkBlobPost(baseUrl, cloudClient);
      checkBlobPostMd5(baseUrl, cloudClient);
    }
  }

  static void checkBlobPost(String baseUrl, CloudSolrClient cloudClient) throws Exception {
    String url;
    MapWriter map;
    byte[] bytarr = new byte[1024];
    for (int i = 0; i < bytarr.length; i++) bytarr[i] = (byte) (i % 127);
    byte[] bytarr2 = new byte[2048];
    for (int i = 0; i < bytarr2.length; i++) bytarr2[i] = (byte) (i % 127);
    String blobName = "test";
    postAndCheck(cloudClient, baseUrl, blobName, ByteBuffer.wrap(bytarr), 1);
    postAndCheck(cloudClient, baseUrl, blobName, ByteBuffer.wrap(bytarr2), 2);

    url = baseUrl + "/.system/blob/test/1";
    map = TestSolrConfigHandlerConcurrent.getAsMap(url, cloudClient);
    assertEquals("" + bytarr.length, map._getStr("response/docs[0]/size",null));

    compareInputAndOutput(baseUrl + "/.system/blob/test?wt=filestream", bytarr2, cloudClient);
    compareInputAndOutput(baseUrl + "/.system/blob/test/1?wt=filestream", bytarr, cloudClient);
  }

  static void checkBlobPostMd5(String baseUrl, CloudSolrClient cloudClient) throws Exception {
    String blobName = "md5Test";
    String stringValue = "MHMyugAGUxFzeqbpxVemACGbQ"; // Random string requires padding in md5 hash
    String stringValueMd5 = "02d82dd5aabc47fae54ee3dd236ad83d";
    postAndCheck(cloudClient, baseUrl, blobName, ByteBuffer.wrap(stringValue.getBytes(StandardCharsets.UTF_8)), 1);
    MapWriter map = TestSolrConfigHandlerConcurrent.getAsMap(baseUrl + "/.system/blob/" + blobName, cloudClient);
    assertEquals(stringValueMd5, map._getStr("response/docs[0]/md5", null));
  }

  public static void createSystemCollection(SolrClient client) throws SolrServerException, IOException {
    CollectionAdminResponse response1;
    CollectionAdminRequest.Create createCollectionRequest = CollectionAdminRequest.createCollection(".system",1,2);
    response1 = createCollectionRequest.process(client);
    assertEquals(0, response1.getStatus());
    assertTrue(response1.isSuccess());
  }

  public static void postAndCheck(CloudSolrClient cloudClient, String baseUrl, String blobName, ByteBuffer bytes, int count) throws Exception {
    postData(cloudClient, baseUrl, blobName, bytes);

    String url;
    MapWriter map = null;
    final RTimer timer = new RTimer();
    int i = 0;
    for (; i < 150; i++) {//15 secs
      url = baseUrl + "/.system/blob/" + blobName;
      map = TestSolrConfigHandlerConcurrent.getAsMap(url, cloudClient);
      String numFound = map._getStr(asList("response", "numFound"),null);
      if (!("" + count).equals(numFound)) {
        Thread.sleep(100);
        continue;
      }

      assertEquals("" + bytes.limit(), map._getStr("response/docs[0]/size",null));
      return;
    }
    fail(StrUtils.formatString("Could not successfully add blob after {0} attempts. Expecting {1} items. time elapsed {2}  output  for url is {3}",
        i, count, timer.getTime(), map.toString()));
  }

  static void compareInputAndOutput(String url, byte[] bytarr, CloudSolrClient cloudClient) throws IOException {

    HttpClient httpClient = cloudClient.getLbClient().getHttpClient();

    HttpGet httpGet = new HttpGet(url);
    HttpResponse entity = httpClient.execute(httpGet);
    ByteBuffer b = SimplePostTool.inputStreamToByteArray(entity.getEntity().getContent());
    try {
      assertEquals(b.limit(), bytarr.length);
      for (int i = 0; i < bytarr.length; i++) {
        assertEquals(b.get(i), bytarr[i]);
      }
    } finally {
      httpGet.releaseConnection();
    }

  }

  public static void postData(CloudSolrClient cloudClient, String baseUrl, String blobName, ByteBuffer bytarr) throws IOException {
    HttpPost httpPost = null;
    HttpEntity entity;
    String response = null;
    try {
      httpPost = new HttpPost(baseUrl + "/.system/blob/" + blobName);
      httpPost.setHeader("Content-Type", "application/octet-stream");
      httpPost.setEntity(new ByteArrayEntity(bytarr.array(), bytarr.arrayOffset(), bytarr.limit()));
      entity = cloudClient.getLbClient().getHttpClient().execute(httpPost).getEntity();
      try {
        response = EntityUtils.toString(entity, StandardCharsets.UTF_8);
        Map m = (Map) fromJSONString(response);
        assertFalse("Error in posting blob " + m.toString(), m.containsKey("error"));
      } catch (JSONParser.ParseException e) {
        log.error("$ERROR$", response, e);
        fail();
      }
    } finally {
      httpPost.releaseConnection();
    }
  }
}
