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
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

public class V2ApiIntegrationTest extends SolrCloudTestCase {
  private static String COLL_NAME = "collection1";

  @BeforeClass
  public static void createCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(2)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
        .configure();
    CollectionAdminRequest.createCollection(COLL_NAME, "conf1", 1, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLL_NAME, 1, 2);
  }

  @Test
  public void testWelcomeMessage() throws Exception {
    V2Response res = new V2Request.Builder("").build().process(cluster.getSolrClient());
    assertEquals(0, res.getStatus());

    res = new V2Request.Builder("/_introspect").build().process(cluster.getSolrClient());
    assertEquals(0, res.getStatus());
  }

  private void testException(ResponseParser responseParser, int expectedCode, String path, String payload) throws IOException, SolrServerException {
    V2Request v2Request = new V2Request.Builder(path)
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload(payload)
        .build();
    v2Request.setResponseParser(responseParser);
    HttpSolrClient.RemoteSolrException ex =  expectThrows(HttpSolrClient.RemoteSolrException.class,
        () -> v2Request.process(cluster.getSolrClient()));
    assertEquals(expectedCode, ex.code());
  }

  @Test
  public void testException() throws Exception {
    String notFoundPath = "/c/" + COLL_NAME + "/abccdef";
    String incorrectPayload = "{rebalance-leaders: {maxAtOnce: abc, maxWaitSeconds: xyz}}";
    testException(new XMLResponseParser(),404,
        notFoundPath, incorrectPayload);
    testException(new DelegationTokenResponse.JsonMapResponseParser(),404,
        notFoundPath, incorrectPayload);
    testException(new BinaryResponseParser(),404,
        notFoundPath, incorrectPayload);
    testException(new XMLResponseParser(), 400, "/c/" + COLL_NAME, incorrectPayload);
    testException(new BinaryResponseParser(), 400, "/c/" + COLL_NAME, incorrectPayload);
    testException(new DelegationTokenResponse.JsonMapResponseParser(), 400, "/c/" + COLL_NAME, incorrectPayload);
  }

  private long getStatus(V2Response response) {
    Object header = response.getResponse().get("responseHeader");
    if (header instanceof NamedList) {
      return (int) ((NamedList) header).get("status");
    } else {
      return (long) ((Map) header).get("status");
    }
  }

  @Test
  public void testIntrospect() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command","XXXX");
    params.set("method", "POST");
    @SuppressWarnings({"rawtypes"})
    Map result = resAsMap(cluster.getSolrClient(),
        new V2Request.Builder("/c/"+COLL_NAME+"/_introspect")
            .withParams(params).build());
    assertEquals("Command not found!", Utils.getObjectByPath(result, false, "/spec[0]/commands/XXXX"));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testWTParam() throws Exception {
    V2Request request = new V2Request.Builder("/c/" + COLL_NAME + "/get/_introspect").build();
    // TODO: If possible do this in a better way
    request.setResponseParser(new NoOpResponseParser("bleh"));

    Map resp = resAsMap(cluster.getSolrClient(), request);
    String respString = resp.toString();

    assertFalse(respString.contains("<body><h2>HTTP ERROR 500</h2>"));
    assertFalse(respString.contains("500"));
    assertFalse(respString.contains("NullPointerException"));
    assertFalse(respString.contains("<p>Problem accessing /solr/____v2/c/collection1/get/_introspect. Reason:"));
    // since no-op response writer is used, doing contains match
    assertTrue(respString.contains("/c/collection1/get"));

    // no response parser
    request.setResponseParser(null);
    resp = resAsMap(cluster.getSolrClient(), request);
    respString = resp.toString();

    assertFalse(respString.contains("<body><h2>HTTP ERROR 500</h2>"));
    assertFalse(respString.contains("<p>Problem accessing /solr/____v2/c/collection1/get/_introspect. Reason:"));
    assertEquals("/c/collection1/get", Utils.getObjectByPath(resp, true, "/spec[0]/url/paths[0]"));
    assertEquals(respString, 0, Utils.getObjectByPath(resp, true, "/responseHeader/status"));
  }

  @Test
  public void testSingleWarning() throws Exception {
    @SuppressWarnings({"rawtypes"})
    NamedList resp = cluster.getSolrClient().request(
        new V2Request.Builder("/c/"+COLL_NAME+"/_introspect").build());
    @SuppressWarnings({"rawtypes"})
    List warnings = resp.getAll("WARNING");
    assertEquals(1, warnings.size());
  }

  @Test
  public void testSetPropertyValidationOfCluster() throws IOException, SolrServerException {
    @SuppressWarnings({"rawtypes"})
    NamedList resp = cluster.getSolrClient().request(
      new V2Request.Builder("/cluster").withMethod(SolrRequest.METHOD.POST).withPayload("{set-property: {name: autoAddReplicas, val:false}}").build());
    assertTrue(resp.toString().contains("status=0"));
    resp = cluster.getSolrClient().request(
        new V2Request.Builder("/cluster").withMethod(SolrRequest.METHOD.POST).withPayload("{set-property: {name: autoAddReplicas, val:null}}").build());
    assertTrue(resp.toString().contains("status=0"));
  }

  @Test
  public void testCollectionsApi() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    @SuppressWarnings({"rawtypes"})
    Map result = resAsMap(client, new V2Request.Builder("/c/"+COLL_NAME+"/get/_introspect").build());
    assertEquals("/c/collection1/get", Utils.getObjectByPath(result, true, "/spec[0]/url/paths[0]"));
    result = resAsMap(client, new V2Request.Builder("/collections/"+COLL_NAME+"/get/_introspect").build());
    assertEquals("/collections/collection1/get", Utils.getObjectByPath(result, true, "/spec[0]/url/paths[0]"));
    String tempDir = createTempDir().toFile().getPath();
    Map<String, Object> backupPayload = new HashMap<>();
    Map<String, Object> backupParams = new HashMap<>();
    backupPayload.put("backup-collection", backupParams);
    backupParams.put("name", "backup_test");
    backupParams.put("collection", COLL_NAME);
    backupParams.put("location", tempDir);
    cluster.getJettySolrRunners().forEach(j -> j.getCoreContainer().getAllowPaths().add(Paths.get(tempDir)));
    client.request(new V2Request.Builder("/c")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload(Utils.toJSONString(backupPayload))
        .build());
  }

  @Test
  public void testSelect() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    final V2Response v2Response = new V2Request.Builder("/c/" + COLL_NAME + "/select")
        .withMethod(SolrRequest.METHOD.GET)
        .withParams(params("q", "-*:*"))
        .build()
        .process(cloudClient);
    assertEquals(0, ((SolrDocumentList)v2Response.getResponse().get("response")).getNumFound());
  }
  
  @SuppressWarnings({"rawtypes"})
  private Map resAsMap(CloudSolrClient client, V2Request request) throws SolrServerException, IOException {
    NamedList<Object> rsp = client.request(request);
    return rsp.asMap(100);
  }
}
