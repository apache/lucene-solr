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
package org.apache.solr.security;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter.StringPayloadContentWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.SolrCLI;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;

public class BasicAuthIntegrationTest extends SolrCloudAuthTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION = "authCollection";

  @Before
  public void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 3, 1).process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(COLLECTION, 3, 3);
  }
  
  @After
  public void tearDownCluster() throws Exception {
    shutdownCluster();
  }

  @Test
  //commented 9-Aug-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 21-May-2018
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  @LogLevel("org.apache.solr.security=DEBUG")
  public void testBasicAuth() throws Exception {
    boolean isUseV2Api = random().nextBoolean();
    String authcPrefix = "/admin/authentication";
    String authzPrefix = "/admin/authorization";
    if(isUseV2Api){
      authcPrefix = "/____v2/cluster/security/authentication";
      authzPrefix = "/____v2/cluster/security/authorization";
    }

    NamedList<Object> rsp;
    HttpClient cl = null;
    try {
      cl = HttpClientUtil.createClient(null);

      JettySolrRunner randomJetty = cluster.getRandomJetty(random());
      String baseUrl = randomJetty.getBaseUrl().toString();
      verifySecurityStatus(cl, baseUrl + authcPrefix, "/errorMessages", null, 20);
      zkClient().setData("/security.json", STD_CONF.replaceAll("'", "\"").getBytes(UTF_8), true);
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/class", "solr.BasicAuthPlugin", 20);

      randomJetty.stop();
      
      cluster.waitForJettyToStop(randomJetty);
      
      randomJetty.start();
      
      cluster.waitForAllNodes(30);
      
      cluster.waitForActiveCollection(COLLECTION, 3, 3);
      
      baseUrl = randomJetty.getBaseUrl().toString();
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/class", "solr.BasicAuthPlugin", 20);
      assertNumberOfMetrics(16); // Basic auth metrics available
      assertAuthMetricsMinimums(1, 0, 1, 0, 0, 0);
      assertPkiAuthMetricsMinimums(0, 0, 0, 0, 0, 0);
      
      String command = "{\n" +
          "'set-user': {'harry':'HarryIsCool'}\n" +
          "}";

      final SolrRequest genericReq;
      if (isUseV2Api) {
        genericReq = new V2Request.Builder("/cluster/security/authentication").withMethod(SolrRequest.METHOD.POST).build();
      } else {
        genericReq = new GenericSolrRequest(SolrRequest.METHOD.POST, authcPrefix, new ModifiableSolrParams());
        ((GenericSolrRequest)genericReq).setContentWriter(new StringPayloadContentWriter(command, CommonParams.JSON_MIME));
      }

      // avoid bad connection races due to shutdown
      cluster.getSolrClient().getHttpClient().getConnectionManager().closeExpiredConnections();
      cluster.getSolrClient().getHttpClient().getConnectionManager().closeIdleConnections(1, TimeUnit.MILLISECONDS);
      
      HttpSolrClient.RemoteSolrException exp = expectThrows(HttpSolrClient.RemoteSolrException.class, () -> {
        cluster.getSolrClient().request(genericReq);
      });
      assertEquals(401, exp.code());
      assertAuthMetricsMinimums(2, 0, 2, 0, 0, 0);
      assertPkiAuthMetricsMinimums(0, 0, 0, 0, 0, 0);
      
      command = "{\n" +
          "'set-user': {'harry':'HarryIsUberCool'}\n" +
          "}";

      HttpPost httpPost = new HttpPost(baseUrl + authcPrefix);
      setAuthorizationHeader(httpPost, makeBasicAuthHeader("solr", "SolrRocks"));
      httpPost.setEntity(new ByteArrayEntity(command.getBytes(UTF_8)));
      httpPost.addHeader("Content-Type", "application/json; charset=UTF-8");
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication.enabled", "true", 20);
      HttpResponse r = cl.execute(httpPost);
      int statusCode = r.getStatusLine().getStatusCode();
      Utils.consumeFully(r.getEntity());
      assertEquals("proper_cred sent, but access denied", 200, statusCode);
      assertPkiAuthMetricsMinimums(0, 0, 0, 0, 0, 0);
      assertAuthMetricsMinimums(4, 1, 3, 0, 0, 0);

      baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();

      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/credentials/harry", NOT_NULL_PREDICATE, 20);
      command = "{\n" +
          "'set-user-role': {'harry':'admin'}\n" +
          "}";

      executeCommand(baseUrl + authzPrefix, cl,command, "solr", "SolrRocks");
      assertAuthMetricsMinimums(5, 2, 3, 0, 0, 0);

      baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
      verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/user-role/harry", NOT_NULL_PREDICATE, 20);

      executeCommand(baseUrl + authzPrefix, cl, Utils.toJSONString(singletonMap("set-permission", Utils.makeMap
          ("collection", "x",
              "path", "/update/*",
              "role", "dev"))), "harry", "HarryIsUberCool" );

      verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/permissions[1]/collection", "x", 20);
      assertAuthMetricsMinimums(8, 3, 5, 0, 0, 0);

      executeCommand(baseUrl + authzPrefix, cl,Utils.toJSONString(singletonMap("set-permission", Utils.makeMap
          ("name", "collection-admin-edit", "role", "admin"))), "harry", "HarryIsUberCool"  );
      verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/permissions[2]/name", "collection-admin-edit", 20);
      assertAuthMetricsMinimums(10, 4, 6, 0, 0, 0);

      CollectionAdminRequest.Reload reload = CollectionAdminRequest.reloadCollection(COLLECTION);

      try (HttpSolrClient solrClient = getHttpSolrClient(baseUrl)) {
        expectThrows(HttpSolrClient.RemoteSolrException.class, () -> solrClient.request(reload));
        reload.setMethod(SolrRequest.METHOD.POST);
        expectThrows(HttpSolrClient.RemoteSolrException.class, () -> solrClient.request(reload));
      }
      cluster.getSolrClient().request(CollectionAdminRequest.reloadCollection(COLLECTION)
          .setBasicAuthCredentials("harry", "HarryIsUberCool"));

      expectThrows(HttpSolrClient.RemoteSolrException.class, () -> {
        cluster.getSolrClient().request(CollectionAdminRequest.reloadCollection(COLLECTION)
            .setBasicAuthCredentials("harry", "Cool12345"));
      });
      assertAuthMetricsMinimums(14, 5, 8, 1, 0, 0);

      executeCommand(baseUrl + authzPrefix, cl,"{set-permission : { name : update , role : admin}}", "harry", "HarryIsUberCool");

      UpdateRequest del = new UpdateRequest().deleteByQuery("*:*");
      del.setBasicAuthCredentials("harry","HarryIsUberCool");
      del.setCommitWithin(10);
      del.process(cluster.getSolrClient(), COLLECTION);

      //Test for SOLR-12514. Create a new jetty . This jetty does not have the collection.
      //Make a request to that jetty and it should fail
      JettySolrRunner aNewJetty = cluster.startJettySolrRunner();
      SolrClient aNewClient = aNewJetty.newClient();
      UpdateRequest delQuery = null;
      delQuery = new UpdateRequest().deleteByQuery("*:*");
      delQuery.setBasicAuthCredentials("harry","HarryIsUberCool");
      delQuery.process(aNewClient, COLLECTION);//this should succeed
      try {
        HttpSolrClient.RemoteSolrException e = expectThrows(HttpSolrClient.RemoteSolrException.class, () -> {
          new UpdateRequest().deleteByQuery("*:*").process(aNewClient, COLLECTION);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Authentication failed"));
      } finally {
        aNewClient.close();
        cluster.stopJettySolrRunner(aNewJetty);
      }

      addDocument("harry","HarryIsUberCool","id", "4");

      executeCommand(baseUrl + authcPrefix, cl, "{set-property : { blockUnknown: true}}", "harry", "HarryIsUberCool");
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/blockUnknown", "true", 20, "harry", "HarryIsUberCool");
      verifySecurityStatus(cl, baseUrl + "/admin/info/key", "key", NOT_NULL_PREDICATE, 20);
      assertAuthMetricsMinimums(17, 8, 8, 1, 0, 0);

      String[] toolArgs = new String[]{
          "status", "-solr", baseUrl};
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream stdoutSim = new PrintStream(baos, true, StandardCharsets.UTF_8.name());
      SolrCLI.StatusTool tool = new SolrCLI.StatusTool(stdoutSim);
      try {
        System.setProperty("basicauth", "harry:HarryIsUberCool");
        tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), toolArgs));
        Map obj = (Map) Utils.fromJSON(new ByteArrayInputStream(baos.toByteArray()));
        assertTrue(obj.containsKey("version"));
        assertTrue(obj.containsKey("startTime"));
        assertTrue(obj.containsKey("uptime"));
        assertTrue(obj.containsKey("memory"));
      } catch (Exception e) {
        log.error("RunExampleTool failed due to: " + e +
            "; stdout from tool prior to failure: " + baos.toString(StandardCharsets.UTF_8.name()));
      }

      SolrParams params = new MapSolrParams(Collections.singletonMap("q", "*:*"));
      // Query that fails due to missing credentials
      exp = expectThrows(HttpSolrClient.RemoteSolrException.class, () -> {
        cluster.getSolrClient().query(COLLECTION, params);
      });
      assertEquals(401, exp.code());
      assertAuthMetricsMinimums(19, 8, 8, 1, 2, 0);
      assertPkiAuthMetricsMinimums(3, 3, 0, 0, 0, 0);

      // Query that succeeds
      GenericSolrRequest req = new GenericSolrRequest(SolrRequest.METHOD.GET, "/select", params);
      req.setBasicAuthCredentials("harry", "HarryIsUberCool");
      cluster.getSolrClient().request(req, COLLECTION);
      
      assertAuthMetricsMinimums(20, 8, 8, 1, 2, 0);
      assertPkiAuthMetricsMinimums(10, 10, 0, 0, 0, 0);

      addDocument("harry","HarryIsUberCool","id", "5");
      assertAuthMetricsMinimums(23, 11, 9, 1, 2, 0);
      assertPkiAuthMetricsMinimums(14, 14, 0, 0, 0, 0);

      // Reindex collection depends on streaming request that needs to authenticate against new collection
      CollectionAdminRequest.ReindexCollection reindexReq = CollectionAdminRequest.reindexCollection(COLLECTION);
      reindexReq.setBasicAuthCredentials("harry", "HarryIsUberCool");
      cluster.getSolrClient().request(reindexReq, COLLECTION);
      assertAuthMetricsMinimums(24, 12, 9, 1, 2, 0);
      assertPkiAuthMetricsMinimums(15, 15, 0, 0, 0, 0);

      // Validate forwardCredentials
      assertEquals(1, executeQuery(params("q", "id:5"), "harry", "HarryIsUberCool").getResults().getNumFound());
      assertAuthMetricsMinimums(25, 13, 9, 1, 2, 0);
      assertPkiAuthMetricsMinimums(19, 19, 0, 0, 0, 0);
      executeCommand(baseUrl + authcPrefix, cl, "{set-property : { forwardCredentials: true}}", "harry", "HarryIsUberCool");
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/forwardCredentials", "true", 20, "harry", "HarryIsUberCool");
      assertEquals(1, executeQuery(params("q", "id:5"), "harry", "HarryIsUberCool").getResults().getNumFound());
      assertAuthMetricsMinimums(32, 20, 9, 1, 2, 0);
      assertPkiAuthMetricsMinimums(19, 19, 0, 0, 0, 0);
      
      executeCommand(baseUrl + authcPrefix, cl, "{set-property : { blockUnknown: false}}", "harry", "HarryIsUberCool");
    } finally {
      if (cl != null) {
        HttpClientUtil.close(cl);
      }
    }
  }

  private void assertNumberOfMetrics(int num) {
    MetricRegistry registry0 = cluster.getJettySolrRunner(0).getCoreContainer().getMetricManager().registry("solr.node");
    assertNotNull(registry0);

    assertEquals(num, registry0.getMetrics().entrySet().stream().filter(e -> e.getKey().startsWith("SECURITY")).count());
  }

  private QueryResponse executeQuery(ModifiableSolrParams params, String user, String pass) throws IOException, SolrServerException {
    SolrRequest req = new QueryRequest(params);
    req.setBasicAuthCredentials(user, pass);
    QueryResponse resp = (QueryResponse) req.process(cluster.getSolrClient(), COLLECTION);
    assertNull(resp.getException());
    assertEquals(0, resp.getStatus());
    return resp;
  }

  private void addDocument(String user, String pass, String... fields) throws IOException, SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();
    boolean isKey = true;
    String key = null;
    for (String field : fields) {
      if (isKey) {
        key = field;
        isKey = false;
      } else {
        doc.setField(key, field);
      }
    }
    UpdateRequest update = new UpdateRequest();
    update.setBasicAuthCredentials(user, pass);
    update.add(doc);
    cluster.getSolrClient().request(update, COLLECTION);
    update.commit(cluster.getSolrClient(), COLLECTION);
  }

  public static void executeCommand(String url, HttpClient cl, String payload,
                                    String user, String pwd) throws Exception {

    // HACK: work around for SOLR-13464...
    //
    // note the authz/authn objects in use on each node before executing the command,
    // then wait until we see new objects on every node *after* executing the command
    // before returning...
    final Set<Map.Entry<String,Object>> initialPlugins
      = getAuthPluginsInUseForCluster(url).entrySet();

    HttpPost httpPost;
    HttpResponse r;
    httpPost = new HttpPost(url);
    setAuthorizationHeader(httpPost, makeBasicAuthHeader(user, pwd));
    httpPost.setEntity(new ByteArrayEntity(payload.getBytes(UTF_8)));
    httpPost.addHeader("Content-Type", "application/json; charset=UTF-8");
    r = cl.execute(httpPost);
    String response = IOUtils.toString(r.getEntity().getContent(), StandardCharsets.UTF_8);
    assertEquals("Non-200 response code. Response was " + response, 200, r.getStatusLine().getStatusCode());
    assertFalse("Response contained errors: " + response, response.contains("errorMessages"));
    Utils.consumeFully(r.getEntity());

    // HACK (continued)...
    final TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeout.waitFor("core containers never fully updated their auth plugins",
                    () -> {
                      final Set<Map.Entry<String,Object>> tmpSet
                        = getAuthPluginsInUseForCluster(url).entrySet();
                      tmpSet.retainAll(initialPlugins);
                      return tmpSet.isEmpty();
                    });
  }

  public static Replica getRandomReplica(DocCollection coll, Random random) {
    ArrayList<Replica> l = new ArrayList<>();

    for (Slice slice : coll.getSlices()) {
      l.addAll(slice.getReplicas());
    }
    Collections.shuffle(l, random);
    return l.isEmpty() ? null : l.get(0);
  }

  //the password is 'SolrRocks'
  //this could be generated everytime. But , then we will not know if there is any regression
  protected static final String STD_CONF = "{\n" +
      "  'authentication':{\n" +
      "    'blockUnknown':'false',\n" +
      "    'class':'solr.BasicAuthPlugin',\n" +
      "    'credentials':{'solr':'orwp2Ghgj39lmnrZOTm7Qtre1VqHFDfwAEzr0ApbN3Y= Ju5osoAqOX8iafhWpPP01E5P+sg8tK8tHON7rCYZRRw='}},\n" +
      "  'authorization':{\n" +
      "    'class':'solr.RuleBasedAuthorizationPlugin',\n" +
      "    'user-role':{'solr':'admin'},\n" +
      "    'permissions':[{'name':'security-edit','role':'admin'}]}}";
}
