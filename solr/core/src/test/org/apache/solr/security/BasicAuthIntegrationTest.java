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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Predicate;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.AbstractHttpMessage;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.TestMiniSolrCloudClusterBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.SolrCLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static org.apache.solr.SolrTestCaseJ4.getHttpSolrClient;
import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;

public class BasicAuthIntegrationTest extends TestMiniSolrCloudClusterBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  protected void doExtraTests(MiniSolrCloudCluster miniCluster, SolrZkClient zkClient, ZkStateReader zkStateReader,
                              CloudSolrClient cloudSolrClient, String defaultCollName) throws Exception {


    String authcPrefix = "/admin/authentication";
    String authzPrefix = "/admin/authorization";

    String old = cloudSolrClient.getDefaultCollection();
    cloudSolrClient.setDefaultCollection(null);

    NamedList<Object> rsp;
    HttpClient cl = null;
    try {
      cl = HttpClientUtil.createClient(null);
      String baseUrl = getRandomReplica(zkStateReader.getClusterState().getCollection(defaultCollName), random()).getStr(BASE_URL_PROP);
      verifySecurityStatus(cl, baseUrl + authcPrefix, "/errorMessages", null, 20);
      zkClient.setData("/security.json", STD_CONF.replaceAll("'", "\"").getBytes(UTF_8), true);
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/class", "solr.BasicAuthPlugin", 20);

      boolean found = false;
      for (JettySolrRunner jettySolrRunner : miniCluster.getJettySolrRunners()) {
        if(baseUrl.contains(String.valueOf(jettySolrRunner.getLocalPort()))){
          found = true;
          jettySolrRunner.stop();
          jettySolrRunner.start();
          verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/class", "solr.BasicAuthPlugin", 20);
          break;
        }
      }

      assertTrue("No server found to restart , looking for : "+baseUrl , found);

      String command = "{\n" +
          "'set-user': {'harry':'HarryIsCool'}\n" +
          "}";

      GenericSolrRequest genericReq = new GenericSolrRequest(SolrRequest.METHOD.POST, authcPrefix, new ModifiableSolrParams());
      genericReq.setContentStreams(Collections.singletonList(new ContentStreamBase.ByteArrayStream(command.getBytes(UTF_8), "")));
      try {
        cloudSolrClient.request(genericReq);
        fail("Should have failed with a 401");
      } catch (HttpSolrClient.RemoteSolrException e) {
      }
      command = "{\n" +
          "'set-user': {'harry':'HarryIsUberCool'}\n" +
          "}";

      HttpPost httpPost = new HttpPost(baseUrl + authcPrefix);
      setBasicAuthHeader(httpPost, "solr", "SolrRocks");
      httpPost.setEntity(new ByteArrayEntity(command.getBytes(UTF_8)));
      httpPost.addHeader("Content-Type", "application/json; charset=UTF-8");
      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication.enabled", "true", 20);
      HttpResponse r = cl.execute(httpPost);
      int statusCode = r.getStatusLine().getStatusCode();
      Utils.consumeFully(r.getEntity());
      assertEquals("proper_cred sent, but access denied", 200, statusCode);
      baseUrl = getRandomReplica(zkStateReader.getClusterState().getCollection(defaultCollName), random()).getStr(BASE_URL_PROP);

      verifySecurityStatus(cl, baseUrl + authcPrefix, "authentication/credentials/harry", NOT_NULL_PREDICATE, 20);
      command = "{\n" +
          "'set-user-role': {'harry':'admin'}\n" +
          "}";

      executeCommand(baseUrl + authzPrefix, cl,command, "solr", "SolrRocks");

      baseUrl = getRandomReplica(zkStateReader.getClusterState().getCollection(defaultCollName), random()).getStr(BASE_URL_PROP);
      verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/user-role/harry", NOT_NULL_PREDICATE, 20);

      executeCommand(baseUrl + authzPrefix, cl, Utils.toJSONString(singletonMap("set-permission", Utils.makeMap
          ("collection", "x",
              "path", "/update/*",
              "role", "dev"))), "harry", "HarryIsUberCool" );

      verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/permissions[1]/collection", "x", 20);

      executeCommand(baseUrl + authzPrefix, cl,Utils.toJSONString(singletonMap("set-permission", Utils.makeMap
          ("name", "collection-admin-edit", "role", "admin"))), "harry", "HarryIsUberCool"  );
      verifySecurityStatus(cl, baseUrl + authzPrefix, "authorization/permissions[2]/name", "collection-admin-edit", 20);

      CollectionAdminRequest.Reload reload = CollectionAdminRequest.reloadCollection(defaultCollName);

      try (HttpSolrClient solrClient = getHttpSolrClient(baseUrl)) {
        try {
          rsp = solrClient.request(reload);
          fail("must have failed");
        } catch (HttpSolrClient.RemoteSolrException e) {

        }
        reload.setMethod(SolrRequest.METHOD.POST);
        try {
          rsp = solrClient.request(reload);
          fail("must have failed");
        } catch (HttpSolrClient.RemoteSolrException e) {

        }
      }
      cloudSolrClient.request(CollectionAdminRequest.reloadCollection(defaultCollName)
          .setBasicAuthCredentials("harry", "HarryIsUberCool"));

      try {
        cloudSolrClient.request(CollectionAdminRequest.reloadCollection(defaultCollName)
            .setBasicAuthCredentials("harry", "Cool12345"));
        fail("This should not succeed");
      } catch (HttpSolrClient.RemoteSolrException e) {

      }

      cloudSolrClient.setDefaultCollection(old);
      executeCommand(baseUrl + authzPrefix, cl,"{set-permission : { name : update , role : admin}}", "harry", "HarryIsUberCool");

      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id","4");
      UpdateRequest update = new UpdateRequest();
      update.setBasicAuthCredentials("harry","HarryIsUberCool");
      update.add(doc);
      update.setCommitWithin(100);
      cloudSolrClient.request(update);


      executeCommand(baseUrl + authzPrefix, cl, "{set-property : { blockUnknown: true}}", "harry", "HarryIsUberCool");
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
      executeCommand(baseUrl + authzPrefix, cl, "{set-property : { blockUnknown: false}}", "harry", "HarryIsUberCool");
    } finally {
      if (cl != null) {
        HttpClientUtil.close(cl);
      }
    }
  }

  public static void executeCommand(String url, HttpClient cl, String payload, String user, String pwd) throws IOException {
    HttpPost httpPost;
    HttpResponse r;
    httpPost = new HttpPost(url);
    setBasicAuthHeader(httpPost, user, pwd);
    httpPost.setEntity(new ByteArrayEntity(payload.getBytes(UTF_8)));
    httpPost.addHeader("Content-Type", "application/json; charset=UTF-8");
    r = cl.execute(httpPost);
    assertEquals(200, r.getStatusLine().getStatusCode());
    Utils.consumeFully(r.getEntity());
  }

  public static void verifySecurityStatus(HttpClient cl, String url, String objPath, Object expected, int count) throws Exception {
    boolean success = false;
    String s = null;
    List<String> hierarchy = StrUtils.splitSmart(objPath, '/');
    for (int i = 0; i < count; i++) {
      HttpGet get = new HttpGet(url);
      HttpResponse rsp = cl.execute(get);
      s = EntityUtils.toString(rsp.getEntity());
      Map m = (Map) Utils.fromJSONString(s);
      Utils.consumeFully(rsp.getEntity());
      Object actual = Utils.getObjectByPath(m, true, hierarchy);
      if (expected instanceof Predicate) {
        Predicate predicate = (Predicate) expected;
        if (predicate.test(actual)) {
          success = true;
          break;
        }
      } else if (Objects.equals(actual == null ? null : String.valueOf(actual), expected)) {
        success = true;
        break;
      }
      Thread.sleep(50);
    }
    assertTrue("No match for " + objPath + " = " + expected + ", full response = " + s, success);

  }

  public static void setBasicAuthHeader(AbstractHttpMessage httpMsg, String user, String pwd) {
    String userPass = user + ":" + pwd;
    String encoded = Base64.byteArrayToBase64(userPass.getBytes(UTF_8));
    httpMsg.setHeader(new BasicHeader("Authorization", "Basic " + encoded));
    log.info("Added Basic Auth security Header {}",encoded );
  }

  public static Replica getRandomReplica(DocCollection coll, Random random) {
    ArrayList<Replica> l = new ArrayList<>();

    for (Slice slice : coll.getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        l.add(replica);
      }
    }
    Collections.shuffle(l, random);
    return l.isEmpty() ? null : l.get(0);
  }

  static final Predicate NOT_NULL_PREDICATE = o -> o != null;

  //the password is 'SolrRocks'
  //this could be generated everytime. But , then we will not know if there is any regression
  private static final String STD_CONF = "{\n" +
      "  'authentication':{\n" +
      "    'class':'solr.BasicAuthPlugin',\n" +
      "    'credentials':{'solr':'orwp2Ghgj39lmnrZOTm7Qtre1VqHFDfwAEzr0ApbN3Y= Ju5osoAqOX8iafhWpPP01E5P+sg8tK8tHON7rCYZRRw='}},\n" +
      "  'authorization':{\n" +
      "    'class':'solr.RuleBasedAuthorizationPlugin',\n" +
      "    'user-role':{'solr':'admin'},\n" +
      "    'permissions':[{'name':'security-edit','role':'admin'}]}}";
}
