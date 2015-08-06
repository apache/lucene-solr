package org.apache.solr.security;

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
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.TestMiniSolrCloudCluster;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.CommandOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;


public class BasicAuthIntegrationTest extends TestMiniSolrCloudCluster {

  private static final Logger log = LoggerFactory.getLogger(BasicAuthIntegrationTest.class);


  @Override
  protected void doExtraTests(MiniSolrCloudCluster miniCluster, SolrZkClient zkClient, ZkStateReader zkStateReader,
                              CloudSolrClient cloudSolrClient, String defaultCollName) throws Exception {

    NamedList<Object> rsp = cloudSolrClient.request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/authentication", new ModifiableSolrParams()));
    assertNotNull(rsp.get(CommandOperation.ERR_MSGS));
    zkClient.setData("/security.json", STD_CONF.replaceAll("'", "\"").getBytes(UTF_8), true);
    String baseUrl = getRandomReplica(zkStateReader.getClusterState().getCollection(defaultCollName), random()).getStr(BASE_URL_PROP);

    HttpClient cl = cloudSolrClient.getLbClient().getHttpClient();
    verifySecurityStatus(cl, baseUrl + "/admin/authentication", "authentication/class", "solr.BasicAuthPlugin", 20);

    String command = "{\n" +
        "'set-user': {'harry':'HarryIsCool'}\n" +
        "}";

    GenericSolrRequest genericReq = new GenericSolrRequest(SolrRequest.METHOD.POST, "/admin/authentication", new ModifiableSolrParams());
    genericReq.setContentStreams(Collections.<ContentStream>singletonList(new ContentStreamBase.ByteArrayStream(command.getBytes(UTF_8), "")));
    try {
      cloudSolrClient.request(genericReq);
      fail("Should have failed with a 401");
    } catch (HttpSolrClient.RemoteSolrException e) {
    }
    command = "{\n" +
        "'set-user': {'harry':'HarryIsUberCool'}\n" +
        "}";

    HttpPost httpPost = new HttpPost(baseUrl + "/admin/authentication");
    setBasicAuthHeader(httpPost, "solr", "SolrRocks");
    httpPost.setEntity(new ByteArrayEntity(command.getBytes(UTF_8)));
    httpPost.addHeader("Content-Type", "application/json; charset=UTF-8");
    verifySecurityStatus(cl, baseUrl + "/admin/authentication", "authentication.enabled", "true", 20);
    HttpResponse r = cl.execute(httpPost);
    int statusCode = r.getStatusLine().getStatusCode();
    assertEquals("proper_cred sent, but access denied", 200, statusCode);
    baseUrl = getRandomReplica(zkStateReader.getClusterState().getCollection(defaultCollName), random()).getStr(BASE_URL_PROP);

    verifySecurityStatus(cl, baseUrl + "/admin/authentication", "authentication/credentials/harry", NOT_NULL_PREDICATE, 20);
    command = "{\n" +
        "'set-user-role': {'harry':'admin'}\n" +
        "}";

    httpPost = new HttpPost(baseUrl + "/admin/authorization");
    setBasicAuthHeader(httpPost, "solr", "SolrRocks");
    httpPost.setEntity(new ByteArrayEntity(command.getBytes(UTF_8)));
    httpPost.addHeader("Content-Type", "application/json; charset=UTF-8");
    r = cl.execute(httpPost);
    assertEquals(200, r.getStatusLine().getStatusCode());

    baseUrl = getRandomReplica(zkStateReader.getClusterState().getCollection(defaultCollName), random()).getStr(BASE_URL_PROP);
    verifySecurityStatus(cl, baseUrl+"/admin/authorization", "authorization/user-role/harry", NOT_NULL_PREDICATE, 20);


    httpPost = new HttpPost(baseUrl + "/admin/authorization");
    setBasicAuthHeader(httpPost, "harry", "HarryIsUberCool");
    httpPost.setEntity(new ByteArrayEntity(Utils.toJSON(singletonMap("set-permission", Utils.makeMap
        ("name", "x-update",
            "collection", "x",
            "path", "/update/*",
            "role", "dev")))));

    httpPost.addHeader("Content-Type", "application/json; charset=UTF-8");
    verifySecurityStatus(cl, baseUrl + "/admin/authorization", "authorization/user-role/harry", NOT_NULL_PREDICATE, 20);
    r = cl.execute(httpPost);
    assertEquals(200, r.getStatusLine().getStatusCode());

    verifySecurityStatus(cl, baseUrl+"/admin/authorization", "authorization/permissions/x-update/collection", "x", 20);

  }

  public static void verifySecurityStatus(HttpClient cl, String url, String objPath, Object expected, int count) throws Exception {
    boolean success = false;
    String s = null;
    List<String> hierarchy = StrUtils.splitSmart(objPath, '/');
    for (int i = 0; i < count; i++) {
      HttpGet get = new HttpGet(url);
      s = EntityUtils.toString(cl.execute(get).getEntity());
      Map m = (Map) Utils.fromJSONString(s);

      Object actual = Utils.getObjectByPath(m, true, hierarchy);
      if (expected instanceof Predicate) {
        Predicate predicate = (Predicate) expected;
        if (predicate.test(actual)) {
          success = true;
          break;
        }
      } else if (Objects.equals(String.valueOf(actual), expected)) {
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

  static final Predicate NOT_NULL_PREDICATE = new Predicate() {
    @Override
    public boolean test(Object o) {
      return o != null;
    }
  };


  @Override
  public void testErrorsInStartup() throws Exception {
    //don't do anything
  }

  @Override
  public void testErrorsInShutdown() throws Exception {
  }

  //the password is 'SolrRocks'
  //this could be generated everytime. But , then we will not know if there is any regression
  private static final String STD_CONF = "{\n" +
      "  'authentication':{\n" +
      "    'class':'solr.BasicAuthPlugin',\n" +
      "    'credentials':{'solr':'orwp2Ghgj39lmnrZOTm7Qtre1VqHFDfwAEzr0ApbN3Y= Ju5osoAqOX8iafhWpPP01E5P+sg8tK8tHON7rCYZRRw='}},\n" +
      "  'authorization':{\n" +
      "    'class':'solr.RuleBasedAuthorizationPlugin',\n" +
      "    'user-role':{'solr':'admin'},\n" +
      "    'permissions':{'security-edit':{'role':'admin'}}}}";
}
