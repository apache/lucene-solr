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

import java.lang.invoke.MethodHandles;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.Slow
public class TestAuthorizationFramework extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final int TIMEOUT = 10000;
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    try (ZkStateReader zkStateReader = new ZkStateReader(zkServer.getZkAddress(),
        TIMEOUT, TIMEOUT)) {
      zkStateReader.getZkClient().create(ZkStateReader.SOLR_SECURITY_CONF_PATH,
          "{\"authorization\":{\"class\":\"org.apache.solr.security.MockAuthorizationPlugin\"}}".getBytes(StandardCharsets.UTF_8),
          CreateMode.PERSISTENT, true);
    }
  }


  @Test
  public void authorizationFrameworkTest() throws Exception {
    MockAuthorizationPlugin.denyUsers.add("user1");
    MockAuthorizationPlugin.denyUsers.add("user1");
    waitForThingsToLevelOut(10);
    String baseUrl = jettys.get(0).getBaseUrl().toString();
    verifySecurityStatus(cloudClient.getLbClient().getHttpClient(), baseUrl + "/admin/authorization", "authorization/class", MockAuthorizationPlugin.class.getName(), 20);
    log.info("Starting test");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    // This should work fine.
    cloudClient.query(params);

    // This user is blacklisted in the mock. The request should return a 403.
    params.add("uname", "user1");
    try {
      cloudClient.query(params);
      fail("This should have failed");
    } catch (Exception e) {}
    log.info("Ending test");
  }

  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    MockAuthorizationPlugin.denyUsers.clear();

  }

  public static void verifySecurityStatus(HttpClient cl, String url, String objPath, Object expected, int count) throws Exception {
    boolean success = false;
    String s = null;
    List<String> hierarchy = StrUtils.splitSmart(objPath, '/');
    for (int i = 0; i < count; i++) {
      HttpGet get = new HttpGet(url);
      s = EntityUtils.toString(cl.execute(get, HttpClientUtil.createNewHttpClientRequestContext()).getEntity());
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
}
