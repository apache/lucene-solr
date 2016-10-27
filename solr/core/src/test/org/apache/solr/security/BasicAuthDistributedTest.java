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

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.handler.admin.SecurityConfHandlerLocalForTesting;
import org.junit.Test;

import static org.apache.solr.security.BasicAuthIntegrationTest.STD_CONF;
import static org.apache.solr.security.BasicAuthIntegrationTest.verifySecurityStatus;

/**
 * Tests basicAuth in a multi shard env
 */
@Slow
public class BasicAuthDistributedTest extends BaseDistributedSearchTestCase {
  public BasicAuthDistributedTest() {
    super();
    schemaString = "schema.xml";
  }

  private SecurityConfHandlerLocalForTesting securityConfHandler;

  @Test
  public void test() throws Exception {
    index();
    testAuth();
  }

  private void index() throws Exception {
    del("*:*");
    indexr(id, "1", "text", "doc one");
    indexr(id, "2", "text", "doc two");
    indexr(id, "3", "text", "doc three");
    indexr(id, "4", "text", "doc four");
    indexr(id, "5", "text", "doc five");

    commit();  // try to ensure there's more than one segment

    indexr(id, "6", "text", "doc six");
    indexr(id, "7", "text", "doc seven");
    indexr(id, "8", "text", "doc eight");
    indexr(id, "9", "text", "doc nine");
    indexr(id, "10", "text", "doc ten");

    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);
    handle.put("_version_", SKIPVAL);
    Thread.sleep(1000);
  }
  
  private void testAuth() throws Exception {
    System.out.println("Got "+jettys.size()+" jettys and "+ clients.size() +" clients");
    QueryResponse rsp = query("q","text:doc", "fl", "id,text", "sort", "id asc");
    assertEquals(10, rsp.getResults().getNumFound());

    for (JettySolrRunner j : jettys) {
      securityConfHandler = new SecurityConfHandlerLocalForTesting(j.getCoreContainer());
      securityConfHandler.persistConf(new SecurityConfHandler.SecurityConfig()
          .setData(Utils.fromJSONString(ALL_CONF.replaceAll("'", "\""))));
      j.getCoreContainer().securityNodeChanged();
    }

    HttpSolrClient.RemoteSolrException expected = expectThrows(HttpSolrClient.RemoteSolrException.class, () -> {
      query("q","text:doc", "fl", "id,text", "sort", "id asc");
    });
    assertEquals(401, expected.code());
    
    // TODO: Query with auth
    
    // Remove auth again
    for (JettySolrRunner j : jettys) {
      securityConfHandler = new SecurityConfHandlerLocalForTesting(j.getCoreContainer());
      Files.delete(Paths.get(j.getSolrHome()).resolve("security.json"));
      j.getCoreContainer().securityNodeChanged();
    }

  }
  
  protected static final String ALL_CONF = "{\n" +
      "  'authentication':{\n" +
      "    'blockUnknown':true,\n" +
      "    'class':'solr.BasicAuthPlugin',\n" +
      "    'credentials':{'solr':'orwp2Ghgj39lmnrZOTm7Qtre1VqHFDfwAEzr0ApbN3Y= Ju5osoAqOX8iafhWpPP01E5P+sg8tK8tHON7rCYZRRw='}},\n" +
      "  'authorization':{\n" +
      "    'class':'solr.RuleBasedAuthorizationPlugin',\n" +
      "    'user-role':{'solr':'admin'},\n" +
      "    'permissions':[{'name':'all','role':'admin'}]}}";
  
}
