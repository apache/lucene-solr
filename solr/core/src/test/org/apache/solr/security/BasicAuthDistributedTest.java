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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.handler.admin.SecurityConfHandlerLocalForTesting;
import org.apache.solr.util.LogLevel;
import org.junit.Test;

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
  @LogLevel("org.apache.solr=DEBUG")
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
  }

  private void testAuth() throws Exception {
    QueryResponse rsp = query("q","text:doc", "fl", "id,text", "sort", "id asc");
    assertEquals(10, rsp.getResults().getNumFound());

    // Enable authentication
    for (JettySolrRunner j : jettys) {
      writeSecurityJson(j.getCoreContainer());
    }

    HttpSolrClient.RemoteSolrException expected = expectThrows(HttpSolrClient.RemoteSolrException.class, () -> {
      query("q","text:doc-fail", "fl", "id,text", "sort", "id asc");
    });
    assertEquals(401, expected.code());

    // Add auth
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "text:doc").add("fl", "id,text").add("sort", "id asc");
    QueryRequest req = new QueryRequest(params);
    req.setBasicAuthCredentials("solr", "SolrRocks");
    rsp = req.process(clients.get(0), null);
    if (jettys.size() > 1) {
      assertTrue(rsp.getResults().getNumFound() < 10);
      rsp = query(true, params, "solr", "SolrRocks");
    }
    assertEquals(10, rsp.getResults().getNumFound());

    // Disable auth
    for (JettySolrRunner j : jettys) {
      deleteSecurityJson(j.getCoreContainer());
    }

  }

  private void deleteSecurityJson(CoreContainer coreContainer) throws IOException {
    securityConfHandler = new SecurityConfHandlerLocalForTesting(coreContainer);
    Files.delete(Paths.get(coreContainer.getSolrHome()).resolve("security.json"));
    coreContainer.securityNodeChanged();
  }

  private void writeSecurityJson(CoreContainer coreContainer) throws IOException {
    securityConfHandler = new SecurityConfHandlerLocalForTesting(coreContainer);
    securityConfHandler.persistConf(new SecurityConfHandler.SecurityConfig()
        .setData(Utils.fromJSONString(ALL_CONF.replaceAll("'", "\""))));
    coreContainer.securityNodeChanged();
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
