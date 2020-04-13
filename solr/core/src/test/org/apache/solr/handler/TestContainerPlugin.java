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
import java.util.Collections;

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.hamcrest.CoreMatchers.containsString;

public class TestContainerPlugin extends SolrCloudTestCase {

  @Before
  public void setup() {
    System.setProperty("enable.packages", "true");
  }

  @After
  public void teardown() {
    System.clearProperty("enable.packages");
  }

  @Test
  public void testApi() throws Exception {
    MiniSolrCloudCluster cluster =
        configureCluster(4)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", configset("cloud-minimal"))
            .configure();
    String errPath = "/error/details[0]/errorMessages[0]";
    try {
      PluginMeta plugin = new PluginMeta();
      plugin.name = "testplugin";
      plugin.klass = C2.class.getName();
      V2Request req = new V2Request.Builder("/cluster/plugins")
          .forceV2(true)
          .withMethod(POST)
          .withPayload(Collections.singletonMap("add", plugin))
          .build();
      expectError(req, cluster.getSolrClient(), errPath, "Class must be public and static :");

      plugin.klass = C1.class.getName();
      expectError(req, cluster.getSolrClient(), errPath, "Invalid class, no @EndPoint annotation");
      plugin.klass = C3.class.getName();
      req.process(cluster.getSolrClient());

      V2Response rsp = new V2Request.Builder("/cluster/plugins")
          .forceV2(true)
          .withMethod(GET)
          .build()
          .process(cluster.getSolrClient());
      assertEquals(C3.class.getName(), rsp._getStr("/plugins/testplugin/class", ""));
      System.out.println("");


    } finally {
      cluster.shutdown();
    }
  }

  public static class C1 {

  }

  @EndPoint(
      method = GET,
      path = "/cluster/my/plugin",
      permission = PermissionNameProvider.Name.COLL_READ_PERM)
  public class C2 {


  }

  @EndPoint(
      method = GET,
      path = "/cluster/my/plugin",
      permission = PermissionNameProvider.Name.COLL_READ_PERM)
  public static class C3 {
    @Command
    public void read(SolrQueryRequest req, SolrQueryResponse rsp) {
      rsp.add("testkey", "testval");
    }

  }


  private void expectError(V2Request req, SolrClient client, String errPath, String expectErrorMsg) throws IOException, SolrServerException {
    try {
      req.process(client);
      fail("should have failed with message : " + expectErrorMsg);
    } catch (BaseHttpSolrClient.RemoteExecutionException e) {
      String msg = e.getMetaData()._getStr(errPath, "");
      assertThat(msg, containsString(expectErrorMsg));
    }
  }
}
