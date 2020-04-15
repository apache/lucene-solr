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
import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.Package;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.util.Utils;
import org.apache.solr.filestore.PackageStoreAPI;
import org.apache.solr.filestore.TestDistribPackageStore;
import org.apache.solr.pkg.TestPackages;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.filestore.TestDistribPackageStore.readFile;
import static org.apache.solr.filestore.TestDistribPackageStore.uploadKey;
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
            .configure();
    String errPath = "/error/details[0]/errorMessages[0]";
    try {
      PluginMeta plugin = new PluginMeta();
      plugin.name = "testplugin";
      plugin.klass = C2.class.getName();
      V2Request req = new V2Request.Builder("/cluster/plugin")
          .forceV2(true)
          .withMethod(POST)
          .withPayload(singletonMap("add", plugin))
          .build();
      expectError(req, cluster.getSolrClient(), errPath, "Must have a no-arg constructor or CoreContainer constructor and it must not be a non static inner class");

      plugin.klass = C1.class.getName();
      expectError(req, cluster.getSolrClient(), errPath, "Invalid class, no @EndPoint annotation");

      plugin.klass = C3.class.getName();
      req.process(cluster.getSolrClient());

      V2Response rsp = new V2Request.Builder("/cluster/plugin")
          .forceV2(true)
          .withMethod(GET)
          .build()
          .process(cluster.getSolrClient());
      assertEquals(C3.class.getName(), rsp._getStr("/plugin/testplugin/class", null));

      TestDistribPackageStore.assertResponseValues(10,
          () -> new V2Request.Builder("/plugin/my/plugin")
              .forceV2(true)
              .withMethod(GET)
              .build().process(cluster.getSolrClient()),
          ImmutableMap.of("/testkey", "testval"));

      new V2Request.Builder("/cluster/plugin")
          .withMethod(POST)
          .forceV2(true)
          .withPayload("{remove : testplugin}")
          .build()
          .process(cluster.getSolrClient());

      rsp = new V2Request.Builder("/cluster/plugin")
          .forceV2(true)
          .withMethod(GET)
          .build()
          .process(cluster.getSolrClient());
      assertEquals(null, rsp._get("/plugin/testplugin/class", null));

    } finally {
      cluster.shutdown();
    }
  }
  @Test
  public void testApiFromPackage() throws Exception {
    MiniSolrCloudCluster cluster =
        configureCluster(4)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .configure();
    String FILE1 = "/myplugin/v1.jar";
    String FILE2 = "/myplugin/v2.jar";

    String errPath = "/error/details[0]/errorMessages[0]";
    try {
      byte[] derFile = readFile("cryptokeys/pub_key512.der");
      uploadKey(derFile, PackageStoreAPI.KEYS_DIR+"/pub_key512.der", cluster);
      TestPackages.postFileAndWait(cluster, "runtimecode/containerplugin.v.1.jar.bin", FILE1,
          "pmrmWCDafdNpYle2rueAGnU2J6NYlcAey9mkZYbqh+5RdYo2Ln+llLF9voyRj+DDivK9GV1XdtKvD9rgCxlD7Q==");
     TestPackages.postFileAndWait(cluster, "runtimecode/containerplugin.v.2.jar.bin", FILE2,
          "StR3DmqaUSL7qjDOeVEiCqE+ouiZAkW99fsL48F9oWG047o7NGgwwZ36iGgzDC3S2tPaFjRAd9Zg4UK7OZLQzg==");

      Package.AddVersion add = new Package.AddVersion();
      add.version = "1.0";
      add.pkg = "mypkg";
      add.files = List.of(FILE1);
      V2Request addPkgVersionReq = new V2Request.Builder("/cluster/package")
          .forceV2(true)
          .withMethod(POST)
          .withPayload(singletonMap("add", add))
          .build();
      addPkgVersionReq.process(cluster.getSolrClient());

      TestDistribPackageStore.assertResponseValues(10,
          () -> new V2Request.Builder("/cluster/package").
              withMethod(GET)
              .build().process(cluster.getSolrClient()),
          Utils.makeMap(
              ":result:packages:mypkg[0]:version", "1.0",
              ":result:packages:mypkg[0]:files[0]", FILE1
          ));

      PluginMeta plugin = new PluginMeta();
      plugin.name = "myplugin";
      plugin.klass = "mypkg:org.apache.solr.handler.MyPlugin";
      plugin.version = add.version;
      V2Request req1 = new V2Request.Builder("/cluster/plugin")
          .forceV2(true)
          .withMethod(POST)
          .withPayload(singletonMap("add", plugin))
          .build();
      req1.process(cluster.getSolrClient());
      TestDistribPackageStore.assertResponseValues(10,
          () -> new V2Request.Builder("/cluster/plugin").
              withMethod(GET)
              .build().process(cluster.getSolrClient()),
          ImmutableMap.of(
              "/plugin/myplugin/class", plugin.klass,
              "/plugin/myplugin/version", plugin.version
          ));
      Callable<NavigableObject> invokePlugin = () -> new V2Request.Builder("/plugin/my/path")
          .forceV2(true)
          .withMethod(GET)
          .build().process(cluster.getSolrClient());
      TestDistribPackageStore.assertResponseValues(10,
          invokePlugin,
          ImmutableMap.of("/myplugin.version", "1.0"));

      add.version = "2.0";
      add.files = List.of(FILE2);
      addPkgVersionReq.process(cluster.getSolrClient());

      plugin.version = add.version;
      new V2Request.Builder("/cluster/plugin")
          .forceV2(true)
          .withMethod(POST)
          .withPayload(singletonMap("update", plugin))
          .build()
      .process(cluster.getSolrClient());

      TestDistribPackageStore.assertResponseValues(10,
          () -> new V2Request.Builder("/cluster/plugin").
              withMethod(GET)
              .build().process(cluster.getSolrClient()),
          ImmutableMap.of(
              "/plugin/myplugin/class", plugin.klass,
              "/plugin/myplugin/version", "2.0"
          ));
      TestDistribPackageStore.assertResponseValues(10,
          invokePlugin,
          ImmutableMap.of("/myplugin.version", "2.0"));
    } finally {
      cluster.shutdown();
    }
  }

  public static class C1 {

  }

  @EndPoint(
      method = GET,
      path = "/plugin/my/plugin",
      permission = PermissionNameProvider.Name.COLL_READ_PERM)
  public class C2 {


  }

  @EndPoint(
      method = GET,
      path = "/plugin/my/plugin",
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
