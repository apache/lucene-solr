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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteExecutionException;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.Package;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.filestore.PackageStoreAPI;
import org.apache.solr.filestore.TestDistribPackageStore;
import org.apache.solr.filestore.TestDistribPackageStore.Fetcher;
import org.apache.solr.pkg.TestPackages;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.filestore.TestDistribPackageStore.readFile;
import static org.apache.solr.filestore.TestDistribPackageStore.uploadKey;

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
      //test with an invalid class
      V2Request req = new V2Request.Builder("/cluster/plugin")
          .forceV2(true)
          .withMethod(POST)
          .withPayload(singletonMap("add", plugin))
          .build();
      expectError(req, cluster.getSolrClient(), errPath, "No method with @Command in class");

      //test with an invalid class
      plugin.klass = C1.class.getName();
      expectError(req, cluster.getSolrClient(), errPath, "No @EndPoints");

      //test with a valid class. This should succeed now
      plugin.klass = C3.class.getName();
      req.process(cluster.getSolrClient());

      //just check if the plugin is indeed registered
      V2Request readPluginState = new V2Request.Builder("/cluster/plugin")
          .forceV2(true)
          .withMethod(GET)
          .build();
      V2Response rsp = readPluginState.process(cluster.getSolrClient());
      assertEquals(C3.class.getName(), rsp._getStr("/plugin/testplugin/class", null));

      //let's test the plugin
      TestDistribPackageStore.assertResponseValues(10,
          () -> new V2Request.Builder("/plugin/my/plugin")
              .forceV2(true)
              .withMethod(GET)
              .build().process(cluster.getSolrClient()),
          ImmutableMap.of("/testkey", "testval"));

      //now remove the plugin
      new V2Request.Builder("/cluster/plugin")
          .withMethod(POST)
          .forceV2(true)
          .withPayload("{remove : testplugin}")
          .build()
          .process(cluster.getSolrClient());

      //verify it is removed
      rsp = readPluginState.process(cluster.getSolrClient());
      assertEquals(null, rsp._get("/plugin/testplugin/class", null));

      //test with a class  @EndPoint methods. This also uses a template in the path name
      plugin.klass = C4.class.getName();
      plugin.name = "collections";
      plugin.pathPrefix = "collections";
      expectError(req, cluster.getSolrClient(), errPath, "path must not have a prefix: collections");

      plugin.name = "my-random-name";
      plugin.pathPrefix = "my-random-prefix";

      req.process(cluster.getSolrClient());

      //let's test the plugin
      TestDistribPackageStore.assertResponseValues(10,
          () -> new V2Request.Builder("/my-random-name/my/plugin")
              .forceV2(true)
              .withMethod(GET)
              .build().process(cluster.getSolrClient()),
          ImmutableMap.of("/method.name", "m1"));

  TestDistribPackageStore.assertResponseValues(10,
          () -> new V2Request.Builder("/my-random-prefix/their/plugin")
              .forceV2(true)
              .withMethod(GET)
              .build().process(cluster.getSolrClient()),
          ImmutableMap.of("/method.name", "m2"));
      //now remove the plugin
      new V2Request.Builder("/cluster/plugin")
          .withMethod(POST)
          .forceV2(true)
          .withPayload("{remove : my-random-name}")
          .build()
          .process(cluster.getSolrClient());

      expectFail( () -> new V2Request.Builder("/my-random-prefix/their/plugin")
          .forceV2(true)
          .withMethod(GET)
          .build()
          .process(cluster.getSolrClient()));
      expectFail(() -> new V2Request.Builder("/my-random-prefix/their/plugin")
          .forceV2(true)
          .withMethod(GET)
          .build()
          .process(cluster.getSolrClient()));
    } finally {
      cluster.shutdown();
    }
  }

  private void expectFail(ThrowingRunnable runnable) throws Exception {
    for(int i=0;i< 20;i++) {
      try {
        runnable.run();
      } catch (Throwable throwable) {
        return;
      }
      Thread.sleep(100);
    }
    fail("should have failed with an exception");
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

     // We have two versions of the plugin in 2 different jar files. they are already uploaded to the package store
      Package.AddVersion add = new Package.AddVersion();
      add.version = "1.0";
      add.pkg = "mypkg";
      add.files = singletonList(FILE1);
      V2Request addPkgVersionReq = new V2Request.Builder("/cluster/package")
          .forceV2(true)
          .withMethod(POST)
          .withPayload(singletonMap("add", add))
          .build();
      addPkgVersionReq.process(cluster.getSolrClient());

      waitForAllNodesToSync(cluster, "/cluster/package", Utils.makeMap(
              ":result:packages:mypkg[0]:version", "1.0",
              ":result:packages:mypkg[0]:files[0]", FILE1
      ));

      // Now lets create a plugin using v1 jar file
      PluginMeta plugin = new PluginMeta();
      plugin.name = "myplugin";
      plugin.klass = "mypkg:org.apache.solr.handler.MyPlugin";
      plugin.version = add.version;
      final V2Request req1 = new V2Request.Builder("/cluster/plugin")
          .forceV2(true)
          .withMethod(POST)
          .withPayload(singletonMap("add", plugin))
          .build();
      req1.process(cluster.getSolrClient());
      //verify the plugin creation
      TestDistribPackageStore.assertResponseValues(10,
          () -> new V2Request.Builder("/cluster/plugin").
              withMethod(GET)
              .build().process(cluster.getSolrClient()),
          ImmutableMap.of(
              "/plugin/myplugin/class", plugin.klass,
              "/plugin/myplugin/version", plugin.version
          ));
      //let's test this now
      Callable<NavigableObject> invokePlugin = () -> new V2Request.Builder("/plugin/my/path")
          .forceV2(true)
          .withMethod(GET)
          .build().process(cluster.getSolrClient());
      TestDistribPackageStore.assertResponseValues(10,
          invokePlugin,
          ImmutableMap.of("/myplugin.version", "1.0"));

      //now let's upload the jar file for version 2.0 of the plugin
      add.version = "2.0";
      add.files = singletonList(FILE2);
      addPkgVersionReq.process(cluster.getSolrClient());

      //here the plugin version is updated
      plugin.version = add.version;
      new V2Request.Builder("/cluster/plugin")
          .forceV2(true)
          .withMethod(POST)
          .withPayload(singletonMap("update", plugin))
          .build()
      .process(cluster.getSolrClient());

      //now verify if it is indeed updated
      TestDistribPackageStore.assertResponseValues(10,
          () -> new V2Request.Builder("/cluster/plugin").
              withMethod(GET)
              .build().process(cluster.getSolrClient()),
          ImmutableMap.of(
              "/plugin/myplugin/class", plugin.klass,
              "/plugin/myplugin/version", "2.0"
          ));
      // invoke the plugin and test thye output
      TestDistribPackageStore.assertResponseValues(10,
          invokePlugin,
          ImmutableMap.of("/myplugin.version", "2.0"));

      plugin.name = "plugin2";
      plugin.klass = "mypkg:"+ C5.class.getName();
      plugin.version = "2.0";
      req1.process(cluster.getSolrClient());
      assertNotNull(C5.classData);
      assertEquals( 1452, C5.classData.limit());
    } finally {
      cluster.shutdown();
    }
  }

  public static class C5 implements ResourceLoaderAware {
    static ByteBuffer classData;
    private  SolrResourceLoader resourceLoader;

    @Override
    @SuppressWarnings("unchecked")
    public void inform(ResourceLoader loader) throws IOException {
      this.resourceLoader = (SolrResourceLoader) loader;
      try {
        InputStream is = resourceLoader.openResource("org/apache/solr/handler/MyPlugin.class");
        byte[] buf = new byte[1024*5];
        int sz = IOUtils.read(is, buf);
        classData = ByteBuffer.wrap(buf, 0,sz);
      } catch (IOException e) {
        //do not do anything
      }
    }

    @EndPoint(method = GET,
        path = "/$plugin-name/m2",
        permission = PermissionNameProvider.Name.COLL_READ_PERM)
    public void m2() {


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

  public static class C4 {

    @EndPoint(method = GET,
        path = "$plugin-name/my/plugin",
        permission = PermissionNameProvider.Name.READ_PERM)
    public void m1(SolrQueryRequest req, SolrQueryResponse rsp) {
      rsp.add("method.name", "m1");
    }

    @EndPoint(method = GET,
        path = "$path-prefix/their/plugin",
        permission = PermissionNameProvider.Name.READ_PERM)
    public void m2(SolrQueryRequest req, SolrQueryResponse rsp) {
      rsp.add("method.name", "m2");
    }

  }

  @SuppressWarnings("unchecked")
  public static void waitForAllNodesToSync(MiniSolrCloudCluster cluster, String path, Map<String,Object> expected) throws Exception {
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      String baseUrl = jettySolrRunner.getBaseUrl().toString().replace("/solr", "/api");
      String url = baseUrl + path + "?wt=javabin";
      TestDistribPackageStore.assertResponseValues(10, new Fetcher(url, jettySolrRunner), expected);
    }
  }

  private void expectError(V2Request req, SolrClient client, String errPath, String expectErrorMsg) throws IOException, SolrServerException {
    RemoteExecutionException e = expectThrows(RemoteExecutionException.class, () -> req.process(client));
    String msg = e.getMetaData()._getStr(errPath, "");
    assertTrue(expectErrorMsg, msg.contains(expectErrorMsg));
  }
}
