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
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.MemClassLoader;
import org.apache.solr.core.RuntimeLib;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.eclipse.jetty.server.Server;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import static org.apache.solr.cloud.TestCryptoKeys.readFile;
import static org.apache.solr.common.params.CommonParams.JAVABIN;
import static org.apache.solr.common.params.CommonParams.WT;
import static org.apache.solr.common.util.Utils.getObjectByPath;
import static org.apache.solr.core.TestDynamicLoading.getFileContent;
import static org.apache.solr.core.TestDynamicLoadingUrl.runHttpServer;

@SolrTestCaseJ4.SuppressSSL
@LogLevel("org.apache.solr.common.cloud.ZkStateReader=DEBUG;org.apache.solr.handler.admin.CollectionHandlerApi=DEBUG;org.apache.solr.core.LibListener=DEBUG;org.apache.solr.common.cloud.ClusterProperties=DEBUG")
public class TestContainerReqHandler extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("enable.runtime.lib", "true");

  }

  static void assertResponseValues(int repeats, SolrClient client, SolrRequest req, Map vals) throws Exception {
    for (int i = 0; i < repeats; i++) {
      if (i > 0) {
        Thread.sleep(100);
      }
      SolrResponse rsp = null;
      try {
        rsp = req.process(client);
      } catch (Exception e) {
        if (i >= repeats - 1) throw e;
      }
      for (Object e : vals.entrySet()) {
        Map.Entry entry = (Map.Entry) e;
        String key = (String) entry.getKey();
        Object val = entry.getValue();
        Predicate p = val instanceof Predicate ? (Predicate) val : o -> {
          String v = o == null ? null : String.valueOf(o);
          return Objects.equals(val, o);
        };
        boolean isPass = p.test(rsp.getResponse()._get(key, null));
        if(isPass) return;
        else if (i >= repeats - 1) {
          fail("attempt: " + i + " Mismatch for value : '" + key + "' in response " + Utils.toJSONString(rsp));
        }

      }

    }


  }

  private static Map<String, Object> assertVersionInSync(SolrZkClient zkClient, SolrClient solrClient) throws SolrServerException, IOException {
    Stat stat = new Stat();
    Map<String, Object> map = new ClusterProperties(zkClient).getClusterProperties(stat);
    assertEquals(String.valueOf(stat.getVersion()), getExtResponse(solrClient)._getStr("metadata/version", null));
    return map;
  }

  private static V2Response getExtResponse(SolrClient solrClient) throws SolrServerException, IOException {
    return new V2Request.Builder("/node/ext")
        .withMethod(SolrRequest.METHOD.GET)
        .build().process(solrClient);
  }

  @Test
  public void testRuntimeLib() throws Exception {
    Map<String, Object> jars = Utils.makeMap(
        "/jar1.jar", getFileContent("runtimecode/runtimelibs.jar.bin"),
        "/jar2.jar", getFileContent("runtimecode/runtimelibs_v2.jar.bin"),
        "/jar3.jar", getFileContent("runtimecode/runtimelibs_v3.jar.bin"));

    Pair<Server, Integer> server = runHttpServer(jars);
    int port = server.second();
    MiniSolrCloudCluster cluster = configureCluster(4).configure();
    try {
      String payload = null;
      try {
        payload = "{add-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar1.jar', " +
            "sha512 : 'wrong-sha512'}}";
        new V2Request.Builder("/cluster")
            .withPayload(payload)
            .withMethod(SolrRequest.METHOD.POST)
            .build().process(cluster.getSolrClient());
        fail("Expected error");
      } catch (BaseHttpSolrClient.RemoteExecutionException e) {
        assertTrue("actual output : " + Utils.toJSONString(e.getMetaData()), e.getMetaData()._getStr("error/details[0]/errorMessages[0]", "").contains("expected sha512 hash :"));
      }

      try {
        payload = "{add-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar0.jar', " +
            "sha512 : 'd01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420'}}";
        new V2Request.Builder("/cluster")
            .withPayload(payload)
            .withMethod(SolrRequest.METHOD.POST)
            .build().process(cluster.getSolrClient());
        fail("Expected error");
      } catch (BaseHttpSolrClient.RemoteExecutionException e) {
        assertTrue("Actual output : " + Utils.toJSONString(e.getMetaData()), e.getMetaData()._getStr("error/details[0]/errorMessages[0]", "").contains("no such resource available: foo"));
      }

      payload = "{add-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar1.jar', " +
          "sha512 : 'd01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420'}}";
      new V2Request.Builder("/cluster")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "add-runtimelib/sha512"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "runtimeLib/foo/sha512"));


      new V2Request.Builder("/cluster")
          .withPayload("{add-requesthandler:{name : 'bar', class : 'org.apache.solr.core.RuntimeLibReqHandler'}}")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      Map<String, Object> map = new ClusterProperties(cluster.getZkClient()).getClusterProperties();


      V2Request request = new V2Request.Builder("/node/ext/bar")
          .withMethod(SolrRequest.METHOD.POST)
          .build();
      assertResponseValues(10, cluster.getSolrClient(), request, Utils.makeMap(
          "class", "org.apache.solr.core.RuntimeLibReqHandler",
          "loader", MemClassLoader.class.getName(),
          "version", null));


      assertEquals("org.apache.solr.core.RuntimeLibReqHandler",
          getObjectByPath(map, true, Arrays.asList("requestHandler", "bar", "class")));


      payload = "{update-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar3.jar', " +
          "sha512 : '60ec88c2a2e9b409f7afc309273383810a0d07a078b482434eda9674f7e25b8adafa8a67c9913c996cbfb78a7f6ad2b9db26dbd4fe0ca4068f248d5db563f922'}}";
      new V2Request.Builder("/cluster")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "update-runtimelib/sha512"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "runtimeLib/foo/sha512"));


      request = new V2Request.Builder("/node/ext/bar")
          .withMethod(SolrRequest.METHOD.POST)
          .build();
      assertResponseValues(10, cluster.getSolrClient(), request, Utils.makeMap(
          "class", "org.apache.solr.core.RuntimeLibReqHandler",
          "loader", MemClassLoader.class.getName(),
          "version", "3")
      );


      new V2Request.Builder("/cluster")
          .withPayload("{delete-requesthandler: 'bar'}")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      request = new V2Request.Builder("/node/ext")
          .withMethod(SolrRequest.METHOD.POST)
          .build();
      assertResponseValues(10, cluster.getSolrClient(), request, ImmutableMap.of(SolrRequestHandler.TYPE,
          (Predicate<Object>) o -> o instanceof List && ((List) o).isEmpty()));
      new V2Request.Builder("/cluster")
          .withPayload("{delete-runtimelib : 'foo'}")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertResponseValues(10, cluster.getSolrClient(), request, ImmutableMap.of(RuntimeLib.TYPE,
          (Predicate<Object>) o -> o instanceof List && ((List) o).isEmpty()));


    } finally {
      cluster.shutdown();
      server.first().stop();
    }
  }

  @Test
  public void testRuntimeLibWithSig2048() throws Exception {
    Map<String, Object> jars = Utils.makeMap(
        "/jar1.jar", getFileContent("runtimecode/runtimelibs.jar.bin"),
        "/jar2.jar", getFileContent("runtimecode/runtimelibs_v2.jar.bin"),
        "/jar3.jar", getFileContent("runtimecode/runtimelibs_v3.jar.bin"));

    Pair<Server, Integer> server = runHttpServer(jars);
    int port = server.second();
    MiniSolrCloudCluster cluster = configureCluster(4).configure();

    try {

      byte[] derFile = readFile("cryptokeys/pub_key2048.der");
      cluster.getZkClient().makePath("/keys/exe", true);
      cluster.getZkClient().create("/keys/exe/pub_key2048.der", derFile, CreateMode.PERSISTENT, true);

      String signature = "NaTm3+i99/ZhS8YRsLc3NLz2Y6VuwEbu7DihY8GAWwWIGm+jpXgn1JiuaenfxFCcfNKCC9WgZmEgbTZTzmV/OZMVn90u642YJbF3vTnzelW1pHB43ZRAJ1iesH0anM37w03n3es+vFWQtuxc+2Go888fJoMkUX2C6Zk6Jn116KE45DWjeyPM4mp3vvGzwGvdRxP5K9Q3suA+iuI/ULXM7m9mV4ruvs/MZvL+ELm5Jnmk1bBtixVJhQwJP2z++8tQKJghhyBxPIC/2fkAHobQpkhZrXu56JjP+v33ul3Ku4bbvfVMY/LVwCAEnxlvhk+C6uRCKCeFMrzQ/k5inasXLw==";

      String payload = "{add-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar1.jar', " +
          "sig : 'EdYkvRpMZbvElN93/xUmyKXcj6xHP16AVk71TlTascEwCb5cFQ2AeKhPIlwYpkLWXEOcLZKfeXoWwOLaV5ZNhg==' ," +
          "sha512 : 'd01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420'}}";
      try {
        new V2Request.Builder("/cluster")
            .withPayload(payload)
            .withMethod(SolrRequest.METHOD.POST)
            .build().process(cluster.getSolrClient());
      } catch (BaseHttpSolrClient.RemoteExecutionException e) {
        //No key matched signature for jar
        assertTrue(e.getMetaData()._getStr("error/details[0]/errorMessages[0]", "").contains("No key matched signature for jar"));
      }


      payload = "{add-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar1.jar', " +
          "sig : '" + signature + "'," +
          "sha512 : 'd01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420'}}";

      new V2Request.Builder("/cluster")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "add-runtimelib/sha512"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "runtimeLib/foo/sha512"));

      new V2Request.Builder("/cluster")
          .withPayload("{add-requesthandler:{name : 'bar', class : 'org.apache.solr.core.RuntimeLibReqHandler'}}")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      Map<String, Object> map = new ClusterProperties(cluster.getZkClient()).getClusterProperties();


      V2Request request = new V2Request.Builder("/node/ext/bar")
          .withMethod(SolrRequest.METHOD.POST)
          .build();
      assertResponseValues(10, cluster.getSolrClient(), request, Utils.makeMap(
          "class", "org.apache.solr.core.RuntimeLibReqHandler",
          "loader", MemClassLoader.class.getName(),
          "version", null));


      assertEquals("org.apache.solr.core.RuntimeLibReqHandler",
          getObjectByPath(map, true, Arrays.asList("requestHandler", "bar", "class")));

      payload = "{update-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar3.jar', " +
          "sig : 'YxFr6SpYrDwG85miDfRWHTjU9UltjtIWQZEhcV55C2rczRUVowCYBxmsDv5mAM8j0CTv854xpI1DtBT86wpoTdbF95LQuP9FJId4TS1j8bZ9cxHP5Cqyz1uBHFfUUNUrnpzTHQkVTp02O9NAjh3c2W41bL4U7j6jQ32+4CW2M+x00TDG0y0H75rQDR8zbLt31oWCz+sBOdZ3rGKJgAvdoGm/wVCTmsabZN+xoz4JaDeBXF16O9Uk9SSq4G0dz5YXFuLxHK7ciB5t0+q6pXlF/tdlDqF76Abze0R3d2/0MhXBzyNp3UxJmj6DiprgysfB0TbQtJG0XGfdSmx0VChvcA==' ," +
          "sha512 : '60ec88c2a2e9b409f7afc309273383810a0d07a078b482434eda9674f7e25b8adafa8a67c9913c996cbfb78a7f6ad2b9db26dbd4fe0ca4068f248d5db563f922'}}";

      new V2Request.Builder("/cluster")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "update-runtimelib/sha512"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "runtimeLib/foo/sha512"));


      request = new V2Request.Builder("/node/ext/bar")
          .withMethod(SolrRequest.METHOD.POST)
          .build();
      assertResponseValues(10, cluster.getSolrClient(), request, Utils.makeMap(
          "class", "org.apache.solr.core.RuntimeLibReqHandler",
          "loader", MemClassLoader.class.getName(),
          "version", "3"));


    } finally {
      server.first().stop();
      cluster.shutdown();
    }

  }

  @Test
  public void testRuntimeLibWithSig512() throws Exception {
    Map<String, Object> jars = Utils.makeMap(
        "/jar1.jar", getFileContent("runtimecode/runtimelibs.jar.bin"),
        "/jar2.jar", getFileContent("runtimecode/runtimelibs_v2.jar.bin"),
        "/jar3.jar", getFileContent("runtimecode/runtimelibs_v3.jar.bin"));

    Pair<Server, Integer> server = runHttpServer(jars);
    int port = server.second();
    MiniSolrCloudCluster cluster = configureCluster(4).configure();

    try {

      byte[] derFile = readFile("cryptokeys/pub_key512.der");
      cluster.getZkClient().makePath("/keys/exe", true);
      cluster.getZkClient().create("/keys/exe/pub_key512.der", derFile, CreateMode.PERSISTENT, true);

      String signature = "L3q/qIGs4NaF6JiO0ZkMUFa88j0OmYc+I6O7BOdNuMct/xoZ4h73aZHZGc0+nmI1f/U3bOlMPINlSOM6LK3JpQ==";

      String payload = "{add-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar1.jar', " +
          "sig : '" + signature + "'," +
          "sha512 : 'd01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420'}}";

      new V2Request.Builder("/cluster")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "add-runtimelib/sha512"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "runtimeLib/foo/sha512"));

      new V2Request.Builder("/cluster")
          .withPayload("{add-requesthandler:{name : 'bar', class : 'org.apache.solr.core.RuntimeLibReqHandler'}}")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      Map<String, Object> map = new ClusterProperties(cluster.getZkClient()).getClusterProperties();


      V2Request request = new V2Request.Builder("/node/ext/bar")
          .withMethod(SolrRequest.METHOD.POST)
          .build();
      assertResponseValues(10, cluster.getSolrClient(), request, Utils.makeMap(
          "class", "org.apache.solr.core.RuntimeLibReqHandler",
          "loader", MemClassLoader.class.getName(),
          "version", null));


      assertEquals("org.apache.solr.core.RuntimeLibReqHandler",
          getObjectByPath(map, true, Arrays.asList("requestHandler", "bar", "class")));

      payload = "{update-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar3.jar', " +
          "sig : 'a400n4T7FT+2gM0SC6+MfSOExjud8MkhTSFylhvwNjtWwUgKdPFn434Wv7Qc4QEqDVLhQoL3WqYtQmLPti0G4Q==' ," +
          "sha512 : '60ec88c2a2e9b409f7afc309273383810a0d07a078b482434eda9674f7e25b8adafa8a67c9913c996cbfb78a7f6ad2b9db26dbd4fe0ca4068f248d5db563f922'}}";

      new V2Request.Builder("/cluster")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "update-runtimelib/sha512"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "runtimeLib/foo/sha512"));


      request = new V2Request.Builder("/node/ext/bar")
          .withMethod(SolrRequest.METHOD.POST)
          .build();
      assertResponseValues(10, cluster.getSolrClient(), request, Utils.makeMap(
          "class", "org.apache.solr.core.RuntimeLibReqHandler",
          "loader", MemClassLoader.class.getName(),
          "version", "3"));

    } finally {
      server.first().stop();
      cluster.shutdown();
    }

  }

  @Test
  public void testSetClusterReqHandler() throws Exception {
    MiniSolrCloudCluster cluster = configureCluster(4).configure();
    try {
      SolrZkClient zkClient = cluster.getZkClient();
      new V2Request.Builder("/cluster")
          .withPayload("{add-requesthandler:{name : 'foo', class : 'org.apache.solr.handler.DumpRequestHandler'}}")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());

      Map<String, Object> map = assertVersionInSync(zkClient, cluster.getSolrClient());

      assertEquals("org.apache.solr.handler.DumpRequestHandler",
          getObjectByPath(map, true, Arrays.asList("requestHandler", "foo", "class")));

      assertVersionInSync(zkClient, cluster.getSolrClient());
      V2Response rsp = new V2Request.Builder("/node/ext/foo")
          .withMethod(SolrRequest.METHOD.GET)
          .withParams(new MapSolrParams((Map) Utils.makeMap("testkey", "testval")))
          .build().process(cluster.getSolrClient());
      assertEquals("testval", rsp._getStr("params/testkey", null));

      new V2Request.Builder("/cluster")
          .withPayload("{delete-requesthandler: 'foo'}")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());

      assertNull(getObjectByPath(map, true, Arrays.asList("requestHandler", "foo")));
    } finally {
      cluster.shutdown();
    }

  }

  public void testPluginGlobalLoader() throws Exception {
    String COLLECTION_NAME = "globalLoaderColl";
    Map<String, Object> jars = Utils.makeMap(
        "/jar1.jar", getFileContent("runtimecode/runtimelibs.jar.bin"),
        "/jar2.jar", getFileContent("runtimecode/runtimelibs_v2.jar.bin"),
        "/jar3.jar", getFileContent("runtimecode/runtimelibs_v3.jar.bin"));

    Pair<Server, Integer> server = runHttpServer(jars);
    int port = server.second();
    System.setProperty("enable.runtime.lib", "true");
    MiniSolrCloudCluster cluster = configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    try {
      CollectionAdminRequest
          .createCollection(COLLECTION_NAME, "conf", 2, 1)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());


      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);
      String payload = "{add-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar1.jar', " +
          "sha512 : 'd01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420'}}";
      new V2Request.Builder("/cluster")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "add-runtimelib/sha512"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "runtimeLib/foo/sha512"));


      payload = "{\n" +
          "'create-requesthandler' : { 'name' : '/runtime', 'class': 'org.apache.solr.core.RuntimeLibReqHandler' , 'runtimeLib':global }," +
          "'create-searchcomponent' : { 'name' : 'get', 'class': 'org.apache.solr.core.RuntimeLibSearchComponent' , 'runtimeLib':global }," +
          "'create-queryResponseWriter' : { 'name' : 'json1', 'class': 'org.apache.solr.core.RuntimeLibResponseWriter' , 'runtimeLib':global }" +
          "}";
      cluster.getSolrClient().request(new ConfigRequest(payload) {
        @Override
        public String getCollection() {
          return COLLECTION_NAME;
        }
      });


      SolrParams params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME, WT, JAVABIN));
      assertResponseValues(10,
          cluster.getSolrClient(),
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/config/overlay", params),
          Utils.makeMap(
              "overlay/queryResponseWriter/json1/class", "org.apache.solr.core.RuntimeLibResponseWriter",
              "overlay/searchComponent/get/class", "org.apache.solr.core.RuntimeLibSearchComponent"
          ));

      assertResponseValues(10,
          cluster.getSolrClient(),
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/runtime", params),
          Utils.makeMap("class", "org.apache.solr.core.RuntimeLibReqHandler",
              "loader", MemClassLoader.class.getName()));

      assertResponseValues(10,
          cluster.getSolrClient(),
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/get?abc=xyz", params),
          Utils.makeMap("get", "org.apache.solr.core.RuntimeLibSearchComponent",
              "loader", MemClassLoader.class.getName()));

      GenericSolrRequest req = new GenericSolrRequest(SolrRequest.METHOD.GET, "/runtime",
          new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME, WT, "json1")));
      req.setResponseParser(new ResponseParser() {
        @Override
        public String getWriterType() {
          return "json1";
        }

        @Override
        public NamedList<Object> processResponse(InputStream body, String encoding) {
          return new NamedList<>((Map) Utils.fromJSON(body));
        }

        @Override
        public NamedList<Object> processResponse(Reader reader) {
          return new NamedList<>((Map) Utils.fromJSON(reader));

        }

      });
      assertResponseValues(10,
          cluster.getSolrClient(),
          req,
          Utils.makeMap("wt", "org.apache.solr.core.RuntimeLibResponseWriter",
              "loader", MemClassLoader.class.getName()));


      payload = "{update-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar2.jar', " +
          "sha512 : 'bc5ce45ad281b6a08fb7e529b1eb475040076834816570902acb6ebdd809410e31006efdeaa7f78a6c35574f3504963f5f7e4d92247d0eb4db3fc9abdda5d417'}}";
      new V2Request.Builder("/cluster")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "update-runtimelib/sha512"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "runtimeLib/foo/sha512"));

      try {
        new V2Request.Builder("/cluster")
            .withPayload(payload)
            .withMethod(SolrRequest.METHOD.POST)
            .build().process(cluster.getSolrClient());
        fail("should have failed");
      } catch (BaseHttpSolrClient.RemoteExecutionException e) {
        assertTrue("actual output : " + Utils.toJSONString(e.getMetaData()), e.getMetaData()._getStr("error/details[0]/errorMessages[0]", "").contains("Trying to update a jar with the same sha512"));
      }


      assertResponseValues(10,
          cluster.getSolrClient(),
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/get?abc=xyz", params),
          Utils.makeMap("get", "org.apache.solr.core.RuntimeLibSearchComponent",
              "loader", MemClassLoader.class.getName(),
              "Version", "2"));
    } finally {
      cluster.deleteAllCollections();
      cluster.shutdown();
      server.first().stop();
    }

  }

}
