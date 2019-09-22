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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.core.MemClassLoader;
import org.apache.solr.core.PackageBag;
import org.apache.solr.core.RuntimeLib;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.apache.solr.cloud.TestCryptoKeys.readFile;
import static org.apache.solr.common.params.CommonParams.JAVABIN;
import static org.apache.solr.common.params.CommonParams.WT;
import static org.apache.solr.common.util.Utils.JAVABINCONSUMER;
import static org.apache.solr.common.util.Utils.getObjectByPath;
import static org.apache.solr.common.util.Utils.newBytesConsumer;
import static org.apache.solr.core.BlobRepository.sha256Digest;
import static org.apache.solr.core.TestDynamicLoading.getFileContent;

@SolrTestCaseJ4.SuppressSSL
@LogLevel("org.apache.solr.common.cloud.ZkStateReader=DEBUG;org.apache.solr.handler.admin.CollectionHandlerApi=DEBUG;org.apache.solr.core.PackageBag=DEBUG;org.apache.solr.common.cloud.ClusterProperties=DEBUG")
public class TestPackages extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("enable.package", "true");

  }

  static NavigableObject assertResponseValues(int repeats, SolrClient client, SolrRequest req, Map vals) throws Exception {
    Callable<NavigableObject> callable = () -> req.process(client);

    return assertResponseValues(repeats, callable,vals);
  }

  static NavigableObject assertResponseValues(int repeats,  Callable<NavigableObject> callable,Map vals) throws Exception {
    NavigableObject rsp = null;

    for (int i = 0; i < repeats; i++) {
      if (i > 0) {
        Thread.sleep(100);
      }
      try {
        rsp = callable.call();
      } catch (Exception e) {
        if (i >= repeats - 1) throw e;
        continue;
      }
      for (Object e : vals.entrySet()) {
        Map.Entry entry = (Map.Entry) e;
        String k = (String) entry.getKey();
        List<String> key = StrUtils.split(k, '/');

        Object val = entry.getValue();
        Predicate p = val instanceof Predicate ? (Predicate) val : o -> {
          String v = o == null ? null : String.valueOf(o);
          return Objects.equals(val, o);
        };
        boolean isPass = p.test(rsp._get(key, null));
        if (isPass) return rsp;
        else if (i >= repeats - 1) {
          fail("req: " + callable.toString() +" . attempt: " + i + " Mismatch for value : '" + key + "' in response , " + Utils.toJSONString(rsp));
        }

      }

    }
    return rsp;
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
  public void testPackageAPI() throws Exception {
    System.setProperty("enable.package", "true");
    MiniSolrCloudCluster cluster = configureCluster(4)
        .withJettyConfig(jetty -> jetty.enableV2(true))
        .configure();
    byte[] derFile = readFile("cryptokeys/pub_key512.der");
    cluster.getZkClient().makePath("/keys/exe", true);
    cluster.getZkClient().create("/keys/exe/pub_key512.der", derFile, CreateMode.PERSISTENT, true);
    try {

      postBlob(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs.jar.bin"), "e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc");
      postBlob(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs_v2.jar.bin"), "79298d7d5c3e60d91154efe7d72f4536eac46698edfa22ab894b85492d562ed4");
      postBlob(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs_v3.jar.bin"), "20e0bfaec71b2e93c4da9f2ed3745dda04dc3fc915b66cc0275863982e73b2a3");

      String payload = null;
      try {
        payload = "{add:{name : 'global' , version:'0.1', blob: {sha256 : 'wrong-sha256' , sig:'wrong-sig'}}}";
        new V2Request.Builder("/cluster/package")
            .withPayload(payload)
            .withMethod(SolrRequest.METHOD.POST)
            .build().process(cluster.getSolrClient());
        fail("Expected error");
      } catch (BaseHttpSolrClient.RemoteExecutionException e) {
        assertTrue("actual output : " + Utils.toJSONString(e.getMetaData()), e.getMetaData()._getStr("error/details[0]/errorMessages[0]", "").contains("No such blob: "));
      }


      payload = "{add:{name : 'global', version  :'1' , blob : {" +
          " sha256 : 'e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc' , " +
          "sig : 'L3q/qIGs4NaF6JiO0ZkMUFa88j0OmYc+I6O7BOdNuMct/xoZ4h73aZHZGc0+nmI1f/U3bOlMPINlSOM6LK3JpQ==' }}}";
      new V2Request.Builder("/cluster/package")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "add/blob/sha256"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "packages/global/blob/sha256"));


      new V2Request.Builder("/cluster")
          .withPayload("{add-requesthandler:{name : 'bar', class : 'org.apache.solr.core.RuntimeLibReqHandler', package : global}}")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      Map<String, Object> map = new ClusterProperties(cluster.getZkClient()).getClusterProperties();


      V2Request request = new V2Request.Builder("/node/ext/bar")
          .withMethod(SolrRequest.METHOD.POST)
          .build();
      assertResponseValues(10, cluster.getSolrClient(), request, Utils.makeMap(
          "class", "org.apache.solr.core.RuntimeLibReqHandler",
          "loader", PackageBag.PackageResourceLoader.class.getName(),
          "version", null));


      assertEquals("org.apache.solr.core.RuntimeLibReqHandler",
          getObjectByPath(map, true, asList("requestHandler", "bar", "class")));


      payload = "{update:{name : 'global' , version: '3'," +
          " blob: {sha256 : '20e0bfaec71b2e93c4da9f2ed3745dda04dc3fc915b66cc0275863982e73b2a3', " +
          "sig: 'a400n4T7FT+2gM0SC6+MfSOExjud8MkhTSFylhvwNjtWwUgKdPFn434Wv7Qc4QEqDVLhQoL3WqYtQmLPti0G4Q==' }}}";
      new V2Request.Builder("/cluster/package")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      Map<String, Object> clusterProperties = new ClusterProperties(cluster.getZkClient()).getClusterProperties();
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "update/blob/sha256"),
          getObjectByPath(clusterProperties, true, "packages/global/blob/sha256"));
      assertEquals("e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc",
          getObjectByPath(clusterProperties, true, "packages/global/blobs.old[0]"));


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
      new V2Request.Builder("/cluster/package")
          .withPayload("{delete : 'global'}")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());

      assertResponseValues(10, cluster.getSolrClient(),
          new V2Request.Builder("/cluster/package")
              .forceV2(true)
              .withMethod(SolrRequest.METHOD.GET) .build(),
          ImmutableMap.of(CommonParams.PACKAGES,
          (Predicate<Object>) o -> o instanceof Map && ((Map) o).isEmpty()));


      String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();
      try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl).build()) {
        V2Response rsp = new V2Request.Builder("/node/blob")
            .withMethod(SolrRequest.METHOD.GET)
            .forceV2(true)
            .build()
            .process(client);
        assertNotNull(rsp._get(asList("blob", "e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc"), null));
        assertNotNull(rsp._get(asList("blob", "20e0bfaec71b2e93c4da9f2ed3745dda04dc3fc915b66cc0275863982e73b2a3"), null));

        ByteBuffer buf = Utils.executeGET(client.getHttpClient(),
            baseUrl.replace("/solr", "/api") + "/node/blob/e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc",
            newBytesConsumer(Integer.MAX_VALUE));
        assertEquals("e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc", sha256Digest(buf));


        buf = Utils.executeGET(client.getHttpClient(), baseUrl.replace("/solr", "/api") + "/node/blob/20e0bfaec71b2e93c4da9f2ed3745dda04dc3fc915b66cc0275863982e73b2a3",
            newBytesConsumer(Integer.MAX_VALUE));
        assertEquals("20e0bfaec71b2e93c4da9f2ed3745dda04dc3fc915b66cc0275863982e73b2a3", sha256Digest(buf));
      }


    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testRuntimeLibWithSig2048() throws Exception {
    MiniSolrCloudCluster cluster = configureCluster(4)
        .withJettyConfig(jetty -> jetty.enableV2(true))
        .configure();

    try {
      postBlob(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs.jar.bin"), "e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc");
      postBlob(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs_v2.jar.bin"), "79298d7d5c3e60d91154efe7d72f4536eac46698edfa22ab894b85492d562ed4");
      postBlob(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs_v3.jar.bin"), "20e0bfaec71b2e93c4da9f2ed3745dda04dc3fc915b66cc0275863982e73b2a3");

      byte[] derFile = readFile("cryptokeys/pub_key2048.der");
      cluster.getZkClient().makePath("/keys/exe", true);
      cluster.getZkClient().create("/keys/exe/pub_key2048.der", derFile, CreateMode.PERSISTENT, true);

      String signature = "NaTm3+i99/ZhS8YRsLc3NLz2Y6VuwEbu7DihY8GAWwWIGm+jpXgn1JiuaenfxFCcfNKCC9WgZmEgbTZTzmV/OZMVn90u642YJbF3vTnzelW1pHB43ZRAJ1iesH0anM37w03n3es+vFWQtuxc+2Go888fJoMkUX2C6Zk6Jn116KE45DWjeyPM4mp3vvGzwGvdRxP5K9Q3suA+iuI/ULXM7m9mV4ruvs/MZvL+ELm5Jnmk1bBtixVJhQwJP2z++8tQKJghhyBxPIC/2fkAHobQpkhZrXu56JjP+v33ul3Ku4bbvfVMY/LVwCAEnxlvhk+C6uRCKCeFMrzQ/k5inasXLw==";

      String payload = "{add:{name : 'global', version: '1', blob: {" +
          "  sig : 'EdYkvRpMZbvElN93/xUmyKXcj6xHP16AVk71TlTascEwCb5cFQ2AeKhPIlwYpkLWXEOcLZKfeXoWwOLaV5ZNhg==' ," +
          "sha256 : 'e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc'}}}";
      try {
        new V2Request.Builder("/cluster/package")
            .withPayload(payload)
            .withMethod(SolrRequest.METHOD.POST)
            .build().process(cluster.getSolrClient());
        fail("should have failed");
      } catch (BaseHttpSolrClient.RemoteExecutionException e) {
        //No key matched signature for jar
        assertTrue(e.getMetaData()._getStr("/error/details[0]/errorMessages[0]", "")
            .contains("Invalid signature for blob"));
      }


      payload = "{add:{name : 'global', version : '1', blob:{  sig : '" + signature +
          "', sha256 : 'e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc'}}}";

      new V2Request.Builder("/cluster/package")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "add/blob/sha256"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "packages/global/blob/sha256"));

      new V2Request.Builder("/cluster")
          .withPayload("{add-requesthandler:{name : 'bar', class : 'org.apache.solr.core.RuntimeLibReqHandler' package : global}}")
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
          getObjectByPath(map, true, asList("requestHandler", "bar", "class")));

      payload = "{update:{name : 'global', version : '3', blob:{ sig : 'YxFr6SpYrDwG85miDfRWHTjU9UltjtIWQZEhcV55C2rczRUVowCYBxmsDv5mAM8j0CTv854xpI1DtBT86wpoTdbF95LQuP9FJId4TS1j8bZ9cxHP5Cqyz1uBHFfUUNUrnpzTHQkVTp02O9NAjh3c2W41bL4U7j6jQ32+4CW2M+x00TDG0y0H75rQDR8zbLt31oWCz+sBOdZ3rGKJgAvdoGm/wVCTmsabZN+xoz4JaDeBXF16O9Uk9SSq4G0dz5YXFuLxHK7ciB5t0+q6pXlF/tdlDqF76Abze0R3d2/0MhXBzyNp3UxJmj6DiprgysfB0TbQtJG0XGfdSmx0VChvcA==' ," +
          "sha256 : '20e0bfaec71b2e93c4da9f2ed3745dda04dc3fc915b66cc0275863982e73b2a3'}}}";

      new V2Request.Builder("/cluster/package")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "update/blob/sha256"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "packages/global/blob/sha256"));


      request = new V2Request.Builder("/node/ext/bar")
          .withMethod(SolrRequest.METHOD.POST)
          .build();
      assertResponseValues(10, cluster.getSolrClient(), request, Utils.makeMap(
          "class", "org.apache.solr.core.RuntimeLibReqHandler",
          "loader", MemClassLoader.class.getName(),
          "version", "3"));
      assertResponseValues(5, cluster.getSolrClient(),
          new V2Request.Builder("/cluster/package")
          .forceV2(true)
          .withMethod(SolrRequest.METHOD.GET)
          .build(),
          Utils.makeMap("/packages/global/blob/sha256","20e0bfaec71b2e93c4da9f2ed3745dda04dc3fc915b66cc0275863982e73b2a3"));

    } finally {
      cluster.shutdown();
    }

  }


  @Test
  public void testSetClusterReqHandler() throws Exception {
    MiniSolrCloudCluster cluster = configureCluster(4)
        .withJettyConfig(jetty -> jetty.enableV2(true))
        .configure();
    try {
      SolrZkClient zkClient = cluster.getZkClient();
      new V2Request.Builder("/cluster")
          .withPayload("{add-requesthandler:{name : 'foo', class : 'org.apache.solr.handler.DumpRequestHandler'}}")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());

      Map<String, Object> map = assertVersionInSync(zkClient, cluster.getSolrClient());

      assertEquals("org.apache.solr.handler.DumpRequestHandler",
          getObjectByPath(map, true, asList("requestHandler", "foo", "class")));

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

      assertNull(getObjectByPath(map, true, asList("requestHandler", "foo")));
    } finally {
      cluster.shutdown();
    }

  }

  public void testPluginFrompackage() throws Exception {
    String COLLECTION_NAME = "globalLoaderColl";

    System.setProperty("enable.runtime.lib", "true");
    MiniSolrCloudCluster cluster = configureCluster(4)
        .withJettyConfig(jetty -> jetty.enableV2(true))
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    byte[] derFile = readFile("cryptokeys/pub_key512.der");
    cluster.getZkClient().makePath("/keys/exe", true);
    cluster.getZkClient().create("/keys/exe/pub_key512.der", derFile, CreateMode.PERSISTENT, true);

    try {
      postBlob(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs.jar.bin"), "e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc");
      postBlob(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs_v2.jar.bin"), "79298d7d5c3e60d91154efe7d72f4536eac46698edfa22ab894b85492d562ed4");
      postBlob(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs_v3.jar.bin"), "20e0bfaec71b2e93c4da9f2ed3745dda04dc3fc915b66cc0275863982e73b2a3");

      CollectionAdminRequest
          .createCollection(COLLECTION_NAME, "conf", 2, 1)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());


      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);
      String payload = "{add:{name : 'global', version : '1'," +
          " blob:{ sha256 : 'e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc' ," +
          "sig : 'L3q/qIGs4NaF6JiO0ZkMUFa88j0OmYc+I6O7BOdNuMct/xoZ4h73aZHZGc0+nmI1f/U3bOlMPINlSOM6LK3JpQ=='}}}";
      new V2Request.Builder("/cluster/package")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      String sha256 = (String) getObjectByPath(Utils.fromJSONString(payload), true, "add/blob/sha256");

      assertEquals(sha256,
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "packages/global/blob/sha256"));


      payload = "{\n" +
          "'create-requesthandler' : { 'name' : '/runtime', 'class': 'org.apache.solr.core.RuntimeLibReqHandler' , 'package':global }," +
          "'create-searchcomponent' : { 'name' : 'get', 'class': 'org.apache.solr.core.RuntimeLibSearchComponent' , 'package':global }," +
          "'create-queryResponseWriter' : { 'name' : 'json1', 'class': 'org.apache.solr.core.RuntimeLibResponseWriter' , 'package':global }" +
          "}";
      cluster.getSolrClient().request(new ConfigRequest(payload) {
        @Override
        public String getCollection() {
          return COLLECTION_NAME;
        }
      });

      SolrParams params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME,
          WT, JAVABIN,
          "meta", "true"));

      GenericSolrRequest req1 = new GenericSolrRequest(SolrRequest.METHOD.GET, "/config/queryResponseWriter/json1", params);
//      SimpleSolrResponse rsp = req1.process(cluster.getSolrClient());
//      System.out.println(rsp.jsonStr());
      assertResponseValues(10,
          cluster.getSolrClient(),
          req1,
          Utils.makeMap(
              "/config/queryResponseWriter/json1/_packageinfo_/blob/sha256", sha256
          ));

      params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME,
          WT, JAVABIN,
          "meta", "true"));

      assertResponseValues(10,
          cluster.getSolrClient(),
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/config/searchComponent/get", params),
          Utils.makeMap(
              "config/searchComponent/get/_packageinfo_/blob/sha256", sha256
          ));

      params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME,
          WT, JAVABIN,
          "meta", "true"));

      assertResponseValues(10,
          cluster.getSolrClient(),
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/config/requestHandler/runtime", params),
          Utils.makeMap(
              ":config:requestHandler:/runtime:_packageinfo_:blob:sha256", sha256
          ));


      params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME, WT, JAVABIN));
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
              "loader", PackageBag.PackageResourceLoader.class.getName()));

      assertResponseValues(10,
          cluster.getSolrClient(),
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/get?abc=xyz", params),
          Utils.makeMap("get", "org.apache.solr.core.RuntimeLibSearchComponent",
              "loader", PackageBag.PackageResourceLoader.class.getName()));

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
              "loader", PackageBag.PackageResourceLoader.class.getName()));


      payload = "{update:{name : 'global', version : '2'" +
          "blob : { sha256 : '79298d7d5c3e60d91154efe7d72f4536eac46698edfa22ab894b85492d562ed4'," +
          " sig : 'j+Rflxi64tXdqosIhbusqi6GTwZq8znunC/dzwcWW0/dHlFGKDurOaE1Nz9FSPJuXbHkVLj638yZ0Lp1ssnoYA=='}}}";
      new V2Request.Builder("/cluster/package")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      sha256 = (String) getObjectByPath(Utils.fromJSONString(payload), true, "update/sha256");

      assertEquals(sha256,
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "package/global/sha256"));

      params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME,
          WT, JAVABIN,
          "meta", "true"));

      assertResponseValues(10,
          cluster.getSolrClient(),
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/config/queryResponseWriter/json1", params),
          Utils.makeMap(
              "/config/queryResponseWriter/json1/_packageinfo_/sha256", sha256
          ));

      params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME,
          WT, JAVABIN,
          "meta", "true"));

      assertResponseValues(10,
          cluster.getSolrClient(),
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/config/searchComponent/get", params),
          Utils.makeMap(
              "/config/searchComponent/get/_packageinfo_/sha256", sha256
          ));

      params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME,
          WT, JAVABIN,
          "meta", "true"));

      assertResponseValues(10,
          cluster.getSolrClient(),
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/config/requestHandler/runtime", params),
          Utils.makeMap(
              ":config:requestHandler:/runtime:_packageinfo_:sha256", sha256
          ));


      try {
        new V2Request.Builder("/cluster/package")
            .withPayload(payload)
            .withMethod(SolrRequest.METHOD.POST)
            .build().process(cluster.getSolrClient());
        fail("should have failed");
      } catch (BaseHttpSolrClient.RemoteExecutionException e) {
        assertTrue("actual output : " + Utils.toJSONString(e.getMetaData()), e.getMetaData()._getStr("error/details[0]/errorMessages[0]", "").contains("Trying to update a package with the same data"));
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
    }

  }

  //  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-13650")
  @Ignore
  public void testCacheLoadFromPackage() throws Exception {
    String COLLECTION_NAME = "globalCacheColl";

    String overlay = "{" +
        "    \"props\":{\"query\":{\"documentCache\":{\n" +
        "          \"class\":\"org.apache.solr.core.MyDocCache\",\n" +
        "          \"size\":\"512\",\n" +
        "          \"initialSize\":\"512\" , \"package\":\"cache_pkg\"}}}}";
    MiniSolrCloudCluster cluster = configureCluster(4)
        .withJettyConfig(jetty -> jetty.enableV2(true))
        .addConfig("conf", configset("cloud-minimal"),
            Collections.singletonMap(ConfigOverlay.RESOURCE_NAME, overlay.getBytes(UTF_8)))
        .configure();
    try {
      postBlob(cluster.getSolrClient(), getFileContent("runtimecode/cache.jar.bin"),"32e8b5b2a95ea306538b52017f0954aa1b0f8a8b2d0acbc498fd0e66a223f7bd");
      postBlob(cluster.getSolrClient(), getFileContent("runtimecode/cache_v2.jar.bin"),"0f670f6dcc2b00f9a448a7ebd457d4ff985ab702c85cdb3608dcae9889e8d702");
      String payload = "{add:{name : 'cache_pkg', version : '1', " +
           " blob: { sha256 : '32e8b5b2a95ea306538b52017f0954aa1b0f8a8b2d0acbc498fd0e66a223f7bd', " +
          "sig : 'A2CDnReirpII005KRN1C3pvt4NM4kItsagQPNaa3ljj/5R3LKVgiPuNvqBsffU8n81LOAfr5VMyGFcb4QMHpyg==' }}}";

      try {
        new V2Request.Builder("/cluster/package")
            .withPayload(payload)
            .withMethod(SolrRequest.METHOD.POST)
            .build().process(cluster.getSolrClient());
        fail("should have failed");
      } catch (BaseHttpSolrClient.RemoteExecutionException e) {
        //No key matched signature for jar
        assertTrue(e.getMetaData()._getStr("/error/details[0]/errorMessages[0]", "")
            .contains("No public keys in ZK : /keys/exe"));
      }

      byte[] derFile = readFile("cryptokeys/pub_key512.der");
      cluster.getZkClient().makePath("/keys/exe", true);
      cluster.getZkClient().create("/keys/exe/pub_key512.der", derFile, CreateMode.PERSISTENT, true);



      new V2Request.Builder("/cluster/package")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());

      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "add/blob/sha256"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "packages/cache_pkg/blob/sha256"));

      CollectionAdminRequest
          .createCollection(COLLECTION_NAME, "conf", 2, 1)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());


      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);
      SolrParams params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME, WT, JAVABIN));

      NamedList<Object> rsp = cluster.getSolrClient().request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/config/overlay", params));
      assertEquals("org.apache.solr.core.MyDocCache", rsp._getStr("overlay/props/query/documentCache/class", null));

      String sha256 = (String) getObjectByPath(Utils.fromJSONString(payload), true, "add/sha256");


      params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME,
          WT, JAVABIN,
          "meta", "true"));

      assertResponseValues(10,
          cluster.getSolrClient(),
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/config/query/documentCache", params),
          Utils.makeMap(
              "/config/query/documentCache/_packageinfo_/blob/sha256", sha256
          ));


      UpdateRequest req = new UpdateRequest();

      req.add("id", "1", "desc_s", "document 1")
          .setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true)
          .setWaitSearcher(true);
      cluster.getSolrClient().request(req, COLLECTION_NAME);

      SolrQuery solrQuery = new SolrQuery("q", "id:1", "collection", COLLECTION_NAME);
      assertResponseValues(10,
          cluster.getSolrClient(),
          new QueryRequest(solrQuery),
          Utils.makeMap("/response[0]/my_synthetic_fld_s", "version_1"));


      payload = "{update:{name : 'cache_pkg', version : '2', " +
          "blob: { sha256 : '0f670f6dcc2b00f9a448a7ebd457d4ff985ab702c85cdb3608dcae9889e8d702' ," +
          " sig : 'SOrekHt+uup+z2z+nZU5indk2huRRfmbM+W+vQ0variHrcZEG9EXt5LuPFl8Ki9Ahr6klMHdVP8nj4wuQhu/Hg==' }}}";

      new V2Request.Builder("/cluster/package")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      sha256 = (String) getObjectByPath(Utils.fromJSONString(payload), true, "update/blob/sha256");
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "update/blob/sha256"),
          getObjectByPath(new ClusterProperties(cluster.getZkClient()).getClusterProperties(), true, "packages/cache_pkg/blob/sha256"));

      params = new MapSolrParams((Map) Utils.makeMap("collection", COLLECTION_NAME,
          WT, JAVABIN,
          "meta", "true"));

      assertResponseValues(10,
          cluster.getSolrClient(),
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/config/query/documentCache", params),
          Utils.makeMap(
              "/config/query/documentCache/_packageinfo_/blob/sha256", sha256
          ));
      req = new UpdateRequest();
      req.add("id", "2", "desc_s", "document 1")
          .setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true)
          .setWaitSearcher(true);
      cluster.getSolrClient().request(req, COLLECTION_NAME);


      solrQuery = new SolrQuery("q", "id:2", "collection", COLLECTION_NAME);
      NavigableObject result = assertResponseValues(10,
          cluster.getSolrClient(),
          new QueryRequest(solrQuery),
          Utils.makeMap("response[0]/my_synthetic_fld_s", "version_2"));

    } finally {
      cluster.deleteAllCollections();
      cluster.shutdown();
    }
  }

  public void testBlobManagement() throws Exception {
    MiniSolrCloudCluster cluster = configureCluster(4)
        .withJettyConfig(jetty -> jetty.enableV2(true))
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    try {
      postBlob(cluster.getSolrClient(), getFileContent("runtimecode/runtimelibs.jar.bin"),
          "e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc" );

      Map expected = Utils.makeMap("/blob/e1f9e23988c19619402f1040c9251556dcd6e02b9d3e3b966a129ea1be5c70fc", (Predicate<?>) o -> o != null);
      for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
        String url =  jettySolrRunner.getBaseUrl().toString().replace("/solr", "/api") + "/node/blob?wt=javabin";
        assertResponseValues(20, new Callable<>() {
          @Override
          public NavigableObject call() throws Exception {
            try (HttpSolrClient solrClient = (HttpSolrClient) jettySolrRunner.newClient()) {
              return (NavigableObject) Utils.executeGET(solrClient.getHttpClient(), url, JAVABINCONSUMER);
            }
          }

          @Override
          public String toString() {
            return url;
          }
        }, expected);

      }


    } finally {
      cluster.shutdown();
    }


  }

  public void testRepoCRUD() throws Exception{
    MiniSolrCloudCluster cluster = configureCluster(4)
        .withJettyConfig(jetty -> jetty.enableV2(true))
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    try {
      String payload = "{add : {name : myrepo, url: 'http://localhost/abc' , version : '1.1'}}";
      new V2Request.Builder("/cluster/repository")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      Map repojson = Utils.getJson(cluster.getZkClient(), ZkStateReader.PACKAGE_REPO, true);

      assertEquals("http://localhost/abc", Utils.getObjectByPath(repojson, true, "/repository/myrepo/url"));
      assertEquals("1.1", Utils.getObjectByPath(repojson, true, "/repository/myrepo/version"));
      payload = "{update : {name : myrepo, url: 'http://localhost/abc' , version : '1.2'}}";
    new V2Request.Builder("/cluster/repository")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      repojson = Utils.getJson(cluster.getZkClient(), ZkStateReader.PACKAGE_REPO, true);

      assertEquals("http://localhost/abc", Utils.getObjectByPath(repojson, true, "/repository/myrepo/url"));
      assertEquals("1.2", Utils.getObjectByPath(repojson, true, "/repository/myrepo/version"));

      payload = "{delete :  myrepo}";
    new V2Request.Builder("/cluster/repository")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      repojson = Utils.getJson(cluster.getZkClient(), ZkStateReader.PACKAGE_REPO, true);

      assertNull( Utils.getObjectByPath(repojson, true, "/repository/myrepo"));


    }finally {
      cluster.shutdown();
    }

  }

  private void postBlob(SolrClient client, ByteBuffer blob, String sh256) throws SolrServerException, IOException {
    V2Response rsp = new V2Request.Builder("/cluster/blob")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload(blob)
        .forceV2(true)
        .withMimeType("application/octet-stream")
        .build()
        .process(client);
    assertEquals(sh256, rsp.getResponse().get(RuntimeLib.SHA256));
  }

}
