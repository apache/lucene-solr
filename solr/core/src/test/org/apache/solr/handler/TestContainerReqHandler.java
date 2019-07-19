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
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.MapSolrParams;
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

import static org.apache.solr.cloud.TestCryptoKeys.readFile;
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
      try {
        SolrResponse rsp = req.process(client);
        try {
          for (Object e : vals.entrySet()) {
            Map.Entry entry = (Map.Entry) e;
            String key = (String) entry.getKey();
            Object val = entry.getValue();
            Predicate p = val instanceof Predicate ? (Predicate) val : o -> {
              String v = o == null ? null : String.valueOf(o);
              return Objects.equals(val, o);
            };
            assertTrue("attempt: " + i + " Mismatch for value : '" + key + "' in response " + Utils.toJSONString(rsp),
                p.test(rsp.getResponse()._get(key, null)));

          }
          return;
        } catch (Exception e) {
          if (i >= repeats - 1) throw e;
          continue;
        }

      } catch (Exception e) {
        if (i >= repeats - 1) throw e;
        log.error("exception in request", e);
        continue;
      }
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

      Map<String, Object> map = assertVersionInSync(zkClient);

      assertEquals("org.apache.solr.handler.DumpRequestHandler",
          getObjectByPath(map, true, Arrays.asList("requestHandler", "foo", "class")));

      assertVersionInSync(zkClient);
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
      shutdownCluster();
    }

  }

  private Map<String, Object> assertVersionInSync(SolrZkClient zkClient) throws SolrServerException, IOException {
    Stat stat = new Stat();
    Map<String, Object> map = new ClusterProperties(zkClient).getClusterProperties(stat);
    assertEquals(String.valueOf(stat.getVersion()), getExtResponse()._getStr("metadata/version", null));
    return map;
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
          "sha512 : 'f67a7735a89b4348e273ca29e4651359d6d976ba966cb871c4b468ea1dbd452e42fcde9d188b7788e5a1ef668283c690606032922364759d19588666d5862653'}}";
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
      server.first().stop();
      cluster.shutdown();
    }
  }

  @Test
  public void testRutimeLibWithSig() throws Exception {
    Map<String, Object> jars = Utils.makeMap(
        "/jar1.jar", getFileContent("runtimecode/runtimelibs.jar.bin"),
        "/jar2.jar", getFileContent("runtimecode/runtimelibs_v2.jar.bin"),
        "/jar3.jar", getFileContent("runtimecode/runtimelibs_v3.jar.bin"));

    Pair<Server, Integer> server = runHttpServer(jars);
    int port = server.second();
    MiniSolrCloudCluster cluster =  configureCluster(4).configure();

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
          "sig : 'BSx/v0eKWX+LzkWF+iIAzwGL9rezWMePsyRzi4TvV6boATZ9cSfeUAqUgRW50f/hAHX4/hrHr2Piy8za9tIUoXbLqn3xJNNroOqpcVEgwh1Zii4c7zPwUSB9gtd9zlAK4LAPLdjxILS8NXpTD2zLycc8kSpcyTpSTITqz6HA3HsPGC81WIq2k3IRqYAkacn46viW+nnEjA7OxDCOqoL//evjxDWQ6R1YggTGh4u5MSWZJCiCPJNQnTlPRzUZOAJjtX7PblDrKeiunKGbjtiOhFLYkupe1lSlIRLiJV/qqopO4TQGO1bhbxeCKAX2vEz5Ch5bGOa+VZLJJGaDo318UQ==' ," +
          "sha512 : 'f67a7735a89b4348e273ca29e4651359d6d976ba966cb871c4b468ea1dbd452e42fcde9d188b7788e5a1ef668283c690606032922364759d19588666d5862653'}}";

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

  private V2Response getExtResponse() throws SolrServerException, IOException {
    return new V2Request.Builder("/node/ext")
        .withMethod(SolrRequest.METHOD.GET)
        .build().process(cluster.getSolrClient());
  }
}
