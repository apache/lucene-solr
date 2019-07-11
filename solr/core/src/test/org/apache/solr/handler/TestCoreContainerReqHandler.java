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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.MemClassLoader;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.util.Utils.getObjectByPath;
import static org.apache.solr.core.TestDynamicLoading.getFileContent;

public class TestCoreContainerReqHandler extends SolrCloudTestCase {
  @BeforeClass
  public static void enableRuntimeLib() throws Exception {
    System.setProperty("enable.runtime.lib", "true");
    configureCluster(1).configure();
  }

  public static Pair<Server,Integer> runHttpServer(Map<String, Object> jars) throws Exception {
    int port = 0;
    int start = 30000 + random().nextInt(10000);
    for (int i = 0; i < 10000; i++) {
      try {
        new ServerSocket(start + i).close();
        port = 35000 + i;
        break;
      } catch (IOException e) {
        continue;
      }
    }
    if (port == 0) {
      fail("No port to be found");
    }
    Server server = null;
    server = new Server(port);
    server.setHandler(new AbstractHandler() {
      @Override
      public void handle(String s, Request request, HttpServletRequest req, HttpServletResponse rsp)
          throws IOException {
        ByteBuffer b = (ByteBuffer) jars.get(s);
        if (b != null) {
          rsp.getOutputStream().write(b.array(), 0, b.limit());
          rsp.setContentType("application/octet-stream");
          rsp.setStatus(HttpServletResponse.SC_OK);
          request.setHandled(true);
        }
      }
    });
    server.start();
    return new Pair<>(server, port);
  }

  public void testSetClusterReqHandler() throws Exception {
    new V2Request.Builder("/cluster")
        .withPayload("{add-requesthandler:{name : 'foo', class : 'org.apache.solr.handler.DumpRequestHandler'}}")
        .withMethod(SolrRequest.METHOD.POST)
        .build().process(cluster.getSolrClient());
    Map<String, Object> map = new ClusterProperties(zkClient()).getClusterProperties();

    assertEquals("org.apache.solr.handler.DumpRequestHandler",
        getObjectByPath(map, true, Arrays.asList("requestHandler", "foo", "class")));

    V2Response rsp = new V2Request.Builder("/node/ext/foo")
        .withMethod(SolrRequest.METHOD.GET)
        .withParams(new MapSolrParams((Map)Utils.makeMap("testkey", "testval")))
        .build().process(cluster.getSolrClient());
    assertEquals( "testval", rsp._getStr("parama/testkey",null));

    new V2Request.Builder("/cluster")
        .withPayload("{delete-requesthandler: 'foo'}")
        .withMethod(SolrRequest.METHOD.POST)
        .build().process(cluster.getSolrClient());

    assertNull(getObjectByPath(map, true, Arrays.asList("requestHandler", "foo")));

  }

  @Test
  public void testRuntimeLib() throws Exception {
    Map<String, Object> jars = Utils.makeMap(
        "/jar1.jar", getFileContent("runtimecode/runtimelibs.jar.bin"),
        "/jar2.jar", getFileContent("runtimecode/runtimelibs_v2.jar.bin"),
        "/jar3.jar", getFileContent("runtimecode/runtimelibs_v3.jar.bin"));

    Pair<Server,Integer> server = runHttpServer(jars);
    int port = server.second();

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
        assertTrue( e.getMetaData()._getStr("error/details[0]/errorMessages[0]", "").contains("expected sha512 hash :"));
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
        assertTrue( e.getMetaData()._getStr("error/details[0]/errorMessages[0]", "").contains("no such resource available: foo"));
      }

      payload = "{add-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar1.jar', " +
          "sha512 : 'd01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420'}}";
      new V2Request.Builder("/cluster")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "add-runtimelib/sha512"),
          getObjectByPath(new ClusterProperties(zkClient()).getClusterProperties(), true, "runtimeLib/foo/sha512"));



      new V2Request.Builder("/cluster")
          .withPayload("{add-requesthandler:{name : 'bar', class : 'org.apache.solr.core.RuntimeLibReqHandler'}}")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      Map<String, Object> map = new ClusterProperties(zkClient()).getClusterProperties();

      V2Response rsp = new V2Request.Builder("/node/ext/bar")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals("org.apache.solr.core.RuntimeLibReqHandler", rsp._getStr("class",null));
      assertEquals(MemClassLoader.class.getName(), rsp._getStr("loader",null));
      assertEquals(null, rsp._getStr("version",null));


      assertEquals("org.apache.solr.core.RuntimeLibReqHandler",
          getObjectByPath(map, true, Arrays.asList("requestHandler", "bar", "class")));


      payload =  "{update-runtimelib:{name : 'foo', url: 'http://localhost:" + port + "/jar3.jar', " +
          "sha512 : 'f67a7735a89b4348e273ca29e4651359d6d976ba966cb871c4b468ea1dbd452e42fcde9d188b7788e5a1ef668283c690606032922364759d19588666d5862653'}}";
      new V2Request.Builder("/cluster")
          .withPayload(payload)
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals(getObjectByPath(Utils.fromJSONString(payload), true, "update-runtimelib/sha512"),
          getObjectByPath(new ClusterProperties(zkClient()).getClusterProperties(), true, "runtimeLib/foo/sha512"));

      rsp = new V2Request.Builder("/node/ext/bar")
          .withMethod(SolrRequest.METHOD.POST)
          .build().process(cluster.getSolrClient());
      assertEquals("org.apache.solr.core.RuntimeLibReqHandler", rsp._getStr("class",null));
      assertEquals(MemClassLoader.class.getName(), rsp._getStr("loader",null));
      assertEquals("3", rsp._getStr("version",null));
    } finally {
      server.first().stop();
    }
  }
}
