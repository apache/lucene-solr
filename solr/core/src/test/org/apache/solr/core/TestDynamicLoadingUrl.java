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

package org.apache.solr.core;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.util.Pair;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.BeforeClass;

import static java.util.Arrays.asList;
import static org.apache.solr.core.TestDynamicLoading.getFileContent;
import static org.apache.solr.handler.TestSolrConfigHandlerCloud.compareValues;

@SolrTestCaseJ4.SuppressSSL
public class TestDynamicLoadingUrl extends AbstractFullDistribZkTestBase {

  @BeforeClass
  public static void enableRuntimeLib() throws Exception {
    System.setProperty("enable.runtime.lib", "true");
  }

  public static Pair<Server, Integer> runHttpServer(Map<String, Object> jars) throws Exception {
    final Server server = new Server();
    final ServerConnector connector = new ServerConnector(server);
    server.setConnectors(new Connector[] { connector });
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
    return new Pair<>(server, connector.getLocalPort());
  }

  public void testDynamicLoadingUrl() throws Exception {
    setupRestTestHarnesses();
    Pair<Server, Integer> pair = runHttpServer(ImmutableMap.of("/jar1.jar", getFileContent("runtimecode/runtimelibs.jar.bin")));
    Integer port = pair.second();

    try {
      String payload = "{\n" +
          "'add-runtimelib' : { 'name' : 'urljar', url : 'http://localhost:" + port + "/jar1.jar'" +
          "  'sha512':'e01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420'}" +
          "}";
      RestTestHarness client = randomRestTestHarness();
      TestSolrConfigHandler.runConfigCommandExpectFailure(client, "/config", payload, "Invalid jar");


      payload = "{\n" +
          "'add-runtimelib' : { 'name' : 'urljar', url : 'http://localhost:" + port + "/jar1.jar'" +
          "  'sha512':'d01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420'}" +
          "}";
      client = randomRestTestHarness();
      TestSolrConfigHandler.runConfigCommand(client, "/config", payload);
      TestSolrConfigHandler.testForResponseElement(client,
          null,
          "/config/overlay",
          null,
          Arrays.asList("overlay", "runtimeLib", "urljar", "sha512"),
          "d01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420", 120);

      payload = "{\n" +
          "'create-requesthandler' : { 'name' : '/runtime', 'class': 'org.apache.solr.core.RuntimeLibReqHandler', 'runtimeLib' : true}" +
          "}";
      client = randomRestTestHarness();
      TestSolrConfigHandler.runConfigCommand(client, "/config", payload);

      TestSolrConfigHandler.testForResponseElement(client,
          null,
          "/config/overlay",
          null,
          Arrays.asList("overlay", "requestHandler", "/runtime", "class"),
          "org.apache.solr.core.RuntimeLibReqHandler", 120);

      @SuppressWarnings({"rawtypes"})
      Map result = TestSolrConfigHandler.testForResponseElement(client,
          null,
          "/runtime",
          null,
          Arrays.asList("class"),
          "org.apache.solr.core.RuntimeLibReqHandler", 120);
      compareValues(result, MemClassLoader.class.getName(), asList("loader"));
    } finally {
      pair.first().stop();

    }


  }
}

