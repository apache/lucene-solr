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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.RestTestHarness;
import org.junit.BeforeClass;

import static java.util.Arrays.asList;
import static org.apache.solr.handler.TestSolrConfigHandlerCloud.compareValues;

public class TestDynamicLoadingUrl extends AbstractFullDistribZkTestBase {

  @BeforeClass
  public static void enableRuntimeLib() throws Exception {
    System.setProperty("enable.runtime.lib", "true");
  }

  public static class JarHandler extends RequestHandlerBase {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      rsp.add(ReplicationHandler.FILE_STREAM, (SolrCore.RawWriter) os -> {

        ByteBuffer buf = TestDynamicLoading.getFileContent("runtimecode/runtimelibs.jar.bin");
        if (buf == null) {
          //should never happen unless a user wrote this document directly
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "Invalid document . No field called blob");
        } else {
          os.write(buf.array(), 0, buf.limit());
        }
      });

    }

    @Override
    public String getDescription() {
      return "serves jar files";
    }
  }


  public void testDynamicLoadingUrl() throws Exception {
    setupRestTestHarnesses();
    String payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/jarhandler', 'class': " + JarHandler.class.getName() +
        ", registerPath: '/solr,/v2' }\n" +
        "}";

    RestTestHarness client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client, "/config", payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay",
        null,
        Arrays.asList("overlay", "requestHandler", "/jarhandler", "class"),
        JarHandler.class.getName(), 10);
      payload = "{\n" +
          "'add-runtimelib' : { 'name' : 'urljar', url : '" + client.getBaseURL() + "/jarhandler?wt=filestream'" +
          "  'sha512':'e01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420'}" +
          "}";
      client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommandExpectFailure(client, "/config", payload, "Invalid jar");


//    String url = client
    payload = "{\n" +
        "'add-runtimelib' : { 'name' : 'urljar', url : '" + client.getBaseURL() + "/jarhandler?wt=filestream'" +
        "  'sha512':'d01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420'}" +
        "}";
    client = randomRestTestHarness();
    TestSolrConfigHandler.runConfigCommand(client, "/config", payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay",
        null,
        Arrays.asList("overlay", "runtimeLib", "urljar", "sha512"),
        "d01b51de67ae1680a84a813983b1de3b592fc32f1a22b662fc9057da5953abd1b72476388ba342cad21671cd0b805503c78ab9075ff2f3951fdf75fa16981420", 10);

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
        "org.apache.solr.core.RuntimeLibReqHandler", 10);

    Map result = TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/runtime",
        null,
        Arrays.asList("class"),
        "org.apache.solr.core.RuntimeLibReqHandler", 10);
    compareValues(result, MemClassLoader.class.getName(), asList("loader"));


  }
}

