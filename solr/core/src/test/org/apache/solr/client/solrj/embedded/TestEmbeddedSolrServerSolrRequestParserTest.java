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
package org.apache.solr.client.solrj.embedded;


import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Test;


public class TestEmbeddedSolrServerSolrRequestParserTest extends SolrTestCaseJ4 {

  private static final String NEWCORE = "newcore";
  private static final String NEWCORE_STREAM = "newcore-stream";
  private static final String MINIMAL = "minimal";
  private static final String MINIMAL_WITH_STREAM_BODY = "minimal-steam-body-enabled";
  private final static String body = "AMANAPLANPANAMA";

  @Test(expected = SolrException.class)
  public void testStreamBodyException() throws Exception {
    Path path = createTempDir();
    SolrResourceLoader loader = new SolrResourceLoader(path);
    NodeConfig config = new NodeConfig.NodeConfigBuilder("testnode", loader)
        .setConfigSetBaseDirectory(Paths.get(TEST_HOME()).resolve("configsets").toString())
        .build();

    EmbeddedSolrServer server = new EmbeddedSolrServer(config, NEWCORE);
    CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
    createRequest.setCoreName(NEWCORE);
    createRequest.setConfigSet(MINIMAL);
    server.request(createRequest);
    SolrCore core = server.getCoreContainer().getCore(NEWCORE);
    server.close();
    core.close();
    EmbeddedSolrServer solrServer = new EmbeddedSolrServer(core);
    ModifiableSolrParams query = new ModifiableSolrParams();
    query.set(CommonParams.STREAM_BODY, body);
    try {
      solrServer.query(query);
    } finally {
      solrServer.close();
    }
  }

  @Test
  public void testStreamBodySuccessRequest() throws Exception {
    Path path = createTempDir();
    SolrResourceLoader loader = new SolrResourceLoader(path);
    NodeConfig config = new NodeConfig.NodeConfigBuilder("testnode", loader)
        .setConfigSetBaseDirectory(Paths.get(TEST_HOME()).resolve("configsets").toString())
        .build();

    EmbeddedSolrServer server = new EmbeddedSolrServer(config, NEWCORE_STREAM);
    CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
    createRequest.setCoreName(NEWCORE_STREAM);
    createRequest.setConfigSet(MINIMAL_WITH_STREAM_BODY);
    server.request(createRequest);
    SolrCore core = server.getCoreContainer().getCore(NEWCORE_STREAM);
    server.close();
    core.close();
    EmbeddedSolrServer solrServer = new EmbeddedSolrServer(core);
    ModifiableSolrParams query = new ModifiableSolrParams();
    query.set(CommonParams.STREAM_BODY, body);
    solrServer.close();
  }


}
