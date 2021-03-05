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
package org.apache.solr.request;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.util.RestTestBase;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.core.TestSolrConfigHandler.runConfigCommand;

public class TestStreamBody extends RestTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";
  
  @Before
  public void before() throws Exception {
    File tmpSolrHome = createTempDir().toFile();
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());

    final SortedMap<ServletHolder, String> extraServlets = new TreeMap<>();

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-minimal.xml", "schema-rest.xml",
        "/solr", true, extraServlets);
    if (random().nextBoolean()) {
      log.info("These tests are run with V2 API");
      restTestHarness.setServerProvider(() -> jetty.getBaseUrl().toString() + "/____v2/cores/" + DEFAULT_TEST_CORENAME);
    }
  }

  @After
  public void after() throws Exception {
    if (jetty != null) {
      jetty.stop();
      jetty = null;
    }
    if (client != null) {
      client.close();
      client = null;
    }
    if (restTestHarness != null) {
      restTestHarness.close();
      restTestHarness = null;
    }
  }

  // SOLR-3161
  @Test
  public void testQtUpdateFails() throws Exception {
    enableStreamBody(true);
    SolrQuery query = new SolrQuery();
    query.setQuery( "*:*" );//for anything
    query.add("echoHandler","true");
    //sneaky sneaky
    query.add("qt","/update");
    query.add(CommonParams.STREAM_BODY,"<delete><query>*:*</query></delete>");

    QueryRequest queryRequest = new QueryRequest(query) {
      @Override
      public String getPath() { //don't let superclass substitute qt for the path
        return "/select";
      }
    };
    try {
      queryRequest.process(getSolrClient());
      fail();
    } catch (SolrException se) {
      assertTrue(se.getMessage(), se.getMessage().contains("Bad contentType for search handler :text/xml"));
    }
  }

  // Tests that stream.body is disabled by default, and can be edited through Config API
  @Test
  public void testStreamBodyDefaultAndConfigApi() throws Exception {
    SolrQuery query = new SolrQuery();
    query.add(CommonParams.STREAM_BODY,"<delete><query>*:*</query></delete>");
    query.add("commit","true");

    QueryRequest queryRequest = new QueryRequest(query) {
      @Override
      public String getPath() { //don't let superclass substitute qt for the path
        return "/update";
      }
    };
    SolrException se = expectThrows(SolrException.class, () -> queryRequest.process(getSolrClient()));
    assertTrue(se.getMessage(), se.getMessage().contains("Stream Body is disabled"));
    enableStreamBody(true);
    queryRequest.process(getSolrClient());
  }

  // Enables/disables stream.body through Config API
  private void enableStreamBody(boolean enable) throws Exception {
    RestTestHarness harness = restTestHarness;
    String payload = "{ 'set-property' : { 'requestDispatcher.requestParsers.enableStreamBody':" + enable + "} }";
    runConfigCommand(harness, "/config?wt=json", payload);
  }
}
