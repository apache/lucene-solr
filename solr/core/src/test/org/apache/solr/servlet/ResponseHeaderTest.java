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
package org.apache.solr.servlet;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;


public class ResponseHeaderTest extends SolrJettyTestBase {
  
  private static File solrHomeDirectory;
  private static JettySolrRunner jetty;

  @BeforeClass
  public static void beforeTest() throws Exception {
    solrHomeDirectory = SolrTestUtil.createTempDir().toFile();
    setupJettyTestHome(solrHomeDirectory, "collection1");
    String top = SolrTestUtil.TEST_HOME() + "/collection1/conf";
    FileUtils.copyFile(new File(top, "solrconfig-headers.xml"), new File(solrHomeDirectory + "/collection1/conf", "solrconfig.xml"));
    jetty = createAndStartJetty(solrHomeDirectory.getAbsolutePath());
  }
  
  @AfterClass
  public static void afterTest() throws Exception {
    if (null != solrHomeDirectory) {
      cleanUpJettyHome(solrHomeDirectory);
    }
  }
  
  @Test
  @Ignore // MRM TODO: use Http2SolrClient#GET
  public void testHttpResponse() throws SolrServerException, IOException {
    Http2SolrClient client = (Http2SolrClient) getSolrClient(jetty);

    URI uri = URI.create(client.getBaseURL() + "/withHeaders?q=*:*");
    HttpGet httpGet = new HttpGet(uri);
    // HttpResponse response = httpClient.execute(httpGet);
    //   Header[] headers = response.getAllHeaders();
    boolean containsWarningHeader = false;
    //    for (Header header:headers) {
    //      if ("Warning".equals(header.getName())) {
    //        containsWarningHeader = true;
    //        assertEquals("This is a test warning", header.getValue());
    //        break;
    //      }
    //    }
    assertTrue("Expected header not found", containsWarningHeader);

  }
  
  public static class ComponentThatAddsHeader extends SearchComponent {
    
    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
      rb.rsp.addHttpHeader("Warning", "This is a test warning");
    }
    
    @Override
    public void process(ResponseBuilder rb) throws IOException {}
    
    @Override
    public String getDescription() {
      return null;
    }
  }
  
}
