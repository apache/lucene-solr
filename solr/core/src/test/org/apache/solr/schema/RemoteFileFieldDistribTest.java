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
package org.apache.solr.schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.util.BaseTestHarness;
import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.restlet.ext.servlet.ServerServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteFileFieldDistribTest extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private List<RestTestHarness> restTestHarnesses = new ArrayList<>();
  private static final String SUCCESS_XPATH = "/response/lst[@name='responseHeader']/int[@name='status'][.='0']";
  
  public RemoteFileFieldDistribTest() {
    super();
    schemaString =  "schema-eff.xml";
  }
  
  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-managed-schema.xml";
  }
  
  @BeforeClass
  public static void initSysProperties() {
    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "true");
  }

  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    for (RestTestHarness h : restTestHarnesses) {
      h.close();
    }
  }
  
  @Override
  public SortedMap<ServletHolder,String> getExtraServlets() {
    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();
    final ServletHolder solrRestApi = new ServletHolder("SolrSchemaRestApi", ServerServlet.class);
    solrRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi");
    

    final ServletHolder getExternalFile = new ServletHolder("external", GetExternalFile.class);
    extraServlets.put(getExternalFile, "/rfftest/*");
    
    extraServlets.put(solrRestApi, "/schema/*");  // '/schema/*' matches '/schema', '/schema/', and '/schema/whatever...'
    return extraServlets;
  }
  
  private void setupHarnesses() {
    for (final SolrClient client : clients) {
      RestTestHarness harness = new RestTestHarness(new RESTfulServerProvider() {
        @Override
        public String getBaseURL() {
          return ((HttpSolrClient)client).getBaseURL();
        }
      });
      restTestHarnesses.add(harness);
    }
  }
  
  public static class GetExternalFile extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {   
      
      log.info("Streaming out file");
      final String testHome = SolrTestCaseJ4.getFile("solr/collection1").getParent();
      String filename = "external_eff";
      FileInputStream is = new FileInputStream(new File(testHome + "/" + filename));
      GZIPOutputStream gzos  = new GZIPOutputStream(resp.getOutputStream());
      
      resp.setHeader("Content-Encoding", "gzip");
      
      IOUtils.copy(is, gzos);
      
      gzos.finish();
      is.close();
      
      log.info("Streaming out file ... finished");
    }
  }
  
  private String getRandomRemoteFileUrl() throws Exception {
    return new URL(new URL(publisher().getBaseURL()),"rfftest").toString();
  }
  
  private RestTestHarness publisher() {
    return restTestHarnesses.get(r.nextInt(restTestHarnesses.size())); 
  }
  
  private void addDocuments() throws Exception {
    for (int i = 1; i <= 10; i++) {
      String id = Integer.toString(i);
      index("id", id);
    }
    
     commit();
  }

  private void assertSameConfigOnAllNoes() {
    //TODO
    /*
    for(RestTestHarness r: restTestHarnesses) {
      String resp = r.query("/schema/remote-files/rff?wt=json");
    }*/
  }

  private void assertSameResponseOnAllNodes() throws Exception {
    for(RestTestHarness r: restTestHarnesses) {
      addDocuments();
      SolrQuery q = new SolrQuery();
      q.setQuery("*:*");
      q.setSort("rff", ORDER.asc);
      
      {
        String response = r.query("/select" + q.toQueryString());
        String err = BaseTestHarness.validateXPath(response,
            "//result/doc[position()=1]/str[.='3']",
            "//result/doc[position()=2]/str[.='1']",
            "//result/doc[position()=10]/str[.='8']");

        assertNull(response + err, err);
      }
    }
  }
  
  
  @Test
  public void testSort() throws Exception {
    setupHarnesses();
    
    String updateMapping = "{ managedMap: { rff: { url:\"" + getRandomRemoteFileUrl() +"\", gzipped: true } } }";
    
    {
      String response = publisher().put("/schema/remote-files/rff?wt=xml", updateMapping);
      assertNull(BaseTestHarness.validateXPath(response, SUCCESS_XPATH));
    }

    //Wait for zk version to match up
    Thread.sleep(1000);

    assertSameConfigOnAllNoes();
    
    {
      String response = publisher().query("/update-rffs");
      assertNull(BaseTestHarness.validateXPath(response, SUCCESS_XPATH));
    }

    assertSameResponseOnAllNodes();
    
  }
  
}
