package org.apache.solr.schema;
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

import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.util.BaseTestHarness;
import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.restlet.ext.servlet.ServerServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class TestCloudManagedSchemaAddField extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(TestCloudManagedSchemaAddField.class);

  public TestCloudManagedSchemaAddField() {
    super();
    fixShardCount = true;

    sliceCount = 4;
    shardCount = 8;
  }

  @BeforeClass
  public static void initSysProperties() {
    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "true");
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-managed-schema.xml";
  }
  
  @Override
  public SortedMap<ServletHolder,String> getExtraServlets() {
    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<ServletHolder,String>();
    final ServletHolder solrRestApi = new ServletHolder("SolrRestApi", ServerServlet.class);
    solrRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrRestApi");
    extraServlets.put(solrRestApi, "/schema/*");  // '/schema/*' matches '/schema', '/schema/', and '/schema/whatever...'
    return extraServlets;
  }
  
  private List<RestTestHarness> restTestHarnesses = new ArrayList<RestTestHarness>();
  
  private void setupHarnesses() {
    for (int i = 0 ; i < clients.size() ; ++i) {
      final HttpSolrServer client = (HttpSolrServer)clients.get(i);
      RestTestHarness harness = new RestTestHarness(new RESTfulServerProvider() {
        @Override
        public String getBaseURL() {
          return client.getBaseURL();
        }
      });
      restTestHarnesses.add(harness);
    }
  }
  
  @Override                                                                                                                 
  public void doTest() throws Exception {
    setupHarnesses();
    
    // First. add a bunch of fields, and verify each is present in all shards' schemas
    int numFields = 25;
    for (int i = 1 ; i <= numFields ; ++i) {
      RestTestHarness publisher = restTestHarnesses.get(r.nextInt(restTestHarnesses.size()));
      String newFieldName = "newfield" + i;
      final String content = "{\"type\":\"text\",\"stored\":\"false\"}";
      String request = "/schema/fields/" + newFieldName + "?wt=xml";             
      String response = publisher.put(request, content);
      final long addFieldTime = System.currentTimeMillis(); 
      String result = publisher.validateXPath
          (response, "/response/lst[@name='responseHeader']/int[@name='status'][.='0']");
      if (null != result) {
        fail("PUT REQUEST FAILED: xpath=" + result + "  request=" + request 
            + "  content=" + content + "  response=" + response);
      }
        
      int maxAttempts = 20;
      long retryPauseMillis = 20;

      for (RestTestHarness client : restTestHarnesses) {
        boolean stillTrying = true;
        for (int attemptNum = 1; stillTrying && attemptNum <= maxAttempts ; ++attemptNum) {
          request = "/schema/fields/" + newFieldName + "?wt=xml";
          response = client.query(request);
          long elapsedTimeMillis = System.currentTimeMillis() - addFieldTime;
          result = client.validateXPath(response,
                                        "/response/lst[@name='responseHeader']/int[@name='status'][.='0']",
                                        "/response/lst[@name='field']/str[@name='name'][.='" + newFieldName + "']");
          if (null == result) {
            stillTrying = false;
            if (attemptNum > 1) {
              log.info("On attempt #" + attemptNum + ", successful request " + request + " against server "
                      + client.getBaseURL() + " after " + elapsedTimeMillis + " ms");
            }
          } else {
            if (attemptNum == maxAttempts || ! response.contains("Field '" + newFieldName + "' not found.")) {
              String msg = "QUERY FAILED: xpath=" + result + "  request=" + request + "  response=" + response;
              if (attemptNum == maxAttempts) {
                msg = "Max retry count " + maxAttempts + " exceeded after " + elapsedTimeMillis +" ms.  " + msg;
              }
              log.error(msg);
              fail(msg);
            }
            Thread.sleep(retryPauseMillis);
          }
        }
      }
    }
    
    // Add a doc with one of the newly created fields
    String fieldName = "newfield" + (r.nextInt(numFields) + 1);
    
    int addDocClientNum = r.nextInt(restTestHarnesses.size());
    RestTestHarness client = restTestHarnesses.get(addDocClientNum);
    String updateResult = client.validateUpdate(adoc(fieldName, "word1 word2", "id", "88"));
    assertNull("Error adding a document with field " + fieldName + ": " + updateResult, updateResult);
    updateResult = client.validateUpdate(BaseTestHarness.commit());
    assertNull("Error committing: " + updateResult, updateResult);
    
    // Query for the newly added doc against a different client
    int queryDocClientNum = r.nextInt(restTestHarnesses.size());
    while (queryDocClientNum == addDocClientNum) {
      queryDocClientNum = r.nextInt(restTestHarnesses.size()); 
    }
    client = restTestHarnesses.get(queryDocClientNum);
    String response = client.query("/select?q=" + fieldName + ":word2");
    String queryResult = client.validateXPath(response,
                                              "/response/result[@name='response'][@numFound='1']",
                                              "count(/response/result[@name='response']/doc/int[@name='id']) = 1",
                                              "/response/result[@name='response']/doc/int[@name='id'] = '88'");
    assertNull("Error querying for a document with field " + fieldName + ": " + queryResult
              + "  response=" + response, queryResult);
  }
}
