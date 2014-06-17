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

import org.apache.solr.client.solrj.SolrServer;
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
import java.util.concurrent.TimeUnit;

public class TestCloudManagedSchemaConcurrent extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(TestCloudManagedSchemaConcurrent.class);
  private static final String SUCCESS_XPATH = "/response/lst[@name='responseHeader']/int[@name='status'][.='0']";

  public TestCloudManagedSchemaConcurrent() {
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
    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();
    final ServletHolder solrRestApi = new ServletHolder("SolrSchemaRestApi", ServerServlet.class);
    solrRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi");
    extraServlets.put(solrRestApi, "/schema/*");  // '/schema/*' matches '/schema', '/schema/', and '/schema/whatever...'
    return extraServlets;
  }
  
  private List<RestTestHarness> restTestHarnesses = new ArrayList<>();
  
  private void setupHarnesses() {
    for (final SolrServer client : clients) {
      RestTestHarness harness = new RestTestHarness(new RESTfulServerProvider() {
        @Override
        public String getBaseURL() {
          return ((HttpSolrServer)client).getBaseURL();
        }
      });
      restTestHarnesses.add(harness);
    }
  }

  private void verifySuccess(String request, String response) throws Exception {
    String result = BaseTestHarness.validateXPath(response, SUCCESS_XPATH);
    if (null != result) {
      String msg = "QUERY FAILED: xpath=" + result + "  request=" + request + "  response=" + response;
      log.error(msg);
      fail(msg);
    }
  }

  private void addFieldPut(RestTestHarness publisher, String fieldName) throws Exception {
    final String content = "{\"type\":\"text\",\"stored\":\"false\"}";
    String request = "/schema/fields/" + fieldName + "?wt=xml";
    String response = publisher.put(request, content);
    verifySuccess(request, response);
  }

  private void addFieldPost(RestTestHarness publisher, String fieldName) throws Exception {
    final String content = "[{\"name\":\""+fieldName+"\",\"type\":\"text\",\"stored\":\"false\"}]";
    String request = "/schema/fields/?wt=xml";
    String response = publisher.post(request, content);
    verifySuccess(request, response);
  }

  private void copyField(RestTestHarness publisher, String source, String dest) throws Exception {
    final String content = "[{\"source\":\""+source+"\",\"dest\":[\""+dest+"\"]}]";
    String request = "/schema/copyfields/?wt=xml";
    String response = publisher.post(request, content);
    verifySuccess(request, response);
  }

  private String[] getExpectedFieldResponses(int numAddFieldPuts, int numAddFieldPosts) {
    String[] expectedAddFields = new String[1 + numAddFieldPuts + numAddFieldPosts];
    expectedAddFields[0] = SUCCESS_XPATH;

    for (int i = 0; i < numAddFieldPuts; ++i) {
      String newFieldName = "newfieldPut" + i;
      expectedAddFields[1 + i] 
          = "/response/arr[@name='fields']/lst/str[@name='name'][.='" + newFieldName + "']";
    }

    for (int i = 0; i < numAddFieldPosts; ++i) {
      String newFieldName = "newfieldPost" + i;
      expectedAddFields[1 + numAddFieldPuts + i]
          = "/response/arr[@name='fields']/lst/str[@name='name'][.='" + newFieldName + "']";
    }

    return expectedAddFields;
  }

  private String[] getExpectedCopyFieldResponses(List<CopyFieldInfo> copyFields) {
    ArrayList<String> expectedCopyFields = new ArrayList<>();
    expectedCopyFields.add(SUCCESS_XPATH);
    for (CopyFieldInfo cpi : copyFields) {
      String expectedSourceName = cpi.getSourceField();
      expectedCopyFields.add
          ("/response/arr[@name='copyFields']/lst/str[@name='source'][.='" + expectedSourceName + "']");
      String expectedDestName = cpi.getDestField();
      expectedCopyFields.add
          ("/response/arr[@name='copyFields']/lst/str[@name='dest'][.='" + expectedDestName + "']");
    }

    return expectedCopyFields.toArray(new String[expectedCopyFields.size()]);
  }

  @Override
  public void doTest() throws Exception {
    setupHarnesses();
    
    // First, add a bunch of fields via PUT and POST, as well as copyFields,
    // but do it fast enough and verify shards' schemas after all of them are added
    int numFields = 100;
    int numAddFieldPuts = 0;
    int numAddFieldPosts = 0;
    List<CopyFieldInfo> copyFields = new ArrayList<>();

    for (int i = 0; i <= numFields ; ++i) {
      RestTestHarness publisher = restTestHarnesses.get(r.nextInt(restTestHarnesses.size()));

      int type = random().nextInt(3);
      if (type == 0) { // send an add field via PUT
        addFieldPut(publisher, "newfieldPut" + numAddFieldPuts++);
      }
      else if (type == 1) { // send an add field via POST
        addFieldPost(publisher, "newfieldPost" + numAddFieldPosts++);
      }
      else if (type == 2) { // send a copy field
        String sourceField = null;
        String destField = null;

        int sourceType = random().nextInt(3);
        if (sourceType == 0) {  // existing
          sourceField = "name";
        } else if (sourceType == 1) { // newly created
          sourceField = "copySource" + i;
          addFieldPut(publisher, sourceField);
        } else { // dynamic
          sourceField = "*_dynamicSource" + i + "_t";
          // * only supported if both src and dst use it
          destField = "*_dynamicDest" + i + "_t";
        }
        
        if (destField == null) {
          int destType = random().nextInt(2);
          if (destType == 0) {  // existing
            destField = "title";
          } else { // newly created
            destField = "copyDest" + i;
            addFieldPut(publisher, destField);
          }
        }
        copyField(publisher, sourceField, destField);
        copyFields.add(new CopyFieldInfo(sourceField, destField));
      }
    }

    String[] expectedAddFields = getExpectedFieldResponses(numAddFieldPuts, numAddFieldPosts);
    String[] expectedCopyFields = getExpectedCopyFieldResponses(copyFields);

    boolean success = false;
    long maxTimeoutMillis = 100000;
    long startTime = System.nanoTime();
    String request = null;
    String response = null;
    String result = null;

    while ( ! success 
           && TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) < maxTimeoutMillis) {
      Thread.sleep(100);

      for (RestTestHarness client : restTestHarnesses) {
        // verify addFieldPuts and addFieldPosts
        request = "/schema/fields?wt=xml";
        response = client.query(request);
        result = BaseTestHarness.validateXPath(response, expectedAddFields);
        if (result != null) {
          break;
        }

        // verify copyFields
        request = "/schema/copyfields?wt=xml";
        response = client.query(request);
        result = BaseTestHarness.validateXPath(response, expectedCopyFields);
        if (result != null) {
          break;
        }
      }
      success = (result == null);
    }
    if ( ! success) {
      String msg = "QUERY FAILED: xpath=" + result + "  request=" + request + "  response=" + response;
      log.error(msg);
      fail(msg);
    }
  }

  private static class CopyFieldInfo {
    private String sourceField;
    private String destField;

    public CopyFieldInfo(String sourceField, String destField) {
      this.sourceField = sourceField;
      this.destField = destField;
    }

    public String getSourceField() { return sourceField; }
    public String getDestField() { return destField; }
  }
}
