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
  private static final String PUT_DYNAMIC_FIELDNAME = "newdynamicfieldPut";
  private static final String POST_DYNAMIC_FIELDNAME = "newdynamicfieldPost";
  private static final String PUT_FIELDNAME = "newfieldPut";
  private static final String POST_FIELDNAME = "newfieldPost";

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

  private static void verifySuccess(String request, String response) throws Exception {
    String result = BaseTestHarness.validateXPath(response, SUCCESS_XPATH);
    if (null != result) {
      String msg = "QUERY FAILED: xpath=" + result + "  request=" + request + "  response=" + response;
      log.error(msg);
      fail(msg);
    }
  }

  private static void addFieldPut(RestTestHarness publisher, String fieldName) throws Exception {
    final String content = "{\"type\":\"text\",\"stored\":\"false\"}";
    String request = "/schema/fields/" + fieldName + "?wt=xml";
    String response = publisher.put(request, content);
    verifySuccess(request, response);
  }

  private static void addFieldPost(RestTestHarness publisher, String fieldName) throws Exception {
    final String content = "[{\"name\":\""+fieldName+"\",\"type\":\"text\",\"stored\":\"false\"}]";
    String request = "/schema/fields/?wt=xml";
    String response = publisher.post(request, content);
    verifySuccess(request, response);
  }

  private static void addDynamicFieldPut(RestTestHarness publisher, String dynamicFieldPattern) throws Exception {
    final String content = "{\"type\":\"text\",\"stored\":\"false\"}";
    String request = "/schema/dynamicfields/" + dynamicFieldPattern + "?wt=xml";
    String response = publisher.put(request, content);
    verifySuccess(request, response);
  }

  private static void addDynamicFieldPost(RestTestHarness publisher, String dynamicFieldPattern) throws Exception {
    final String content = "[{\"name\":\""+dynamicFieldPattern+"\",\"type\":\"text\",\"stored\":\"false\"}]";
    String request = "/schema/dynamicfields/?wt=xml";
    String response = publisher.post(request, content);
    verifySuccess(request, response);
  }

  private static void copyField(RestTestHarness publisher, String source, String dest) throws Exception {
    final String content = "[{\"source\":\""+source+"\",\"dest\":[\""+dest+"\"]}]";
    String request = "/schema/copyfields/?wt=xml";
    String response = publisher.post(request, content);
    verifySuccess(request, response);
  }

  private String[] getExpectedFieldResponses(Info info) {
    String[] expectedAddFields = new String[1 + info.numAddFieldPuts + info.numAddFieldPosts];
    expectedAddFields[0] = SUCCESS_XPATH;

    for (int i = 0; i < info.numAddFieldPuts; ++i) {
      String newFieldName = PUT_FIELDNAME + info.fieldNameSuffix + i;
      expectedAddFields[1 + i] 
          = "/response/arr[@name='fields']/lst/str[@name='name'][.='" + newFieldName + "']";
    }

    for (int i = 0; i < info.numAddFieldPosts; ++i) {
      String newFieldName = POST_FIELDNAME + info.fieldNameSuffix + i;
      expectedAddFields[1 + info.numAddFieldPuts + i]
          = "/response/arr[@name='fields']/lst/str[@name='name'][.='" + newFieldName + "']";
    }

    return expectedAddFields;
  }

  private String[] getExpectedDynamicFieldResponses(Info info) {
    String[] expectedAddDynamicFields = new String[1 + info.numAddDynamicFieldPuts + info.numAddDynamicFieldPosts];
    expectedAddDynamicFields[0] = SUCCESS_XPATH;

    for (int i = 0; i < info.numAddDynamicFieldPuts; ++i) {
      String newDynamicFieldPattern = PUT_DYNAMIC_FIELDNAME + info.fieldNameSuffix + i + "_*";
      expectedAddDynamicFields[1 + i]
          = "/response/arr[@name='dynamicFields']/lst/str[@name='name'][.='" + newDynamicFieldPattern + "']";
    }

    for (int i = 0; i < info.numAddDynamicFieldPosts; ++i) {
      String newDynamicFieldPattern = POST_DYNAMIC_FIELDNAME + info.fieldNameSuffix + i + "_*";
      expectedAddDynamicFields[1 + info.numAddDynamicFieldPuts + i]
          = "/response/arr[@name='dynamicFields']/lst/str[@name='name'][.='" + newDynamicFieldPattern + "']";
    }

    return expectedAddDynamicFields;
  }

  private String[] getExpectedCopyFieldResponses(Info info) {
    ArrayList<String> expectedCopyFields = new ArrayList<>();
    expectedCopyFields.add(SUCCESS_XPATH);
    for (CopyFieldInfo cpi : info.copyFields) {
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
    concurrentOperationsTest();
    schemaLockTest();
  }
  
  private class Info {
    int numAddFieldPuts = 0;
    int numAddFieldPosts = 0;
    int numAddDynamicFieldPuts = 0;
    int numAddDynamicFieldPosts = 0;
    public String fieldNameSuffix;
    List<CopyFieldInfo> copyFields = new ArrayList<>();

    public Info(String fieldNameSuffix) {
      this.fieldNameSuffix = fieldNameSuffix;
    }
  }

  private enum Operation {
    PUT_AddField {
      @Override public void execute(RestTestHarness publisher, int fieldNum, Info info) throws Exception {
        String fieldname = PUT_FIELDNAME + info.numAddFieldPuts++;
        addFieldPut(publisher, fieldname);
      }
    },
    POST_AddField {
      @Override public void execute(RestTestHarness publisher, int fieldNum, Info info) throws Exception {
        String fieldname = POST_FIELDNAME + info.numAddFieldPosts++;
        addFieldPost(publisher, fieldname);
      }
    },
    PUT_AddDynamicField {
      @Override public void execute(RestTestHarness publisher, int fieldNum, Info info) throws Exception {
        addDynamicFieldPut(publisher, PUT_DYNAMIC_FIELDNAME + info.numAddDynamicFieldPuts++ + "_*");
      }
    },
    POST_AddDynamicField {
      @Override public void execute(RestTestHarness publisher, int fieldNum, Info info) throws Exception {
        addDynamicFieldPost(publisher, POST_DYNAMIC_FIELDNAME + info.numAddDynamicFieldPosts++ + "_*");
      }
    },
    POST_AddCopyField {
      @Override public void execute(RestTestHarness publisher, int fieldNum, Info info) throws Exception {
        String sourceField = null;
        String destField = null;

        int sourceType = random().nextInt(3);
        if (sourceType == 0) {  // existing
          sourceField = "name";
        } else if (sourceType == 1) { // newly created
          sourceField = "copySource" + fieldNum;
          addFieldPut(publisher, sourceField);
        } else { // dynamic
          sourceField = "*_dynamicSource" + fieldNum + "_t";
          // * only supported if both src and dst use it
          destField = "*_dynamicDest" + fieldNum + "_t";
        }

        if (destField == null) {
          int destType = random().nextInt(2);
          if (destType == 0) {  // existing
            destField = "title";
          } else { // newly created
            destField = "copyDest" + fieldNum;
            addFieldPut(publisher, destField);
          }
        }
        copyField(publisher, sourceField, destField);
        info.copyFields.add(new CopyFieldInfo(sourceField, destField));
      }
    };
    
    public abstract void execute(RestTestHarness publisher, int fieldNum, Info info) throws Exception;

    private static final Operation[] VALUES = values();
    public static Operation randomOperation()  {
      return VALUES[r.nextInt(VALUES.length)];
    }
  }

  private void concurrentOperationsTest() throws Exception {
    
    // First, add a bunch of fields and dynamic fields via PUT and POST, as well as copyFields,
    // but do it fast enough and verify shards' schemas after all of them are added
    int numFields = 100;
    Info info = new Info("");

    for (int fieldNum = 0; fieldNum <= numFields ; ++fieldNum) {
      RestTestHarness publisher = restTestHarnesses.get(r.nextInt(restTestHarnesses.size()));
      Operation.randomOperation().execute(publisher, fieldNum, info);
    }

    String[] expectedAddFields = getExpectedFieldResponses(info);
    String[] expectedAddDynamicFields = getExpectedDynamicFieldResponses(info);
    String[] expectedCopyFields = getExpectedCopyFieldResponses(info);

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

        // verify addDynamicFieldPuts and addDynamicFieldPosts
        request = "/schema/dynamicfields?wt=xml";
        response = client.query(request);
        result = BaseTestHarness.validateXPath(response, expectedAddDynamicFields);
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

  private abstract class PutPostThread extends Thread {
    RestTestHarness harness;
    Info info;
    public String fieldName;

    public PutPostThread(RestTestHarness harness, Info info) {
      this.harness = harness;
      this.info = info;
    }

    public abstract void run();
  }
  
  private class PutFieldThread extends PutPostThread {
    public PutFieldThread(RestTestHarness harness, Info info) {
      super(harness, info);
      fieldName = PUT_FIELDNAME + "Thread" + info.numAddFieldPuts++;
    }
    public void run() {
      try {
        addFieldPut(harness, fieldName);
      } catch (Exception e) {
        // log.error("###ACTUAL FAILURE!");
        throw new RuntimeException(e);
      }
    }
  }
  
  private class PostFieldThread extends PutPostThread {
    public PostFieldThread(RestTestHarness harness, Info info) {
      super(harness, info);
      fieldName = POST_FIELDNAME + "Thread" + info.numAddFieldPosts++;
    }
    public void run() {
      try {
        addFieldPost(harness, fieldName);
      } catch (Exception e) {
        // log.error("###ACTUAL FAILURE!");
        throw new RuntimeException(e);
      }
    }
  }

  private class PutDynamicFieldThread extends PutPostThread {
    public PutDynamicFieldThread(RestTestHarness harness, Info info) {
      super(harness, info);
      fieldName = PUT_FIELDNAME + "Thread" + info.numAddFieldPuts++;
    }
    public void run() {
      try {
        addFieldPut(harness, fieldName);
      } catch (Exception e) {
        // log.error("###ACTUAL FAILURE!");
        throw new RuntimeException(e);
      }
    }
  }

  private class PostDynamicFieldThread extends PutPostThread {
    public PostDynamicFieldThread(RestTestHarness harness, Info info) {
      super(harness, info);
      fieldName = POST_FIELDNAME + "Thread" + info.numAddFieldPosts++;
    }
    public void run() {
      try {
        addFieldPost(harness, fieldName);
      } catch (Exception e) {
        // log.error("###ACTUAL FAILURE!");
        throw new RuntimeException(e);
      }
    }
  }

  private void schemaLockTest() throws Exception {

    // First, add a bunch of fields via PUT and POST, as well as copyFields,
    // but do it fast enough and verify shards' schemas after all of them are added
    int numFields = 25;
    Info info = new Info("Thread");

    for (int i = 0; i <= numFields ; ++i) {
      // System.err.println("###ITERATION: " + i);
      RestTestHarness publisher = restTestHarnesses.get(r.nextInt(restTestHarnesses.size()));
      PostFieldThread postFieldThread = new PostFieldThread(publisher, info);
      postFieldThread.start();

      publisher = restTestHarnesses.get(r.nextInt(restTestHarnesses.size()));
      PutFieldThread putFieldThread = new PutFieldThread(publisher, info);
      putFieldThread.start();

      publisher = restTestHarnesses.get(r.nextInt(restTestHarnesses.size()));
      PostDynamicFieldThread postDynamicFieldThread = new PostDynamicFieldThread(publisher, info);
      postDynamicFieldThread.start();

      publisher = restTestHarnesses.get(r.nextInt(restTestHarnesses.size()));
      PutDynamicFieldThread putDynamicFieldThread = new PutDynamicFieldThread(publisher, info);
      putDynamicFieldThread.start();

      postFieldThread.join();
      putFieldThread.join();
      postDynamicFieldThread.join();
      putDynamicFieldThread.join();

      String[] expectedAddFields = getExpectedFieldResponses(info);
      String[] expectedAddDynamicFields = getExpectedDynamicFieldResponses(info);

      boolean success = false;
      long maxTimeoutMillis = 100000;
      long startTime = System.nanoTime();
      String request = null;
      String response = null;
      String result = null;

      while ( ! success
          && TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) < maxTimeoutMillis) {
        Thread.sleep(10);

        // int j = 0;
        for (RestTestHarness client : restTestHarnesses) {
          // System.err.println("###CHECKING HARNESS: " + j++ + " for iteration: " + i);

          // verify addFieldPuts and addFieldPosts
          request = "/schema/fields?wt=xml";
          response = client.query(request);
          //System.err.println("###RESPONSE: " + response);
          result = BaseTestHarness.validateXPath(response, expectedAddFields);
          
          if (result != null) {
            // System.err.println("###FAILURE!");
            break;
          }

          // verify addDynamicFieldPuts and addDynamicFieldPosts
          request = "/schema/dynamicfields?wt=xml";
          response = client.query(request);
          //System.err.println("###RESPONSE: " + response);
          result = BaseTestHarness.validateXPath(response, expectedAddDynamicFields);

          if (result != null) {
            // System.err.println("###FAILURE!");
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
