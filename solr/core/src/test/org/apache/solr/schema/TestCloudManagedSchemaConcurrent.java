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
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.BaseTestHarness;
import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.restlet.ext.servlet.ServerServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.data.Stat;

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
  private static final String PUT_FIELDTYPE = "newfieldtypePut";
  private static final String POST_FIELDTYPE = "newfieldtypePost";

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

  private static void addFieldPut(RestTestHarness publisher, String fieldName, int updateTimeoutSecs) throws Exception {
    final String content = "{\"type\":\"text\",\"stored\":\"false\"}";
    String request = "/schema/fields/" + fieldName + "?wt=xml";
    if (updateTimeoutSecs > 0)
      request += "&updateTimeoutSecs="+updateTimeoutSecs;
    String response = publisher.put(request, content);
    verifySuccess(request, response);
  }

  private static void addFieldPost(RestTestHarness publisher, String fieldName, int updateTimeoutSecs) throws Exception {
    final String content = "[{\"name\":\""+fieldName+"\",\"type\":\"text\",\"stored\":\"false\"}]";
    String request = "/schema/fields/?wt=xml";
    if (updateTimeoutSecs > 0)
      request += "&updateTimeoutSecs="+updateTimeoutSecs;
    String response = publisher.post(request, content);
    verifySuccess(request, response);
  }

  private static void addDynamicFieldPut(RestTestHarness publisher, String dynamicFieldPattern, int updateTimeoutSecs) throws Exception {
    final String content = "{\"type\":\"text\",\"stored\":\"false\"}";
    String request = "/schema/dynamicfields/" + dynamicFieldPattern + "?wt=xml";
    if (updateTimeoutSecs > 0)
      request += "&updateTimeoutSecs="+updateTimeoutSecs;
    String response = publisher.put(request, content);
    verifySuccess(request, response);
  }

  private static void addDynamicFieldPost(RestTestHarness publisher, String dynamicFieldPattern, int updateTimeoutSecs) throws Exception {
    final String content = "[{\"name\":\""+dynamicFieldPattern+"\",\"type\":\"text\",\"stored\":\"false\"}]";
    String request = "/schema/dynamicfields/?wt=xml";
    if (updateTimeoutSecs > 0)
      request += "&updateTimeoutSecs="+updateTimeoutSecs;
    String response = publisher.post(request, content);
    verifySuccess(request, response);
  }

  private static void copyField(RestTestHarness publisher, String source, String dest, int updateTimeoutSecs) throws Exception {
    final String content = "[{\"source\":\""+source+"\",\"dest\":[\""+dest+"\"]}]";
    String request = "/schema/copyfields/?wt=xml";
    if (updateTimeoutSecs > 0)
      request += "&updateTimeoutSecs="+updateTimeoutSecs;
    String response = publisher.post(request, content);
    verifySuccess(request, response);
  }

  private static void addFieldTypePut(RestTestHarness publisher, String typeName, int updateTimeoutSecs) throws Exception {
    final String content = "{\"class\":\"solr.TrieIntField\"}";
    String request = "/schema/fieldtypes/" + typeName + "?wt=xml";
    if (updateTimeoutSecs > 0)
      request += "&updateTimeoutSecs="+updateTimeoutSecs;
    String response = publisher.put(request, content);
    verifySuccess(request, response);
  }

  private static void addFieldTypePost(RestTestHarness publisher, String typeName, int updateTimeoutSecs) throws Exception {
    final String content = "[{\"name\":\""+typeName+"\",\"class\":\"solr.TrieIntField\"}]";
    String request = "/schema/fieldtypes/?wt=xml";
    if (updateTimeoutSecs > 0)
      request += "&updateTimeoutSecs="+updateTimeoutSecs;
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

  private String[] getExpectedFieldTypeResponses(Info info) {
    String[] expectedAddFieldTypes = new String[1 + info.numAddFieldTypePuts + info.numAddFieldTypePosts];
    expectedAddFieldTypes[0] = SUCCESS_XPATH;

    for (int i = 0; i < info.numAddFieldTypePuts; ++i) {
      String newFieldTypeName = PUT_FIELDTYPE + info.fieldNameSuffix + i;
      expectedAddFieldTypes[1 + i]
          = "/response/arr[@name='fieldTypes']/lst/str[@name='name'][.='" + newFieldTypeName + "']";
    }

    for (int i = 0; i < info.numAddFieldTypePosts; ++i) {
      String newFieldTypeName = POST_FIELDTYPE + info.fieldNameSuffix + i;
      expectedAddFieldTypes[1 + info.numAddFieldTypePuts + i]
          = "/response/arr[@name='fieldTypes']/lst/str[@name='name'][.='" + newFieldTypeName + "']";
    }

    return expectedAddFieldTypes;
  }
  

  @Override
  public void doTest() throws Exception {
    verifyWaitForSchemaUpdateToPropagate();
    setupHarnesses();
    concurrentOperationsTest();
    schemaLockTest();
  }
  
  private class Info {
    int numAddFieldPuts = 0;
    int numAddFieldPosts = 0;
    int numAddDynamicFieldPuts = 0;
    int numAddDynamicFieldPosts = 0;
    int numAddFieldTypePuts = 0;
    int numAddFieldTypePosts = 0;
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
        addFieldPut(publisher, fieldname, 15);
      }
    },
    POST_AddField {
      @Override public void execute(RestTestHarness publisher, int fieldNum, Info info) throws Exception {
        String fieldname = POST_FIELDNAME + info.numAddFieldPosts++;
        addFieldPost(publisher, fieldname, 15);
      }
    },
    PUT_AddDynamicField {
      @Override public void execute(RestTestHarness publisher, int fieldNum, Info info) throws Exception {
        addDynamicFieldPut(publisher, PUT_DYNAMIC_FIELDNAME + info.numAddDynamicFieldPuts++ + "_*", 15);
      }
    },
    POST_AddDynamicField {
      @Override public void execute(RestTestHarness publisher, int fieldNum, Info info) throws Exception {
        addDynamicFieldPost(publisher, POST_DYNAMIC_FIELDNAME + info.numAddDynamicFieldPosts++ + "_*", 15);
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
          addFieldPut(publisher, sourceField, 15);
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
            addFieldPut(publisher, destField, 15);
          }
        }
        copyField(publisher, sourceField, destField, 15);
        info.copyFields.add(new CopyFieldInfo(sourceField, destField));
      }
    },
    PUT_AddFieldType {
      @Override public void execute(RestTestHarness publisher, int fieldNum, Info info) throws Exception {
        String typeName = PUT_FIELDTYPE + info.numAddFieldTypePuts++;
        addFieldTypePut(publisher, typeName, 15);
      }
    },
    POST_AddFieldType {
      @Override public void execute(RestTestHarness publisher, int fieldNum, Info info) throws Exception {
        String typeName = POST_FIELDTYPE + info.numAddFieldTypePosts++;
        addFieldTypePost(publisher, typeName, 15);
      }
    };


    public abstract void execute(RestTestHarness publisher, int fieldNum, Info info) throws Exception;

    private static final Operation[] VALUES = values();
    public static Operation randomOperation()  {
      return VALUES[r.nextInt(VALUES.length)];
    }
  }

  private void verifyWaitForSchemaUpdateToPropagate() throws Exception {
    String testCollectionName = "collection1";

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    Replica shard1Leader = clusterState.getLeader(testCollectionName, "shard1");
    final String coreUrl = (new ZkCoreNodeProps(shard1Leader)).getCoreUrl();
    assertNotNull(coreUrl);

    RestTestHarness harness = new RestTestHarness(new RESTfulServerProvider() {
      public String getBaseURL() {
        return coreUrl.endsWith("/") ? coreUrl.substring(0, coreUrl.length()-1) : coreUrl;
      }
    });

    addFieldTypePut(harness, "fooInt", 15);

    // go into ZK to get the version of the managed schema after the update
    SolrZkClient zkClient = cloudClient.getZkStateReader().getZkClient();
    Stat stat = new Stat();
    String znodePath = "/configs/conf1/managed-schema";
    byte[] managedSchemaBytes = zkClient.getData(znodePath, null, stat, false);
    int schemaZkVersion = stat.getVersion();

    // now loop over all replicas and verify each has the same schema version
    Replica randomReplicaNotLeader = null;
    for (Slice slice : clusterState.getActiveSlices(testCollectionName)) {
      for (Replica replica : slice.getReplicas()) {
        validateZkVersion(replica, schemaZkVersion, 0, false);

        // save a random replica to test zk watcher behavior
        if (randomReplicaNotLeader == null && !replica.getName().equals(shard1Leader.getName()))
          randomReplicaNotLeader = replica;
      }
    }
    assertNotNull(randomReplicaNotLeader);

    // now update the data and then verify the znode watcher fires correctly
    // before an after a zk session expiration (see SOLR-6249)
    zkClient.setData(znodePath, managedSchemaBytes, schemaZkVersion, false);
    stat = new Stat();
    managedSchemaBytes = zkClient.getData(znodePath, null, stat, false);
    int updatedSchemaZkVersion = stat.getVersion();
    assertTrue(updatedSchemaZkVersion > schemaZkVersion);
    validateZkVersion(randomReplicaNotLeader, updatedSchemaZkVersion, 2, true);

    // ok - looks like the watcher fired correctly on the replica
    // now, expire that replica's zk session and then verify the watcher fires again (after reconnect)
    JettySolrRunner randomReplicaJetty =
        getJettyOnPort(getReplicaPort(randomReplicaNotLeader));
    assertNotNull(randomReplicaJetty);
    chaosMonkey.expireSession(randomReplicaJetty);

    // update the data again to cause watchers to fire
    zkClient.setData(znodePath, managedSchemaBytes, updatedSchemaZkVersion, false);
    stat = new Stat();
    managedSchemaBytes = zkClient.getData(znodePath, null, stat, false);
    updatedSchemaZkVersion = stat.getVersion();
    // give up to 10 secs for the replica to recover after zk session loss and see the update
    validateZkVersion(randomReplicaNotLeader, updatedSchemaZkVersion, 10, true);
  }

  /**
   * Sends a GET request to get the zk schema version from a specific replica.
   */
  protected void validateZkVersion(Replica replica, int schemaZkVersion, int waitSecs, boolean retry) throws Exception {
    final String replicaUrl = (new ZkCoreNodeProps(replica)).getCoreUrl();
    RestTestHarness testHarness = new RestTestHarness(new RESTfulServerProvider() {
      public String getBaseURL() {
        return replicaUrl.endsWith("/") ? replicaUrl.substring(0, replicaUrl.length()-1) : replicaUrl;
      }
    });

    long waitMs = waitSecs * 1000L;
    if (waitMs > 0) Thread.sleep(waitMs); // wait a moment for the zk watcher to fire

    try {
      testHarness.validateQuery("/schema/zkversion?wt=xml", "//zkversion=" + schemaZkVersion);
    } catch (Exception exc) {
      if (retry) {
        // brief wait before retrying
        Thread.sleep(waitMs > 0 ? waitMs : 2000L);

        testHarness.validateQuery("/schema/zkversion?wt=xml", "//zkversion=" + schemaZkVersion);
      } else {
        throw exc;
      }
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
    String[] expectedAddFieldTypes = getExpectedFieldTypeResponses(info);

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
        // verify addFieldTypePuts and addFieldTypePosts
        request = "/schema/fieldtypes?wt=xml";
        response = client.query(request);
        result = BaseTestHarness.validateXPath(response, expectedAddFieldTypes);
        if (result != null) {
          break;
        }

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
        // don't have the client side wait for all replicas to see the update or that defeats the purpose
        // of testing the locking support on the server-side
        addFieldPut(harness, fieldName, -1);
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
        addFieldPost(harness, fieldName, -1);
      } catch (Exception e) {
        // log.error("###ACTUAL FAILURE!");
        throw new RuntimeException(e);
      }
    }
  }

  private class PutFieldTypeThread extends PutPostThread {
    public PutFieldTypeThread(RestTestHarness harness, Info info) {
      super(harness, info);
      fieldName = PUT_FIELDTYPE + "Thread" + info.numAddFieldTypePuts++;
    }
    public void run() {
      try {
        addFieldTypePut(harness, fieldName, -1);
      } catch (Exception e) {
        // log.error("###ACTUAL FAILURE!");
        throw new RuntimeException(e);
      }
    }
  }

  private class PostFieldTypeThread extends PutPostThread {
    public PostFieldTypeThread(RestTestHarness harness, Info info) {
      super(harness, info);
      fieldName = POST_FIELDTYPE + "Thread" + info.numAddFieldTypePosts++;
    }
    public void run() {
      try {
        addFieldTypePost(harness, fieldName, -1);
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
        addFieldPut(harness, fieldName, -1);
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
        addFieldPost(harness, fieldName, -1);
      } catch (Exception e) {
        // log.error("###ACTUAL FAILURE!");
        throw new RuntimeException(e);
      }
    }
  }

  private void schemaLockTest() throws Exception {

    // First, add a bunch of fields via PUT and POST, as well as copyFields,
    // but do it fast enough and verify shards' schemas after all of them are added
    int numFields = 5;
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

      publisher = restTestHarnesses.get(r.nextInt(restTestHarnesses.size()));
      PostFieldTypeThread postFieldTypeThread = new PostFieldTypeThread(publisher, info);
      postFieldTypeThread.start();

      publisher = restTestHarnesses.get(r.nextInt(restTestHarnesses.size()));
      PutFieldTypeThread putFieldTypeThread = new PutFieldTypeThread(publisher, info);
      putFieldTypeThread.start();

      postFieldThread.join();
      putFieldThread.join();
      postDynamicFieldThread.join();
      putDynamicFieldThread.join();
      postFieldTypeThread.join();
      putFieldTypeThread.join();

      String[] expectedAddFields = getExpectedFieldResponses(info);
      String[] expectedAddFieldTypes = getExpectedFieldTypeResponses(info);
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

          request = "/schema/fieldtypes?wt=xml";
          response = client.query(request);
          //System.err.println("###RESPONSE: " + response);
          result = BaseTestHarness.validateXPath(response, expectedAddFieldTypes);

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
