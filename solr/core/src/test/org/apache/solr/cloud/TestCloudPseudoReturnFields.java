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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.Field;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse.FieldResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.TestPseudoReturnFields;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/** 
 * @see TestPseudoReturnFields 
 * @see TestRandomFlRTGCloud
 */
public class TestCloudPseudoReturnFields extends SolrCloudTestCase {
  
  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = DEBUG_LABEL + "_collection";
  
  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;
  /** One client per node */
  private static final ArrayList<HttpSolrClient> CLIENTS = new ArrayList<>(5);

  @BeforeClass
  private static void createMiniSolrCloudCluster() throws Exception {
    // multi replicas should matter...
    final int repFactor = usually() ? 1 : 2;;
    // ... but we definitely want to ensure forwarded requests to other shards work ...
    final int numShards = 2;
    // ... including some forwarded requests from nodes not hosting a shard
    final int numNodes = 1 + (numShards * repFactor);
   
    final String configName = DEBUG_LABEL + "_config-set";
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");
    
    configureCluster(numNodes).addConfig(configName, configDir).configure();
    
    Map<String, String> collectionProperties = new HashMap<>();
    collectionProperties.put("config", "solrconfig-tlog.xml");
    collectionProperties.put("schema", "schema-psuedo-fields.xml");
    CollectionAdminRequest.createCollection(COLLECTION_NAME, configName, numShards, repFactor)
        .setProperties(collectionProperties)
        .process(cluster.getSolrClient());

    CLOUD_CLIENT = cluster.getSolrClient();
    CLOUD_CLIENT.setDefaultCollection(COLLECTION_NAME);

    waitForRecoveriesToFinish(CLOUD_CLIENT);

    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      CLIENTS.add(getHttpSolrClient(jetty.getBaseUrl() + "/" + COLLECTION_NAME + "/"));
    }

    assertEquals(0, CLOUD_CLIENT.add(sdoc("id", "42", "val_i", "1", "ssto", "X", "subject", "aaa")).getStatus());
    assertEquals(0, CLOUD_CLIENT.add(sdoc("id", "43", "val_i", "9", "ssto", "X", "subject", "bbb")).getStatus());
    assertEquals(0, CLOUD_CLIENT.add(sdoc("id", "44", "val_i", "4", "ssto", "X", "subject", "aaa")).getStatus());
    assertEquals(0, CLOUD_CLIENT.add(sdoc("id", "45", "val_i", "6", "ssto", "X", "subject", "aaa")).getStatus());
    assertEquals(0, CLOUD_CLIENT.add(sdoc("id", "46", "val_i", "3", "ssto", "X", "subject", "ggg")).getStatus());
    assertEquals(0, CLOUD_CLIENT.commit().getStatus());;
    
  }
  
  @Before
  private void addUncommittedDoc99() throws Exception {
    // uncommitted doc in transaction log at start of every test
    // Even if an RTG causes ulog to re-open realtime searcher, next test method
    // will get another copy of doc 99 in the ulog
    assertEquals(0, CLOUD_CLIENT.add(sdoc("id", "99", "val_i", "1", "ssto", "X",
                                          "subject", "uncommitted")).getStatus());
  }
  
  @AfterClass
  private static void afterClass() throws Exception {
    if (null != CLOUD_CLIENT) {
      CLOUD_CLIENT.close();
      CLOUD_CLIENT = null;
    }
    for (HttpSolrClient client : CLIENTS) {
      client.close();
    }
    CLIENTS.clear();
  }

  public void testMultiValued() throws Exception {
    // the response writers used to consult isMultiValued on the field
    // but this doesn't work when you alias a single valued field to
    // a multi valued field (the field value is copied first, then
    // if the type lookup is done again later, we get the wrong thing). SOLR-4036

    // score as psuedo field - precondition checks
    for (String name : new String[] {"score", "val_ss"}) {
      try {
        FieldResponse frsp = new Field(name, params("includeDynamic","true",
                                                    "showDefaults","true")).process(CLOUD_CLIENT);
        assertNotNull("Test depends on a (dynamic) field matching '"+name+"', Null response", frsp);
        assertEquals("Test depends on a (dynamic) field matching '"+name+"', bad status: " + frsp.toString(),
                     0, frsp.getStatus());
        assertNotNull("Test depends on a (dynamic) field matching '"+name+
                      "', schema was changed out from under us? ... " + frsp.toString(), frsp.getField());
        assertEquals("Test depends on a multivalued dynamic field matching '"+name+
                     "', schema was changed out from under us? ... " + frsp.toString(),
                     Boolean.TRUE, frsp.getField().get("multiValued"));
      } catch (SolrServerException e) {
        assertEquals("Couldn't fetch field for '"+name+"' ... schema changed out from under us?",
                     null, e);
      }
    }

    SolrDocument doc = null;
    
    // score as psuedo field
    doc = assertSearchOneDoc(params("q","*:*", "fq", "id:42", "fl","id,score,val_ss,val2_ss"));
    assertEquals("42", doc.getFieldValue("id"));
    assertEquals(1.0F, doc.getFieldValue("score"));
    assertEquals(""+doc, 2, doc.size()); // no value for val2_ss or val_ss ... yet...
    
    // TODO: update this test & TestPseudoReturnFields to index docs using a (multivalued) "val_ss" instead of "ssto"
    //
    // that way we can first sanity check a single value in a multivalued field is returned correctly
    // as a "List" of one element, *AND* then we could be testing that a (single valued) psuedo-field correctly
    // overrides that actual (real) value in a multivalued field (ie: not returning a an List)
    //
    // (NOTE: not doing this yet due to how it will impact most other tests, many of which are currently
    // @AwaitsFix'ed)
    //
    //assertTrue(doc.getFieldValue("val_ss").getClass().toString(),
    //           doc.getFieldValue("val_ss") instanceof List);
    
    // single value int using alias that matches multivalued dynamic field
    doc = assertSearchOneDoc(params("q","id:42", "fl","val_ss:val_i, val2_ss:10"));
    assertEquals(""+doc, 2, doc.size());
    assertEquals(""+doc, 1, doc.getFieldValue("val_ss"));
    assertEquals(""+doc, 10L, doc.getFieldValue("val2_ss"));
  }
  
  public void testMultiValuedRTG() throws Exception {
    SolrDocument doc = null;

    // check same results as testMultiValued via RTG (committed doc)
    doc = getRandClient(random()).getById("42", params("fl","val_ss:val_i, val2_ss:10, subject"));
    assertEquals(""+doc, 3, doc.size());
    assertEquals(""+doc, 1, doc.getFieldValue("val_ss"));
    assertEquals(""+doc, 10L, doc.getFieldValue("val2_ss"));
    assertEquals(""+doc, "aaa", doc.getFieldValue("subject"));

    // also check real-time-get from transaction log (uncommitted doc)
    doc = getRandClient(random()).getById("99", params("fl","val_ss:val_i, val2_ss:10, subject"));
    assertEquals(""+doc, 3, doc.size());
    assertEquals(""+doc, 1, doc.getFieldValue("val_ss"));
    assertEquals(""+doc, 10L, doc.getFieldValue("val2_ss"));
    assertEquals(""+doc, "uncommitted", doc.getFieldValue("subject"));
  }
  
  public void testAllRealFields() throws Exception {

    for (String fl : TestPseudoReturnFields.ALL_REAL_FIELDS) {
      SolrDocumentList docs = assertSearch(params("q", "*:*", "rows", "10", "fl",fl));
      // shouldn't matter what doc we pick...
      for (SolrDocument doc : docs) {
        assertEquals(fl + " => " + doc, 5, doc.size());
        assertTrue(fl + " => " + doc, doc.getFieldValue("id") instanceof String);
        assertTrue(fl + " => " + doc, doc.getFieldValue("val_i") instanceof Integer);
        assertTrue(fl + " => " + doc, doc.getFieldValue("subject") instanceof String);
        assertTrue(fl + " => " + doc, doc.getFieldValue("ssto") instanceof String); // TODO: val_ss: List<String>
      }
    }
  }
  
  public void testAllRealFieldsRTG() throws Exception {
    // shouldn't matter if we use RTG (committed or otherwise)
    for (String fl : TestPseudoReturnFields.ALL_REAL_FIELDS) {
      for (int i : Arrays.asList(42, 43, 44, 45, 46, 99)) {
        SolrDocument doc = getRandClient(random()).getById(""+i, params("fl",fl));
        assertEquals(fl + " => " + doc, 5, doc.size());
        assertTrue(fl + " => " + doc, doc.getFieldValue("id") instanceof String);
        assertTrue(fl + " => " + doc, doc.getFieldValue("val_i") instanceof Integer);
        assertTrue(fl + " => " + doc, doc.getFieldValue("subject") instanceof String);
        assertTrue(fl + " => " + doc, doc.getFieldValue("ssto") instanceof String); // TODO: val_ss: List<String>
        
      }
    }
  }
  
  public void testFilterAndOneRealFieldRTG() throws Exception {
    SolrParams params = params("fl","id,val_i",
                               "fq","{!field f='subject' v=$my_var}",
                               "my_var","uncommitted");
    SolrDocumentList docs = getRandClient(random()).getById(Arrays.asList("42","99"), params);
    final String msg = params + " => " + docs;
    assertEquals(msg, 1, docs.size());
    assertEquals(msg, 1, docs.getNumFound());
    
    SolrDocument doc = docs.get(0);
    assertEquals(msg, 2, doc.size());
    assertEquals(msg, "99", doc.getFieldValue("id"));
    assertEquals(msg, 1, doc.getFieldValue("val_i"));
  }

  public void testScoreAndAllRealFields() throws Exception {
    for (String fl : TestPseudoReturnFields.SCORE_AND_REAL_FIELDS) {
      SolrDocumentList docs = assertSearch(params("q", "*:*", "rows", "10", "fl",fl));
      // shouldn't matter what doc we pick...
      for (SolrDocument doc : docs) {
        assertEquals(fl + " => " + doc, 6, doc.size());
        assertTrue(fl + " => " + doc, doc.getFieldValue("id") instanceof String);
        assertTrue(fl + " => " + doc, doc.getFieldValue("score") instanceof Float);
        assertTrue(fl + " => " + doc, doc.getFieldValue("val_i") instanceof Integer);
        assertTrue(fl + " => " + doc, doc.getFieldValue("subject") instanceof String);
        assertTrue(fl + " => " + doc, doc.getFieldValue("ssto") instanceof String); // TODO: val_ss: List<String>
      }
    }
  }
  
  public void testScoreAndAllRealFieldsRTG() throws Exception {
    // also shouldn't matter if we use RTG (committed or otherwise) .. score should be ignored
    for (String fl : TestPseudoReturnFields.SCORE_AND_REAL_FIELDS) {
      for (int i : Arrays.asList(42, 43, 44, 45, 46, 99)) {
        SolrDocument doc = getRandClient(random()).getById(""+i, params("fl",fl));
        assertEquals(fl + " => " + doc, 5, doc.size());
        assertTrue(fl + " => " + doc, doc.getFieldValue("id") instanceof String);
        assertTrue(fl + " => " + doc, doc.getFieldValue("val_i") instanceof Integer);
        assertTrue(fl + " => " + doc, doc.getFieldValue("subject") instanceof String);
        assertTrue(fl + " => " + doc, doc.getFieldValue("ssto") instanceof String); // TODO: val_ss: List<String>
      }
    }
  }

  public void testScoreAndExplicitRealFields() throws Exception {
    
    SolrDocumentList docs = null;
    SolrDocument doc = null;

    for (SolrParams p : Arrays.asList(params("q","*:*", "rows", "1", "fl","score,val_i"),
                                      params("q","*:*", "rows", "1", "fl","score", "fl","val_i"))) {
      docs = assertSearch(p);
      assertEquals(p + " => " + docs, 5, docs.getNumFound());
      doc = docs.get(0); // doesn't really matter which one
      assertEquals(p + " => " + doc, 2, doc.size());
      assertTrue(p + " => " + doc, doc.getFieldValue("val_i") instanceof Integer);
      assertTrue(p + " => " + doc, doc.getFieldValue("score") instanceof Float);
    }
    
    docs = assertSearch(params("q","*:*", "rows", "1", "fl","val_i"));
    assertEquals("" + docs, 5, docs.getNumFound());
    doc = docs.get(0); // doesn't really matter which one
    assertEquals("" + doc, 1, doc.size());
    assertTrue("" + doc, doc.getFieldValue("val_i") instanceof Integer);
  }
  
  public void testScoreAndExplicitRealFieldsRTG() throws Exception {
    SolrDocumentList docs = null;
    SolrDocument doc = null;
    
    // shouldn't matter if we use RTG (committed or otherwise) .. score should be ignored
    for (int i : Arrays.asList(42, 43, 44, 45, 46, 99)) {
      for (SolrParams p : Arrays.asList(params("fl","score,val_i"),
                                        params("fl","score", "fl","val_i"))) {
        doc = getRandClient(random()).getById(""+i, p);
        assertEquals(p + " => " + doc, 1, doc.size());
        assertTrue(p + " => " + doc, doc.getFieldValue("val_i") instanceof Integer);
      }
    }
  }

  public void testFunctions() throws Exception {

    SolrDocumentList docs = assertSearch(params("q","*:*","rows","1","fl","log(val_i)"));
    assertEquals(""+docs, 5, docs.getNumFound());
    SolrDocument doc = docs.get(0); // doesn't really matter which one
    assertEquals(""+doc, 1, doc.size());
    assertTrue(""+doc, doc.getFieldValue("log(val_i)") instanceof Double);
    
    for (SolrParams p : Arrays.asList(params("q","*:*", "rows", "1", "fl","log(val_i),abs(val_i)"),
                                      params("q","*:*", "rows", "1", "fl","log(val_i)", "fl","abs(val_i)"))) {
      docs = assertSearch(p);
      assertEquals(p + " => " + docs, 5, docs.getNumFound());
      doc = docs.get(0); // doesn't really matter which one
      assertEquals(p + " => " + doc, 2, doc.size());
      assertTrue(p + " => " + doc, doc.getFieldValue("log(val_i)") instanceof Double);
      assertTrue(p + " => " + doc, doc.getFieldValue("abs(val_i)") instanceof Float);
    }
  }

  public void testFunctionsRTG() throws Exception {
    // if we use RTG (committed or otherwise) functions should behave the same
    for (String id : Arrays.asList("42","99")) {
      for (SolrParams p : Arrays.asList(params("fl","log(val_i),abs(val_i)"),
                                        params("fl","log(val_i)","fl", "abs(val_i)"))) {
        SolrDocument doc = getRandClient(random()).getById(id, p);
        String msg = id + "," + p + " => " + doc;
        assertEquals(msg, 2, doc.size());
        assertTrue(msg, doc.getFieldValue("log(val_i)") instanceof Double);
        assertTrue(msg, doc.getFieldValue("abs(val_i)") instanceof Float);
        // true for both these specific docs
        assertEquals(msg, 0.0D, doc.getFieldValue("log(val_i)"));
        assertEquals(msg, 1.0F, doc.getFieldValue("abs(val_i)"));
      }
    }
  }

  public void testFunctionsAndExplicit() throws Exception {
    for (SolrParams p : Arrays.asList(params("q","*:*", "rows", "1", "fl","log(val_i),val_i"),
                                      params("q","*:*", "rows", "1", "fl","log(val_i)", "fl","val_i"))) {
      SolrDocumentList docs = assertSearch(p);
      assertEquals(p + " => " + docs, 5, docs.getNumFound());
      SolrDocument doc = docs.get(0); // doesn't really matter which one
      assertEquals(p + " => " + doc, 2, doc.size());
      assertTrue(p + " => " + doc, doc.getFieldValue("log(val_i)") instanceof Double);
      assertTrue(p + " => " + doc, doc.getFieldValue("val_i") instanceof Integer);
    }
  }
  
  public void testFunctionsAndExplicitRTG() throws Exception {
    // shouldn't matter if we use RTG (committed or otherwise)
    for (String id : Arrays.asList("42","99")) {
      for (SolrParams p : Arrays.asList(params("fl","log(val_i),val_i"),
                                        params("fl","log(val_i)","fl","val_i"))) {
        SolrDocument doc = getRandClient(random()).getById(id, p);
        String msg = id + "," + p + " => " + doc;
        assertEquals(msg, 2, doc.size());
        assertTrue(msg, doc.getFieldValue("log(val_i)") instanceof Double);
        assertTrue(msg, doc.getFieldValue("val_i") instanceof Integer);
        // true for both these specific docs
        assertEquals(msg, 0.0D, doc.getFieldValue("log(val_i)"));
        assertEquals(msg, 1, doc.getFieldValue("val_i"));
      }
    }
  }


  public void testFunctionsAndScore() throws Exception {

    for (SolrParams p : Arrays.asList(params("fl","log(val_i),score"),
                                      params("fl","log(val_i)","fl","score"))) {
      SolrDocumentList docs = assertSearch(SolrParams.wrapDefaults(p, params("q", "*:*", "rows", "10")));
      assertEquals(p + " => " + docs, 5, docs.getNumFound());
      // shouldn't matter what doc we pick...
      for (SolrDocument doc : docs) {
        assertEquals(p + " => " + doc, 2, doc.size());
        assertTrue(p + " => " + doc, doc.getFieldValue("score") instanceof Float);
        assertTrue(p + " => " + doc, doc.getFieldValue("log(val_i)") instanceof Double);
      }
    }
    for (SolrParams p : Arrays.asList(params("fl","log(val_i),abs(val_i),score"),
                                      params("fl","log(val_i),abs(val_i)","fl","score"),
                                      params("fl","log(val_i)","fl","abs(val_i),score"),
                                      params("fl","log(val_i)","fl","abs(val_i)","fl","score"))) {
      SolrDocumentList docs = assertSearch(SolrParams.wrapDefaults(p, params("q", "*:*", "rows", "10")));
      assertEquals(p + " => " + docs, 5, docs.getNumFound());
      // shouldn't matter what doc we pick...
      for (SolrDocument doc : docs) {
        assertEquals(p + " => " + doc, 3, doc.size());
        assertTrue(p + " => " + doc, doc.getFieldValue("score") instanceof Float);
        assertTrue(p + " => " + doc, doc.getFieldValue("abs(val_i)") instanceof Float);
        assertTrue(p + " => " + doc, doc.getFieldValue("log(val_i)") instanceof Double);
      }
    }
  }

  public void testFunctionsAndScoreRTG() throws Exception {

    // if we use RTG (committed or otherwise) score should be ignored
    for (String id : Arrays.asList("42","99")) {
      for (SolrParams p : Arrays.asList(params("fl","score","fl","log(val_i)","fl","abs(val_i)"),
                                        params("fl","score","fl","log(val_i),abs(val_i)"),
                                        params("fl","score,log(val_i)","fl","abs(val_i)"),
                                        params("fl","score,log(val_i),abs(val_i)"))) {
        SolrDocument doc = getRandClient(random()).getById(id, p);
        String msg = id + "," + p + " => " + doc;
        assertEquals(msg, 2, doc.size());
        assertTrue(msg, doc.getFieldValue("log(val_i)") instanceof Double);
        assertTrue(msg, doc.getFieldValue("abs(val_i)") instanceof Float);
        // true for both these specific docs
        assertEquals(msg, 0.0D, doc.getFieldValue("log(val_i)"));
        assertEquals(msg, 1.0F, doc.getFieldValue("abs(val_i)"));
      }
    }
  }

  public void testGlobs() throws Exception {
    SolrDocumentList docs = assertSearch(params("q", "*:*", "rows", "10", "fl","val_*"));
    assertEquals(5, docs.getNumFound());
    // shouldn't matter what doc we pick...
    for (SolrDocument doc : docs) {
      assertEquals(doc.toString(), 1, doc.size());
      assertTrue(doc.toString(), doc.getFieldValue("val_i") instanceof Integer);
    }
    for (SolrParams p : Arrays.asList(params("q", "*:*", "rows", "10", "fl","val_*,subj*,ss*"),
                                      params("q", "*:*", "rows", "10", "fl","val_*","fl","subj*,ss*"),
                                      params("q", "*:*", "rows", "10", "fl","val_*","fl","subj*","fl","ss*"))) {
      docs = assertSearch(p);
      // shouldn't matter what doc we pick...
      for (SolrDocument doc : docs) {
        String msg = p + " => " + doc;
        assertEquals(msg, 3, doc.size());
        assertTrue(msg, doc.getFieldValue("val_i") instanceof Integer);
        assertTrue(msg, doc.getFieldValue("subject") instanceof String); 
        assertTrue(msg, doc.getFieldValue("ssto") instanceof String); // TODO: val_ss: List<String>
        assertEquals(msg, "X", doc.getFieldValue("ssto"));
      }
    }
  }

  public void testGlobsRTG() throws Exception {
    // behavior shouldn't matter if we are committed or uncommitted
    for (String id : Arrays.asList("42","99")) {
      
      SolrDocument doc = getRandClient(random()).getById(id, params("fl","val_*"));
      String msg = id + ": fl=val_* => " + doc;
      assertEquals(msg, 1, doc.size());
      assertTrue(msg, doc.getFieldValue("val_i") instanceof Integer);
      assertEquals(msg, 1, doc.getFieldValue("val_i"));
      
      for (SolrParams p : Arrays.asList(params("fl","val_*,subj*,ss*"),
                                        params("fl","val_*","fl","subj*,ss*"))) {
        doc = getRandClient(random()).getById(id, p);
        msg = id + ": " + p + " => " + doc;
        
        assertEquals(msg, 3, doc.size());
        assertTrue(msg, doc.getFieldValue("val_i") instanceof Integer);
        assertEquals(msg, 1, doc.getFieldValue("val_i"));
        assertTrue(msg, doc.getFieldValue("subject") instanceof String); 
        // NOTE: 'subject' is diff between two docs
        assertTrue(msg, doc.getFieldValue("ssto") instanceof String); // TODO: val_ss: List<String>
        assertEquals(msg, "X", doc.getFieldValue("ssto"));
      }
    }
  }

  public void testGlobsAndExplicit() throws Exception {
    SolrDocumentList docs = assertSearch(params("q", "*:*", "rows", "10", "fl","val_*,id"));
    assertEquals(5, docs.getNumFound());
    // shouldn't matter what doc we pick...
    for (SolrDocument doc : docs) {
      assertEquals(doc.toString(), 2, doc.size());
      assertTrue(doc.toString(), doc.getFieldValue("val_i") instanceof Integer);
      assertTrue(doc.toString(), doc.getFieldValue("id") instanceof String);
    }

    for (SolrParams p : Arrays.asList(params("q", "*:*", "rows", "10", "fl","val_*,subj*,id"),
                                      params("q", "*:*", "rows", "10", "fl","val_*","fl","subj*","fl","id"),
                                      params("q", "*:*", "rows", "10", "fl","val_*","fl","subj*,id"))) {
      docs = assertSearch(p);
      assertEquals(p + " => " + docs, 5, docs.getNumFound());
      // shouldn't matter what doc we pick...
      for (SolrDocument doc : docs) {
        String msg = p + " => " + doc;
        assertEquals(msg, 3, doc.size());
        assertTrue(msg, doc.getFieldValue("val_i") instanceof Integer);
        assertTrue(msg, doc.getFieldValue("subject") instanceof String); 
        assertTrue(msg, doc.getFieldValue("id") instanceof String);
      }
    }
  }

  public void testGlobsAndExplicitRTG() throws Exception {
    // behavior shouldn't matter if we are committed or uncommitted
    for (String id : Arrays.asList("42","99")) {
      SolrDocument doc = getRandClient(random()).getById(id, params("fl","val_*,id"));
      String msg = id + ": fl=val_*,id => " + doc;
      assertEquals(msg, 2, doc.size());
      assertTrue(msg, doc.getFieldValue("id") instanceof String);
      assertTrue(msg, doc.getFieldValue("val_i") instanceof Integer);
      assertEquals(msg, 1, doc.getFieldValue("val_i"));

      for (SolrParams p : Arrays.asList(params("fl","val_*,subj*,id"),
                                        params("fl","val_*","fl","subj*","fl","id"),
                                        params("fl","val_*","fl","subj*,id"))) {
        doc = getRandClient(random()).getById(id, p);
        msg = id + ": " + p + " => " + doc;
        assertEquals(msg, 3, doc.size());
        assertTrue(msg, doc.getFieldValue("val_i") instanceof Integer);
        assertEquals(msg, 1, doc.getFieldValue("val_i"));
        assertTrue(msg, doc.getFieldValue("subject") instanceof String); 
        assertTrue(msg, doc.getFieldValue("id") instanceof String);
      }
    }
  }

  public void testGlobsAndScore() throws Exception {
    SolrDocumentList docs = assertSearch(params("q", "*:*", "rows", "10", "fl","val_*,score"));
    assertEquals(5, docs.getNumFound());
    // shouldn't matter what doc we pick...
    for (SolrDocument doc : docs) {
      assertEquals(doc.toString(), 2, doc.size());
      assertTrue(doc.toString(), doc.getFieldValue("val_i") instanceof Integer);
      assertTrue(doc.toString(), doc.getFieldValue("score") instanceof Float);
    }

    for (SolrParams p : Arrays.asList(params("q", "*:*", "rows", "10", "fl","val_*,subj*,score"),
                                      params("q", "*:*", "rows", "10", "fl","val_*","fl","subj*","fl","score"),
                                      params("q", "*:*", "rows", "10", "fl","val_*","fl","subj*,score"))) {
      docs = assertSearch(p);
      assertEquals(p + " => " + docs, 5, docs.getNumFound());
     // shouldn't matter what doc we pick...
      for (SolrDocument doc : docs) {
        String msg = p + " => " + doc;
        assertEquals(msg, 3, doc.size());
        assertTrue(msg, doc.getFieldValue("val_i") instanceof Integer);
        assertTrue(msg, doc.getFieldValue("subject") instanceof String); 
        assertTrue(msg, doc.getFieldValue("score") instanceof Float);
      }
    }
  }

  public void testGlobsAndScoreRTG() throws Exception {
    // behavior shouldn't matter if we are committed or uncommitted, score should be ignored
    for (String id : Arrays.asList("42","99")) {
      SolrDocument doc = getRandClient(random()).getById(id, params("fl","val_*,score"));
      String msg = id + ": fl=val_*,score => " + doc;
      assertEquals(msg, 1, doc.size());
      assertTrue(msg, doc.getFieldValue("val_i") instanceof Integer);
      assertEquals(msg, 1, doc.getFieldValue("val_i"));

      for (SolrParams p : Arrays.asList(params("fl","val_*,subj*,score"),
                                        params("fl","val_*","fl","subj*","fl","score"),
                                        params("fl","val_*","fl","subj*,score"))) {
        doc = getRandClient(random()).getById(id, p);
        msg = id + ": " + p + " => " + doc;
        assertEquals(msg, 2, doc.size());
        assertTrue(msg, doc.getFieldValue("val_i") instanceof Integer);
        assertEquals(msg, 1, doc.getFieldValue("val_i"));
        assertTrue(msg, doc.getFieldValue("subject") instanceof String); 
      }
    }
  }

  public void testAugmenters() throws Exception {
    SolrDocumentList docs = assertSearch(params("q", "*:*", "rows", "10", "fl","[docid]"));
    assertEquals(5, docs.getNumFound());
    // shouldn't matter what doc we pick...
    for (SolrDocument doc : docs) {
      assertEquals(doc.toString(), 1, doc.size());
      assertTrue(doc.toString(), doc.getFieldValue("[docid]") instanceof Integer);
    }
    
    for (SolrParams p : Arrays.asList(params("q","*:*", "fl","[docid],[shard],[explain],x_alias:[value v=10 t=int]"),
                                      params("q","*:*", "fl","[docid],[shard]","fl","[explain],x_alias:[value v=10 t=int]"),
                                      params("q","*:*", "fl","[docid]","fl","[shard]","fl","[explain]","fl","x_alias:[value v=10 t=int]"))) {
      docs = assertSearch(p);
      assertEquals(p + " => " + docs, 5, docs.getNumFound());
      // shouldn't matter what doc we pick...
      for (SolrDocument doc : docs) {
        String msg = p + " => " + doc;
        assertEquals(msg, 4, doc.size());
        assertTrue(msg, doc.getFieldValue("[docid]") instanceof Integer);
        assertTrue(msg, doc.getFieldValue("[shard]") instanceof String); 
        assertTrue(msg, doc.getFieldValue("[explain]") instanceof String);
        assertTrue(msg, doc.getFieldValue("x_alias") instanceof Integer);
        assertEquals(msg, 10, doc.getFieldValue("x_alias"));
      }
    }
  }

  public void testDocIdAugmenterRTG() throws Exception {
    // for an uncommitted doc, we should get -1
    for (String id : Arrays.asList("42","99")) {
      SolrDocument doc = getRandClient(random()).getById(id, params("fl","[docid]"));
      String msg = id + ": fl=[docid] => " + doc;
      assertEquals(msg, 1, doc.size());
      assertTrue(msg, doc.getFieldValue("[docid]") instanceof Integer);
      assertTrue(msg, -1 <= ((Integer)doc.getFieldValue("[docid]")).intValue());
    }
  }
  
  public void testAugmentersRTG() throws Exception {
    // behavior shouldn't matter if we are committed or uncommitted
    for (String id : Arrays.asList("42","99")) {
      for (SolrParams p : Arrays.asList
             (params("fl","[docid],[shard],[explain],x_alias:[value v=10 t=int]"),
              params("fl","[docid],[shard]","fl","[explain],x_alias:[value v=10 t=int]"),
              params("fl","[docid]","fl","[shard]","fl","[explain]","fl","x_alias:[value v=10 t=int]"))) {
        
        SolrDocument doc = getRandClient(random()).getById(id, p);
        String msg = id + ": " + p + " => " + doc;
        
        assertEquals(msg, 3, doc.size());
        assertTrue(msg, doc.getFieldValue("[shard]") instanceof String);
        // RTG: [explain] should be ignored
        assertTrue(msg, doc.getFieldValue("x_alias") instanceof Integer);
        assertEquals(msg, 10, doc.getFieldValue("x_alias"));
        assertTrue(msg, doc.getFieldValue("[docid]") instanceof Integer);
        assertTrue(msg, -1 <= ((Integer)doc.getFieldValue("[docid]")).intValue());
      }
    }
  }
  
  public void testAugmentersAndExplicit() throws Exception {
    for (SolrParams p : Arrays.asList(params("q", "*:*", "fl","id,[docid],[explain],x_alias:[value v=10 t=int]"),
                                      params("q", "*:*", "fl","id","fl","[docid],[explain],x_alias:[value v=10 t=int]"),
                                      params("q", "*:*", "fl","id","fl","[docid]","fl","[explain]","fl","x_alias:[value v=10 t=int]"))) {
      SolrDocumentList docs = assertSearch(p);
      assertEquals(p + " => " + docs, 5, docs.getNumFound());
      // shouldn't matter what doc we pick...
      for (SolrDocument doc : docs) {
        String msg = p + " => " + doc;
        assertEquals(msg, 4, doc.size());
        assertTrue(msg, doc.getFieldValue("id") instanceof String);
        assertTrue(msg, doc.getFieldValue("[docid]") instanceof Integer);
        assertTrue(msg, doc.getFieldValue("[explain]") instanceof String);
        assertTrue(msg, doc.getFieldValue("x_alias") instanceof Integer);
        assertEquals(msg, 10, doc.getFieldValue("x_alias"));
      }
    }
  }
  
  public void testAugmentersAndExplicitRTG() throws Exception {
    // behavior shouldn't matter if we are committed or uncommitted
    for (String id : Arrays.asList("42","99")) {
      for (SolrParams p : Arrays.asList(params("fl","id,[docid],[explain],x_alias:[value v=10 t=int]"),
                                        params("fl","id,[docid]","fl","[explain],x_alias:[value v=10 t=int]"),
                                        params("fl","id","fl","[docid]","fl","[explain]","fl","x_alias:[value v=10 t=int]"))) {
        SolrDocument doc = getRandClient(random()).getById(id, p);
        String msg = id + ": " + p + " => " + doc;
        
        assertEquals(msg, 3, doc.size());
        assertTrue(msg, doc.getFieldValue("id") instanceof String);
        // RTG: [explain] should be missing (ignored)
        assertTrue(msg, doc.getFieldValue("x_alias") instanceof Integer);
        assertEquals(msg, 10, doc.getFieldValue("x_alias"));
        assertTrue(msg, doc.getFieldValue("[docid]") instanceof Integer);
        assertTrue(msg, -1 <= ((Integer)doc.getFieldValue("[docid]")).intValue());
      }
    }
  }

  public void testAugmentersAndScore() throws Exception {
    SolrParams params = params("q","*:*", "fl","[docid],x_alias:[value v=10 t=int],score");
    SolrDocumentList docs = assertSearch(params);
    assertEquals(params + " => " + docs, 5, docs.getNumFound());
    // shouldn't matter what doc we pick...
    for (SolrDocument doc : docs) {
      String msg = params + " => " + doc;
      assertEquals(msg, 3, doc.size());
      assertTrue(msg, doc.getFieldValue("[docid]") instanceof Integer);
      assertTrue(msg, doc.getFieldValue("x_alias") instanceof Integer);
      assertEquals(msg, 10, doc.getFieldValue("x_alias")); 
      assertTrue(msg, doc.getFieldValue("score") instanceof Float);
    }
    for (SolrParams p : Arrays.asList(params("q","*:*","fl","[docid],x_alias:[value v=10 t=int],[explain],score"),
                                      params("q","*:*","fl","[docid]","fl","x_alias:[value v=10 t=int],[explain]","fl","score"),
                                      params("q","*:*","fl","[docid]","fl","x_alias:[value v=10 t=int]","fl","[explain]","fl","score"))) {

      docs = assertSearch(p);
      assertEquals(p + " => " + docs, 5, docs.getNumFound());
      // shouldn't matter what doc we pick...
      for (SolrDocument doc : docs) {
        String msg = p + " => " + doc;
        assertEquals(msg, 4, doc.size());
        assertTrue(msg, doc.getFieldValue("[docid]") instanceof Integer);
        assertTrue(msg, doc.getFieldValue("x_alias") instanceof Integer);
        assertEquals(msg, 10, doc.getFieldValue("x_alias"));
        assertTrue(msg, doc.getFieldValue("[explain]") instanceof String);
        assertTrue(msg, doc.getFieldValue("score") instanceof Float);
      }
    }
  }
  
  public void testAugmentersAndScoreRTG() throws Exception {
    // if we use RTG (committed or otherwise) score should be ignored
    for (String id : Arrays.asList("42","99")) {
      SolrDocument doc = getRandClient(random()).getById(id, params("fl","x_alias:[value v=10 t=int],score"));
      String msg = id + " => " + doc;
      
      assertEquals(msg, 1, doc.size());
      assertTrue(msg, doc.getFieldValue("x_alias") instanceof Integer);
      assertEquals(msg, 10, doc.getFieldValue("x_alias"));

      for (SolrParams p : Arrays.asList(params("fl","d_alias:[docid],x_alias:[value v=10 t=int],[explain],score"),
                                        params("fl","d_alias:[docid],x_alias:[value v=10 t=int],[explain]","fl","score"),
                                        params("fl","d_alias:[docid]","fl","x_alias:[value v=10 t=int]","fl","[explain]","fl","score"))) {
        
        doc = getRandClient(random()).getById(id, p);
        msg = id + ": " + p + " => " + doc;
        
        assertEquals(msg, 2, doc.size());
        assertTrue(msg, doc.getFieldValue("x_alias") instanceof Integer);
        assertEquals(msg, 10, doc.getFieldValue("x_alias"));
        // RTG: [explain] and score should be missing (ignored)
        assertTrue(msg, doc.getFieldValue("d_alias") instanceof Integer);
        assertTrue(msg, -1 <= ((Integer)doc.getFieldValue("d_alias")).intValue());
      }
    }
  }

  public void testAugmentersGlobsExplicitAndScoreOhMy() throws Exception {
    Random random = random();

    // NOTE: 'ssto' is the missing one
    final List<String> fl = Arrays.asList
      ("id","[docid]","[explain]","score","val_*","subj*");
    
    final int iters = atLeast(random, 10);
    for (int i = 0; i< iters; i++) {
      
      Collections.shuffle(fl, random);

      final SolrParams singleFl = params("q","*:*", "rows", "1","fl",String.join(",", fl));
      final ModifiableSolrParams multiFl = params("q","*:*", "rows", "1");
      for (String item : fl) {
        multiFl.add("fl",item);
      }
      for (SolrParams params : Arrays.asList(singleFl, multiFl)) {
        SolrDocumentList docs = assertSearch(params);
        assertEquals(params + " => " + docs, 5, docs.getNumFound());
        // shouldn't matter what doc we pick...
        for (SolrDocument doc : docs) {
          String msg = params + " => " + doc;
          assertEquals(msg, 6, doc.size());
          assertTrue(msg, doc.getFieldValue("id") instanceof String);
          assertTrue(msg, doc.getFieldValue("[docid]") instanceof Integer);
          assertTrue(msg, doc.getFieldValue("[explain]") instanceof String);
          assertTrue(msg, doc.getFieldValue("score") instanceof Float);
          assertTrue(msg, doc.getFieldValue("val_i") instanceof Integer);
          assertTrue(msg, doc.getFieldValue("subject") instanceof String);
        }
      }
    }
  }

  public void testAugmentersGlobsExplicitAndScoreOhMyRTG() throws Exception {
    Random random = random();

    // NOTE: 'ssto' is the missing one
    final List<String> fl = Arrays.asList
      ("id","[docid]","[explain]","score","val_*","subj*");
    
    final int iters = atLeast(random, 10);
    for (int i = 0; i< iters; i++) {
      
      Collections.shuffle(fl, random);

      final SolrParams singleFl = params("fl",String.join(",", fl));
      final ModifiableSolrParams multiFl = params();
      for (String item : fl) {
        multiFl.add("fl",item);
      }

      // RTG behavior should be consistent, (committed or otherwise) 
      for (String id : Arrays.asList("42","99")) { 
        for (SolrParams params : Arrays.asList(singleFl, multiFl)) {
          SolrDocument doc = getRandClient(random()).getById(id, params);
          String msg = id + ": " + params + " => " + doc;
        
          assertEquals(msg, 4, doc.size());
          assertTrue(msg, doc.getFieldValue("id") instanceof String);
          assertTrue(msg, doc.getFieldValue("val_i") instanceof Integer);
          assertEquals(msg, 1, doc.getFieldValue("val_i"));
          assertTrue(msg, doc.getFieldValue("subject") instanceof String);
          assertTrue(msg, doc.getFieldValue("[docid]") instanceof Integer);
          assertTrue(msg, -1 <= ((Integer)doc.getFieldValue("[docid]")).intValue());
          // RTG: [explain] and score should be missing (ignored)
        }
      }
    }
  }
  


  /** 
   * Given a set of query params, executes as a Query against a random SolrClient and 
   * asserts that exactly one document is returned 
   */
  public static SolrDocument assertSearchOneDoc(SolrParams p) throws Exception {
    SolrDocumentList docs = assertSearch(p);
    assertEquals("does not match exactly one doc: " + p.toString() + " => " + docs.toString(),
                 1, docs.getNumFound());
    assertEquals("does not contain exactly one doc: " + p.toString() + " => " + docs.toString(),
                 1, docs.size());
    return docs.get(0);
  }
  
  /** 
   * Given a set of query params, executes as a Query against a random SolrClient and 
   * asserts that at least 1 doc is matched and at least 1 doc is returned
   */
  public static SolrDocumentList assertSearch(SolrParams p) throws Exception {
    QueryResponse rsp = getRandClient(random()).query(p);
    assertEquals("failed request: " + p.toString() + " => " + rsp.toString(), 0, rsp.getStatus());
    assertTrue("does not match at least one doc: " + p.toString() + " => " + rsp.toString(),
               1 <= rsp.getResults().getNumFound());
    assertTrue("rsp does not contain at least one doc: " + p.toString() + " => " + rsp.toString(),
               1 <= rsp.getResults().size());
    return rsp.getResults();
  }

  /** 
   * returns a random SolrClient -- either a CloudSolrClient, or an HttpSolrClient pointed 
   * at a node in our cluster 
   */
  public static SolrClient getRandClient(Random rand) {
    int numClients = CLIENTS.size();
    int idx = TestUtil.nextInt(rand, 0, numClients);
    return (idx == numClients) ? CLOUD_CLIENT : CLIENTS.get(idx);
  }

  public static void waitForRecoveriesToFinish(CloudSolrClient client) throws Exception {
    assert null != client.getDefaultCollection();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(client.getDefaultCollection(),
                                                        client.getZkStateReader(),
                                                        true, true, 330);
  }

}
