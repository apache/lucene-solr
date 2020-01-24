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
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.Field;
import org.apache.solr.client.solrj.response.RequestStatusState;

import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.CoreDescriptor;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSegmentSorting extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int NUM_SERVERS = 5;
  private static final int NUM_SHARDS = 2;
  private static final int REPLICATION_FACTOR = 2;
  private static final String configName = MethodHandles.lookup().lookupClass() + "_configSet";
  
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NUM_SERVERS)
      .addConfig(configName, Paths.get(TEST_HOME(), "collection1", "conf"))
      .configure();
  }
  
  @Rule public TestName testName = new TestName();
  
  @After
  public void ensureClusterEmpty() throws Exception {
    cluster.deleteAllCollections();
    cluster.getSolrClient().setDefaultCollection(null);
  }

  @Before
  public void createCollection() throws Exception {

    final String collectionName = testName.getMethodName();
    final CloudSolrClient cloudSolrClient = cluster.getSolrClient();
    
    final Map<String, String> collectionProperties = new HashMap<>();
    collectionProperties.put(CoreDescriptor.CORE_CONFIG, "solrconfig-sortingmergepolicyfactory.xml");
    
    CollectionAdminRequest.Create cmd = 
      CollectionAdminRequest.createCollection(collectionName, configName,
                                              NUM_SHARDS, REPLICATION_FACTOR)
      .setProperties(collectionProperties);

    if (random().nextBoolean()) {
      assertTrue( cmd.process(cloudSolrClient).isSuccess() );
    } else { // async
      assertEquals(RequestStatusState.COMPLETED, cmd.processAndWait(cloudSolrClient, 30));
    }
    
    ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
    cluster.waitForActiveCollection(collectionName, NUM_SHARDS, NUM_SHARDS * REPLICATION_FACTOR);
    
    cloudSolrClient.setDefaultCollection(collectionName);
  }


  // 12-Jun-2018 @Test@BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void testSegmentTerminateEarly() throws Exception {

    final SegmentTerminateEarlyTestState tstes = new SegmentTerminateEarlyTestState(random());
    final CloudSolrClient cloudSolrClient = cluster.getSolrClient();

    // add some documents, then optimize to get merged-sorted segments
    tstes.addDocuments(cloudSolrClient, 10, 10, true);
    
    // CommonParams.SEGMENT_TERMINATE_EARLY parameter intentionally absent
    tstes.queryTimestampDescending(cloudSolrClient);
    
    // add a few more documents, but don't optimize to have some not-merge-sorted segments
    tstes.addDocuments(cloudSolrClient, 2, 10, false);
    
    // CommonParams.SEGMENT_TERMINATE_EARLY parameter now present
    tstes.queryTimestampDescendingSegmentTerminateEarlyYes(cloudSolrClient);
    tstes.queryTimestampDescendingSegmentTerminateEarlyNo(cloudSolrClient);
    
    // CommonParams.SEGMENT_TERMINATE_EARLY parameter present but it won't be used
    tstes.queryTimestampDescendingSegmentTerminateEarlyYesGrouped(cloudSolrClient);
    tstes.queryTimestampAscendingSegmentTerminateEarlyYes(cloudSolrClient); // uses a sort order that is _not_ compatible with the merge sort order
    
  }

  /** 
   * Verify that atomic updates against our (DVO) segment sort field doesn't cause errors.
   * In this situation, the updates should *NOT* be done inplace, because that would
   * break the index sorting
   */
  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 26-Mar-2018
  public void testAtomicUpdateOfSegmentSortField() throws Exception {

    final CloudSolrClient cloudSolrClient = cluster.getSolrClient();
    final String updateField = SegmentTerminateEarlyTestState.TIMESTAMP_FIELD;

    // sanity check that updateField is in fact a DocValues only field, meaning it
    // would normally be eligable for inplace updates -- if it weren't also used for merge sorting
    final Map<String,Object> schemaOpts
      = new Field(updateField, params("includeDynamic", "true",
                                      "showDefaults","true")).process(cloudSolrClient).getField();
    assertEquals(true, schemaOpts.get("docValues"));
    assertEquals(false, schemaOpts.get("indexed"));
    assertEquals(false, schemaOpts.get("stored"));
    
    // add some documents
    final int numDocs = atLeast(1000);
    for (int id = 1; id <= numDocs; id++) {
      cloudSolrClient.add(sdoc("id", id, updateField, random().nextInt(60)));
                               
    }
    cloudSolrClient.commit();

    // do some random iterations of replacing docs, atomic updates against segment sort field, and commits
    // (at this point we're just sanity checking no serious failures)
    for (int iter = 0; iter < 20; iter++) {
      final int iterSize = atLeast(20);
      for (int i = 0; i < iterSize; i++) {
        // replace
        cloudSolrClient.add(sdoc("id", TestUtil.nextInt(random(), 1, numDocs),
                                 updateField, random().nextInt(60)));
        // atomic update
        cloudSolrClient.add(sdoc("id", TestUtil.nextInt(random(), 1, numDocs),
                                 updateField, map("set", random().nextInt(60))));
      }
      cloudSolrClient.commit();
    }

    
    // pick a random doc, and verify that doing an atomic update causes the docid to change
    // ie: not an inplace update
    final int id = TestUtil.nextInt(random(), 1, numDocs);
    final int oldDocId = (Integer) cloudSolrClient.getById(""+id, params("fl","[docid]")).get("[docid]");
    
    cloudSolrClient.add(sdoc("id", id, updateField, map("inc","666")));
    cloudSolrClient.commit();
    
    // loop incase we're waiting for a newSearcher to be opened
    int newDocId = -1;
    int attempts = 10;
    while ((newDocId < 0) && (0 < attempts--)) {
      SolrDocumentList docs = cloudSolrClient.query(params("q", "id:"+id,
                                                           "fl","[docid]",
                                                           "fq", updateField + "[666 TO *]")).getResults();
      if (0 < docs.size()) {
        newDocId = (Integer)docs.get(0).get("[docid]");
      } else {
        Thread.sleep(50);
      }
    }
    assertTrue(oldDocId != newDocId);
  }
}
