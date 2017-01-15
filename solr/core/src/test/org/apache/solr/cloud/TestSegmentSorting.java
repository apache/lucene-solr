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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.TieredMergePolicy;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.index.TieredMergePolicyFactory;

import org.junit.After;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSegmentSorting extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int NUM_SERVERS = 5;
  private static final int NUM_SHARDS = 2;
  private static final int REPLICATION_FACTOR = 2;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NUM_SERVERS).configure();
  }

  @After
  public void ensureClusterEmpty() throws Exception {
    cluster.deleteAllCollections();
    cluster.getSolrClient().setDefaultCollection(null);
  }
  
  private void createCollection(MiniSolrCloudCluster miniCluster, String collectionName, String createNodeSet, String asyncId,
      Boolean indexToPersist, Map<String,String> collectionProperties) throws Exception {
    String configName = "solrCloudCollectionConfig";
    miniCluster.uploadConfigSet(SolrTestCaseJ4.TEST_PATH().resolve("collection1").resolve("conf"), configName);

    final boolean persistIndex = (indexToPersist != null ? indexToPersist.booleanValue() : random().nextBoolean());
    if (collectionProperties == null) {
      collectionProperties = new HashMap<>();
    }
    collectionProperties.putIfAbsent(CoreDescriptor.CORE_CONFIG, "solrconfig-tlog.xml");
    collectionProperties.putIfAbsent("solr.tests.maxBufferedDocs", "100000");
    collectionProperties.putIfAbsent("solr.tests.ramBufferSizeMB", "100");
    // use non-test classes so RandomizedRunner isn't necessary
    if (random().nextBoolean()) {
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICY, TieredMergePolicy.class.getName());
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICY, "true");
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICYFACTORY, "false");
    } else {
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY, TieredMergePolicyFactory.class.getName());
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICYFACTORY, "true");
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICY, "false");
    }
    collectionProperties.putIfAbsent("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
    collectionProperties.putIfAbsent("solr.directoryFactory", (persistIndex ? "solr.StandardDirectoryFactory" : "solr.RAMDirectoryFactory"));

    if (asyncId == null) {
      CollectionAdminRequest.createCollection(collectionName, configName, NUM_SHARDS, REPLICATION_FACTOR)
          .setCreateNodeSet(createNodeSet)
          .setProperties(collectionProperties)
          .process(miniCluster.getSolrClient());
    }
    else {
      CollectionAdminRequest.createCollection(collectionName, configName, NUM_SHARDS, REPLICATION_FACTOR)
          .setCreateNodeSet(createNodeSet)
          .setProperties(collectionProperties)
          .processAndWait(miniCluster.getSolrClient(), 30);
    }
  }


  public void testSegmentTerminateEarly() throws Exception {

    final String collectionName = "testSegmentTerminateEarlyCollection";

    final SegmentTerminateEarlyTestState tstes = new SegmentTerminateEarlyTestState(random());
    
    final CloudSolrClient cloudSolrClient = cluster.getSolrClient();
    cloudSolrClient.setDefaultCollection(collectionName);

    // create collection
    {
      final String asyncId = (random().nextBoolean() ? null : "asyncId("+collectionName+".create)="+random().nextInt());
      final Map<String, String> collectionProperties = new HashMap<>();
      collectionProperties.put(CoreDescriptor.CORE_CONFIG, "solrconfig-sortingmergepolicyfactory.xml");
      createCollection(cluster, collectionName, null, asyncId, Boolean.TRUE, collectionProperties);
    }
    
    ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(collectionName, zkStateReader, true, true, 330);
    
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
}
