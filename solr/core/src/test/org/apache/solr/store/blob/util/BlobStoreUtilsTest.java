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

package org.apache.solr.store.blob.util;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.store.shared.SolrCloudSharedStoreTestCase;
import org.junit.After;
import org.junit.Test;

/**
 * Unit tests for {@link BlobStoreUtils}
 */
public class BlobStoreUtilsTest extends SolrCloudSharedStoreTestCase {

  @After
  public void doAfter() throws Exception {
    shutdownCluster();
  }

  /**
   * Tests that the core properties returned by {@link BlobStoreUtils#getSharedCoreProperties(ZkStateReader, DocCollection, Replica)}
   * match the core properties of a core created in normal flow i.e. create collection, add shard or add replica. They all
   * should essentially produce same set of properties. Here we are using create collection. 
   *
   * These properties are used to create a missing core against a SHARED replica. 
   */
  @Test
  public void testMissingSharedCoreProperties() throws Exception {
    setupCluster(1);
    String collectionName = "sharedCollection";
    String shardName = "shard1";
    setupSharedCollectionWithShardNames(collectionName, 1, 1, shardName);
    CloudSolrClient cloudClient = cluster.getSolrClient();
    DocCollection coll = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica rep = coll.getLeader(shardName);
    CoreContainer cc = getCoreContainer(rep.getNodeName());

    Path corePropertiesPath = cc.getCoreRootDirectory().resolve(rep.getCoreName()).resolve(CORE_PROPERTIES_FILENAME);
    Properties expectedCoreProperties = new Properties();
    try (InputStreamReader is = new InputStreamReader(new FileInputStream(corePropertiesPath.toFile()), StandardCharsets.UTF_8)) {
      expectedCoreProperties.load(is);
    }

    Map<String, String> coreProperties = BlobStoreUtils.getSharedCoreProperties(cloudClient.getZkStateReader(), coll, rep);

    // name is separately passed as core name, therefore, it is not part of the core properties
    expectedCoreProperties.remove("name");
    /** see comment inside {@link BlobStoreUtils#getSharedCoreProperties(ZkStateReader, DocCollection, Replica)}*/
    expectedCoreProperties.remove("numShards");

    assertEquals("wrong number of core properties", expectedCoreProperties.size(), coreProperties.size());
    for (Object key : expectedCoreProperties.keySet()) {
      assertTrue(key + " is missing", coreProperties.containsKey(key));
      assertEquals(key + "'s value is wrong", expectedCoreProperties.get(key), coreProperties.get(key));
    }
  }
}
