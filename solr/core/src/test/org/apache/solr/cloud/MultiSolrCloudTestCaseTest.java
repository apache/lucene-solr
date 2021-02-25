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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MultiSolrCloudTestCaseTest extends MultiSolrCloudTestCase {

  private int numClouds;
  private int numCollectionsPerCloud;

  private int numShards;
  private int numReplicas;
  private int maxShardsPerNode;
  private int nodesPerCluster;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    numClouds = random().nextInt(TEST_NIGHTLY ? 4 : 2); //  0..3
    final String[] clusterIds = new String[numClouds];
    for (int ii=0; ii<numClouds; ++ii) {
      clusterIds[ii] = "cloud"+(ii+1);
    }

    numCollectionsPerCloud = random().nextInt(TEST_NIGHTLY ? 3 : 1); //  0..2
    final String[] collections = new String[numCollectionsPerCloud];
    for (int ii=0; ii<numCollectionsPerCloud; ++ii) {
      collections[ii] = "collection"+(ii+1);
    }

    numShards = 1+random().nextInt(TEST_NIGHTLY ? 2 : 1);
    numReplicas = 1+random().nextInt(TEST_NIGHTLY ? 2 : 1);
    maxShardsPerNode = 1+random().nextInt(TEST_NIGHTLY ? 2 : 1);
    nodesPerCluster = (numShards*numReplicas + (maxShardsPerNode-1))/maxShardsPerNode;

    doSetupClusters(
        clusterIds,
        new DefaultClusterCreateFunction() {
          @Override
          protected int nodesPerCluster(String clusterId) {
            return nodesPerCluster;
          }
        },
        new DefaultClusterInitFunction(numShards, numReplicas, maxShardsPerNode) {
          @Override
          public void accept(String clusterId, MiniSolrCloudCluster cluster) {
            for (final String collection : collections) {
              if (random().nextBoolean()) {
                doAccept(collection, cluster); // same collection name in different clouds
              } else {
                doAccept(collection+"_in_"+clusterId, cluster); // globally unique collection name
              }
            }
          }
        });
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    numClouds = 0;
    numCollectionsPerCloud = 0;

    numShards = 0;
    numReplicas = 0;
    maxShardsPerNode =0;
    nodesPerCluster = 0;
  }

  @Test
  public void test() throws Exception {
    assertEquals("numClouds", numClouds, clusterId2cluster.size());
  }

}
