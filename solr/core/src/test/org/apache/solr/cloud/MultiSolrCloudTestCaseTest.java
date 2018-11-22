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

import org.junit.BeforeClass;
import org.junit.Test;

public class MultiSolrCloudTestCaseTest extends MultiSolrCloudTestCase {

  private static int numClouds;
  private static int numCollectionsPerCloud;

  private static int numShards;
  private static int numReplicas;
  private static int maxShardsPerNode;
  private static int nodesPerCluster;

  @BeforeClass
  public static void setupClusters() throws Exception {

    numClouds = random().nextInt(4); //  0..3
    final String[] clusterIds = new String[numClouds];
    for (int ii=0; ii<numClouds; ++ii) {
      clusterIds[ii] = "cloud"+(ii+1);
    }

    numCollectionsPerCloud = random().nextInt(3); //  0..2
    final String[] collections = new String[numCollectionsPerCloud];
    for (int ii=0; ii<numCollectionsPerCloud; ++ii) {
      collections[ii] = "collection"+(ii+1);
    }

    numShards = 1+random().nextInt(2);
    numReplicas = 1+random().nextInt(2);
    maxShardsPerNode = 1+random().nextInt(2);
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

  @Test
  public void test() throws Exception {
    assertEquals("numClouds", numClouds, clusterId2cluster.size());
  }

}
