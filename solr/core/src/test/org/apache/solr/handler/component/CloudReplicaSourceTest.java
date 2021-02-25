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

package org.apache.solr.handler.component;

import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.routing.ReplicaListTransformer;
import org.apache.solr.cloud.ClusterStateMockUtil;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link CloudReplicaSource}
 */
@SolrTestCaseJ4.SuppressSSL // lots of assumptions about http: in this test
public class CloudReplicaSourceTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void setup() {
    assumeWorkingMockito();
  }

  @Test
  public void testSimple_ShardsParam() {
    ReplicaListTransformer replicaListTransformer = Mockito.mock(ReplicaListTransformer.class);
    HttpShardHandlerFactory.WhitelistHostChecker whitelistHostChecker = Mockito.mock(HttpShardHandlerFactory.WhitelistHostChecker.class);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("shards", "slice1,slice2");
    try (ZkStateReader zkStateReader = ClusterStateMockUtil.buildClusterState("csr*sr2", "baseUrl1:8983_", "baseUrl2:8984_")) {
      CloudReplicaSource cloudReplicaSource = new CloudReplicaSource.Builder()
          .collection("collection1")
          .onlyNrt(false)
          .zkStateReader(zkStateReader)
          .replicaListTransformer(replicaListTransformer)
          .whitelistHostChecker(whitelistHostChecker)
          .params(params)
          .build();
      assertEquals(2, cloudReplicaSource.getSliceCount());
      assertEquals(2, cloudReplicaSource.getSliceNames().size());
      assertEquals(1, cloudReplicaSource.getReplicasBySlice(0).size());
      assertEquals("http://baseUrl1:8983/slice1_replica1/", cloudReplicaSource.getReplicasBySlice(0).get(0));
      assertEquals(1, cloudReplicaSource.getReplicasBySlice(1).size());
      assertEquals("http://baseUrl2:8984/slice2_replica2/", cloudReplicaSource.getReplicasBySlice(1).get(0));
    }
  }

  @Test
  public void testShardsParam_DeadNode() {
    ReplicaListTransformer replicaListTransformer = Mockito.mock(ReplicaListTransformer.class);
    HttpShardHandlerFactory.WhitelistHostChecker whitelistHostChecker = Mockito.mock(HttpShardHandlerFactory.WhitelistHostChecker.class);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("shards", "slice1,slice2");
    // here node2 is not live so there should be no replicas found for slice2
    try (ZkStateReader zkStateReader = ClusterStateMockUtil.buildClusterState("csr*sr2", "baseUrl1:8983_")) {
      CloudReplicaSource cloudReplicaSource = new CloudReplicaSource.Builder()
          .collection("collection1")
          .onlyNrt(false)
          .zkStateReader(zkStateReader)
          .replicaListTransformer(replicaListTransformer)
          .whitelistHostChecker(whitelistHostChecker)
          .params(params)
          .build();
      assertEquals(2, cloudReplicaSource.getSliceCount());
      assertEquals(2, cloudReplicaSource.getSliceNames().size());
      assertEquals(1, cloudReplicaSource.getReplicasBySlice(0).size());
      assertEquals("http://baseUrl1:8983/slice1_replica1/", cloudReplicaSource.getReplicasBySlice(0).get(0));
      assertEquals(0, cloudReplicaSource.getReplicasBySlice(1).size());
    }
  }

  @Test
  public void testShardsParam_DownReplica() {
    ReplicaListTransformer replicaListTransformer = Mockito.mock(ReplicaListTransformer.class);
    HttpShardHandlerFactory.WhitelistHostChecker whitelistHostChecker = Mockito.mock(HttpShardHandlerFactory.WhitelistHostChecker.class);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("shards", "slice1,slice2");
    // here replica3 is in DOWN state so only 1 replica should be returned for slice2
    try (ZkStateReader zkStateReader = ClusterStateMockUtil.buildClusterState("csr*sr2r3D", "baseUrl1:8983_", "baseUrl2:8984_", "baseUrl3:8985_")) {
      CloudReplicaSource cloudReplicaSource = new CloudReplicaSource.Builder()
          .collection("collection1")
          .onlyNrt(false)
          .zkStateReader(zkStateReader)
          .replicaListTransformer(replicaListTransformer)
          .whitelistHostChecker(whitelistHostChecker)
          .params(params)
          .build();
      assertEquals(2, cloudReplicaSource.getSliceCount());
      assertEquals(2, cloudReplicaSource.getSliceNames().size());
      assertEquals(1, cloudReplicaSource.getReplicasBySlice(0).size());
      assertEquals("http://baseUrl1:8983/slice1_replica1/", cloudReplicaSource.getReplicasBySlice(0).get(0));
      assertEquals(1, cloudReplicaSource.getReplicasBySlice(1).size());
      assertEquals(1, cloudReplicaSource.getReplicasBySlice(1).size());
      assertEquals("http://baseUrl2:8984/slice2_replica2/", cloudReplicaSource.getReplicasBySlice(1).get(0));
    }
  }

  @Test
  public void testMultipleCollections() {
    ReplicaListTransformer replicaListTransformer = Mockito.mock(ReplicaListTransformer.class);
    HttpShardHandlerFactory.WhitelistHostChecker whitelistHostChecker = Mockito.mock(HttpShardHandlerFactory.WhitelistHostChecker.class);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("collection", "collection1,collection2");
    try (ZkStateReader zkStateReader = ClusterStateMockUtil.buildClusterState("csr*sr2csr*", "baseUrl1:8983_", "baseUrl2:8984_")) {
      CloudReplicaSource cloudReplicaSource = new CloudReplicaSource.Builder()
          .collection("collection1")
          .onlyNrt(false)
          .zkStateReader(zkStateReader)
          .replicaListTransformer(replicaListTransformer)
          .whitelistHostChecker(whitelistHostChecker)
          .params(params)
          .build();
      assertEquals(3, cloudReplicaSource.getSliceCount());
      List<String> sliceNames = cloudReplicaSource.getSliceNames();
      assertEquals(3, sliceNames.size());
      for (int i = 0; i < cloudReplicaSource.getSliceCount(); i++) {
        String sliceName = sliceNames.get(i);
        assertEquals(1, cloudReplicaSource.getReplicasBySlice(i).size());

        // need a switch here because unlike the testShards* tests which always returns slices in the order they were specified,
        // using the collection param can return slice names in any order
        switch (sliceName) {
          case "collection1_slice1":
            assertEquals("http://baseUrl1:8983/slice1_replica1/", cloudReplicaSource.getReplicasBySlice(i).get(0));
            break;
          case "collection1_slice2":
            assertEquals("http://baseUrl2:8984/slice2_replica2/", cloudReplicaSource.getReplicasBySlice(i).get(0));
            break;
          case "collection2_slice1":
            assertEquals("http://baseUrl1:8983/slice1_replica3/", cloudReplicaSource.getReplicasBySlice(i).get(0));
            break;
        }
      }
    }
  }

  @Test
  public void testSimple_UsingClusterState() {
    ReplicaListTransformer replicaListTransformer = Mockito.mock(ReplicaListTransformer.class);
    HttpShardHandlerFactory.WhitelistHostChecker whitelistHostChecker = Mockito.mock(HttpShardHandlerFactory.WhitelistHostChecker.class);
    ModifiableSolrParams params = new ModifiableSolrParams();
    try (ZkStateReader zkStateReader = ClusterStateMockUtil.buildClusterState("csr*sr2", "baseUrl1:8983_", "baseUrl2:8984_")) {
      CloudReplicaSource cloudReplicaSource = new CloudReplicaSource.Builder()
          .collection("collection1")
          .onlyNrt(false)
          .zkStateReader(zkStateReader)
          .replicaListTransformer(replicaListTransformer)
          .whitelistHostChecker(whitelistHostChecker)
          .params(params)
          .build();
      assertEquals(2, cloudReplicaSource.getSliceCount());
      List<String> sliceNames = cloudReplicaSource.getSliceNames();
      assertEquals(2, sliceNames.size());
      for (int i = 0; i < cloudReplicaSource.getSliceCount(); i++) {
        String sliceName = sliceNames.get(i);
        assertEquals(1, cloudReplicaSource.getReplicasBySlice(i).size());

        // need to switch because without a shards param, the order of slices is not deterministic
        switch (sliceName) {
          case "slice1":
            assertEquals("http://baseUrl1:8983/slice1_replica1/", cloudReplicaSource.getReplicasBySlice(i).get(0));
            break;
          case "slice2":
            assertEquals("http://baseUrl2:8984/slice2_replica2/", cloudReplicaSource.getReplicasBySlice(i).get(0));
            break;
        }
      }
    }
  }

  @Test
  public void testSimple_OnlyNrt() {
    ReplicaListTransformer replicaListTransformer = Mockito.mock(ReplicaListTransformer.class);
    HttpShardHandlerFactory.WhitelistHostChecker whitelistHostChecker = Mockito.mock(HttpShardHandlerFactory.WhitelistHostChecker.class);
    ModifiableSolrParams params = new ModifiableSolrParams();
    // the cluster state will have slice2 with two tlog replicas out of which the first one will be the leader
    try (ZkStateReader zkStateReader = ClusterStateMockUtil.buildClusterState("csrr*st2t2", "baseUrl1:8983_", "baseUrl2:8984_")) {
      CloudReplicaSource cloudReplicaSource = new CloudReplicaSource.Builder()
          .collection("collection1")
          .onlyNrt(true) // enable only nrt mode
          .zkStateReader(zkStateReader)
          .replicaListTransformer(replicaListTransformer)
          .whitelistHostChecker(whitelistHostChecker)
          .params(params)
          .build();
      assertEquals(2, cloudReplicaSource.getSliceCount());
      List<String> sliceNames = cloudReplicaSource.getSliceNames();
      assertEquals(2, sliceNames.size());
      for (int i = 0; i < cloudReplicaSource.getSliceCount(); i++) {
        String sliceName = sliceNames.get(i);
        // need to switch because without a shards param, the order of slices is not deterministic
        switch (sliceName) {
          case "slice1":
            assertEquals(2, cloudReplicaSource.getReplicasBySlice(i).size());
            assertEquals("http://baseUrl1:8983/slice1_replica1/", cloudReplicaSource.getReplicasBySlice(i).get(0));
            break;
          case "slice2":
            assertEquals(1, cloudReplicaSource.getReplicasBySlice(i).size());
            assertEquals("http://baseUrl2:8984/slice2_replica3/", cloudReplicaSource.getReplicasBySlice(i).get(0));
            break;
        }
      }
    }
  }

  @Test
  public void testMultipleCollections_OnlyNrt() {
    ReplicaListTransformer replicaListTransformer = Mockito.mock(ReplicaListTransformer.class);
    HttpShardHandlerFactory.WhitelistHostChecker whitelistHostChecker = Mockito.mock(HttpShardHandlerFactory.WhitelistHostChecker.class);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("collection", "collection1,collection2");
    // the cluster state will have collection1 with slice2 with two tlog replicas out of which the first one will be the leader
    // and collection2 with just a single slice and a tlog replica that will be leader
    try (ZkStateReader zkStateReader = ClusterStateMockUtil.buildClusterState("csrr*st2t2cst", "baseUrl1:8983_", "baseUrl2:8984_")) {
      CloudReplicaSource cloudReplicaSource = new CloudReplicaSource.Builder()
          .collection("collection1")
          .onlyNrt(true) // enable only nrt mode
          .zkStateReader(zkStateReader)
          .replicaListTransformer(replicaListTransformer)
          .whitelistHostChecker(whitelistHostChecker)
          .params(params)
          .build();
      assertEquals(3, cloudReplicaSource.getSliceCount());
      List<String> sliceNames = cloudReplicaSource.getSliceNames();
      assertEquals(3, sliceNames.size());
      for (int i = 0; i < cloudReplicaSource.getSliceCount(); i++) {
        String sliceName = sliceNames.get(i);
        // need to switch because without a shards param, the order of slices is not deterministic
        switch (sliceName) {
          case "collection1_slice1":
            assertEquals(2, cloudReplicaSource.getReplicasBySlice(i).size());
            assertEquals("http://baseUrl1:8983/slice1_replica1/", cloudReplicaSource.getReplicasBySlice(i).get(0));
            break;
          case "collection1_slice2":
            assertEquals(1, cloudReplicaSource.getReplicasBySlice(i).size());
            assertEquals("http://baseUrl2:8984/slice2_replica3/", cloudReplicaSource.getReplicasBySlice(i).get(0));
            break;
          case "collection2_slice1":
            assertEquals(1, cloudReplicaSource.getReplicasBySlice(i).size());
            assertEquals("http://baseUrl1:8983/slice1_replica5/", cloudReplicaSource.getReplicasBySlice(i).get(0));
            break;
        }
      }
    }
  }
}
