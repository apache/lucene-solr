package org.apache.solr.cloud;

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

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.easymock.Capture;
import org.easymock.IAnswer;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OverseerCollectionProcessorTest extends SolrTestCaseJ4 {
  
  private static final String ADMIN_PATH = "/admin/cores";
  private static final String COLLECTION_NAME = "mycollection";
  private static final String CONFIG_NAME = "myconfig";
  
  private OverseerCollectionProcessor underTest;
  private DistributedQueue workQueueMock;
  private ShardHandler shardHandlerMock;
  private ZkStateReader zkStateReaderMock;
  private ClusterState clusterStateMock;
  private SolrZkClient solrZkClientMock;
  
  private Thread thread;
  private Queue<byte[]> queue = new BlockingArrayQueue<byte[]>();
  
  private class OverseerCollectionProcessorToBeTested extends
      OverseerCollectionProcessor {
    
    public OverseerCollectionProcessorToBeTested(ZkStateReader zkStateReader,
        String myId, ShardHandler shardHandler, String adminPath,
        DistributedQueue workQueue) {
      super(zkStateReader, myId, shardHandler, adminPath, workQueue);
    }
    
    @Override
    protected boolean amILeader() {
      return true;
    }
    
  }
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    workQueueMock = createMock(DistributedQueue.class);
    shardHandlerMock = createMock(ShardHandler.class);
    zkStateReaderMock = createMock(ZkStateReader.class);
    clusterStateMock = createMock(ClusterState.class);
    solrZkClientMock = createMock(SolrZkClient.class);
    underTest = new OverseerCollectionProcessorToBeTested(zkStateReaderMock,
        "1234", shardHandlerMock, ADMIN_PATH, workQueueMock);
  }
  
  @After
  public void tearDown() throws Exception {
    underTest.close();
    thread.interrupt();
    stopComponentUnderTest();
    super.tearDown();
  }
  
  protected Set<String> commonMocks(int liveNodesCount) throws Exception {
    workQueueMock.peek(true);
    expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        Object result;
        while ((result = queue.peek()) == null) {
          Thread.sleep(1000);
        }
        return result;
      }
    }).anyTimes();
    
    workQueueMock.remove();
    expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        return queue.poll();
      }
    }).anyTimes();
    
    zkStateReaderMock.getClusterState();
    expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        return clusterStateMock;
      }
    }).anyTimes();
    
    zkStateReaderMock.getZkClient();
    expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        return solrZkClientMock;
      }
    }).anyTimes();
    
    
    clusterStateMock.getCollections();
    expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        return new HashSet<String>();
      }
    }).anyTimes();
    final Set<String> liveNodes = new HashSet<String>();
    for (int i = 0; i < liveNodesCount; i++) {
      final String address = "localhost:" + (8963 + i) + "_solr";
      liveNodes.add(address);
      
      solrZkClientMock.getBaseUrlForNodeName(address);
      expectLastCall().andAnswer(new IAnswer<Object>() {
        @Override
        public Object answer() throws Throwable {
          // This works as long as this test does not use a 
          // webapp context with an underscore in it
          return address.replaceAll("_", "/");
        }
      }).anyTimes();
      
    }
    clusterStateMock.getLiveNodes();
    expectLastCall().andAnswer(new IAnswer<Object>() {
      @Override
      public Object answer() throws Throwable {
        return liveNodes;
      }
    }).anyTimes();
    
    return liveNodes;
  }
  
  protected void startComponentUnderTest() {
    thread = new Thread(underTest);
    thread.start();
  }
  
  protected void stopComponentUnderTest() throws Exception {
    thread.join();
  }
  
  private class SubmitCapture {
    public Capture<ShardRequest> shardRequestCapture = new Capture<ShardRequest>();
    public Capture<String> nodeUrlsWithoutProtocolPartCapture = new Capture<String>();
    public Capture<ModifiableSolrParams> params = new Capture<ModifiableSolrParams>();
  }
  
  protected List<SubmitCapture> mockShardHandlerForCreateJob(
      Integer numberOfSlices, Integer numberOfReplica) {
    List<SubmitCapture> submitCaptures = new ArrayList<SubmitCapture>();
    for (int i = 0; i < (numberOfSlices * numberOfReplica); i++) {
      SubmitCapture submitCapture = new SubmitCapture();
      shardHandlerMock.submit(capture(submitCapture.shardRequestCapture),
          capture(submitCapture.nodeUrlsWithoutProtocolPartCapture),
          capture(submitCapture.params));
      expectLastCall();
      submitCaptures.add(submitCapture);
      ShardResponse shardResponseWithoutException = new ShardResponse();
      expect(shardHandlerMock.takeCompletedOrError()).andReturn(
          shardResponseWithoutException);
    }
    expect(shardHandlerMock.takeCompletedOrError()).andReturn(null);
    return submitCaptures;
  }
  
  protected void issueCreateJob(Integer numberOfSlices,
      Integer replicationFactor, Integer maxShardsPerNode) {
    ZkNodeProps props = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        OverseerCollectionProcessor.CREATECOLLECTION,
        OverseerCollectionProcessor.REPLICATION_FACTOR,
        replicationFactor.toString(), "name", COLLECTION_NAME,
        "collection.configName", CONFIG_NAME,
        OverseerCollectionProcessor.NUM_SLICES, numberOfSlices.toString(),
        OverseerCollectionProcessor.MAX_SHARDS_PER_NODE,
        maxShardsPerNode.toString());
    queue.add(ZkStateReader.toJSON(props));
  }
  
  protected void verifySubmitCaptures(List<SubmitCapture> submitCaptures,
      Integer numberOfSlices, Integer numberOfReplica, Set<String> liveNodes) {
    List<String> coreNames = new ArrayList<String>();
    Map<String,Map<String,Integer>> sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMap = new HashMap<String,Map<String,Integer>>();
    List<String> nodeUrlWithoutProtocolPartForLiveNodes = new ArrayList<String>(
        liveNodes.size());
    for (String nodeName : liveNodes) {
      String nodeUrlWithoutProtocolPart = nodeName.replaceAll("_", "/");
      if (nodeUrlWithoutProtocolPart.startsWith("http://")) nodeUrlWithoutProtocolPart = nodeUrlWithoutProtocolPart
          .substring(7);
      nodeUrlWithoutProtocolPartForLiveNodes.add(nodeUrlWithoutProtocolPart);
    }
    
    for (SubmitCapture submitCapture : submitCaptures) {
      ShardRequest shardRequest = submitCapture.shardRequestCapture.getValue();
      assertEquals(CoreAdminAction.CREATE.toString(),
          shardRequest.params.get(CoreAdminParams.ACTION));
      // assertEquals(shardRequest.params, submitCapture.params);
      String coreName = shardRequest.params.get(CoreAdminParams.NAME);
      assertFalse("Core with name " + coreName + " created twice",
          coreNames.contains(coreName));
      coreNames.add(coreName);
      assertEquals(CONFIG_NAME,
          shardRequest.params.get("collection.configName"));
      assertEquals(COLLECTION_NAME,
          shardRequest.params.get(CoreAdminParams.COLLECTION));
      assertEquals(numberOfSlices.toString(),
          shardRequest.params.get(ZkStateReader.NUM_SHARDS_PROP));
      assertEquals(ADMIN_PATH, shardRequest.params.get("qt"));
      assertEquals(1, shardRequest.purpose);
      assertEquals(1, shardRequest.shards.length);
      assertEquals(submitCapture.nodeUrlsWithoutProtocolPartCapture.getValue(),
          shardRequest.shards[0]);
      assertTrue("Shard " + coreName + " created on wrong node "
          + shardRequest.shards[0],
          nodeUrlWithoutProtocolPartForLiveNodes
              .contains(shardRequest.shards[0]));
      assertEquals(shardRequest.shards, shardRequest.actualShards);
      
      String sliceName = shardRequest.params.get(CoreAdminParams.SHARD);
      if (!sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMap
          .containsKey(sliceName)) {
        sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMap.put(
            sliceName, new HashMap<String,Integer>());
      }
      Map<String,Integer> nodeUrlsWithoutProtocolPartToNumberOfShardsRunningMap = sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMap
          .get(sliceName);
      Integer existingCount;
      nodeUrlsWithoutProtocolPartToNumberOfShardsRunningMap
          .put(
              shardRequest.shards[0],
              ((existingCount = nodeUrlsWithoutProtocolPartToNumberOfShardsRunningMap
                  .get(shardRequest.shards[0])) == null) ? 1
                  : (existingCount + 1));
    }
    
    assertEquals(numberOfSlices * numberOfReplica, coreNames.size());
    for (int i = 1; i <= numberOfSlices; i++) {
      for (int j = 1; j <= numberOfReplica; j++) {
        String coreName = COLLECTION_NAME + "_shard" + i + "_replica" + j;
        assertTrue("Shard " + coreName + " was not created",
            coreNames.contains(coreName));
      }
    }
    
    assertEquals(numberOfSlices.intValue(),
        sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMap.size());
    for (int i = 1; i <= numberOfSlices; i++) {
      sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMap.keySet()
          .contains("shard" + i);
    }
    int minShardsPerSlicePerNode = numberOfReplica / liveNodes.size();
    int numberOfNodesSupposedToRunMaxShards = numberOfReplica
        % liveNodes.size();
    int numberOfNodesSupposedToRunMinShards = liveNodes.size()
        - numberOfNodesSupposedToRunMaxShards;
    int maxShardsPerSlicePerNode = (numberOfNodesSupposedToRunMaxShards == 0) ? minShardsPerSlicePerNode
        : (minShardsPerSlicePerNode + 1);
    boolean diffBetweenMinAndMaxShardsPerSlicePerNode = (maxShardsPerSlicePerNode != minShardsPerSlicePerNode);
    
    for (Entry<String,Map<String,Integer>> sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMapEntry : sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMap
        .entrySet()) {
      int numberOfShardsRunning = 0;
      int numberOfNodesRunningMinShards = 0;
      int numberOfNodesRunningMaxShards = 0;
      int numberOfNodesRunningAtLeastOneShard = 0;
      for (String nodeUrlsWithoutProtocolPart : sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMapEntry
          .getValue().keySet()) {
        int numberOfShardsRunningOnThisNode = sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMapEntry
            .getValue().get(nodeUrlsWithoutProtocolPart);
        numberOfShardsRunning += numberOfShardsRunningOnThisNode;
        numberOfNodesRunningAtLeastOneShard++;
        assertTrue(
            "Node "
                + nodeUrlsWithoutProtocolPart
                + " is running wrong number of shards. Supposed to run "
                + minShardsPerSlicePerNode
                + (diffBetweenMinAndMaxShardsPerSlicePerNode ? (" or " + maxShardsPerSlicePerNode)
                    : ""),
            (numberOfShardsRunningOnThisNode == minShardsPerSlicePerNode)
                || (numberOfShardsRunningOnThisNode == maxShardsPerSlicePerNode));
        if (numberOfShardsRunningOnThisNode == minShardsPerSlicePerNode) numberOfNodesRunningMinShards++;
        if (numberOfShardsRunningOnThisNode == maxShardsPerSlicePerNode) numberOfNodesRunningMaxShards++;
      }
      if (minShardsPerSlicePerNode == 0) numberOfNodesRunningMinShards = (liveNodes
          .size() - numberOfNodesRunningAtLeastOneShard);
      assertEquals(
          "Too many shards are running under slice "
              + sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMapEntry
                  .getKey(),
          numberOfReplica.intValue(), numberOfShardsRunning);
      assertEquals(numberOfNodesSupposedToRunMinShards,
          numberOfNodesRunningMinShards);
      assertEquals(numberOfNodesSupposedToRunMaxShards,
          numberOfNodesRunningMaxShards);
    }
  }
  
  protected void waitForEmptyQueue(long maxWait) throws Exception {
    long start = System.currentTimeMillis();
    while (queue.peek() != null) {
      if ((System.currentTimeMillis() - start) > maxWait) fail("Queue not empty within "
          + maxWait + " ms");
      Thread.sleep(100);
    }
  }
  
  protected void testTemplate(Integer numberOfNodes, Integer replicationFactor,
      Integer numberOfSlices, Integer maxShardsPerNode,
      boolean collectionExceptedToBeCreated) throws Exception {
    Set<String> liveNodes = commonMocks(numberOfNodes);
    
    List<SubmitCapture> submitCaptures = null;
    if (collectionExceptedToBeCreated) {
      submitCaptures = mockShardHandlerForCreateJob(numberOfSlices,
          replicationFactor);
    }
    
    replay(workQueueMock);
    replay(solrZkClientMock);
    replay(zkStateReaderMock);
    replay(clusterStateMock);
    replay(shardHandlerMock);
    startComponentUnderTest();
    
    issueCreateJob(numberOfSlices, replicationFactor, maxShardsPerNode);
    
    waitForEmptyQueue(10000);
    
    verify(shardHandlerMock);
    
    if (collectionExceptedToBeCreated) {
      verifySubmitCaptures(submitCaptures, numberOfSlices, replicationFactor,
          liveNodes);
    }
  }
  
  @Test
  public void testNoReplicationEqualNumberOfSlicesPerNode() throws Exception {
    Integer numberOfNodes = 4;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 8;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }
  
  @Test
  public void testReplicationEqualNumberOfSlicesPerNode() throws Exception {
    Integer numberOfNodes = 4;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 4;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }
  
  @Test
  public void testNoReplicationUnequalNumberOfSlicesPerNode() throws Exception {
    Integer numberOfNodes = 4;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 6;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }
  
  @Test
  public void testReplicationUnequalNumberOfSlicesPerNode() throws Exception {
    Integer numberOfNodes = 4;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 3;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }
  
  @Test
  public void testNoReplicationCollectionNotCreatedDueToMaxShardsPerNodeLimit()
      throws Exception {
    Integer numberOfNodes = 4;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 6;
    Integer maxShardsPerNode = 1;
    testTemplate(numberOfNodes, replicationFactor, numberOfSlices,
        maxShardsPerNode, false);
  }
  
  @Test
  public void testReplicationCollectionNotCreatedDueToMaxShardsPerNodeLimit()
      throws Exception {
    Integer numberOfNodes = 4;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 3;
    Integer maxShardsPerNode = 1;
    testTemplate(numberOfNodes, replicationFactor, numberOfSlices,
        maxShardsPerNode, false);
  }
  
}
