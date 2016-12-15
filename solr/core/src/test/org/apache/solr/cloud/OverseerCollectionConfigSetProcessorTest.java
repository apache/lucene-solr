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
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.Overseer.LeaderStatus;
import org.apache.solr.cloud.OverseerTaskQueue.QueueEvent;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.easymock.EasyMock.*;

public class OverseerCollectionConfigSetProcessorTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final String ADMIN_PATH = "/admin/cores";
  private static final String COLLECTION_NAME = "mycollection";
  private static final String CONFIG_NAME = "myconfig";
  
  private static OverseerTaskQueue workQueueMock;
  private static DistributedMap runningMapMock;
  private static DistributedMap completedMapMock;
  private static DistributedMap failureMapMock;
  private static ShardHandlerFactory shardHandlerFactoryMock;
  private static ShardHandler shardHandlerMock;
  private static ZkStateReader zkStateReaderMock;
  private static ClusterState clusterStateMock;
  private static SolrZkClient solrZkClientMock;
  private final Map zkMap = new HashMap();
  private final Set collectionsSet = new HashSet();
  private SolrResponse lastProcessMessageResult;


  private OverseerCollectionConfigSetProcessorToBeTested underTest;
  
  private Thread thread;
  private Queue<QueueEvent> queue = new ArrayBlockingQueue<>(10);

  private class OverseerCollectionConfigSetProcessorToBeTested extends
      OverseerCollectionConfigSetProcessor {
    

    public OverseerCollectionConfigSetProcessorToBeTested(ZkStateReader zkStateReader,
        String myId, ShardHandlerFactory shardHandlerFactory,
        String adminPath,
        OverseerTaskQueue workQueue, DistributedMap runningMap,
        DistributedMap completedMap,
        DistributedMap failureMap) {
      super(zkStateReader, myId, shardHandlerFactory, adminPath, new Overseer.Stats(), null, new OverseerNodePrioritizer(zkStateReader, adminPath, shardHandlerFactory), workQueue, runningMap, completedMap, failureMap);
    }
    
    @Override
    protected LeaderStatus amILeader() {
      return LeaderStatus.YES;
    }
    
  }
  
  @BeforeClass
  public static void setUpOnce() throws Exception {
    workQueueMock = createMock(OverseerTaskQueue.class);
    runningMapMock = createMock(DistributedMap.class);
    completedMapMock = createMock(DistributedMap.class);
    failureMapMock = createMock(DistributedMap.class);
    shardHandlerFactoryMock = createMock(ShardHandlerFactory.class);
    shardHandlerMock = createMock(ShardHandler.class);
    zkStateReaderMock = createMock(ZkStateReader.class);
    clusterStateMock = createMock(ClusterState.class);
    solrZkClientMock = createMock(SolrZkClient.class);
  }
  
  @AfterClass
  public static void tearDownOnce() {
    workQueueMock = null;
    runningMapMock = null;
    completedMapMock = null;
    failureMapMock = null;
    shardHandlerFactoryMock = null;
    shardHandlerMock = null;
    zkStateReaderMock = null;
    clusterStateMock = null;
    solrZkClientMock = null;
  }
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    queue.clear();
    reset(workQueueMock);
    reset(runningMapMock);
    reset(completedMapMock);
    reset(failureMapMock);
    reset(shardHandlerFactoryMock);
    reset(shardHandlerMock);
    reset(zkStateReaderMock);
    reset(clusterStateMock);
    reset(solrZkClientMock);

    zkMap.clear();
    collectionsSet.clear();
  }
  
  @After
  public void tearDown() throws Exception {
    stopComponentUnderTest();
    super.tearDown();
  }
  
  protected Set<String> commonMocks(int liveNodesCount) throws Exception {
    shardHandlerFactoryMock.getShardHandler();
    expectLastCall().andAnswer(() -> {
      log.info("SHARDHANDLER");
      return shardHandlerMock;
    }).anyTimes();
    
    workQueueMock.peekTopN(EasyMock.anyInt(), anyObject(Predicate.class), EasyMock.anyLong());
    expectLastCall().andAnswer(() -> {
      Object result;
      int count = 0;
      while ((result = queue.peek()) == null) {
        Thread.sleep(1000);
        count++;
        if (count > 1) return null;
      }

      return Arrays.asList(result);
    }).anyTimes();

    workQueueMock.getTailId();
    expectLastCall().andAnswer(() -> {
      Object result = null;
      Iterator iter = queue.iterator();
      while(iter.hasNext()) {
        result = iter.next();
      }
      return result==null ? null : ((QueueEvent)result).getId();
    }).anyTimes();

    workQueueMock.peek(true);
    expectLastCall().andAnswer(() -> {
      Object result;
      while ((result = queue.peek()) == null) {
        Thread.sleep(1000);
      }
      return result;
    }).anyTimes();
    
    workQueueMock.remove(anyObject(QueueEvent.class));
    expectLastCall().andAnswer(() -> {
      queue.remove(getCurrentArguments()[0]);
      return null;
    }).anyTimes();
    
    workQueueMock.poll();
    expectLastCall().andAnswer(() -> queue.poll()).anyTimes();
    
    zkStateReaderMock.getZkClient();
    expectLastCall().andAnswer(() -> solrZkClientMock).anyTimes();
    
    zkStateReaderMock.getClusterState();
    expectLastCall().andAnswer(() -> clusterStateMock).anyTimes();

    zkStateReaderMock.updateClusterState();

    clusterStateMock.getCollections();
    expectLastCall().andAnswer(() -> collectionsSet).anyTimes();
    final Set<String> liveNodes = new HashSet<>();
    for (int i = 0; i < liveNodesCount; i++) {
      final String address = "localhost:" + (8963 + i) + "_solr";
      liveNodes.add(address);
      
      zkStateReaderMock.getBaseUrlForNodeName(address);
      expectLastCall().andAnswer(() -> {
        // This works as long as this test does not use a
        // webapp context with an underscore in it
        return address.replaceAll("_", "/");
      }).anyTimes();
      
    }

    zkStateReaderMock.getClusterProperty("legacyCloud", "true");
    expectLastCall().andAnswer(() -> "true");

    solrZkClientMock.getZkClientTimeout();
    expectLastCall().andAnswer(() -> 30000).anyTimes();
    
    clusterStateMock.hasCollection(anyObject(String.class));
    expectLastCall().andAnswer(() -> {
      String key = (String) getCurrentArguments()[0];
      return collectionsSet.contains(key);
    }).anyTimes();


    clusterStateMock.getLiveNodes();
    expectLastCall().andAnswer(() -> liveNodes).anyTimes();
    solrZkClientMock.create(anyObject(String.class), anyObject(byte[].class), anyObject(CreateMode.class), anyBoolean());
    expectLastCall().andAnswer(() -> {
      String key = (String) getCurrentArguments()[0];
      zkMap.put(key, null);
      handleCreateCollMessage((byte[]) getCurrentArguments()[1]);
      return key;
    }).anyTimes();

    solrZkClientMock.makePath(anyObject(String.class), anyObject(byte[].class), anyBoolean());
    expectLastCall().andAnswer(() -> {
      String key = (String) getCurrentArguments()[0];
      return key;
    }).anyTimes();

    solrZkClientMock.makePath(anyObject(String.class), anyObject(byte[].class), anyObject(CreateMode.class), anyBoolean());
    expectLastCall().andAnswer(() -> {
      String key = (String) getCurrentArguments()[0];
      return key;
    }).anyTimes();
    
    solrZkClientMock.makePath(anyObject(String.class), anyObject(byte[].class), anyObject(CreateMode.class), anyObject(Watcher.class), anyBoolean());
    expectLastCall().andAnswer(() -> {
      String key = (String) getCurrentArguments()[0];
      return key;
    }).anyTimes();
    
    solrZkClientMock.makePath(anyObject(String.class), anyObject(byte[].class), anyObject(CreateMode.class), anyObject(Watcher.class), anyBoolean(), anyBoolean(), anyInt());
    expectLastCall().andAnswer(() -> {
      String key = (String) getCurrentArguments()[0];
      return key;
    }).anyTimes();

    solrZkClientMock.exists(anyObject(String.class),anyBoolean());
    expectLastCall().andAnswer(() -> {
      String key = (String) getCurrentArguments()[0];
      return zkMap.containsKey(key);
    }).anyTimes();
    
    zkMap.put("/configs/myconfig", null);
    
    return liveNodes;
  }

  private void handleCreateCollMessage(byte[] bytes) {
    try {
      ZkNodeProps props = ZkNodeProps.load(bytes);
      if(CollectionParams.CollectionAction.CREATE.isEqual(props.getStr("operation"))){
        String collName = props.getStr("name") ;
        if(collName != null) collectionsSet.add(collName);
      }
    } catch (Exception e) { }
  }

  protected void startComponentUnderTest() {
    thread = new Thread(underTest);
    thread.start();
  }
  
  protected void stopComponentUnderTest() throws Exception {
    underTest.close();
    thread.interrupt();
    thread.join();
  }
  
  private class SubmitCapture {
    public Capture<ShardRequest> shardRequestCapture = new Capture<>();
    public Capture<String> nodeUrlsWithoutProtocolPartCapture = new Capture<>();
    public Capture<ModifiableSolrParams> params = new Capture<>();
  }
  
  protected List<SubmitCapture> mockShardHandlerForCreateJob(
      Integer numberOfSlices, Integer numberOfReplica) {
    List<SubmitCapture> submitCaptures = new ArrayList<>();
    for (int i = 0; i < (numberOfSlices * numberOfReplica); i++) {
      SubmitCapture submitCapture = new SubmitCapture();
      shardHandlerMock.submit(capture(submitCapture.shardRequestCapture),
          capture(submitCapture.nodeUrlsWithoutProtocolPartCapture),
          capture(submitCapture.params));
      expectLastCall();
      submitCaptures.add(submitCapture);
      ShardResponse shardResponseWithoutException = new ShardResponse();
      shardResponseWithoutException.setSolrResponse(new QueryResponse());
      expect(shardHandlerMock.takeCompletedOrError()).andReturn(
          shardResponseWithoutException);
    }
    expect(shardHandlerMock.takeCompletedOrError()).andReturn(null);
    return submitCaptures;
  }
  
  protected void issueCreateJob(Integer numberOfSlices,
      Integer replicationFactor, Integer maxShardsPerNode, List<String> createNodeList, boolean sendCreateNodeList, boolean createNodeSetShuffle) {
    Map<String,Object> propMap = Utils.makeMap(
        Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.CREATE.toLower(),
        ZkStateReader.REPLICATION_FACTOR, replicationFactor.toString(),
        "name", COLLECTION_NAME,
        "collection.configName", CONFIG_NAME,
        OverseerCollectionMessageHandler.NUM_SLICES, numberOfSlices.toString(),
        ZkStateReader.MAX_SHARDS_PER_NODE, maxShardsPerNode.toString()
    );
    if (sendCreateNodeList) {
      propMap.put(OverseerCollectionMessageHandler.CREATE_NODE_SET,
          (createNodeList != null)?StrUtils.join(createNodeList, ','):null);
      if (OverseerCollectionMessageHandler.CREATE_NODE_SET_SHUFFLE_DEFAULT != createNodeSetShuffle || random().nextBoolean()) {
        propMap.put(OverseerCollectionMessageHandler.CREATE_NODE_SET_SHUFFLE, createNodeSetShuffle);
      }
    }

    ZkNodeProps props = new ZkNodeProps(propMap);
    QueueEvent qe = new QueueEvent("id", Utils.toJSON(props), null){
      @Override
      public void setBytes(byte[] bytes) {
        lastProcessMessageResult = SolrResponse.deserialize( bytes);
      }
    };
    queue.add(qe);
  }
  
  protected void verifySubmitCaptures(List<SubmitCapture> submitCaptures,
      Integer numberOfSlices, Integer numberOfReplica, Collection<String> createNodes, boolean dontShuffleCreateNodeSet) {
    List<String> coreNames = new ArrayList<>();
    Map<String,Map<String,Integer>> sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMap = new HashMap<>();
    List<String> nodeUrlWithoutProtocolPartForLiveNodes = new ArrayList<>(
        createNodes.size());
    for (String nodeName : createNodes) {
      String nodeUrlWithoutProtocolPart = nodeName.replaceAll("_", "/");
      if (nodeUrlWithoutProtocolPart.startsWith("http://")) nodeUrlWithoutProtocolPart = nodeUrlWithoutProtocolPart
          .substring(7);
      nodeUrlWithoutProtocolPartForLiveNodes.add(nodeUrlWithoutProtocolPart);
    }
    final Map<String,String> coreName_TO_nodeUrlWithoutProtocolPartForLiveNodes_map = new HashMap<>();
    
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
      coreName_TO_nodeUrlWithoutProtocolPartForLiveNodes_map.put(coreName, shardRequest.shards[0]);
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
        
        if (dontShuffleCreateNodeSet) {
          final String expectedNodeName = nodeUrlWithoutProtocolPartForLiveNodes.get((numberOfReplica * (i - 1) + (j - 1)) % nodeUrlWithoutProtocolPartForLiveNodes.size());
          assertFalse("expectedNodeName is null for coreName="+coreName, null == expectedNodeName);
          
          final String actualNodeName = coreName_TO_nodeUrlWithoutProtocolPartForLiveNodes_map.get(coreName);
          assertFalse("actualNodeName is null for coreName="+coreName, null == actualNodeName);

          assertTrue("node name mismatch for coreName="+coreName+" ( actual="+actualNodeName+" versus expected="+expectedNodeName+" )", actualNodeName.equals(expectedNodeName));
        }
      }
    }
    
    assertEquals(numberOfSlices.intValue(),
        sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMap.size());
    for (int i = 1; i <= numberOfSlices; i++) {
      sliceToNodeUrlsWithoutProtocolPartToNumberOfShardsRunningMapMap.keySet()
          .contains("shard" + i);
    }
    int minShardsPerSlicePerNode = numberOfReplica / createNodes.size();
    int numberOfNodesSupposedToRunMaxShards = numberOfReplica
        % createNodes.size();
    int numberOfNodesSupposedToRunMinShards = createNodes.size()
        - numberOfNodesSupposedToRunMaxShards;
    int maxShardsPerSlicePerNode = (minShardsPerSlicePerNode + 1);
    if (numberOfNodesSupposedToRunMaxShards == 0) {
      numberOfNodesSupposedToRunMaxShards = numberOfNodesSupposedToRunMinShards;
      maxShardsPerSlicePerNode = minShardsPerSlicePerNode;
    }
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
      if (minShardsPerSlicePerNode == 0) numberOfNodesRunningMinShards = (createNodes
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
    final TimeOut timeout = new TimeOut(maxWait, TimeUnit.MILLISECONDS);
    while (queue.peek() != null) {
      if (timeout.hasTimedOut())
        fail("Queue not empty within " + maxWait + " ms");
      Thread.sleep(100);
    }
  }
  
  protected enum CreateNodeListOptions {
    SEND,
    DONT_SEND,
    SEND_NULL
  }
  protected void testTemplate(Integer numberOfNodes, Integer numberOfNodesToCreateOn, CreateNodeListOptions createNodeListOption, Integer replicationFactor,
      Integer numberOfSlices, Integer maxShardsPerNode,
      boolean collectionExceptedToBeCreated) throws Exception {
    assertTrue("Wrong usage of testTemplate. numberOfNodesToCreateOn " + numberOfNodesToCreateOn + " is not allowed to be higher than numberOfNodes " + numberOfNodes, numberOfNodes.intValue() >= numberOfNodesToCreateOn.intValue());
    assertTrue("Wrong usage of testTemplage. createNodeListOption has to be " + CreateNodeListOptions.SEND + " when numberOfNodes and numberOfNodesToCreateOn are unequal", ((createNodeListOption == CreateNodeListOptions.SEND) || (numberOfNodes.intValue() == numberOfNodesToCreateOn.intValue())));
    
    Set<String> liveNodes = commonMocks(numberOfNodes);
    List<String> createNodeList = new ArrayList<>();
    int i = 0;
    for (String node : liveNodes) {
      if (i++ < numberOfNodesToCreateOn) {
        createNodeList.add(node);
      }
    }
    
    if (random().nextBoolean()) Collections.shuffle(createNodeList, OverseerCollectionMessageHandler.RANDOM);
    
    List<SubmitCapture> submitCaptures = null;
    if (collectionExceptedToBeCreated) {
      submitCaptures = mockShardHandlerForCreateJob(numberOfSlices,
          replicationFactor);
    }
    
    replay(solrZkClientMock);
    replay(zkStateReaderMock);
    replay(workQueueMock);
    replay(clusterStateMock);
    replay(shardHandlerFactoryMock);
    replay(shardHandlerMock);
    
    
    underTest = new OverseerCollectionConfigSetProcessorToBeTested(zkStateReaderMock,
        "1234", shardHandlerFactoryMock, ADMIN_PATH, workQueueMock, runningMapMock,
        completedMapMock, failureMapMock);


    log.info("clusterstate " + clusterStateMock.hashCode());

    startComponentUnderTest();
    
    final List<String> createNodeListToSend = ((createNodeListOption != CreateNodeListOptions.SEND_NULL) ? createNodeList : null);
    final boolean sendCreateNodeList = (createNodeListOption != CreateNodeListOptions.DONT_SEND);
    final boolean dontShuffleCreateNodeSet = (createNodeListToSend != null) && sendCreateNodeList && random().nextBoolean();
    issueCreateJob(numberOfSlices, replicationFactor, maxShardsPerNode, createNodeListToSend, sendCreateNodeList, !dontShuffleCreateNodeSet);
    waitForEmptyQueue(10000);
    
    if (collectionExceptedToBeCreated) {
      assertNotNull(lastProcessMessageResult.getResponse().toString(), lastProcessMessageResult);
    }
    verify(shardHandlerFactoryMock);
    verify(shardHandlerMock);

    if (collectionExceptedToBeCreated) {
      verifySubmitCaptures(submitCaptures, numberOfSlices, replicationFactor,
          createNodeList, dontShuffleCreateNodeSet);
    }
  }
    @Test
  public void testNoReplicationEqualNumberOfSlicesPerNode() throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.DONT_SEND;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 8;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }
  
  @Test
  public void testReplicationEqualNumberOfSlicesPerNode() throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.DONT_SEND;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 4;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }
  
  @Test
  public void testNoReplicationEqualNumberOfSlicesPerNodeSendCreateNodesEqualToLiveNodes() throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 8;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }
  
  @Test
  public void testReplicationEqualNumberOfSlicesPerNodeSendCreateNodesEqualToLiveNodes() throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 4;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }
  
  @Test
  public void testNoReplicationEqualNumberOfSlicesPerNodeSendNullCreateNodes() throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND_NULL;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 8;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }
  
  @Test
  public void testReplicationEqualNumberOfSlicesPerNodeSendNullCreateNodes() throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND_NULL;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 4;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }  
  
  @Test
  public void testNoReplicationUnequalNumberOfSlicesPerNode() throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.DONT_SEND;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 6;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }
  
  @Test
  public void testReplicationUnequalNumberOfSlicesPerNode() throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.DONT_SEND;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 3;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }
  
  @Test
  public void testNoReplicationCollectionNotCreatedDueToMaxShardsPerNodeLimit()
      throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.DONT_SEND;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 6;
    Integer maxShardsPerNode = 1;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, false);
  }
  
  @Test
  public void testReplicationCollectionNotCreatedDueToMaxShardsPerNodeLimit()
      throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.DONT_SEND;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 3;
    Integer maxShardsPerNode = 1;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, false);
  }

  @Test
  public void testNoReplicationLimitedNodesToCreateOn()
      throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 2;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 6;
    Integer maxShardsPerNode = 3;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }
  
  @Test
  public void testReplicationLimitedNodesToCreateOn()
      throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 2;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 3;
    Integer maxShardsPerNode = 3;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, true);
  }

  @Test
  public void testNoReplicationCollectionNotCreatedDueToMaxShardsPerNodeAndNodesToCreateOnLimits()
      throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 3;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 8;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, false);
  }
  
  @Test
  public void testReplicationCollectionNotCreatedDueToMaxShardsPerNodeAndNodesToCreateOnLimits()
      throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 3;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 4;
    Integer maxShardsPerNode = 2;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        maxShardsPerNode, false);
  }

}
