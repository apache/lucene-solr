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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.HttpClient;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.VersionedData;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.cloud.Overseer.LeaderStatus;
import org.apache.solr.cloud.OverseerTaskQueue.QueueEvent;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.HttpShardHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OverseerCollectionConfigSetProcessorTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final String ADMIN_PATH = "/admin/cores";
  private static final String COLLECTION_NAME = "mycollection";
  private static final String CONFIG_NAME = "myconfig";
  
  private static OverseerTaskQueue workQueueMock;
  private static OverseerTaskQueue stateUpdateQueueMock;
  private static Overseer overseerMock;
  private static DistributedClusterChangeUpdater distributedClusterChangeUpdater;
  private static DistributedClusterChangeUpdater.StateChangeRecorder stateChangeRecorder;
  private static ZkController zkControllerMock;
  private static SolrCloudManager cloudDataProviderMock;
  private static ClusterStateProvider clusterStateProviderMock;
  private static DistributedMap runningMapMock;
  private static DistributedMap completedMapMock;
  private static DistributedMap failureMapMock;
  private static HttpShardHandlerFactory shardHandlerFactoryMock;
  private static HttpShardHandler shardHandlerMock;
  private static ZkStateReader zkStateReaderMock;
  private static ClusterState clusterStateMock;
  private static SolrZkClient solrZkClientMock;
  private static DistribStateManager stateManagerMock;
  private static SolrCloudManager cloudManagerMock;
  private static DistribStateManager distribStateManagerMock;
  private static CoreContainer coreContainerMock;
  private static UpdateShardHandler updateShardHandlerMock;
  private static HttpClient httpClientMock;
  @SuppressWarnings("rawtypes")
  private static PlacementPluginFactory placementPluginFactoryMock;
  private static SolrMetricsContext solrMetricsContextMock;

  private static ObjectCache objectCache;
  private Map<String, byte[]> zkClientData = new HashMap<>();
  private final Map<String, ClusterState.CollectionRef> collectionsSet = new HashMap<>();
  private final List<ZkNodeProps> replicas = new ArrayList<>();
  private SolrResponse lastProcessMessageResult;


  private OverseerCollectionConfigSetProcessorToBeTested underTest;
  
  private Thread thread;
  private Queue<QueueEvent> queue = new ArrayBlockingQueue<>(10);

  private static class OverseerCollectionConfigSetProcessorToBeTested extends
      OverseerCollectionConfigSetProcessor {
    

    public OverseerCollectionConfigSetProcessorToBeTested(ZkStateReader zkStateReader,
        String myId, HttpShardHandlerFactory shardHandlerFactory,
        String adminPath,
        OverseerTaskQueue workQueue, DistributedMap runningMap,
        Overseer overseer,
        DistributedMap completedMap,
        DistributedMap failureMap,
        SolrMetricsContext solrMetricsContext) {
      super(zkStateReader, myId, shardHandlerFactory, adminPath, new Stats(), overseer, new OverseerNodePrioritizer(zkStateReader, overseer, adminPath, shardHandlerFactory), workQueue, runningMap, completedMap, failureMap, solrMetricsContext);
    }
    
    @Override
    protected LeaderStatus amILeader() {
      return LeaderStatus.YES;
    }
    
  }

  @BeforeClass
  public static void setUpOnce() throws Exception {
    assumeWorkingMockito();
    
    workQueueMock = mock(OverseerTaskQueue.class);
    stateUpdateQueueMock = mock(OverseerTaskQueue.class);
    runningMapMock = mock(DistributedMap.class);
    completedMapMock = mock(DistributedMap.class);
    failureMapMock = mock(DistributedMap.class);
    shardHandlerFactoryMock = mock(HttpShardHandlerFactory.class);
    shardHandlerMock = mock(HttpShardHandler.class);
    zkStateReaderMock = mock(ZkStateReader.class);
    clusterStateMock = mock(ClusterState.class);
    solrZkClientMock = mock(SolrZkClient.class);
    overseerMock = mock(Overseer.class);
    distributedClusterChangeUpdater = mock(DistributedClusterChangeUpdater.class);
    stateChangeRecorder = mock(DistributedClusterChangeUpdater.StateChangeRecorder.class);
    zkControllerMock = mock(ZkController.class);
    cloudDataProviderMock = mock(SolrCloudManager.class);
    objectCache = new ObjectCache();
    clusterStateProviderMock = mock(ClusterStateProvider.class);
    stateManagerMock = mock(DistribStateManager.class);
    cloudManagerMock = mock(SolrCloudManager.class);
    distribStateManagerMock = mock(DistribStateManager.class);
    coreContainerMock = mock(CoreContainer.class);
    updateShardHandlerMock = mock(UpdateShardHandler.class);
    httpClientMock = mock(HttpClient.class);
    placementPluginFactoryMock = mock(PlacementPluginFactory.class);
    solrMetricsContextMock = mock(SolrMetricsContext.class);
  }
  
  @AfterClass
  public static void tearDownOnce() {
    workQueueMock = null;
    stateUpdateQueueMock = null;
    runningMapMock = null;
    completedMapMock = null;
    failureMapMock = null;
    shardHandlerFactoryMock = null;
    shardHandlerMock = null;
    zkStateReaderMock = null;
    clusterStateMock = null;
    solrZkClientMock = null;
    overseerMock = null;
    distributedClusterChangeUpdater = null;
    stateChangeRecorder = null;
    zkControllerMock = null;
    cloudDataProviderMock = null;
    clusterStateProviderMock = null;
    stateManagerMock = null;;
    cloudManagerMock = null;
    distribStateManagerMock = null;
    coreContainerMock = null;
    updateShardHandlerMock = null;
    httpClientMock = null;
    placementPluginFactoryMock = null;
    solrMetricsContextMock = null;
  }
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    queue.clear();
    reset(workQueueMock);
    reset(stateUpdateQueueMock);
    reset(runningMapMock);
    reset(completedMapMock);
    reset(failureMapMock);
    reset(shardHandlerFactoryMock);
    reset(shardHandlerMock);
    reset(zkStateReaderMock);
    reset(clusterStateMock);
    reset(solrZkClientMock);
    reset(overseerMock);
    reset(distributedClusterChangeUpdater);
    reset(stateChangeRecorder);
    reset(zkControllerMock);
    reset(cloudDataProviderMock);
    objectCache.clear();
    when(cloudDataProviderMock.getObjectCache()).thenReturn(objectCache);
    when(cloudDataProviderMock.getTimeSource()).thenReturn(TimeSource.NANO_TIME);
    reset(clusterStateProviderMock);
    reset(stateManagerMock);
    reset(cloudManagerMock);
    reset(distribStateManagerMock);
    reset(coreContainerMock);
    reset(updateShardHandlerMock);
    reset(httpClientMock);
    reset(placementPluginFactoryMock);
    reset(solrMetricsContextMock);

    zkClientData.clear();
    collectionsSet.clear();
    replicas.clear();
  }
  
  @After
  public void tearDown() throws Exception {
    stopComponentUnderTest();
    super.tearDown();
  }

  @SuppressWarnings("unchecked")
  protected Set<String> commonMocks(int liveNodesCount, boolean distributedClusterStateUpdates) throws Exception {
    when(shardHandlerFactoryMock.getShardHandler()).thenReturn(shardHandlerMock);
    when(workQueueMock.peekTopN(anyInt(), any(), anyLong())).thenAnswer(invocation -> {
      Object result;
      int count = 0;
      while ((result = queue.peek()) == null) {
        Thread.sleep(1000);
        count++;
        if (count > 1) return null;
      }

      return Arrays.asList(result);
    });

    when(workQueueMock.getTailId()).thenAnswer(invocation -> {
      Object result = null;
      @SuppressWarnings({"rawtypes"})
      Iterator iter = queue.iterator();
      while(iter.hasNext()) {
        result = iter.next();
      }
      return result==null ? null : ((QueueEvent)result).getId();
    });

    when(workQueueMock.peek(true)).thenAnswer(invocation -> {
      Object result;
      while ((result = queue.peek()) == null) {
        Thread.sleep(1000);
      }
      return result;
    });

    doAnswer(invocation -> {
      queue.remove(invocation.getArgument(0));
      return null;
    }).when(workQueueMock).remove(any(QueueEvent.class));

    when(workQueueMock.poll()).thenAnswer(invocation -> {
      queue.poll();
      return null;
    });

    when(zkStateReaderMock.getZkClient()).thenReturn(solrZkClientMock);
    when(zkStateReaderMock.getClusterState()).thenReturn(clusterStateMock);
    when(zkStateReaderMock.getAliases()).thenReturn(Aliases.EMPTY);

    when(clusterStateMock.getCollection(anyString())).thenAnswer(invocation -> {
      String key = invocation.getArgument(0);
      if (!collectionsSet.containsKey(key)) return null;
      DocCollection docCollection = collectionsSet.get(key).get();
      Map<String, Map<String, Replica>> slices = new HashMap<>();
      for (ZkNodeProps replica : replicas) {
        if (!key.equals(replica.getStr(ZkStateReader.COLLECTION_PROP))) continue;

        String slice = replica.getStr(ZkStateReader.SHARD_ID_PROP);
        if (!slices.containsKey(slice)) slices.put(slice, new HashMap<>());
        String replicaName = replica.getStr(ZkStateReader.CORE_NAME_PROP);
        slices.get(slice).put(replicaName, new Replica(replicaName, replica.getProperties(), docCollection.getName(), slice));
      }

      Map<String, Slice> slicesMap = new HashMap<>();
      for (Map.Entry<String, Map<String, Replica>> entry : slices.entrySet()) {
        slicesMap.put(entry.getKey(), new Slice(entry.getKey(), entry.getValue(), null,docCollection.getName()));
      }

      return docCollection.copyWithSlices(slicesMap);
    });
    final Set<String> liveNodes = new HashSet<>();
    for (int i = 0; i < liveNodesCount; i++) {
      final String address = "localhost:" + (8963 + i) + "_solr";
      liveNodes.add(address);
      
      when(zkStateReaderMock.getBaseUrlForNodeName(address)).thenAnswer(invocation -> address.replaceAll("_", "/"));
    }

    when(solrZkClientMock.getZkClientTimeout()).thenReturn(30000);
    
    when(clusterStateMock.hasCollection(anyString())).thenAnswer(invocation -> {
      String key = invocation.getArgument(0);
      return collectionsSet.containsKey(key);
    });

    when(clusterStateMock.getLiveNodes()).thenReturn(liveNodes);

    when(solrZkClientMock.setData(anyString(), any(), anyInt(), anyBoolean())).then(invocation -> {
      System.out.println("set data: " + invocation.getArgument(0) + " " + invocation.getArgument(1));
      if (invocation.getArgument(1) == null) {
        zkClientData.put(invocation.getArgument(0), new byte[0]);
      } else {
        zkClientData.put(invocation.getArgument(0), invocation.getArgument(1));
      }
      return null;
    });
 
    when(solrZkClientMock.getData(anyString(), any(), any(), anyBoolean())).thenAnswer(invocation -> {
        byte[] data = zkClientData.get(invocation.getArgument(0));
        if (data == null || data.length == 0) {
          return null;
        }
        return data;
    });
    
    when(solrZkClientMock.create(any(), any(), any(), anyBoolean())).thenAnswer(invocation -> {
      zkClientData.put(invocation.getArgument(0), invocation.getArgument(1));
      return invocation.getArgument(0);
    });

    when(solrZkClientMock.exists(any(String.class), anyBoolean())).thenAnswer(invocation -> {
      String key = invocation.getArgument(0);
      return zkClientData.containsKey(key);
    });

    when(overseerMock.getZkController()).thenReturn(zkControllerMock);
    when(overseerMock.getSolrCloudManager()).thenReturn(cloudDataProviderMock);
    when(overseerMock.getCoreContainer()).thenReturn(coreContainerMock);
    when(overseerMock.getDistributedClusterChangeUpdater()).thenReturn(distributedClusterChangeUpdater);
    when(distributedClusterChangeUpdater.createStateChangeRecorder(any(), anyBoolean())).thenReturn(stateChangeRecorder);
    when(coreContainerMock.getUpdateShardHandler()).thenReturn(updateShardHandlerMock);
    when(coreContainerMock.getPlacementPluginFactory()).thenReturn(placementPluginFactoryMock);
    when(updateShardHandlerMock.getDefaultHttpClient()).thenReturn(httpClientMock);
    
    when(zkControllerMock.getSolrCloudManager()).thenReturn(cloudDataProviderMock);
    when(cloudDataProviderMock.getClusterStateProvider()).thenReturn(clusterStateProviderMock);
    when(clusterStateProviderMock.getClusterState()).thenReturn(clusterStateMock);
    when(clusterStateProviderMock.getLiveNodes()).thenReturn(liveNodes);
    when(cloudDataProviderMock.getDistribStateManager()).thenReturn(stateManagerMock);
    when(cloudManagerMock.getDistribStateManager()).thenReturn(distribStateManagerMock);

    Mockito.doAnswer(
      new Answer<Void>() {
        public Void answer(InvocationOnMock invocation) {
          System.out.println("set data: " + invocation.getArgument(0) + " " + invocation.getArgument(1));
          if (invocation.getArgument(1) == null) {
            zkClientData.put(invocation.getArgument(0), new byte[0]);
          } else {
            zkClientData.put(invocation.getArgument(0), invocation.getArgument(1));
          }
       
          return null;
        }}).when(distribStateManagerMock).setData(anyString(), any(), anyInt());
    
    when(distribStateManagerMock.getData(anyString(), any())).thenAnswer(invocation -> {
      byte[] data = zkClientData.get(invocation.getArgument(0));
      if (data == null || data.length == 0) {
        return null;
      }
      return new VersionedData(-1, data, CreateMode.PERSISTENT, "");
        
    });
    
    when(distribStateManagerMock.createData(any(), any(), any())).thenAnswer(invocation -> {
      System.out.println("set data: " + invocation.getArgument(0) + " " + invocation.getArgument(1));
      if (invocation.getArgument(1) == null) {
        zkClientData.put(invocation.getArgument(0), new byte[0]);
      } else {
        zkClientData.put(invocation.getArgument(0), invocation.getArgument(1));
      }
      return null;
    });
    
    when(distribStateManagerMock.hasData(anyString()))
    .then(invocation -> zkClientData.containsKey(invocation.getArgument(0)) && zkClientData.get(invocation.getArgument(0)).length > 0);
    
    Mockito.doAnswer(
        new Answer<Void>() {
          public Void answer(InvocationOnMock invocation) {
            System.out.println("set data: " + invocation.getArgument(0) + " " + new byte[0]);
            zkClientData.put(invocation.getArgument(0), new byte[0]);
            return null;
          }}).when(distribStateManagerMock).makePath(anyString());

    when(solrZkClientMock.exists(any(String.class), isNull(), anyBoolean())).thenAnswer(invocation -> {
      String key = invocation.getArgument(0);
      if (zkClientData.containsKey(key)) {
        return new Stat();
      } else {
        return null;
      }
    });
    
    when(cloudManagerMock.getClusterStateProvider()).thenReturn(clusterStateProviderMock);
    when(cloudManagerMock.getTimeSource()).thenReturn(new TimeSource.NanoTimeSource());
    when(cloudManagerMock.getDistribStateManager()).thenReturn(distribStateManagerMock);
    
    when(overseerMock.getSolrCloudManager()).thenReturn(cloudManagerMock);
    
    when(overseerMock.getStateUpdateQueue(any())).thenReturn(stateUpdateQueueMock);
    when(overseerMock.getStateUpdateQueue()).thenReturn(stateUpdateQueueMock);

    // Selecting the cluster state update strategy: Overseer when distributedClusterStateUpdates is false, otherwise distributed updates.
    when(distributedClusterChangeUpdater.isDistributedStateChange()).thenReturn(distributedClusterStateUpdates);

    if (distributedClusterStateUpdates) {
      // Mocking for state change via distributed updates. There are two types of updates done in CreateCollectionCmd:
      // 1. Single line recording and executing a command
      Mockito.doAnswer(
          new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
              handleCreateCollMessageProps(invocation.getArgument(1));
              return null;
            }}).when(distributedClusterChangeUpdater).doSingleStateUpdate(any(), any(), any(), any());

      // 2. Recording a command to be executed as part of a batch of commands
      Mockito.doAnswer(
          new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
              handleCreateCollMessageProps(invocation.getArgument(1));
              return null;
            }}).when(stateChangeRecorder).record(any(), any());
    } else {
      // Mocking for state change via the Overseer queue
      Mockito.doAnswer(
          new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
              try {
                handleCreateCollMessage(invocation.getArgument(0));
                stateUpdateQueueMock.offer(invocation.getArgument(0));
              } catch (KeeperException e) {
                throw new RuntimeException(e);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              return null;
            }
          }).when(overseerMock).offerStateUpdate(any());
    }

    when(zkControllerMock.getZkClient()).thenReturn(solrZkClientMock);
    
    when(cloudManagerMock.getDistribStateManager()).thenReturn(distribStateManagerMock);

    Mockito.doAnswer(
      new Answer<Void>() {
        public Void answer(InvocationOnMock invocation) {
          System.out.println("set data: " + invocation.getArgument(0) + " " + invocation.getArgument(1));
          if (invocation.getArgument(1) == null) {
            zkClientData.put(invocation.getArgument(0), new byte[0]);
          } else {
            zkClientData.put(invocation.getArgument(0), invocation.getArgument(1));
          }
       
          return null;
        }}).when(distribStateManagerMock).setData(anyString(), any(), anyInt());
    
    when(distribStateManagerMock.getData(anyString(), any())).thenAnswer(invocation -> {
      byte[] data = zkClientData.get(invocation.getArgument(0));
      if (data == null || data.length == 0) {
        return null;
      }
      return new VersionedData(-1, data, CreateMode.PERSISTENT, "");
        
    });
    
    when(distribStateManagerMock.createData(any(), any(), any())).thenAnswer(invocation -> {
      System.out.println("set data: " + invocation.getArgument(0) + " " + invocation.getArgument(1));
      if (invocation.getArgument(1) == null) {
        zkClientData.put(invocation.getArgument(0), new byte[0]);
      } else {
        zkClientData.put(invocation.getArgument(0), invocation.getArgument(1));
      }
      return null;
    });
    
    when(distribStateManagerMock.hasData(anyString()))
    .then(invocation -> zkClientData.containsKey(invocation.getArgument(0)) && zkClientData.get(invocation.getArgument(0)).length > 0);
    
    Mockito.doAnswer(
        new Answer<Void>() {
          public Void answer(InvocationOnMock invocation) {
            System.out.println("set data: " + invocation.getArgument(0) + " " + new byte[0]);
            zkClientData.put(invocation.getArgument(0), new byte[0]);
            return null;
          }}).when(distribStateManagerMock).makePath(anyString());

    zkClientData.put("/configs/myconfig", new byte[1]);

    when(solrMetricsContextMock.getChildContext(any(Object.class))).thenReturn(solrMetricsContextMock);

    return liveNodes;
  }

  private void handleCreateCollMessage(byte[] bytes) {
    handleCreateCollMessageProps(ZkNodeProps.load(bytes));
  }

  private void handleCreateCollMessageProps(ZkNodeProps props) {
    log.info("track created replicas / collections");
    try {
      if (CollectionParams.CollectionAction.CREATE.isEqual(props.getStr("operation"))) {
        String collName = props.getStr("name");
        if (collName != null) collectionsSet.put(collName, new ClusterState.CollectionRef(
            new DocCollection(collName, new HashMap<>(), props.getProperties(), DocRouter.DEFAULT)));
      }
      if (CollectionParams.CollectionAction.ADDREPLICA.isEqual(props.getStr("operation"))) {
        replicas.add(props);
      }
    } catch (Exception e) {}
  }

  protected void startComponentUnderTest() {
    thread = new Thread(underTest);
    thread.start();
  }
  
  protected void stopComponentUnderTest() throws Exception {
    if (null != underTest) {
      underTest.close();
      underTest = null;
    }
    if (null != thread) {
      thread.interrupt();
      thread.join();
      thread = null;
    }
  }

  protected void issueCreateJob(Integer numberOfSlices,
      Integer replicationFactor, List<String> createNodeList, boolean sendCreateNodeList, boolean createNodeSetShuffle) {
    Map<String,Object> propMap = Utils.makeMap(
        Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.CREATE.toLower(),
        ZkStateReader.REPLICATION_FACTOR, replicationFactor.toString(),
        "name", COLLECTION_NAME,
        "collection.configName", CONFIG_NAME,
        OverseerCollectionMessageHandler.NUM_SLICES, numberOfSlices.toString()
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
        lastProcessMessageResult = OverseerSolrResponseSerializer.deserialize(bytes);
      }
    };
    queue.add(qe);
  }
  
  protected void verifySubmitCaptures(
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

    ArgumentCaptor<ShardRequest> shardRequestCaptor = ArgumentCaptor.forClass(ShardRequest.class);
    ArgumentCaptor<String> nodeUrlsWithoutProtocolPartCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<ModifiableSolrParams> paramsCaptor = ArgumentCaptor.forClass(ModifiableSolrParams.class);
    verify(shardHandlerMock, times(numberOfReplica * numberOfSlices))
        .submit(shardRequestCaptor.capture(), nodeUrlsWithoutProtocolPartCaptor.capture(), paramsCaptor.capture());

    for (int i = 0; i < shardRequestCaptor.getAllValues().size(); i++) {
      ShardRequest shardRequest = shardRequestCaptor.getAllValues().get(i);
      String nodeUrlsWithoutProtocolPartCapture = nodeUrlsWithoutProtocolPartCaptor.getAllValues().get(i);
      ModifiableSolrParams params = paramsCaptor.getAllValues().get(i);
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
      assertEquals(nodeUrlsWithoutProtocolPartCapture,
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
        String coreName = coreNames.get((i-1) * numberOfReplica + (j-1));
        
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
    final TimeOut timeout = new TimeOut(maxWait, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
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
      Integer numberOfSlices, boolean collectionExceptedToBeCreated, boolean distributedClusterStateUpdates) throws Exception {
    assertTrue("Wrong usage of testTemplate. numberOfNodesToCreateOn " + numberOfNodesToCreateOn + " is not allowed to be higher than numberOfNodes " + numberOfNodes, numberOfNodes.intValue() >= numberOfNodesToCreateOn.intValue());
    assertTrue("Wrong usage of testTemplage. createNodeListOption has to be " + CreateNodeListOptions.SEND + " when numberOfNodes and numberOfNodesToCreateOn are unequal", ((createNodeListOption == CreateNodeListOptions.SEND) || (numberOfNodes.intValue() == numberOfNodesToCreateOn.intValue())));
    
    Set<String> liveNodes = commonMocks(numberOfNodes, distributedClusterStateUpdates);
    List<String> createNodeList = new ArrayList<>();
    int i = 0;
    for (String node : liveNodes) {
      if (i++ < numberOfNodesToCreateOn) {
        createNodeList.add(node);
      }
    }
    
    if (random().nextBoolean()) Collections.shuffle(createNodeList, random());

    underTest = new OverseerCollectionConfigSetProcessorToBeTested(zkStateReaderMock,
        "1234", shardHandlerFactoryMock, ADMIN_PATH, workQueueMock, runningMapMock,
        overseerMock, completedMapMock, failureMapMock, solrMetricsContextMock);


    if (log.isInfoEnabled()) {
      log.info("clusterstate {}", clusterStateMock.hashCode());
    }

    startComponentUnderTest();
    
    final List<String> createNodeListToSend = ((createNodeListOption != CreateNodeListOptions.SEND_NULL) ? createNodeList : null);
    final boolean sendCreateNodeList = (createNodeListOption != CreateNodeListOptions.DONT_SEND);
    final boolean dontShuffleCreateNodeSet = (createNodeListToSend != null) && sendCreateNodeList && random().nextBoolean();
    issueCreateJob(numberOfSlices, replicationFactor, createNodeListToSend, sendCreateNodeList, !dontShuffleCreateNodeSet);
    waitForEmptyQueue(10000);

    if (collectionExceptedToBeCreated) {
      assertNotNull(lastProcessMessageResult.getResponse().toString(), lastProcessMessageResult);
    }

    if (collectionExceptedToBeCreated) {
      verifySubmitCaptures(numberOfSlices, replicationFactor,
          createNodeList, dontShuffleCreateNodeSet);
    }
  }

  // Tests below are being run twice: once with Overseer based updates and once with distributed updates.
  // This is done explicitly here because these tests use mocks than can be configured directly.
  // Tests not using mocks (most other tests) but using the MiniSolrCloudCluster are randomized to sometimes use Overseer
  // and sometimes distributed state updates (but not both for a given test and a given test seed).
  // See the SolrCloudTestCase.Builder constructor and the rest of the Builder class.

  @Test
  public void testNoReplicationEqualNumberOfSlicesPerNodeOverseer() throws Exception {
    testNoReplicationEqualNumberOfSlicesPerNodeInternal(false);
  }

  @Test
  public void testNoReplicationEqualNumberOfSlicesPerNodeDistributedUpdates() throws Exception {
    testNoReplicationEqualNumberOfSlicesPerNodeInternal(true);
  }

  private void testNoReplicationEqualNumberOfSlicesPerNodeInternal(boolean distributedClusterStateUpdates) throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.DONT_SEND;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 8;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        true, distributedClusterStateUpdates);
  }

  @Test
  public void testReplicationEqualNumberOfSlicesPerNodeOverseer() throws Exception {
    testReplicationEqualNumberOfSlicesPerNodeInternal(false);
  }
  @Test
  public void testReplicationEqualNumberOfSlicesPerNodeDistributedUpdates() throws Exception {
    testReplicationEqualNumberOfSlicesPerNodeInternal(true);
  }

  private void testReplicationEqualNumberOfSlicesPerNodeInternal(boolean distributedClusterStateUpdates) throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.DONT_SEND;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 4;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        true, distributedClusterStateUpdates);
  }

  @Test
  public void testNoReplicationEqualNumberOfSlicesPerNodeSendCreateNodesEqualToLiveNodesOverseer() throws Exception {
    testNoReplicationEqualNumberOfSlicesPerNodeSendCreateNodesEqualToLiveNodesInternal(false);
  }

  @Test
  public void testNoReplicationEqualNumberOfSlicesPerNodeSendCreateNodesEqualToLiveNodesDistributedUpdates() throws Exception {
    testNoReplicationEqualNumberOfSlicesPerNodeSendCreateNodesEqualToLiveNodesInternal(true);
  }

  private void testNoReplicationEqualNumberOfSlicesPerNodeSendCreateNodesEqualToLiveNodesInternal(boolean distributedClusterStateUpdates) throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 8;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        true, distributedClusterStateUpdates);
  }

  @Test
  public void testReplicationEqualNumberOfSlicesPerNodeSendCreateNodesEqualToLiveNodesOverseer() throws Exception {
    testReplicationEqualNumberOfSlicesPerNodeSendCreateNodesEqualToLiveNodesInternal(false);
  }

  @Test
  public void testReplicationEqualNumberOfSlicesPerNodeSendCreateNodesEqualToLiveNodesDistributedUpdates() throws Exception {
    testReplicationEqualNumberOfSlicesPerNodeSendCreateNodesEqualToLiveNodesInternal(true);
  }

  private void testReplicationEqualNumberOfSlicesPerNodeSendCreateNodesEqualToLiveNodesInternal(boolean distributedClusterStateUpdates) throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 4;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        true, distributedClusterStateUpdates);
  }

  @Test
  public void testNoReplicationEqualNumberOfSlicesPerNodeSendNullCreateNodesOverseer() throws Exception {
    testNoReplicationEqualNumberOfSlicesPerNodeSendNullCreateNodesInternal(false);
  }

  @Test
  public void testNoReplicationEqualNumberOfSlicesPerNodeSendNullCreateNodesDistributedUpdates() throws Exception {
    testNoReplicationEqualNumberOfSlicesPerNodeSendNullCreateNodesInternal(true);
  }

  private void testNoReplicationEqualNumberOfSlicesPerNodeSendNullCreateNodesInternal(boolean distributedClusterStateUpdates) throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND_NULL;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 8;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        true, distributedClusterStateUpdates);
  }

  @Test
  public void testReplicationEqualNumberOfSlicesPerNodeSendNullCreateNodesOverseer() throws Exception {
    testReplicationEqualNumberOfSlicesPerNodeSendNullCreateNodesInternal(false);
  }

  @Test
  public void testReplicationEqualNumberOfSlicesPerNodeSendNullCreateNodesDistributedUpdates() throws Exception {
    testReplicationEqualNumberOfSlicesPerNodeSendNullCreateNodesInternal(true);
  }

  private void testReplicationEqualNumberOfSlicesPerNodeSendNullCreateNodesInternal(boolean distributedClusterStateUpdates) throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND_NULL;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 4;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        true, distributedClusterStateUpdates);
  }

  @Test
  public void testNoReplicationUnequalNumberOfSlicesPerNodeOverseer() throws Exception {
    testNoReplicationUnequalNumberOfSlicesPerNodeInternal(false);
  }

  @Test
  public void testNoReplicationUnequalNumberOfSlicesPerNodeDistributedUpdates() throws Exception {
    testNoReplicationUnequalNumberOfSlicesPerNodeInternal(true);
  }

  private void testNoReplicationUnequalNumberOfSlicesPerNodeInternal(boolean distributedClusterStateUpdates) throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.DONT_SEND;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 6;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        true, distributedClusterStateUpdates);
  }

  @Test
  public void testReplicationUnequalNumberOfSlicesPerNodeOverseer() throws Exception {
    testReplicationUnequalNumberOfSlicesPerNodeInternal(false);
  }

  @Test
  public void testReplicationUnequalNumberOfSlicesPerNodeDistributedUpdates() throws Exception {
    testReplicationUnequalNumberOfSlicesPerNodeInternal(true);
  }

  private void testReplicationUnequalNumberOfSlicesPerNodeInternal(boolean distributedClusterStateUpdates) throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 4;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.DONT_SEND;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 3;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        true, distributedClusterStateUpdates);
  }

  @Test
  public void testNoReplicationLimitedNodesToCreateOnOverseer() throws Exception {
    testNoReplicationLimitedNodesToCreateOnInternal(false);
  }

  @Test
  public void testNoReplicationLimitedNodesToCreateOnDistributedUpdates() throws Exception {
    testNoReplicationLimitedNodesToCreateOnInternal(true);
  }

  private void testNoReplicationLimitedNodesToCreateOnInternal(boolean distributedClusterStateUpdates) throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 2;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 6;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        true, distributedClusterStateUpdates);
  }

  @Test
  public void testReplicationLimitedNodesToCreateOnOverseer() throws Exception {
    testReplicationLimitedNodesToCreateOnInternal(false);
  }

  @Test
  public void testReplicationLimitedNodesToCreateOnDistributedUpdates() throws Exception {
    testReplicationLimitedNodesToCreateOnInternal(true);
  }

  private void testReplicationLimitedNodesToCreateOnInternal(boolean distributedClusterStateUpdates) throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 2;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 3;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        true, distributedClusterStateUpdates);
  }

  @Test
  public void testNoReplicationCollectionNotCreatedDueToMaxShardsPerNodeAndNodesToCreateOnLimitsOverseer() throws Exception {
    testNoReplicationCollectionNotCreatedDueToMaxShardsPerNodeAndNodesToCreateOnLimitsInternal(false);
  }

  @Test
  public void testNoReplicationCollectionNotCreatedDueToMaxShardsPerNodeAndNodesToCreateOnLimitsDistributedUpdates() throws Exception {
    testNoReplicationCollectionNotCreatedDueToMaxShardsPerNodeAndNodesToCreateOnLimitsInternal(true);
  }

  private void testNoReplicationCollectionNotCreatedDueToMaxShardsPerNodeAndNodesToCreateOnLimitsInternal(boolean distributedClusterStateUpdates) throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 3;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND;
    Integer replicationFactor = 1;
    Integer numberOfSlices = 8;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        false, distributedClusterStateUpdates);
  }

  @Test
  public void testReplicationCollectionNotCreatedDueToMaxShardsPerNodeAndNodesToCreateOnLimitsOverseer() throws Exception {
    testReplicationCollectionNotCreatedDueToMaxShardsPerNodeAndNodesToCreateOnLimitsInternal(false);
  }

  @Test
  public void testReplicationCollectionNotCreatedDueToMaxShardsPerNodeAndNodesToCreateOnLimitsDistributedUpdates() throws Exception {
    testReplicationCollectionNotCreatedDueToMaxShardsPerNodeAndNodesToCreateOnLimitsInternal(true);
  }

  private void testReplicationCollectionNotCreatedDueToMaxShardsPerNodeAndNodesToCreateOnLimitsInternal(boolean distributedClusterStateUpdates) throws Exception {
    Integer numberOfNodes = 4;
    Integer numberOfNodesToCreateOn = 3;
    CreateNodeListOptions createNodeListOptions = CreateNodeListOptions.SEND;
    Integer replicationFactor = 2;
    Integer numberOfSlices = 4;
    testTemplate(numberOfNodes, numberOfNodesToCreateOn, createNodeListOptions, replicationFactor, numberOfSlices,
        false, distributedClusterStateUpdates);
  }

}
