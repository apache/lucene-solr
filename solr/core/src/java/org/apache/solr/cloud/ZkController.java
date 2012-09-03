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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPathConstants;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.WaitForState;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkClientConnectionStrategy;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.Config;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;

import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.DOMUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Handle ZooKeeper interactions.
 * 
 * notes: loads everything on init, creates what's not there - further updates
 * are prompted with Watches.
 * 
 * TODO: exceptions during shutdown on attempts to update cloud state
 * 
 */
public final class ZkController {

  private static Logger log = LoggerFactory.getLogger(ZkController.class);

  static final String NEWL = System.getProperty("line.separator");


  private final static Pattern URL_POST = Pattern.compile("https?://(.*)");
  private final static Pattern URL_PREFIX = Pattern.compile("(https?://).*");

  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");
  
  private final DistributedQueue overseerJobQueue;
  private final DistributedQueue overseerCollectionQueue;
  
  // package private for tests

  static final String CONFIGS_ZKNODE = "/configs";

  public final static String COLLECTION_PARAM_PREFIX="collection.";
  public final static String CONFIGNAME_PROP="configName";

  private final Map<String, ElectionContext> electionContexts = Collections.synchronizedMap(new HashMap<String, ElectionContext>());
  
  private SolrZkClient zkClient;
  private ZkCmdExecutor cmdExecutor;
  private ZkStateReader zkStateReader;

  private LeaderElector leaderElector;
  
  private String zkServerAddress;          // example: 127.0.0.1:54062/solr

  private final String localHostPort;      // example: 54065
  private final String localHostContext;   // example: solr
  private final String localHost;          // example: http://127.0.0.1
  private final String hostName;           // example: 127.0.0.1
  private final String nodeName;           // example: 127.0.0.1:54065_solr
  private final String baseURL;            // example: http://127.0.0.1:54065/solr


  private LeaderElector overseerElector;
  

  // for now, this can be null in tests, in which case recovery will be inactive, and other features
  // may accept defaults or use mocks rather than pulling things from a CoreContainer
  private CoreContainer cc;

  protected volatile Overseer overseer;

  private String leaderVoteWait;

  /**
   * @param cc
   * @param zkServerAddress
   * @param zkClientTimeout
   * @param zkClientConnectTimeout
   * @param localHost
   * @param locaHostPort
   * @param localHostContext
   * @param registerOnReconnect
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws IOException
   */
  public ZkController(final CoreContainer cc, String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout, String localHost, String locaHostPort,
      String localHostContext, final CurrentCoreDescriptorProvider registerOnReconnect) throws InterruptedException,
      TimeoutException, IOException {
    this(cc, zkServerAddress, zkClientTimeout, zkClientConnectTimeout, localHost, locaHostPort, localHostContext, null, registerOnReconnect);
  }
  

  /**
   * @param cc
   * @param zkServerAddress
   * @param zkClientTimeout
   * @param zkClientConnectTimeout
   * @param localHost
   * @param locaHostPort
   * @param localHostContext
   * @param leaderVoteWait
   * @param registerOnReconnect
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws IOException
   */
  public ZkController(final CoreContainer cc, String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout, String localHost, String locaHostPort,
      String localHostContext, String leaderVoteWait, final CurrentCoreDescriptorProvider registerOnReconnect) throws InterruptedException,
      TimeoutException, IOException {
    if (cc == null) throw new IllegalArgumentException("CoreContainer cannot be null.");
    this.cc = cc;
    if (localHostContext.contains("/")) {
      throw new IllegalArgumentException("localHostContext ("
          + localHostContext + ") should not contain a /");
    }
    
    this.zkServerAddress = zkServerAddress;
    this.localHostPort = locaHostPort;
    this.localHostContext = localHostContext;
    this.localHost = getHostAddress(localHost);
    this.hostName = getHostNameFromAddress(this.localHost);
    this.nodeName = this.hostName + ':' + this.localHostPort + '_' + this.localHostContext;
    this.baseURL = this.localHost + ":" + this.localHostPort + "/" + this.localHostContext;
    this.leaderVoteWait = leaderVoteWait;

    zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout, zkClientConnectTimeout,
        // on reconnect, reload cloud info
        new OnReconnect() {

          public void command() {
            try {
              // we need to create all of our lost watches
              
              // seems we dont need to do this again...
              //Overseer.createClientNodes(zkClient, getNodeName());
              ShardHandler shardHandler;
              String adminPath;
              shardHandler = cc.getShardHandlerFactory().getShardHandler();
              adminPath = cc.getAdminPath();
              ZkController.this.overseer = new Overseer(shardHandler, adminPath, zkStateReader);
              ElectionContext context = new OverseerElectionContext(zkClient, overseer, getNodeName());
              overseerElector.joinElection(context);
              zkStateReader.createClusterStateWatchersAndUpdate();
              
              registerAllCoresAsDown(registerOnReconnect);
              

              // we have to register as live first to pick up docs in the buffer
              createEphemeralLiveNode();
              
              List<CoreDescriptor> descriptors = registerOnReconnect.getCurrentDescriptors();
              // re register all descriptors
              if (descriptors  != null) {
                for (CoreDescriptor descriptor : descriptors) {
                  // TODO: we need to think carefully about what happens when it was
                  // a leader that was expired - as well as what to do about leaders/overseers
                  // with connection loss
                  try {
                    register(descriptor.getName(), descriptor, true, true);
                  } catch (Throwable t) {
                    SolrException.log(log, "Error registering SolrCore", t);
                  }
                }
              }
  
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            } catch (Exception e) {
              SolrException.log(log, "", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            }

          }

 
        });
    
    zkClient.getZkClientConnectionStrategy().addDisconnectedListener(new ZkClientConnectionStrategy.DisconnectedListener() {
      
      @Override
      public void disconnected() {
        List<CoreDescriptor> descriptors = registerOnReconnect.getCurrentDescriptors();
        // re register all descriptors
        if (descriptors  != null) {
          for (CoreDescriptor descriptor : descriptors) {
            descriptor.getCloudDescriptor().isLeader = false;
          }
        }
      }
    });
    
    zkClient.getZkClientConnectionStrategy().addConnectedListener(new ZkClientConnectionStrategy.ConnectedListener() {
      
      @Override
      public void connected() {
        List<CoreDescriptor> descriptors = registerOnReconnect.getCurrentDescriptors();
        if (descriptors  != null) {
          for (CoreDescriptor descriptor : descriptors) {
            CloudDescriptor cloudDesc = descriptor.getCloudDescriptor();
            String leaderUrl;
            try {
              leaderUrl = getLeaderProps(cloudDesc.getCollectionName(), cloudDesc.getShardId())
                  .getCoreUrl();
            } catch (InterruptedException e) {
              throw new RuntimeException();
            }
            String ourUrl = ZkCoreNodeProps.getCoreUrl(getBaseUrl(), descriptor.getName());
            boolean isLeader = leaderUrl.equals(ourUrl);
            log.info("SolrCore connected to ZooKeeper - we are " + ourUrl + " and leader is " + leaderUrl);
            cloudDesc.isLeader = isLeader;
          }
        }
      }
    });
    
    this.overseerJobQueue = Overseer.getInQueue(zkClient);
    this.overseerCollectionQueue = Overseer.getCollectionQueue(zkClient);
    cmdExecutor = new ZkCmdExecutor();
    leaderElector = new LeaderElector(zkClient);
    zkStateReader = new ZkStateReader(zkClient);
    
    init(registerOnReconnect);
  }

  public String getLeaderVoteWait() {
    return leaderVoteWait;
  }

  private void registerAllCoresAsDown(
      final CurrentCoreDescriptorProvider registerOnReconnect) {
    List<CoreDescriptor> descriptors = registerOnReconnect
        .getCurrentDescriptors();
    if (descriptors != null) {
      // before registering as live, make sure everyone is in a
      // down state
      for (CoreDescriptor descriptor : descriptors) {
        final String coreZkNodeName = getNodeName() + "_"
            + descriptor.getName();
        try {
          publish(descriptor, ZkStateReader.DOWN);
          waitForLeaderToSeeDownState(descriptor, coreZkNodeName);
        } catch (Exception e) {
          SolrException.log(log, "", e);
        }
      }
    }
  }

  /**
   * Closes the underlying ZooKeeper client.
   */
  public void close() {
    try {
      String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
      // we don't retry if there is a problem - count on ephem timeout
      zkClient.delete(nodePath, -1, false);
    } catch (KeeperException.NoNodeException e) {
      // fine
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (KeeperException e) {
      SolrException.log(log, "Error trying to remove our ephem live node", e);
    }
    
    for (ElectionContext context : electionContexts.values()) {
      context.close();
    }
    
    try {
      overseer.close();
    } catch(Throwable t) {
      log.error("Error closing overseer", t);
    }
    zkClient.close();
  }

  /**
   * @param collection
   * @param fileName
   * @return true if config file exists
   * @throws KeeperException
   * @throws InterruptedException
   */
  public boolean configFileExists(String collection, String fileName)
      throws KeeperException, InterruptedException {
    Stat stat = zkClient.exists(CONFIGS_ZKNODE + "/" + collection + "/" + fileName, null, true);
    return stat != null;
  }

  /**
   * @return information about the cluster from ZooKeeper
   */
  public ClusterState getClusterState() {
    return zkStateReader.getClusterState();
  }

  /**
   * @param zkConfigName
   * @param fileName
   * @return config file data (in bytes)
   * @throws KeeperException
   * @throws InterruptedException
   */
  public byte[] getConfigFileData(String zkConfigName, String fileName)
      throws KeeperException, InterruptedException {
    String zkPath = CONFIGS_ZKNODE + "/" + zkConfigName + "/" + fileName;
    byte[] bytes = zkClient.getData(zkPath, null, null, true);
    if (bytes == null) {
      log.error("Config file contains no data:" + zkPath);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "Config file contains no data:" + zkPath);
    }
    
    return bytes;
  }

  // normalize host to url_prefix://host
  // input can be null, host, or url_prefix://host
  private String getHostAddress(String host) throws IOException {

    if (host == null) {
      host = "http://" + InetAddress.getLocalHost().getHostName();
    } else {
      Matcher m = URL_PREFIX.matcher(host);
      if (m.matches()) {
        String prefix = m.group(1);
        host = prefix + host;
      } else {
        host = "http://" + host;
      }
    }

    return host;
  }

  // extract host from url_prefix://host
  private String getHostNameFromAddress(String addr) {
    Matcher m = URL_POST.matcher(addr);
    if (m.matches()) {
      return m.group(1);
    } else {
      log.error("Unrecognized host:" + addr);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "Unrecognized host:" + addr);
    }
  }
  
  
  
  public String getHostName() {
    return hostName;
  }
  
  public String getHostPort() {
    return localHostPort;
  }

  public SolrZkClient getZkClient() {
    return zkClient;
  }

  /**
   * @return zookeeper server address
   */
  public String getZkServerAddress() {
    return zkServerAddress;
  }

  private void init(CurrentCoreDescriptorProvider registerOnReconnect) {
    registerAllCoresAsDown(registerOnReconnect);
    
    try {
      // makes nodes zkNode
      cmdExecutor.ensureExists(ZkStateReader.LIVE_NODES_ZKNODE, zkClient);
      
      createEphemeralLiveNode();
      cmdExecutor.ensureExists(ZkStateReader.COLLECTIONS_ZKNODE, zkClient);

      ShardHandler shardHandler;
      String adminPath;
      shardHandler = cc.getShardHandlerFactory().getShardHandler();
      adminPath = cc.getAdminPath();
      
      overseerElector = new LeaderElector(zkClient);
      this.overseer = new Overseer(shardHandler, adminPath, zkStateReader);
      ElectionContext context = new OverseerElectionContext(zkClient, overseer, getNodeName());
      overseerElector.setup(context);
      overseerElector.joinElection(context);
      zkStateReader.createClusterStateWatchersAndUpdate();
      
    } catch (IOException e) {
      log.error("", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Can't create ZooKeeperController", e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    } catch (KeeperException e) {
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    }

  }

  public boolean isConnected() {
    return zkClient.isConnected();
  }

  private void createEphemeralLiveNode() throws KeeperException,
      InterruptedException {
    String nodeName = getNodeName();
    String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
    log.info("Register node as live in ZooKeeper:" + nodePath);
   
    try {
      boolean nodeDeleted = true;
      try {
        // we attempt a delete in the case of a quick server bounce -
        // if there was not a graceful shutdown, the node may exist
        // until expiration timeout - so a node won't be created here because
        // it exists, but eventually the node will be removed. So delete
        // in case it exists and create a new node.
        zkClient.delete(nodePath, -1, true);
      } catch (KeeperException.NoNodeException e) {
        // fine if there is nothing to delete
        // TODO: annoying that ZK logs a warning on us
        nodeDeleted = false;
      }
      if (nodeDeleted) {
        log
            .info("Found a previous node that still exists while trying to register a new live node "
                + nodePath + " - removing existing node to create another.");
      }
      zkClient.makePath(nodePath, CreateMode.EPHEMERAL, true);
    } catch (KeeperException e) {
      // its okay if the node already exists
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        throw e;
      }
    }    
  }
  
  public String getNodeName() {
    return nodeName;
  }

  /**
   * @param path
   * @return true if the path exists
   * @throws KeeperException
   * @throws InterruptedException
   */
  public boolean pathExists(String path) throws KeeperException,
      InterruptedException {
    return zkClient.exists(path, true);
  }

  /**
   * @param collection
   * @return config value
   * @throws KeeperException
   * @throws InterruptedException
   */
  public String readConfigName(String collection) throws KeeperException,
      InterruptedException {

    String configName = null;

    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
    if (log.isInfoEnabled()) {
      log.info("Load collection config from:" + path);
    }
    byte[] data = zkClient.getData(path, null, null, true);
    
    if(data != null) {
      ZkNodeProps props = ZkNodeProps.load(data);
      configName = props.get(CONFIGNAME_PROP);
    }
    
    if (configName != null && !zkClient.exists(CONFIGS_ZKNODE + "/" + configName, true)) {
      log.error("Specified config does not exist in ZooKeeper:" + configName);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "Specified config does not exist in ZooKeeper:" + configName);
    }

    return configName;
  }



  /**
   * Register shard with ZooKeeper.
   * 
   * @param coreName
   * @param desc
   * @return the shardId for the SolrCore
   * @throws Exception
   */
  public String register(String coreName, final CoreDescriptor desc) throws Exception {  
    return register(coreName, desc, false, false);
  }
  

  /**
   * Register shard with ZooKeeper.
   * 
   * @param coreName
   * @param desc
   * @param recoverReloadedCores
   * @param afterExpiration
   * @return the shardId for the SolrCore
   * @throws Exception
   */
  public String register(String coreName, final CoreDescriptor desc, boolean recoverReloadedCores, boolean afterExpiration) throws Exception {  
    final String baseUrl = getBaseUrl();
    
    final CloudDescriptor cloudDesc = desc.getCloudDescriptor();
    final String collection = cloudDesc.getCollectionName();

    final String coreZkNodeName = getNodeName() + "_" + coreName;
    
    String shardId = cloudDesc.getShardId();

    Map<String,String> props = new HashMap<String,String>();
 // we only put a subset of props into the leader node
    props.put(ZkStateReader.BASE_URL_PROP, baseUrl);
    props.put(ZkStateReader.CORE_NAME_PROP, coreName);
    props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());


    if (log.isInfoEnabled()) {
        log.info("Register shard - core:" + coreName + " address:"
            + baseUrl + " shardId:" + shardId);
    }

    ZkNodeProps leaderProps = new ZkNodeProps(props);
    
    try {
      joinElection(desc, afterExpiration);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (KeeperException e) {
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (IOException e) {
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }
    
    String leaderUrl = getLeader(cloudDesc);
    
    String ourUrl = ZkCoreNodeProps.getCoreUrl(baseUrl, coreName);
    log.info("We are " + ourUrl + " and leader is " + leaderUrl);
    boolean isLeader = leaderUrl.equals(ourUrl);
    

    SolrCore core = null;
    try {
      core = cc.getCore(desc.getName());

 
      // recover from local transaction log and wait for it to complete before
      // going active
      // TODO: should this be moved to another thread? To recoveryStrat?
      // TODO: should this actually be done earlier, before (or as part of)
      // leader election perhaps?
      // TODO: if I'm the leader, ensure that a replica that is trying to recover waits until I'm
      // active (or don't make me the
      // leader until my local replay is done.

      UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
      if (!core.isReloaded() && ulog != null) {
        Future<UpdateLog.RecoveryInfo> recoveryFuture = core.getUpdateHandler()
            .getUpdateLog().recoverFromLog();
        if (recoveryFuture != null) {
          recoveryFuture.get(); // NOTE: this could potentially block for
          // minutes or more!
          // TODO: public as recovering in the mean time?
          // TODO: in the future we could do peerync in parallel with recoverFromLog
        } else {
          log.info("No LogReplay needed for core="+core.getName() + " baseURL=" + baseUrl);
        }
      }      
      boolean didRecovery = checkRecovery(coreName, desc, recoverReloadedCores, isLeader, cloudDesc,
          collection, coreZkNodeName, shardId, leaderProps, core, cc);
      if (!didRecovery) {
        publish(desc, ZkStateReader.ACTIVE);
      }
    } finally {
      if (core != null) {
        core.close();
      }
    }

    
    // make sure we have an update cluster state right away
    zkStateReader.updateClusterState(true);

    return shardId;
  }

  private String getLeader(final CloudDescriptor cloudDesc) {
    
    String collection = cloudDesc.getCollectionName();
    String shardId = cloudDesc.getShardId();
    // rather than look in the cluster state file, we go straight to the zknodes
    // here, because on cluster restart there could be stale leader info in the
    // cluster state node that won't be updated for a moment
    String leaderUrl;
    try {
      leaderUrl = getLeaderProps(collection, cloudDesc.getShardId())
          .getCoreUrl();
      
      // now wait until our currently cloud state contains the latest leader
      String clusterStateLeader = zkStateReader.getLeaderUrl(collection,
          shardId, 30000);
      int tries = 0;
      while (!leaderUrl.equals(clusterStateLeader)) {
        if (tries == 60) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "There is conflicting information about the leader of shard: "
                  + cloudDesc.getShardId() + " our state says:"
                  + clusterStateLeader + " but zookeeper says:" + leaderUrl);
        }
        Thread.sleep(1000);
        tries++;
        clusterStateLeader = zkStateReader.getLeaderUrl(collection, shardId,
            30000);
        leaderUrl = getLeaderProps(collection, cloudDesc.getShardId())
            .getCoreUrl();
      }
      
    } catch (Exception e) {
      log.error("Error getting leader from zk", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error getting leader from zk", e);
    } 
    return leaderUrl;
  }
  
  /**
   * Get leader props directly from zk nodes.
   * 
   * @param collection
   * @param slice
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  private ZkCoreNodeProps getLeaderProps(final String collection,
      final String slice) throws InterruptedException {
    int iterCount = 60;
    Exception exp = null;
    while (iterCount-- > 0) {
      try {
        byte[] data = zkClient.getData(
            ZkStateReader.getShardLeadersPath(collection, slice), null, null,
            true);
        ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(
            ZkNodeProps.load(data));
        return leaderProps;
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception e) {
        exp = e;
        Thread.sleep(500);
      }
      if (cc.isShutDown()) {
        throw new RuntimeException("CoreContainer is shutdown");
      }
    }
    throw new RuntimeException("Could not get leader props", exp);
  }


  private void joinElection(CoreDescriptor cd, boolean afterExpiration) throws InterruptedException, KeeperException, IOException {
    
    String shardId = cd.getCloudDescriptor().getShardId();
    
    Map<String,String> props = new HashMap<String,String>();
    // we only put a subset of props into the leader node
    props.put(ZkStateReader.BASE_URL_PROP, getBaseUrl());
    props.put(ZkStateReader.CORE_NAME_PROP, cd.getName());
    props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
    
    final String coreZkNodeName = getNodeName() + "_" + cd.getName();
    ZkNodeProps ourProps = new ZkNodeProps(props);
    String collection = cd.getCloudDescriptor()
        .getCollectionName();
    
    ElectionContext context = new ShardLeaderElectionContext(leaderElector, shardId,
        collection, coreZkNodeName, ourProps, this, cc, afterExpiration);

    leaderElector.setup(context);
    electionContexts.put(coreZkNodeName, context);
    leaderElector.joinElection(context);
  }


  /**
   * @param coreName
   * @param desc
   * @param recoverReloadedCores
   * @param isLeader
   * @param cloudDesc
   * @param collection
   * @param shardZkNodeName
   * @param shardId
   * @param leaderProps
   * @param core
   * @param cc
   * @return whether or not a recovery was started
   */
  private boolean checkRecovery(String coreName, final CoreDescriptor desc,
      boolean recoverReloadedCores, final boolean isLeader,
      final CloudDescriptor cloudDesc, final String collection,
      final String shardZkNodeName, String shardId, ZkNodeProps leaderProps,
      SolrCore core, CoreContainer cc) {
    if (SKIP_AUTO_RECOVERY) {
      log.warn("Skipping recovery according to sys prop solrcloud.skip.autorecovery");
      return false;
    }
    boolean doRecovery = true;
    if (!isLeader) {
      
      if (core.isReloaded() && !recoverReloadedCores) {
        doRecovery = false;
      }
      
      if (doRecovery) {
        log.info("Core needs to recover:" + core.getName());
        core.getUpdateHandler().getSolrCoreState().doRecovery(cc, coreName);
        return true;
      }
    } else {
      log.info("I am the leader, no recovery necessary");
    }
    
    return false;
  }


  public String getBaseUrl() {
    return baseURL;
  }

  /**
   * Publish core state to overseer.
   * @param cd
   * @param state
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void publish(final CoreDescriptor cd, final String state) throws KeeperException, InterruptedException {
    //System.out.println(Thread.currentThread().getStackTrace()[3]);
    Integer numShards = cd.getCloudDescriptor().getNumShards();
    if (numShards == null) { //XXX sys prop hack
      numShards = Integer.getInteger(ZkStateReader.NUM_SHARDS_PROP);
    }
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, "state", 
        ZkStateReader.STATE_PROP, state, 
        ZkStateReader.BASE_URL_PROP, getBaseUrl(), 
        ZkStateReader.CORE_NAME_PROP, cd.getName(),
        ZkStateReader.ROLES_PROP, cd.getCloudDescriptor().getRoles(),
        ZkStateReader.NODE_NAME_PROP, getNodeName(),
        ZkStateReader.SHARD_ID_PROP, cd.getCloudDescriptor().getShardId(),
        ZkStateReader.COLLECTION_PROP, cd.getCloudDescriptor()
            .getCollectionName(), ZkStateReader.STATE_PROP, state,
        ZkStateReader.NUM_SHARDS_PROP, numShards != null ? numShards.toString()
            : null);
    overseerJobQueue.offer(ZkStateReader.toJSON(m));
  }

  private boolean needsToBeAssignedShardId(final CoreDescriptor desc,
      final ClusterState state, final String shardZkNodeName) {

    final CloudDescriptor cloudDesc = desc.getCloudDescriptor();
    
    final String shardId = state.getShardId(shardZkNodeName);

    if (shardId != null) {
      cloudDesc.setShardId(shardId);
      return false;
    }
    return true;
  }

  /**
   * @param coreName
   * @param cloudDesc
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void unregister(String coreName, CloudDescriptor cloudDesc)
      throws InterruptedException, KeeperException {
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        "deletecore", ZkStateReader.CORE_NAME_PROP, coreName,
        ZkStateReader.NODE_NAME_PROP, getNodeName(),
        ZkStateReader.COLLECTION_PROP, cloudDesc.getCollectionName());
    overseerJobQueue.offer(ZkStateReader.toJSON(m));

    final String zkNodeName = getNodeName() + "_" + coreName;
    ElectionContext context = electionContexts.remove(zkNodeName);
    if (context != null) {
      context.cancelElection();
    }
  }
  
  public void createCollection(String collection) throws KeeperException,
      InterruptedException {
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        "createcollection", ZkStateReader.NODE_NAME_PROP, getNodeName(),
        ZkStateReader.COLLECTION_PROP, collection);
    overseerJobQueue.offer(ZkStateReader.toJSON(m));
  }

  /**
   * @param dir
   * @param zkPath
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void uploadToZK(File dir, String zkPath) throws IOException, KeeperException, InterruptedException {
    uploadToZK(zkClient, dir, zkPath);
  }
  
  /**
   * @param dir
   * @param configName
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void uploadConfigDir(File dir, String configName) throws IOException, KeeperException, InterruptedException {
    uploadToZK(zkClient, dir, ZkController.CONFIGS_ZKNODE + "/" + configName);
  }

  // convenience for testing
  void printLayoutToStdOut() throws KeeperException, InterruptedException {
    zkClient.printLayoutToStdOut();
  }

  public void createCollectionZkNode(CloudDescriptor cd) throws KeeperException, InterruptedException {
    String collection = cd.getCollectionName();
    
    log.info("Check for collection zkNode:" + collection);
    String collectionPath = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
    
    try {
      if(!zkClient.exists(collectionPath, true)) {
        log.info("Creating collection in ZooKeeper:" + collection);
       SolrParams params = cd.getParams();

        try {
          Map<String,String> collectionProps = new HashMap<String,String>();
          // TODO: if collection.configName isn't set, and there isn't already a conf in zk, just use that?
          String defaultConfigName = System.getProperty(COLLECTION_PARAM_PREFIX+CONFIGNAME_PROP, collection);

          // params passed in - currently only done via core admin (create core commmand).
          if (params != null) {
            Iterator<String> iter = params.getParameterNamesIterator();
            while (iter.hasNext()) {
              String paramName = iter.next();
              if (paramName.startsWith(COLLECTION_PARAM_PREFIX)) {
                collectionProps.put(paramName.substring(COLLECTION_PARAM_PREFIX.length()), params.get(paramName));
              }
            }

            // if the config name wasn't passed in, use the default
            if (!collectionProps.containsKey(CONFIGNAME_PROP))
              getConfName(collection, collectionPath, collectionProps);
            
          } else if(System.getProperty("bootstrap_confdir") != null) {
            // if we are bootstrapping a collection, default the config for
            // a new collection to the collection we are bootstrapping
            log.info("Setting config for collection:" + collection + " to " + defaultConfigName);

            Properties sysProps = System.getProperties();
            for (String sprop : System.getProperties().stringPropertyNames()) {
              if (sprop.startsWith(COLLECTION_PARAM_PREFIX)) {
                collectionProps.put(sprop.substring(COLLECTION_PARAM_PREFIX.length()), sysProps.getProperty(sprop));                
              }
            }
            
            // if the config name wasn't passed in, use the default
            if (!collectionProps.containsKey(CONFIGNAME_PROP))
              collectionProps.put(CONFIGNAME_PROP,  defaultConfigName);

          } else if (Boolean.getBoolean("bootstrap_conf")) {
            // the conf name should should be the collection name of this core
            collectionProps.put(CONFIGNAME_PROP,  cd.getCollectionName());
          } else {
            getConfName(collection, collectionPath, collectionProps);
          }
          
          ZkNodeProps zkProps = new ZkNodeProps(collectionProps);
          zkClient.makePath(collectionPath, ZkStateReader.toJSON(zkProps), CreateMode.PERSISTENT, null, true);

        } catch (KeeperException e) {
          // its okay if the node already exists
          if (e.code() != KeeperException.Code.NODEEXISTS) {
            throw e;
          }
        }
      } else {
        log.info("Collection zkNode exists");
      }
      
    } catch (KeeperException e) {
      // its okay if another beats us creating the node
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        throw e;
      }
    }
    
  }


  private void getConfName(String collection, String collectionPath,
      Map<String,String> collectionProps) throws KeeperException,
      InterruptedException {
    // check for configName
    log.info("Looking for collection configName");
    List<String> configNames = null;
    int retry = 1;
    int retryLimt = 6;
    for (; retry < retryLimt; retry++) {
      if (zkClient.exists(collectionPath, true)) {
        ZkNodeProps cProps = ZkNodeProps.load(zkClient.getData(collectionPath, null, null, true));
        if (cProps.containsKey(CONFIGNAME_PROP)) {
          break;
        }
      }
     
      // if there is only one conf, use that
      try {
        configNames = zkClient.getChildren(CONFIGS_ZKNODE, null,
            true);
      } catch (NoNodeException e) {
        // just keep trying
      }
      if (configNames != null && configNames.size() == 1) {
        // no config set named, but there is only 1 - use it
        log.info("Only one config set found in zk - using it:" + configNames.get(0));
        collectionProps.put(CONFIGNAME_PROP,  configNames.get(0));
        break;
      }
      
      if (configNames != null && configNames.contains(collection)) {
        log.info("Could not find explicit collection configName, but found config name matching collection name - using that set.");
        collectionProps.put(CONFIGNAME_PROP,  collection);
        break;
      }
      
      log.info("Could not find collection configName - pausing for 3 seconds and trying again - try: " + retry);
      Thread.sleep(3000);
    }
    if (retry == retryLimt) {
      log.error("Could not find configName for collection " + collection);
      throw new ZooKeeperException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Could not find configName for collection " + collection + " found:" + configNames);
    }
  }
  
  public ZkStateReader getZkStateReader() {
    return zkStateReader;
  }

  private String doGetShardIdProcess(String coreName, CloudDescriptor descriptor) {
    final String shardZkNodeName = getNodeName() + "_" + coreName;
    int retryCount = 120;
    while (retryCount-- > 0) {
      final String shardId = zkStateReader.getClusterState().getShardId(
          shardZkNodeName);
      if (shardId != null) {
        return shardId;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    
    throw new SolrException(ErrorCode.SERVER_ERROR,
        "Could not get shard_id for core: " + coreName);
  }
  
  public static void uploadToZK(SolrZkClient zkClient, File dir, String zkPath) throws IOException, KeeperException, InterruptedException {
    File[] files = dir.listFiles();
    if (files == null) {
      throw new IllegalArgumentException("Illegal directory: " + dir);
    }
    for(File file : files) {
      if (!file.getName().startsWith(".")) {
        if (!file.isDirectory()) {
          zkClient.makePath(zkPath + "/" + file.getName(), file, false, true);
        } else {
          uploadToZK(zkClient, file, zkPath + "/" + file.getName());
        }
      }
    }
  }
  
  public static void downloadFromZK(SolrZkClient zkClient, String zkPath,
      File dir) throws IOException, KeeperException, InterruptedException {
    List<String> files = zkClient.getChildren(zkPath, null, true);
    
    for (String file : files) {
      List<String> children = zkClient.getChildren(zkPath + "/" + file, null, true);
      if (children.size() == 0) {
        byte[] data = zkClient.getData(zkPath + "/" + file, null, null, true);
        dir.mkdirs(); 
        log.info("Write file " + new File(dir, file));
        FileUtils.writeStringToFile(new File(dir, file), new String(data, "UTF-8"), "UTF-8");
      } else {
        downloadFromZK(zkClient, zkPath + "/" + file, new File(dir, file));
      }
    }
  }
  
  
  private String getCoreNodeName(CoreDescriptor descriptor){
    return getNodeName() + "_"
        + descriptor.getName();
  }
  
  public static void uploadConfigDir(SolrZkClient zkClient, File dir, String configName) throws IOException, KeeperException, InterruptedException {
    uploadToZK(zkClient, dir, ZkController.CONFIGS_ZKNODE + "/" + configName);
  }
  
  public static void downloadConfigDir(SolrZkClient zkClient, String configName, File dir) throws IOException, KeeperException, InterruptedException {
    downloadFromZK(zkClient, ZkController.CONFIGS_ZKNODE + "/" + configName, dir);
  }

  public void preRegister(CoreDescriptor cd) throws KeeperException, InterruptedException {
    // before becoming available, make sure we are not live and active
    // this also gets us our assigned shard id if it was not specified
    publish(cd, ZkStateReader.DOWN); 
    String shardZkNodeName = getCoreNodeName(cd);
    if (cd.getCloudDescriptor().getShardId() == null && needsToBeAssignedShardId(cd, zkStateReader.getClusterState(), shardZkNodeName)) {
      String shardId;
      shardId = doGetShardIdProcess(cd.getName(), cd.getCloudDescriptor());
      cd.getCloudDescriptor().setShardId(shardId);
    }

  }

  private ZkCoreNodeProps waitForLeaderToSeeDownState(
      CoreDescriptor descriptor, final String coreZkNodeName) {
    CloudDescriptor cloudDesc = descriptor.getCloudDescriptor();
    String collection = cloudDesc.getCollectionName();
    String shard = cloudDesc.getShardId();
    ZkCoreNodeProps leaderProps = null;
    
    int retries = 6;
    for (int i = 0; i < retries; i++) {
      try {
        // go straight to zk, not the cloud state - we must have current info
        leaderProps = getLeaderProps(collection, shard);
        break;
      } catch (Exception e) {
        SolrException.log(log, "There was a problem finding the leader in zk", e);
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
        if (i == retries - 1) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "There was a problem finding the leader in zk");
        }
      }
    }
    
    String leaderBaseUrl = leaderProps.getBaseUrl();
    String leaderCoreName = leaderProps.getCoreName();
    
    String ourUrl = ZkCoreNodeProps.getCoreUrl(getBaseUrl(),
        descriptor.getName());
    
    boolean isLeader = leaderProps.getCoreUrl().equals(ourUrl);
    if (!isLeader && !SKIP_AUTO_RECOVERY) {
      HttpSolrServer server = null;
      server = new HttpSolrServer(leaderBaseUrl);
      server.setConnectionTimeout(45000);
      server.setSoTimeout(45000);
      WaitForState prepCmd = new WaitForState();
      prepCmd.setCoreName(leaderCoreName);
      prepCmd.setNodeName(getNodeName());
      prepCmd.setCoreNodeName(coreZkNodeName);
      prepCmd.setState(ZkStateReader.DOWN);
      prepCmd.setPauseFor(0);
      
      // let's retry a couple times - perhaps the leader just went down,
      // or perhaps he is just not quite ready for us yet
      retries = 6;
      for (int i = 0; i < retries; i++) {
        try {
          server.request(prepCmd);
          break;
        } catch (Exception e) {
          SolrException.log(log, "There was a problem making a request to the leader", e);
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
          }
          if (i == retries - 1) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "There was a problem making a request to the leader");
          }
        }
      }
      
      server.shutdown();
    }
    return leaderProps;
  }
  
  public static void linkConfSet(SolrZkClient zkClient, String collection, String confSetName) throws KeeperException, InterruptedException {
    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
    if (log.isInfoEnabled()) {
      log.info("Load collection config from:" + path);
    }
    byte[] data;
    try {
      data = zkClient.getData(path, null, null, true);
    } catch (NoNodeException e) {
      // if there is no node, we will try and create it
      // first try to make in case we are pre configuring
      ZkNodeProps props = new ZkNodeProps(CONFIGNAME_PROP, confSetName);
      try {

        zkClient.makePath(path, ZkStateReader.toJSON(props),
            CreateMode.PERSISTENT, null, true);
      } catch (KeeperException e2) {
        // its okay if the node already exists
        if (e2.code() != KeeperException.Code.NODEEXISTS) {
          throw e;
        }
        // if we fail creating, setdata
        // TODO: we should consider using version
        zkClient.setData(path, ZkStateReader.toJSON(props), true);
      }
      return;
    }
    // we found existing data, let's update it
    ZkNodeProps props = null;
    if(data != null) {
      props = ZkNodeProps.load(data);
      Map<String,String> newProps = new HashMap<String,String>();
      newProps.putAll(props.getProperties());
      newProps.put(CONFIGNAME_PROP, confSetName);
      props = new ZkNodeProps(newProps);
    } else {
      props = new ZkNodeProps(CONFIGNAME_PROP, confSetName);
    }
    
    // TODO: we should consider using version
    zkClient.setData(path, ZkStateReader.toJSON(props), true);

  }
  
  /**
   * If in SolrCloud mode, upload config sets for each SolrCore in solr.xml.
   * 
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static void bootstrapConf(SolrZkClient zkClient, Config cfg, String solrHome) throws IOException,
      KeeperException, InterruptedException {
    log.info("bootstraping config into ZooKeeper using solr.xml");
    NodeList nodes = (NodeList)cfg.evaluate("solr/cores/core", XPathConstants.NODESET);

    for (int i=0; i<nodes.getLength(); i++) {
      Node node = nodes.item(i);
      String rawName = DOMUtil.substituteProperty(DOMUtil.getAttr(node, "name", null), new Properties());
      String instanceDir = DOMUtil.getAttr(node, "instanceDir", null);
      File idir = new File(instanceDir);
      if (!idir.isAbsolute()) {
        idir = new File(solrHome, instanceDir);
      }
      String confName = DOMUtil.substituteProperty(DOMUtil.getAttr(node, "collection", null), new Properties());
      if (confName == null) {
        confName = rawName;
      }
      File udir = new File(idir, "conf");
      log.info("Uploading directory " + udir + " with name " + confName + " for SolrCore " + rawName);
      ZkController.uploadConfigDir(zkClient, udir, confName);
    }
  }

  public DistributedQueue getOverseerJobQueue() {
    return overseerJobQueue;
  }

  public DistributedQueue getOverseerCollectionQueue() {
    return overseerCollectionQueue;
  }

}
