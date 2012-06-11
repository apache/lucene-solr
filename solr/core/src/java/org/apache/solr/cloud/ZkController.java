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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPathConstants;

import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.WaitForState;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.CoreState;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
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
import org.apache.solr.core.SolrResourceLoader;
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
import org.xml.sax.InputSource;

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
  
  // package private for tests

  static final String CONFIGS_ZKNODE = "/configs";

  public final static String COLLECTION_PARAM_PREFIX="collection.";
  public final static String CONFIGNAME_PROP="configName";

  private Map<String, CoreState> coreStates = new HashMap<String, CoreState>();   // key is the local core name
  private long coreStatesVersion; // bumped by 1 each time we serialize coreStates... sync on  coreStates
  private long coreStatesPublishedVersion; // last version published to ZK... sync on coreStatesPublishLock
  private Object coreStatesPublishLock = new Object(); // only publish one at a time

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
  

  // this can be null in which case recovery will be inactive
  private CoreContainer cc;

  /**
   * Bootstraps the current configs for all collections in solr.xml.
   * Takes two params - the zkhost to connect to and the solrhome location
   * to find solr.xml.
   *
   * If you also pass a solrPort, it will be used to start
   * an embedded zk useful for single machine, multi node tests.
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    // start up a tmp zk server first
    String zkServerAddress = args[0];
    
    String solrHome = args[1];
   
    String solrPort = null;
    if (args.length > 2) {
      solrPort = args[2];
    }
    

    SolrZkServer zkServer = null;
    if (solrPort != null) {
      zkServer = new SolrZkServer("true", null, solrHome + "/zoo_data", solrHome, solrPort);
      zkServer.parseConfig();
      zkServer.start();
    }
    
    SolrZkClient zkClient = new SolrZkClient(zkServerAddress, 15000, 5000,
        new OnReconnect() {
          @Override
          public void command() {
          }});
    
    SolrResourceLoader loader = new SolrResourceLoader(solrHome);
    solrHome = loader.getInstanceDir();
    
    InputSource cfgis = new InputSource(new File(solrHome, "solr.xml").toURI().toASCIIString());
    Config cfg = new Config(loader, null, cfgis , null, false);
    bootstrapConf(zkClient, cfg, solrHome);
    if (solrPort != null) {
      zkServer.stop();
    }
  }

  /**
   * @param cc if null, recovery will not be enabled
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
  public ZkController(CoreContainer cc, String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout, String localHost, String locaHostPort,
      String localHostContext, final CurrentCoreDescriptorProvider registerOnReconnect) throws InterruptedException,
      TimeoutException, IOException {
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

    zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout, zkClientConnectTimeout,
        // on reconnect, reload cloud info
        new OnReconnect() {

          public void command() {
            try {
              // we need to create all of our lost watches
              
              // seems we dont need to do this again...
              //Overseer.createClientNodes(zkClient, getNodeName());

              ElectionContext context = new OverseerElectionContext(getNodeName(), zkClient, zkStateReader);
              overseerElector.joinElection(context);
              zkStateReader.createClusterStateWatchersAndUpdate();
              
              List<CoreDescriptor> descriptors = registerOnReconnect
                  .getCurrentDescriptors();
              if (descriptors != null) {
                // before registering as live, make sure everyone is in a
                // down state
                for (CoreDescriptor descriptor : descriptors) {
                  final String coreZkNodeName = getNodeName() + "_"
                      + descriptor.getName();
                  try {
                    publishAsDown(getBaseUrl(), descriptor, coreZkNodeName,
                        descriptor.getName());
                    waitForLeaderToSeeDownState(descriptor, coreZkNodeName);
                  } catch (Exception e) {
                    SolrException.log(log, "", e);
                  }
                }
              }
              

              // we have to register as live first to pick up docs in the buffer
              createEphemeralLiveNode();
              
              // re register all descriptors
              if (descriptors != null) {
                for (CoreDescriptor descriptor : descriptors) {
                  // TODO: we need to think carefully about what happens when it was
                  // a leader that was expired - as well as what to do about leaders/overseers
                  // with connection loss
                  register(descriptor.getName(), descriptor, true);
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
    cmdExecutor = new ZkCmdExecutor();
    leaderElector = new LeaderElector(zkClient);
    zkStateReader = new ZkStateReader(zkClient);
    init();
  }

  /**
   * Closes the underlying ZooKeeper client.
   */
  public void close() {
    try {
      zkClient.close();
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.warn("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    }
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
  public CloudState getCloudState() {
    return zkStateReader.getCloudState();
  }

  /** @return the CoreState for the core, which may not yet be visible to ZooKeeper or other nodes in the cluster */
  public CoreState getCoreState(String coreName) {
    synchronized (coreStates) {
      return coreStates.get(coreName);
    }
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

  private void init() {

    try {
      // makes nodes zkNode
      cmdExecutor.ensureExists(ZkStateReader.LIVE_NODES_ZKNODE, zkClient);
      
      Overseer.createClientNodes(zkClient, getNodeName());
      createEphemeralLiveNode();
      cmdExecutor.ensureExists(ZkStateReader.COLLECTIONS_ZKNODE, zkClient);

      syncNodeState();

      overseerElector = new LeaderElector(zkClient);
      ElectionContext context = new OverseerElectionContext(getNodeName(), zkClient, zkStateReader);
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
  
  /*
   * sync internal state with zk on startup
   */
  private void syncNodeState() throws KeeperException, InterruptedException {
    log.debug("Syncing internal state with zk. Current: " + coreStates);
    final String path = Overseer.STATES_NODE + "/" + getNodeName();

    final byte[] data = zkClient.getData(path, null, null, true);

    if (data != null) {
      CoreState[] states = CoreState.load(data);
      synchronized (coreStates) {
        coreStates.clear();    // TODO: should we do this?
        for(CoreState coreState: states) {
          coreStates.put(coreState.getCoreName(), coreState);
        }
      }
    }
    log.debug("after sync: " + coreStates);
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
   * @throws IOException 
   */
  public String readConfigName(String collection) throws KeeperException,
      InterruptedException, IOException {

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
    return register(coreName, desc, false);
  }
  

  /**
   * Register shard with ZooKeeper.
   * 
   * @param coreName
   * @param desc
   * @param recoverReloadedCores
   * @return the shardId for the SolrCore
   * @throws Exception
   */
  public String register(String coreName, final CoreDescriptor desc, boolean recoverReloadedCores) throws Exception {  
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
      joinElection(desc);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (KeeperException e) {
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (IOException e) {
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }
    
    // rather than look in the cluster state file, we go straight to the zknodes
    // here, because on cluster restart there could be stale leader info in the
    // cluster state node that won't be updated for a moment
    String leaderUrl = getLeaderProps(collection, cloudDesc.getShardId()).getCoreUrl();
    
    // now wait until our currently cloud state contains the latest leader
    String cloudStateLeader = zkStateReader.getLeaderUrl(collection, cloudDesc.getShardId(), 30000);
    int tries = 0;
    while (!leaderUrl.equals(cloudStateLeader)) {
      if (tries == 60) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "There is conflicting information about the leader of shard: "
                + cloudDesc.getShardId());
      }
      Thread.sleep(1000);
      tries++;
      cloudStateLeader = zkStateReader.getLeaderUrl(collection,
          cloudDesc.getShardId(), 30000);
    }
    
    String ourUrl = ZkCoreNodeProps.getCoreUrl(baseUrl, coreName);
    log.info("We are " + ourUrl + " and leader is " + leaderUrl);
    boolean isLeader = leaderUrl.equals(ourUrl);
    

    SolrCore core = null;
    if (cc != null) { // CoreContainer only null in tests
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
          publishAsActive(baseUrl, desc, coreZkNodeName, coreName);
        }
      } finally {
        if (core != null) {
          core.close();
        }
      }
    } else {
      publishAsActive(baseUrl, desc, coreZkNodeName, coreName);
    }
    
    // make sure we have an update cluster state right away
    zkStateReader.updateCloudState(true);

    return shardId;
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
  private ZkCoreNodeProps getLeaderProps(final String collection, final String slice)
      throws KeeperException, InterruptedException {
    int iterCount = 60;
    while (iterCount-- > 0)
      try {
        byte[] data = zkClient.getData(
            ZkStateReader.getShardLeadersPath(collection, slice), null, null,
            true);
        ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(
            ZkNodeProps.load(data));
        return leaderProps;
      } catch (NoNodeException e) {
        Thread.sleep(500);
      }
    throw new RuntimeException("Could not get leader props");
  }


  private void joinElection(CoreDescriptor cd) throws InterruptedException, KeeperException, IOException {
    
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
        collection, coreZkNodeName, ourProps, this, cc);

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
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   * @throws ExecutionException
   */
  private boolean checkRecovery(String coreName, final CoreDescriptor desc,
      boolean recoverReloadedCores, final boolean isLeader,
      final CloudDescriptor cloudDesc, final String collection,
      final String shardZkNodeName, String shardId, ZkNodeProps leaderProps,
      SolrCore core, CoreContainer cc) throws InterruptedException,
      KeeperException, IOException, ExecutionException {
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


  void publishAsActive(String shardUrl,
      final CoreDescriptor cd, String shardZkNodeName, String coreName) {
    Map<String,String> finalProps = new HashMap<String,String>();
    finalProps.put(ZkStateReader.BASE_URL_PROP, shardUrl);
    finalProps.put(ZkStateReader.CORE_NAME_PROP, coreName);
    finalProps.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
    finalProps.put(ZkStateReader.STATE_PROP, ZkStateReader.ACTIVE);

    publishState(cd, shardZkNodeName, coreName, finalProps);
  }

  public void publish(CoreDescriptor cd, String state) {
    Map<String,String> finalProps = new HashMap<String,String>();
    finalProps.put(ZkStateReader.BASE_URL_PROP, getBaseUrl());
    finalProps.put(ZkStateReader.CORE_NAME_PROP, cd.getName());
    finalProps.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
    finalProps.put(ZkStateReader.STATE_PROP, state);
    publishState(cd, getNodeName() + "_" + cd.getName(),
        cd.getName(), finalProps);
  }
  
  void publishAsDown(String baseUrl,
      final CoreDescriptor cd, String shardZkNodeName, String coreName) {
    Map<String,String> finalProps = new HashMap<String,String>();
    finalProps.put(ZkStateReader.BASE_URL_PROP, baseUrl);
    finalProps.put(ZkStateReader.CORE_NAME_PROP, coreName);
    finalProps.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
    finalProps.put(ZkStateReader.STATE_PROP, ZkStateReader.DOWN);
 
    publishState(cd, shardZkNodeName, coreName, finalProps);
  }
  
  void publishAsRecoveryFailed(String baseUrl,
      final CoreDescriptor cd, String shardZkNodeName, String coreName) {
    Map<String,String> finalProps = new HashMap<String,String>();
    finalProps.put(ZkStateReader.BASE_URL_PROP, baseUrl);
    finalProps.put(ZkStateReader.CORE_NAME_PROP, coreName);
    finalProps.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
    finalProps.put(ZkStateReader.STATE_PROP, ZkStateReader.RECOVERY_FAILED);
    publishState(cd, shardZkNodeName, coreName, finalProps);
  }


  private boolean needsToBeAssignedShardId(final CoreDescriptor desc,
      final CloudState state, final String shardZkNodeName) {

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
    synchronized (coreStates) {
      coreStates.remove(coreName);
    }
    publishState();
    final String zkNodeName = getNodeName() + "_" + coreName;
    ElectionContext context = electionContexts.remove(zkNodeName);
    if (context != null) {
      context.cancelElection();
    }
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

  public void createCollectionZkNode(CloudDescriptor cd) throws KeeperException, InterruptedException, IOException {
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
         
          // ping that there is a new collection
          zkClient.setData(ZkStateReader.COLLECTIONS_ZKNODE, (byte[])null, true);
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

  
  private void publishState(CoreDescriptor cd, String shardZkNodeName, String coreName,
      Map<String,String> props) {
    CloudDescriptor cloudDesc = cd.getCloudDescriptor();
    if (cloudDesc.getRoles() != null) {
      props.put(ZkStateReader.ROLES_PROP, cloudDesc.getRoles());
    }
    
    if (cloudDesc.getShardId() == null && needsToBeAssignedShardId(cd, zkStateReader.getCloudState(), shardZkNodeName)) {
      // publish with no shard id so we are assigned one, and then look for it
      doPublish(shardZkNodeName, coreName, props, cloudDesc);
      String shardId;
      try {
        shardId = doGetShardIdProcess(coreName, cloudDesc);
      } catch (InterruptedException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted");
      }
      cloudDesc.setShardId(shardId);
    }
   
    
    if (!props.containsKey(ZkStateReader.SHARD_ID_PROP) && cloudDesc.getShardId() != null) {
      props.put(ZkStateReader.SHARD_ID_PROP, cloudDesc.getShardId());
    }
    
    doPublish(shardZkNodeName, coreName, props, cloudDesc);
  }


  private void doPublish(String shardZkNodeName, String coreName,
      Map<String,String> props, CloudDescriptor cloudDesc) {
    Integer numShards = cloudDesc.getNumShards();
    if (numShards == null) {
      numShards = Integer.getInteger(ZkStateReader.NUM_SHARDS_PROP);
    }
    CoreState coreState = new CoreState(coreName,
        cloudDesc.getCollectionName(), props, numShards);
    
    synchronized (coreStates) {
      coreStates.put(coreName, coreState);
    }
    
    publishState();
  }
  
  private void publishState() {
    final String nodePath = "/node_states/" + getNodeName();

    long version;
    byte[] coreStatesData;
    synchronized (coreStates) {
      version = ++coreStatesVersion;
      coreStatesData = ZkStateReader.toJSON(coreStates.values());
    }

    // if multiple threads are trying to publish state, make sure that we never write
    // an older version after a newer version.
    synchronized (coreStatesPublishLock) {
      try {
        if (version < coreStatesPublishedVersion) {
          log.info("Another thread already published a newer coreStates: ours="+version + " lastPublished=" + coreStatesPublishedVersion);
        } else {
          zkClient.setData(nodePath, coreStatesData, true);
          coreStatesPublishedVersion = version;  // put it after so it won't be set if there's an exception
        }
      } catch (KeeperException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "could not publish node state", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "could not publish node state", e);
      }
    }
  }

  private String doGetShardIdProcess(String coreName, CloudDescriptor descriptor)
      throws InterruptedException {
    final String shardZkNodeName = getNodeName() + "_" + coreName;
    int retryCount = 120;
    while (retryCount-- > 0) {
      final String shardId = zkStateReader.getCloudState().getShardId(
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
  
  public static void uploadConfigDir(SolrZkClient zkClient, File dir, String configName) throws IOException, KeeperException, InterruptedException {
    uploadToZK(zkClient, dir, ZkController.CONFIGS_ZKNODE + "/" + configName);
  }

  public void preRegister(CoreDescriptor cd) {
    // before becoming available, make sure we are not live and active
    // this also gets us our assigned shard id if it was not specified
    publish(cd, ZkStateReader.DOWN); 
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
  
  /**
   * If in SolrCloud mode, upload config sets for each SolrCore in solr.xml.
   * 
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static void bootstrapConf(SolrZkClient zkClient, Config cfg, String solrHome) throws IOException,
      KeeperException, InterruptedException {
    
    NodeList nodes = (NodeList)cfg.evaluate("solr/cores/core", XPathConstants.NODESET);

    for (int i=0; i<nodes.getLength(); i++) {
      Node node = nodes.item(i);
      String rawName = DOMUtil.getAttr(node, "name", null);
      String instanceDir = DOMUtil.getAttr(node, "instanceDir", null);
      File idir = new File(instanceDir);
      if (!idir.isAbsolute()) {
        idir = new File(solrHome, instanceDir);
      }
      String confName = DOMUtil.getAttr(node, "collection", null);
      if (confName == null) {
        confName = rawName;
      }

      ZkController.uploadConfigDir(zkClient, new File(idir, "conf"), confName);
    }
  }

}
