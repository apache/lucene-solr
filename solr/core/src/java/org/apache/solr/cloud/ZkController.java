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
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.WaitForState;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.BeforeReconnect;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DefaultConnectionStrategy;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");
  
  private final DistributedQueue overseerJobQueue;
  private final DistributedQueue overseerCollectionQueue;
  
  public static final String CONFIGS_ZKNODE = "/configs";

  public final static String COLLECTION_PARAM_PREFIX="collection.";
  public final static String CONFIGNAME_PROP="configName";

  static class ContextKey {

    private String collection;
    private String coreNodeName;
    
    public ContextKey(String collection, String coreNodeName) {
      this.collection = collection;
      this.coreNodeName = coreNodeName;
    }
    
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((collection == null) ? 0 : collection.hashCode());
      result = prime * result
          + ((coreNodeName == null) ? 0 : coreNodeName.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      ContextKey other = (ContextKey) obj;
      if (collection == null) {
        if (other.collection != null) return false;
      } else if (!collection.equals(other.collection)) return false;
      if (coreNodeName == null) {
        if (other.coreNodeName != null) return false;
      } else if (!coreNodeName.equals(other.coreNodeName)) return false;
      return true;
    }
  }
  private final Map<ContextKey, ElectionContext> electionContexts = Collections.synchronizedMap(new HashMap<ContextKey, ElectionContext>());
  
  private final SolrZkClient zkClient;
  private final ZkCmdExecutor cmdExecutor;
  private final ZkStateReader zkStateReader;

  private final LeaderElector leaderElector;
  
  private final String zkServerAddress;          // example: 127.0.0.1:54062/solr

  private final String localHostPort;      // example: 54065
  private final String localHostContext;   // example: solr
  private final String hostName;           // example: 127.0.0.1
  private final String nodeName;           // example: 127.0.0.1:54065_solr
  private final String baseURL;            // example: http://127.0.0.1:54065/solr


  private LeaderElector overseerElector;
  

  // for now, this can be null in tests, in which case recovery will be inactive, and other features
  // may accept defaults or use mocks rather than pulling things from a CoreContainer
  private CoreContainer cc;

  protected volatile Overseer overseer;

  private int leaderVoteWait;
  
  private boolean genericCoreNodeNames;

  private int clientTimeout;

  private volatile boolean isClosed;

  public ZkController(final CoreContainer cc, String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout, String localHost, String locaHostPort,
      String localHostContext, int leaderVoteWait, boolean genericCoreNodeNames, final CurrentCoreDescriptorProvider registerOnReconnect) throws InterruptedException,
      TimeoutException, IOException {
    if (cc == null) throw new IllegalArgumentException("CoreContainer cannot be null.");
    this.cc = cc;
    this.genericCoreNodeNames = genericCoreNodeNames;
    // be forgiving and strip this off leading/trailing slashes
    // this allows us to support users specifying hostContext="/" in 
    // solr.xml to indicate the root context, instead of hostContext="" 
    // which means the default of "solr"
    localHostContext = trimLeadingAndTrailingSlashes(localHostContext);
    
    this.zkServerAddress = zkServerAddress;
    this.localHostPort = locaHostPort;
    this.localHostContext = localHostContext;
    this.hostName = normalizeHostName(localHost);
    this.nodeName = generateNodeName(this.hostName, 
                                     this.localHostPort, 
                                     this.localHostContext);

    this.leaderVoteWait = leaderVoteWait;
    this.clientTimeout = zkClientTimeout;
    zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout,
        zkClientConnectTimeout, new DefaultConnectionStrategy(),
        // on reconnect, reload cloud info
        new OnReconnect() {
          
          @Override
          public void command() {
            try {
              
              // this is troublesome - we dont want to kill anything the old
              // leader accepted
              // though I guess sync will likely get those updates back? But
              // only if
              // he is involved in the sync, and he certainly may not be
              // ExecutorUtil.shutdownAndAwaitTermination(cc.getCmdDistribExecutor());
              // we need to create all of our lost watches
              
              // seems we dont need to do this again...
              // Overseer.createClientNodes(zkClient, getNodeName());
              
              cc.cancelCoreRecoveries();
              
              registerAllCoresAsDown(registerOnReconnect, false);
              
              ElectionContext context = new OverseerElectionContext(zkClient,
                  overseer, getNodeName());
              
              ElectionContext prevContext = overseerElector.getContext();
              if (prevContext != null) {
                prevContext.cancelElection();
              }

              overseerElector.setup(context);
              overseerElector.joinElection(context, true);
              zkStateReader.createClusterStateWatchersAndUpdate();
              
              // we have to register as live first to pick up docs in the buffer
              createEphemeralLiveNode();
              
              List<CoreDescriptor> descriptors = registerOnReconnect
                  .getCurrentDescriptors();
              // re register all descriptors
              if (descriptors != null) {
                for (CoreDescriptor descriptor : descriptors) {
                  // TODO: we need to think carefully about what happens when it
                  // was
                  // a leader that was expired - as well as what to do about
                  // leaders/overseers
                  // with connection loss
                  try {
                    register(descriptor.getName(), descriptor, true, true);
                  } catch (Exception e) {
                    SolrException.log(log, "Error registering SolrCore", e);
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
          
        }, new BeforeReconnect() {
          
          @Override
          public void command() {
            try {
              ZkController.this.overseer.close();
            } catch (Exception e) {
              log.error("Error trying to stop any Overseer threads", e);
            }
            markAllAsNotLeader(registerOnReconnect);
          }
        });
    
    this.overseerJobQueue = Overseer.getInQueue(zkClient);
    this.overseerCollectionQueue = Overseer.getCollectionQueue(zkClient);
    cmdExecutor = new ZkCmdExecutor(zkClientTimeout);
    leaderElector = new LeaderElector(zkClient);
    zkStateReader = new ZkStateReader(zkClient);
    
    this.baseURL = zkStateReader.getBaseUrlForNodeName(this.nodeName);
    
    init(registerOnReconnect);
  }

  public int getLeaderVoteWait() {
    return leaderVoteWait;
  }

  public void forceOverSeer(){
    try {
      zkClient.delete("/overseer_elect/leader",-1, true);
      log.info("Forcing me to be leader  {} ", getBaseUrl());
      overseerElector.getContext().runLeaderProcess(true, Overseer.STATE_UPDATE_DELAY+100);
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, " Error becoming overseer ",e);

    }

  }

  private void registerAllCoresAsDown(
      final CurrentCoreDescriptorProvider registerOnReconnect, boolean updateLastPublished) {
    List<CoreDescriptor> descriptors = registerOnReconnect
        .getCurrentDescriptors();
    if (isClosed) return;
    if (descriptors != null) {
      // before registering as live, make sure everyone is in a
      // down state
      for (CoreDescriptor descriptor : descriptors) {
        try {
          descriptor.getCloudDescriptor().setLeader(false);
          publish(descriptor, ZkStateReader.DOWN, updateLastPublished);
        } catch (Exception e) {
          if (isClosed) {
            return;
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
          }
          try {
            publish(descriptor, ZkStateReader.DOWN);
          } catch (Exception e2) {
            SolrException.log(log, "", e2);
            continue;
          }
        }
      }
        
      for (CoreDescriptor descriptor : descriptors) {
        // if it looks like we are going to be the leader, we don't
        // want to wait for the following stuff
        CloudDescriptor cloudDesc = descriptor.getCloudDescriptor();
        String collection = cloudDesc.getCollectionName();
        String slice = cloudDesc.getShardId();
        try {
          
          int children = zkStateReader
              .getZkClient()
              .getChildren(
                  ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
                      + "/leader_elect/" + slice + "/election", null, true).size();
          if (children == 0) {
            log.debug("looks like we are going to be the leader for collection {} shard {}", collection, slice);
            continue;
          }

        } catch (NoNodeException e) {
          log.debug("looks like we are going to be the leader for collection {} shard {}", collection, slice);
          continue;
        } catch (InterruptedException e2) {
          Thread.currentThread().interrupt();
        } catch (KeeperException e) {
          log.warn("", e);
          Thread.currentThread().interrupt();
        }

        final String coreZkNodeName = descriptor.getCloudDescriptor().getCoreNodeName();
        try {
          log.debug("calling waitForLeaderToSeeDownState for coreZkNodeName={} collection={} shard={}", new Object[] {coreZkNodeName,  collection, slice});
          waitForLeaderToSeeDownState(descriptor, coreZkNodeName);
        } catch (Exception e) {
          SolrException.log(log, "", e);
          if (isClosed) {
            return;
          }
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }
  
  private void markAllAsNotLeader(
      final CurrentCoreDescriptorProvider registerOnReconnect) {
    List<CoreDescriptor> descriptors = registerOnReconnect
        .getCurrentDescriptors();
    if (descriptors != null) {
      for (CoreDescriptor descriptor : descriptors) {
        descriptor.getCloudDescriptor().setLeader(false);
      }
    }
  }

  /**
   * Closes the underlying ZooKeeper client.
   */
  public void close() {
    this.isClosed = true;
    try {
      for (ElectionContext context : electionContexts.values()) {
        try {
          context.close();
        } catch (Exception e) {
          log.error("Error closing overseer", e);
        }
      }
    } finally {
      try {
        try {
          overseer.close();
        } catch (Exception e) {
          log.error("Error closing overseer", e);
        }
      } finally {
        try {
          try {
            zkStateReader.close();
          } catch (Exception e) {
            log.error("Error closing zkStateReader", e);
          }
        } finally {
          try {
            zkClient.close();
          } catch (Exception e) {
            log.error("Error closing zkClient", e);
          }
        }
      }
    }
    
  }

  /**
   * Returns true if config file exists
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
   * Returns config file data (in bytes)
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

  // normalize host removing any url scheme.
  // input can be null, host, or url_prefix://host
  private String normalizeHostName(String host) throws IOException {

    if (host == null || host.length() == 0) {
      String hostaddress;
      try {
        hostaddress = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        hostaddress = "127.0.0.1"; // cannot resolve system hostname, fall through
      }
      // Re-get the IP again for "127.0.0.1", the other case we trust the hosts
      // file is right.
      if ("127.0.0.1".equals(hostaddress)) {
        Enumeration<NetworkInterface> netInterfaces = null;
        try {
          netInterfaces = NetworkInterface.getNetworkInterfaces();
          while (netInterfaces.hasMoreElements()) {
            NetworkInterface ni = netInterfaces.nextElement();
            Enumeration<InetAddress> ips = ni.getInetAddresses();
            while (ips.hasMoreElements()) {
              InetAddress ip = ips.nextElement();
              if (ip.isSiteLocalAddress()) {
                hostaddress = ip.getHostAddress();
              }
            }
          }
        } catch (Exception e) {
          SolrException.log(log,
              "Error while looking for a better host name than 127.0.0.1", e);
        }
      }
      host = hostaddress;
    } else {
      if(URLUtil.hasScheme(host)) {
        host = URLUtil.removeScheme(host);
      }
    }

    return host;
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

    try {
      boolean createdWatchesAndUpdated = false;
      if (zkClient.exists(ZkStateReader.LIVE_NODES_ZKNODE, true)) {
        zkStateReader.createClusterStateWatchersAndUpdate();
        createdWatchesAndUpdated = true;
        publishAndWaitForDownStates();
      }
      
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
      overseerElector.joinElection(context, false);
      
      if (!createdWatchesAndUpdated) {
        zkStateReader.createClusterStateWatchersAndUpdate();
      }
      
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

  public void publishAndWaitForDownStates() throws KeeperException,
      InterruptedException {
    
    ClusterState clusterState = zkStateReader.getClusterState();
    Set<String> collections = clusterState.getCollections();
    List<String> updatedNodes = new ArrayList<String>();
    for (String collectionName : collections) {
      DocCollection collection = clusterState.getCollection(collectionName);
      Collection<Slice> slices = collection.getSlices();
      for (Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          if (replica.getNodeName().equals(getNodeName())
              && !(replica.getStr(ZkStateReader.STATE_PROP)
                  .equals(ZkStateReader.DOWN))) {
            ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, "state",
                ZkStateReader.STATE_PROP, ZkStateReader.DOWN,
                ZkStateReader.BASE_URL_PROP, getBaseUrl(),
                ZkStateReader.CORE_NAME_PROP,
                replica.getStr(ZkStateReader.CORE_NAME_PROP),
                ZkStateReader.ROLES_PROP,
                replica.getStr(ZkStateReader.ROLES_PROP),
                ZkStateReader.NODE_NAME_PROP, getNodeName(),
                ZkStateReader.SHARD_ID_PROP,
                replica.getStr(ZkStateReader.SHARD_ID_PROP),
                ZkStateReader.COLLECTION_PROP, collectionName,
                ZkStateReader.CORE_NODE_NAME_PROP, replica.getName());
            updatedNodes.add(replica.getStr(ZkStateReader.CORE_NAME_PROP));
            overseerJobQueue.offer(ZkStateReader.toJSON(m));
          }
        }
      }
    }
    
    // now wait till the updates are in our state
    long now = System.currentTimeMillis();
    long timeout = now + 1000 * 30;
    boolean foundStates = false;
    while (System.currentTimeMillis() < timeout) {
      clusterState = zkStateReader.getClusterState();
      collections = clusterState.getCollections();
      for (String collectionName : collections) {
        DocCollection collection = clusterState.getCollection(collectionName);
        Collection<Slice> slices = collection.getSlices();
        for (Slice slice : slices) {
          Collection<Replica> replicas = slice.getReplicas();
          for (Replica replica : replicas) {
            if (replica.getStr(ZkStateReader.STATE_PROP).equals(
                ZkStateReader.DOWN)) {
              updatedNodes.remove(replica.getStr(ZkStateReader.CORE_NAME_PROP));
              
            }
          }
        }
      }
      
      if (updatedNodes.size() == 0) {
        foundStates = true;
        Thread.sleep(1000);
        break;
      }
      Thread.sleep(1000);
    }
    if (!foundStates) {
      log.warn("Timed out waiting to see all nodes published as DOWN in our cluster state.");
    }
    
  }
  
  /**
   * Validates if the chroot exists in zk (or if it is successfully created).
   * Optionally, if create is set to true this method will create the path in
   * case it doesn't exist
   * 
   * @return true if the path exists or is created false if the path doesn't
   *         exist and 'create' = false
   */
  public static boolean checkChrootPath(String zkHost, boolean create)
      throws KeeperException, InterruptedException {
    if (!containsChroot(zkHost)) {
      return true;
    }
    log.info("zkHost includes chroot");
    String chrootPath = zkHost.substring(zkHost.indexOf("/"), zkHost.length());
    SolrZkClient tmpClient = new SolrZkClient(zkHost.substring(0,
        zkHost.indexOf("/")), 60 * 1000);
    boolean exists = tmpClient.exists(chrootPath, true);
    if (!exists && create) {
      tmpClient.makePath(chrootPath, false, true);
      exists = true;
    }
    tmpClient.close();
    return exists;
  }

  /**
   * Validates if zkHost contains a chroot. See http://zookeeper.apache.org/doc/r3.2.2/zookeeperProgrammers.html#ch_zkSessions
   */
  private static boolean containsChroot(String zkHost) {
    return zkHost.contains("/");
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
   * Returns true if the path exists
   */
  public boolean pathExists(String path) throws KeeperException,
      InterruptedException {
    return zkClient.exists(path, true);
  }


  /**
   * Register shard with ZooKeeper.
   * 
   * @return the shardId for the SolrCore
   */
  public String register(String coreName, final CoreDescriptor desc) throws Exception {  
    return register(coreName, desc, false, false);
  }
  

  /**
   * Register shard with ZooKeeper.
   * 
   * @return the shardId for the SolrCore
   */
  public String register(String coreName, final CoreDescriptor desc, boolean recoverReloadedCores, boolean afterExpiration) throws Exception {  
    // pre register has published our down state
    
    final String baseUrl = getBaseUrl();
    
    final CloudDescriptor cloudDesc = desc.getCloudDescriptor();
    final String collection = cloudDesc.getCollectionName();

    final String coreZkNodeName = desc.getCloudDescriptor().getCoreNodeName();
    assert coreZkNodeName != null : "we should have a coreNodeName by now";
    
    String shardId = cloudDesc.getShardId();

    Map<String,Object> props = new HashMap<String,Object>();
 // we only put a subset of props into the leader node
    props.put(ZkStateReader.BASE_URL_PROP, baseUrl);
    props.put(ZkStateReader.CORE_NAME_PROP, coreName);
    props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());


    if (log.isInfoEnabled()) {
        log.info("Register replica - core:" + coreName + " address:"
            + baseUrl + " collection:" + cloudDesc.getCollectionName() + " shard:" + shardId);
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
    

    // in this case, we want to wait for the leader as long as the leader might 
    // wait for a vote, at least - but also long enough that a large cluster has
    // time to get its act together
    String leaderUrl = getLeader(cloudDesc, leaderVoteWait + 600000);
    
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

      UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
      if (!core.isReloaded() && ulog != null) {
        // disable recovery in case shard is in construction state (for shard splits)
        Slice slice = getClusterState().getSlice(collection, shardId);
        if (!Slice.CONSTRUCTION.equals(slice.getState()) || !isLeader) {
          Future<UpdateLog.RecoveryInfo> recoveryFuture = core.getUpdateHandler()
              .getUpdateLog().recoverFromLog();
          if (recoveryFuture != null) {
            recoveryFuture.get(); // NOTE: this could potentially block for
            // minutes or more!
            // TODO: public as recovering in the mean time?
            // TODO: in the future we could do peersync in parallel with recoverFromLog
          } else {
            log.info("No LogReplay needed for core=" + core.getName() + " baseURL=" + baseUrl);
          }
        }
        boolean didRecovery = checkRecovery(coreName, desc, recoverReloadedCores, isLeader, cloudDesc,
            collection, coreZkNodeName, shardId, leaderProps, core, cc);
        if (!didRecovery) {
          publish(desc, ZkStateReader.ACTIVE);
        }
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

  // timeoutms is the timeout for the first call to get the leader - there is then
  // a longer wait to make sure that leader matches our local state
  private String getLeader(final CloudDescriptor cloudDesc, int timeoutms) {
    
    String collection = cloudDesc.getCollectionName();
    String shardId = cloudDesc.getShardId();
    // rather than look in the cluster state file, we go straight to the zknodes
    // here, because on cluster restart there could be stale leader info in the
    // cluster state node that won't be updated for a moment
    String leaderUrl;
    try {
      leaderUrl = getLeaderProps(collection, cloudDesc.getShardId(), timeoutms)
          .getCoreUrl();
      
      // now wait until our currently cloud state contains the latest leader
      String clusterStateLeaderUrl = zkStateReader.getLeaderUrl(collection,
          shardId, timeoutms * 2); // since we found it in zk, we are willing to
                                   // wait a while to find it in state
      int tries = 0;
      while (!leaderUrl.equals(clusterStateLeaderUrl)) {
        if (tries == 60) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "There is conflicting information about the leader of shard: "
                  + cloudDesc.getShardId() + " our state says:"
                  + clusterStateLeaderUrl + " but zookeeper says:" + leaderUrl);
        }
        Thread.sleep(1000);
        tries++;
        clusterStateLeaderUrl = zkStateReader.getLeaderUrl(collection, shardId,
            timeoutms);
        leaderUrl = getLeaderProps(collection, cloudDesc.getShardId(), timeoutms)
            .getCoreUrl();
      }
      
    } catch (Exception e) {
      log.error("Error getting leader from zk", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error getting leader from zk for shard " + shardId, e);
    } 
    return leaderUrl;
  }
  
  /**
   * Get leader props directly from zk nodes.
   */
  public ZkCoreNodeProps getLeaderProps(final String collection,
      final String slice, int timeoutms) throws InterruptedException {
    return getLeaderProps(collection, slice, timeoutms, false);
  }
  
  /**
   * Get leader props directly from zk nodes.
   * 
   * @return leader props
   */
  public ZkCoreNodeProps getLeaderProps(final String collection,
      final String slice, int timeoutms, boolean failImmediatelyOnExpiration) throws InterruptedException {
    int iterCount = timeoutms / 1000;
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
      } catch (SessionExpiredException e) {
        if (failImmediatelyOnExpiration) {
          throw new RuntimeException("Session has expired - could not get leader props", exp);
        }
        exp = e;
        Thread.sleep(1000);
      }  catch (Exception e) {
        exp = e;
        Thread.sleep(1000);
      }
      if (cc.isShutDown()) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "CoreContainer is shutdown");
      }
    }
    throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Could not get leader props", exp);
  }


  private void joinElection(CoreDescriptor cd, boolean afterExpiration) throws InterruptedException, KeeperException, IOException {
    // look for old context - if we find it, cancel it
    String collection = cd.getCloudDescriptor().getCollectionName();
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();
    
    ContextKey contextKey = new ContextKey(collection, coreNodeName);
    
    ElectionContext prevContext = electionContexts.get(contextKey);
    
    if (prevContext != null) {
      prevContext.cancelElection();
    }
    
    String shardId = cd.getCloudDescriptor().getShardId();
    
    Map<String,Object> props = new HashMap<String,Object>();
    // we only put a subset of props into the leader node
    props.put(ZkStateReader.BASE_URL_PROP, getBaseUrl());
    props.put(ZkStateReader.CORE_NAME_PROP, cd.getName());
    props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
    
 
    ZkNodeProps ourProps = new ZkNodeProps(props);

    
    ElectionContext context = new ShardLeaderElectionContext(leaderElector, shardId,
        collection, coreNodeName, ourProps, this, cc);

    leaderElector.setup(context);
    electionContexts.put(contextKey, context);
    leaderElector.joinElection(context, false);
  }


  /**
   * Returns whether or not a recovery was started
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
        core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
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

  public void publish(final CoreDescriptor cd, final String state) throws KeeperException, InterruptedException {
    publish(cd, state, true);
  }
  
  public void publish(final CoreDescriptor cd, final String state, boolean updateLastState) throws KeeperException, InterruptedException {
    publish(cd, state, true, false);
  }
  
  /**
   * Publish core state to overseer.
   */
  public void publish(final CoreDescriptor cd, final String state, boolean updateLastState, boolean forcePublish) throws KeeperException, InterruptedException {
    if (!forcePublish) {
      SolrCore core = cc.getCore(cd.getName());
      if (core == null) {
        return;
      }
      try {
        if (core.isClosed()) {
          return;
        }
      } finally {
        core.close();
      }
    }
    log.info("publishing core={} state={}", cd.getName(), state);
    //System.out.println(Thread.currentThread().getStackTrace()[3]);
    Integer numShards = cd.getCloudDescriptor().getNumShards();
    if (numShards == null) { //XXX sys prop hack
      log.info("numShards not found on descriptor - reading it from system property");
      numShards = Integer.getInteger(ZkStateReader.NUM_SHARDS_PROP);
    }
    
    assert cd.getCloudDescriptor().getCollectionName() != null && cd.getCloudDescriptor()
        .getCollectionName().length() > 0;
    
    String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();
    //assert cd.getCloudDescriptor().getShardId() != null;
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, "state", 
        ZkStateReader.STATE_PROP, state, 
        ZkStateReader.BASE_URL_PROP, getBaseUrl(), 
        ZkStateReader.CORE_NAME_PROP, cd.getName(),
        ZkStateReader.ROLES_PROP, cd.getCloudDescriptor().getRoles(),
        ZkStateReader.NODE_NAME_PROP, getNodeName(),
        ZkStateReader.SHARD_ID_PROP, cd.getCloudDescriptor().getShardId(),
        ZkStateReader.SHARD_RANGE_PROP, cd.getCloudDescriptor().getShardRange(),
        ZkStateReader.SHARD_STATE_PROP, cd.getCloudDescriptor().getShardState(),
        ZkStateReader.SHARD_PARENT_PROP, cd.getCloudDescriptor().getShardParent(),
        ZkStateReader.COLLECTION_PROP, cd.getCloudDescriptor()
            .getCollectionName(),
        ZkStateReader.NUM_SHARDS_PROP, numShards != null ? numShards.toString()
            : null,
        ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName != null ? coreNodeName
            : null);
    if (updateLastState) {
      cd.getCloudDescriptor().lastPublished = state;
    }
    overseerJobQueue.offer(ZkStateReader.toJSON(m));
  }

  private boolean needsToBeAssignedShardId(final CoreDescriptor desc,
      final ClusterState state, final String coreNodeName) {

    final CloudDescriptor cloudDesc = desc.getCloudDescriptor();
    
    final String shardId = state.getShardId(getBaseUrl(), desc.getName());

    if (shardId != null) {
      cloudDesc.setShardId(shardId);
      return false;
    }
    return true;
  }

  public void unregister(String coreName, CoreDescriptor cd)
      throws InterruptedException, KeeperException {
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();
    final String collection = cd.getCloudDescriptor().getCollectionName();
    assert collection != null;
    ElectionContext context = electionContexts.remove(new ContextKey(collection, coreNodeName));
    
    if (context != null) {
      context.cancelElection();
    }
    
    CloudDescriptor cloudDescriptor = cd.getCloudDescriptor();
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        Overseer.DELETECORE, ZkStateReader.CORE_NAME_PROP, coreName,
        ZkStateReader.NODE_NAME_PROP, getNodeName(),
        ZkStateReader.COLLECTION_PROP, cloudDescriptor.getCollectionName(),
        ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName);
    overseerJobQueue.offer(ZkStateReader.toJSON(m));
  }
  
  public void createCollection(String collection) throws KeeperException,
      InterruptedException {
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        "createcollection", ZkStateReader.NODE_NAME_PROP, getNodeName(),
        ZkStateReader.COLLECTION_PROP, collection);
    overseerJobQueue.offer(ZkStateReader.toJSON(m));
  }

  public void uploadToZK(File dir, String zkPath) throws IOException, KeeperException, InterruptedException {
    uploadToZK(zkClient, dir, zkPath);
  }
  
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
          Map<String,Object> collectionProps = new HashMap<String,Object>();

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
            if (!collectionProps.containsKey(CONFIGNAME_PROP)) {
              // TODO: getting the configName from the collectionPath should fail since we already know it doesn't exist?
              getConfName(collection, collectionPath, collectionProps);
            }
            
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

          collectionProps.remove(ZkStateReader.NUM_SHARDS_PROP);  // we don't put numShards in the collections properties

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
      Map<String,Object> collectionProps) throws KeeperException,
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

  private void doGetShardIdAndNodeNameProcess(CoreDescriptor cd) {
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();

    if (coreNodeName != null) {
      waitForShardId(cd);
    } else {
      // if no explicit coreNodeName, we want to match by base url and core name
      waitForCoreNodeName(cd);
      waitForShardId(cd);
    }
  }

  private void waitForCoreNodeName(CoreDescriptor descriptor) {
    int retryCount = 320;
    log.info("look for our core node name");
    while (retryCount-- > 0) {
      Map<String,Slice> slicesMap = zkStateReader.getClusterState()
          .getSlicesMap(descriptor.getCloudDescriptor().getCollectionName());
      if (slicesMap != null) {
        
        for (Slice slice : slicesMap.values()) {
          for (Replica replica : slice.getReplicas()) {
            // TODO: for really large clusters, we could 'index' on this
            
            String baseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
            String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);
            
            String msgBaseUrl = getBaseUrl();
            String msgCore = descriptor.getName();

            if (baseUrl.equals(msgBaseUrl) && core.equals(msgCore)) {
              descriptor.getCloudDescriptor()
                  .setCoreNodeName(replica.getName());
              return;
            }
          }
        }
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void waitForShardId(CoreDescriptor cd) {
    log.info("waiting to find shard id in clusterstate for " + cd.getName());
    int retryCount = 320;
    while (retryCount-- > 0) {
      final String shardId = zkStateReader.getClusterState().getShardId(getBaseUrl(), cd.getName());
      if (shardId != null) {
        cd.getCloudDescriptor().setShardId(shardId);
        return;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    
    throw new SolrException(ErrorCode.SERVER_ERROR,
        "Could not get shard id for core: " + cd.getName());
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
        FileUtils.writeByteArrayToFile(new File(dir, file), data);
      } else {
        downloadFromZK(zkClient, zkPath + "/" + file, new File(dir, file));
      }
    }
  }
  
  
  public String getCoreNodeName(CoreDescriptor descriptor){
    String coreNodeName = descriptor.getCloudDescriptor().getCoreNodeName();
    if (coreNodeName == null && !genericCoreNodeNames) {
      // it's the default
      return getNodeName() + "_" + descriptor.getName();
    }
    
    return coreNodeName;
  }
  
  public static void uploadConfigDir(SolrZkClient zkClient, File dir, String configName) throws IOException, KeeperException, InterruptedException {
    uploadToZK(zkClient, dir, ZkController.CONFIGS_ZKNODE + "/" + configName);
  }
  
  public static void downloadConfigDir(SolrZkClient zkClient, String configName, File dir) throws IOException, KeeperException, InterruptedException {
    downloadFromZK(zkClient, ZkController.CONFIGS_ZKNODE + "/" + configName, dir);
  }

  public void preRegister(CoreDescriptor cd ) {

    String coreNodeName = getCoreNodeName(cd);
    // before becoming available, make sure we are not live and active
    // this also gets us our assigned shard id if it was not specified
    try {
      CloudDescriptor cloudDesc = cd.getCloudDescriptor();


      // make sure the node name is set on the descriptor
      if (cloudDesc.getCoreNodeName() == null) {
        cloudDesc.setCoreNodeName(coreNodeName);
      }

      publish(cd, ZkStateReader.DOWN, false, true);
    } catch (KeeperException e) {
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }
    
    if (cd.getCloudDescriptor().getShardId() == null && needsToBeAssignedShardId(cd, zkStateReader.getClusterState(), coreNodeName)) {
      doGetShardIdAndNodeNameProcess(cd);
    } else {
      // still wait till we see us in local state
      doGetShardIdAndNodeNameProcess(cd);
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
        if (isClosed) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
              "We have been closed");
        }
        
        // go straight to zk, not the cloud state - we must have current info
        leaderProps = getLeaderProps(collection, shard, 30000);
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
      try {
        server.setConnectionTimeout(15000);
        server.setSoTimeout(120000);
        WaitForState prepCmd = new WaitForState();
        prepCmd.setCoreName(leaderCoreName);
        prepCmd.setNodeName(getNodeName());
        prepCmd.setCoreNodeName(coreZkNodeName);
        prepCmd.setState(ZkStateReader.DOWN);
        
        // let's retry a couple times - perhaps the leader just went down,
        // or perhaps he is just not quite ready for us yet
        retries = 6;
        for (int i = 0; i < retries; i++) {
          if (isClosed) {
            throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
                "We have been closed");
          }
          try {
            server.request(prepCmd);
            break;
          } catch (Exception e) {
            SolrException.log(log,
                "There was a problem making a request to the leader", e);
            try {
              Thread.sleep(2000);
            } catch (InterruptedException e1) {
              Thread.currentThread().interrupt();
            }
            if (i == retries - 1) {
              throw new SolrException(ErrorCode.SERVER_ERROR,
                  "There was a problem making a request to the leader");
            }
          }
        }
      } finally {
        server.shutdown();
      }
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
      Map<String,Object> newProps = new HashMap<String,Object>();
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
   */
  public static void bootstrapConf(SolrZkClient zkClient, CoreContainer cc, String solrHome) throws IOException,
      KeeperException, InterruptedException {

    //List<String> allCoreNames = cfg.getAllCoreNames();
    List<CoreDescriptor> cds = cc.getCoresLocator().discover(cc);
    
    log.info("bootstrapping config for " + cds.size() + " cores into ZooKeeper using solr.xml from " + solrHome);

    for (CoreDescriptor cd : cds) {
      String coreName = cd.getName();
      String confName = cd.getCollectionName();
      if (StringUtils.isEmpty(confName))
        confName = coreName;
      String instanceDir = cd.getInstanceDir();
      File udir = new File(instanceDir, "conf");
      log.info("Uploading directory " + udir + " with name " + confName + " for SolrCore " + coreName);
      ZkController.uploadConfigDir(zkClient, udir, confName);
    }
  }

  public DistributedQueue getOverseerJobQueue() {
    return overseerJobQueue;
  }

  public DistributedQueue getOverseerCollectionQueue() {
    return overseerCollectionQueue;
  }
  
  public int getClientTimeout() {
    return clientTimeout;
  }

  public Overseer getOverseer() {
    return overseer;
  }

  public LeaderElector getOverseerElector() {
    return overseerElector;
  }
  
  /**
   * Returns the nodeName that should be used based on the specified properties.
   *
   * @param hostName - must not be null or the empty string
   * @param hostPort - must consist only of digits, must not be null or the empty string
   * @param hostContext - should not begin or end with a slash (leading/trailin slashes will be ignored), must not be null, may be the empty string to denote the root context
   * @lucene.experimental
   * @see ZkStateReader#getBaseUrlForNodeName
   */
  static String generateNodeName(final String hostName,
                                 final String hostPort,
                                 final String hostContext) {
    try {
      return hostName + ':' + hostPort + '_' + 
        URLEncoder.encode(trimLeadingAndTrailingSlashes(hostContext), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("JVM Does not seem to support UTF-8", e);
    }
  }
  
  /**
   * Utility method for trimming and leading and/or trailing slashes from 
   * it's input.  May return the empty string.  May return null if and only 
   * if the input is null.
   */
  public static String trimLeadingAndTrailingSlashes(final String in) {
    if (null == in) return in;
    
    String out = in;
    if (out.startsWith("/")) {
      out = out.substring(1);
    }
    if (out.endsWith("/")) {
      out = out.substring(0,out.length()-1);
    }
    return out;
  }

  public void rejoinOverseerElection() {
    try {
      overseerElector.retryElection();
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to rejoin election", e);
    }

  }

}
