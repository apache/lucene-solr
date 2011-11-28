package org.apache.solr.cloud;

/**
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
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


  private final static Pattern URL_POST = Pattern.compile("https?://(.*)");
  private final static Pattern URL_PREFIX = Pattern.compile("(https?://).*");

  
  // package private for tests

  static final String CONFIGS_ZKNODE = "/configs";

  public final static String COLLECTION_PARAM_PREFIX="collection.";
  public final static String CONFIGNAME_PROP="configName";

  private final HashMap<String, CoreState> coreStates = new HashMap<String, CoreState>();
  private SolrZkClient zkClient;
  
  private ZkStateReader zkStateReader;

  private SliceLeaderElector leaderElector;
  
  private String zkServerAddress;

  private String localHostPort;
  private String localHostContext;
  private String localHostName;
  private String localHost;

  private String hostName;

  private OverseerElector overseerElector;
  
  private int numShards;

  private Map<String, CoreAssignment> assignments = new HashMap<String, CoreAssignment>();

  public static void main(String[] args) throws Exception {
    // start up a tmp zk server first
    SolrZkServer zkServer = new SolrZkServer("true", null, "example/solr", args[2]);
    zkServer.parseConfig();
    zkServer.start();
    Thread.sleep(5000);
    ZkController zkController = new ZkController(args[0], 15000, 5000, args[1], args[2], args[3], -1, new CurrentCoreDescriptorProvider() {
      
      @Override
      public List<CoreDescriptor> getCurrentDescriptors() {
        // do nothing
        return null;
      }
    });
    
    zkController.uploadConfigDir(new File(args[4]), args[5]);
    
    zkServer.stop();
  }


  /**
   * @param coreContainer
   * @param zkServerAddress
   * @param zkClientTimeout
   * @param zkClientConnectTimeout
   * @param localHost
   * @param locaHostPort
   * @param localHostContext
   * @param numShards 
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws IOException
   */
  public ZkController(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout, String localHost, String locaHostPort,
      String localHostContext, int numShards, final CurrentCoreDescriptorProvider registerOnReconnect) throws InterruptedException,
      TimeoutException, IOException {
 
    this.zkServerAddress = zkServerAddress;
    this.localHostPort = locaHostPort;
    this.localHostContext = localHostContext;
    this.localHost = localHost;
    this.numShards = numShards;

    zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout, zkClientConnectTimeout,
        // on reconnect, reload cloud info
        new OnReconnect() {

          public void command() {
            try {
              // we need to create all of our lost watches
              createEphemeralLiveNode();
              zkStateReader.createClusterStateWatchersAndUpdate();
              
              // re register all descriptors
              List<CoreDescriptor> descriptors = registerOnReconnect
                  .getCurrentDescriptors();
              if (descriptors != null) {
                for (CoreDescriptor descriptor : descriptors) {
                  // nocommit: non reloaded cores will try and
                  // recover - reloaded cores will not
                  register(descriptor.getName(), descriptor);
                }
              }

            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            } catch (Exception e) {
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            }

          }
        });
    
    leaderElector = new SliceLeaderElector(zkClient);
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
    Stat stat = zkClient.exists(CONFIGS_ZKNODE + "/" + collection + "/" + fileName, null);
    return stat != null;
  }

  /**
   * @return information about the cluster from ZooKeeper
   */
  public CloudState getCloudState() {
    return zkStateReader.getCloudState();
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
    byte[] bytes = zkClient.getData(zkPath, null, null);
    if (bytes == null) {
      log.error("Config file contains no data:" + zkPath);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "Config file contains no data:" + zkPath);
    }
    
    return bytes;
  }

  // TODO: consider how this is done
  private String getHostAddress() throws IOException {

    if (localHost == null) {
      localHost = "http://" + InetAddress.getLocalHost().getHostName();
    } else {
      Matcher m = URL_PREFIX.matcher(localHost);
      if (m.matches()) {
        String prefix = m.group(1);
        localHost = prefix + localHost;
      } else {
        localHost = "http://" + localHost;
      }
    }

    return localHost;
  }
  
  public String getHostName() {
    return hostName;
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
      localHostName = getHostAddress();
      Matcher m = URL_POST.matcher(localHostName);

      if (m.matches()) {
        hostName = m.group(1);
      } else {
        log.error("Unrecognized host:" + localHostName);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "Unrecognized host:" + localHostName);
      }
      
      // makes nodes zkNode
      try {
        zkClient.makePath(ZkStateReader.LIVE_NODES_ZKNODE);
      } catch (KeeperException e) {
        // its okay if another beats us creating the node
        if (e.code() != KeeperException.Code.NODEEXISTS) {
          log.error("", e);
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        }
      }
      
      createEphemeralLiveNode();
      setUpCollectionsNode();
      setupOverseerNodes();
      
      byte[] assignments = zkClient.getData(getAssignmentsNode(), new Watcher(){

        @Override
        public void process(WatchedEvent event) {
          //read latest assignments
          try {
            byte[] assignments = zkClient.getData(getAssignmentsNode(), this, null);
            try {
              processAssignmentsUpdate(assignments);
            } catch (IOException e) {
              log.error("Assignment data was malformed", e);
              return;
            }
          } catch (KeeperException e) {
            log.warn("Could not read node assignments.", e);
            return;
          } catch (InterruptedException e) {
            // Restore the interrupted status
            Thread.currentThread().interrupt();
            log.warn("Could not read node assignments.", e);
            return;
          }
          
        }
        
      }, null);

      processAssignmentsUpdate(assignments);
      
      overseerElector = new OverseerElector(zkClient);
      overseerElector.setupForElection();
      overseerElector.joinElection(getNodeName());
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


  private String getAssignmentsNode() {
    return "/node_assignments/" + getNodeName();
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
        zkClient.delete(nodePath, -1);
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
      zkClient.makePath(nodePath, CreateMode.EPHEMERAL);
    } catch (KeeperException e) {
      // its okay if the node already exists
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        throw e;
      }
    }    
  }
  
  public String getNodeName() {
    return hostName + ":" + localHostPort + "_"+ localHostContext;
  }

  /**
   * @param path
   * @return true if the path exists
   * @throws KeeperException
   * @throws InterruptedException
   */
  public boolean pathExists(String path) throws KeeperException,
      InterruptedException {
    return zkClient.exists(path);
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
    byte[] data = zkClient.getData(path, null, null);
    
    if(data != null) {
      ZkNodeProps props = ZkNodeProps.load(data);
      configName = props.get(CONFIGNAME_PROP);
    }
    
    if (configName != null && !zkClient.exists(CONFIGS_ZKNODE + "/" + configName)) {
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
   * @param cloudDesc
   * @return
   * @throws Exception 
   */
  public String register(String coreName, final CoreDescriptor desc) throws Exception {
    // nocommit: TODO: on core reload we don't want to do recovery or anything...
    
    String shardUrl = localHostName + ":" + localHostPort + "/" + localHostContext
        + "/" + coreName;
    
    CloudDescriptor cloudDesc = desc.getCloudDescriptor();
    final String collection = cloudDesc.getCollectionName();
    

    log.info("Attempting to update " + ZkStateReader.CLUSTER_STATE + " version "
        + null);
    CloudState state = CloudState.load(zkClient, zkStateReader.getCloudState().getLiveNodes());
    String shardZkNodeName = getNodeName() + "_" + coreName;
    
    // checkRecovery will have updated the shardId if it already exists...
    String shardId = cloudDesc.getShardId();

    Map<String,String> props = new HashMap<String,String>();
    props.put(ZkStateReader.URL_PROP, shardUrl);
    props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
    props.put(ZkStateReader.ROLES_PROP, cloudDesc.getRoles());
    props.put(ZkStateReader.STATE_PROP, ZkStateReader.RECOVERING);
    if(shardId!=null) {
      props.put("shard_id", shardId);
    }

    if (shardId == null && getShardId(desc, state, shardZkNodeName)) {
      publishState(cloudDesc, shardZkNodeName, props); //need to publish state to get overseer assigned id
      shardId = doGetShardIdProcess(coreName, cloudDesc);
      cloudDesc.setShardId(shardId);
      props.put("shard_id", shardId);
    } else {
      publishState(cloudDesc, shardZkNodeName, props);
    }

    if (log.isInfoEnabled()) {
        log.info("Register shard - core:" + coreName + " address:"
            + shardUrl + "shardId:" + shardId);
      }
    
    leaderElector.setupForSlice(shardId, collection);
    
    // leader election
    ZkNodeProps zkProps = new ZkNodeProps(props);
    doLeaderElectionProcess(shardId, collection, shardZkNodeName, zkProps);
    
    // should be fine if we do this rather than read from cloud state since it's rare?
    String leaderUrl = zkStateReader.getLeader(collection, cloudDesc.getShardId());
    
    boolean iamleader = false;
    boolean doRecovery = true;
    if (leaderUrl.equals(shardUrl)) {
      iamleader = true;
      doRecovery = false;
    } else {
      CoreContainer cc = desc.getCoreContainer();
      if (cc != null) {
        SolrCore core = cc.getCore(desc.getName());
        try {
          if (core.isReloaded()) {
            doRecovery = false;
          }
        } finally {
          core.close();
        }
      } else {
        log.warn("Cannot recover without access to CoreContainer");
        return shardId;
      }

    }
    
    if (doRecovery) {
      doRecovery(collection, desc, cloudDesc, iamleader);
    }
    addToZk(collection, desc, cloudDesc, shardUrl, shardZkNodeName, ZkStateReader.ACTIVE);

    return shardId;
  }


  private boolean getShardId(final CoreDescriptor desc,
      CloudState state, String shardZkNodeName) {

    CloudDescriptor cloudDesc = desc.getCloudDescriptor();
    
    Map<String,Slice> slices = state.getSlices(cloudDesc.getCollectionName());
    if (slices != null) {
      Map<String,String> nodes = new HashMap<String,String>();

      for (Slice s : slices.values()) {
        for (String node : s.getShards().keySet()) {
          nodes.put(node, s.getName());
        }
      }
      if (nodes.containsKey(shardZkNodeName)) {
        // TODO: we where already registered - go into recovery mode
        cloudDesc.setShardId(nodes.get(shardZkNodeName));
        return false;
      }
    }
    return true;
  }


  ZkNodeProps addToZk(String collection, final CoreDescriptor desc, final CloudDescriptor cloudDesc, String shardUrl,
      final String shardZkNodeName, String state)
      throws Exception {

    Map<String,String> props = new HashMap<String,String>();
    props.put(ZkStateReader.URL_PROP, shardUrl);
    
    props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
    
    props.put(ZkStateReader.ROLES_PROP, cloudDesc.getRoles());
    
    props.put(ZkStateReader.STATE_PROP, state);
    
    System.out.println("update state to:" + state);
    ZkNodeProps zkProps = new ZkNodeProps(props);

    Map<String, ZkNodeProps> shardProps = new HashMap<String, ZkNodeProps>();
    shardProps.put(shardZkNodeName, zkProps);
		Slice slice = new Slice(cloudDesc.getShardId(), shardProps);
		
		boolean persisted = false;
		Stat stat = zkClient.exists(ZkStateReader.CLUSTER_STATE, null);
		if (stat == null) {
			log.info(ZkStateReader.CLUSTER_STATE + " does not exist, attempting to create");
			try {
				CloudState clusterState = new CloudState();

				clusterState.addSlice(cloudDesc.getCollectionName(), slice);

				zkClient.create(ZkStateReader.CLUSTER_STATE,
						CloudState.store(clusterState), Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				persisted = true;
				log.info(ZkStateReader.CLUSTER_STATE);
			} catch (KeeperException e) {
				if (e.code() != Code.NODEEXISTS) {
					// If this node exists, no big deal
					throw e;
				}
			}
		}
		if (!persisted) {
	
			boolean updated = false;
			
			// TODO: we don't want to retry forever
			// give up at some point
			while (!updated) {
		    stat = zkClient.exists(ZkStateReader.CLUSTER_STATE, null);
				log.info("Attempting to update " + ZkStateReader.CLUSTER_STATE + " version "
						+ stat.getVersion());
				CloudState clusterState = CloudState.load(zkClient, zkStateReader.getCloudState().getLiveNodes());

				// our second state read - should only need one? (see register)
        slice = clusterState.getSlice(cloudDesc.getCollectionName(), cloudDesc.getShardId());
        
        Map<String, ZkNodeProps> shards = new HashMap<String, ZkNodeProps>();
        shards.putAll(slice.getShards());
        shards.put(shardZkNodeName, zkProps);
        Slice newSlice = new Slice(slice.getName(), shards);
        

        CloudState newClusterState = new CloudState(clusterState.getLiveNodes(), clusterState.getCollectionStates());
        clusterState.addSlice(collection, newSlice);
    

				try {
					zkClient.setData(ZkStateReader.CLUSTER_STATE,
							CloudState.store(clusterState), stat.getVersion());
					updated = true;
				} catch (KeeperException e) {
					if (e.code() != Code.BADVERSION) {
						throw e;
					}
					log.info("Failed to update " + ZkStateReader.CLUSTER_STATE + ", retrying");
					System.out.println("Failed to update " + ZkStateReader.CLUSTER_STATE + ", retrying");
				}

			}
		}
    return zkProps;
  }


  private void doRecovery(String collection, final CoreDescriptor desc,
      final CloudDescriptor cloudDesc, boolean iamleader) throws Exception,
      SolrServerException, IOException {
    log.info("Start recovery process");
    // start buffer updates to tran log
    // and do recovery - either replay via realtime get 
    // or full index replication

    // seems perhaps we cannot do this here since we are not fully running - 
    // we may need to trigger a recovery that happens later
    
    String leaderUrl = zkStateReader.getLeader(collection, cloudDesc.getShardId());
    
    if (!iamleader) {
      // if we are the leader, either we are trying to recover faster
      // then our ephemeral timed out or we are the only node
      
      // TODO: first, issue a hard commit?
      
      
      // if we want to buffer updates while recovering, this
      // will have to trigger later - http is not yet up ???
      
      // use rep handler directly, so we can do this sync rather than async
      SolrCore core = desc.getCoreContainer().getCore(desc.getName());
      try {
        ReplicationHandler replicationHandler = (ReplicationHandler) core
            .getRequestHandler("/replication");
        
        if (replicationHandler == null) {
          log.error("Skipping recovery, no /replication handler found");
          return;
        }
        
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set(ReplicationHandler.MASTER_URL, leaderUrl + "replication");
        solrParams.set(ReplicationHandler.CMD_FORCE, true);

        replicationHandler.doFetch(solrParams);
      } finally {
        core.close();
        log.info("Finished recovery process");
      }
    }
  }

  private void doLeaderElectionProcess(String shardId,
      final String collection, String shardZkNodeName, ZkNodeProps props) throws KeeperException,
      InterruptedException, IOException {
   
    leaderElector.joinElection(shardId, collection, shardZkNodeName, props);
  }

  /**
   * @param coreName
   * @param cloudDesc
   */
  public void unregister(String coreName, CloudDescriptor cloudDesc) {
    // TODO : perhaps mark the core down in zk?
  }

  /**
   * @param dir
   * @param zkPath
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void uploadToZK(File dir, String zkPath) throws IOException, KeeperException, InterruptedException {
    File[] files = dir.listFiles();
    for(File file : files) {
      if (!file.getName().startsWith(".")) {
        if (!file.isDirectory()) {
          zkClient.setData(zkPath + "/" + file.getName(), file);
        } else {
          uploadToZK(file, zkPath + "/" + file.getName());
        }
      }
    }
  }
  
  /**
   * @param dir
   * @param configName
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void uploadConfigDir(File dir, String configName) throws IOException, KeeperException, InterruptedException {
    uploadToZK(dir, ZkController.CONFIGS_ZKNODE + "/" + configName);
  }

  // convenience for testing
  void printLayoutToStdOut() throws KeeperException, InterruptedException {
    zkClient.printLayoutToStdOut();
  }

  private void setUpCollectionsNode() throws KeeperException, InterruptedException {
    try {
      if (!zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE)) {
        if (log.isInfoEnabled()) {
          log.info("creating zk collections node:" + ZkStateReader.COLLECTIONS_ZKNODE);
        }
        // makes collections zkNode if it doesn't exist
        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE, CreateMode.PERSISTENT, null);
      }
    } catch (KeeperException e) {
      // its okay if another beats us creating the node
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      }
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    }
    
  }
  
  private void setupOverseerNodes() throws KeeperException, InterruptedException {
    String nodeName = getAssignmentsNode();
    
    try {
      if (!zkClient.exists(nodeName)) {
        if (log.isInfoEnabled()) {
          log.info("creating node:" + nodeName);
        }
        // makes collections zkNode if it doesn't exist
        zkClient.makePath(nodeName, CreateMode.PERSISTENT, null);
      }
    } catch (KeeperException e) {
      // its okay if another beats us creating the node
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      }
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    }

  }

  public void createCollectionZkNode(CloudDescriptor cd) throws KeeperException, InterruptedException, IOException {
    String collection = cd.getCollectionName();
    
    log.info("Check for collection zkNode:" + collection);
    String collectionPath = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
    
    try {
      if(!zkClient.exists(collectionPath)) {
        log.info("Creating collection in ZooKeeper:" + collection);
       SolrParams params = cd.getParams();

        try {
          Map<String,String> collectionProps = new HashMap<String,String>();
          // TODO: if collection.configName isn't set, and there isn't already a conf in zk, just use that?
          String defaultConfigName = System.getProperty(COLLECTION_PARAM_PREFIX+CONFIGNAME_PROP, "configuration1");

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
              collectionProps.put(CONFIGNAME_PROP,  defaultConfigName);
            
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

          } else {
            // check for configName
            log.info("Looking for collection configName");
            int retry = 1;
            for (; retry < 6; retry++) {
              if (zkClient.exists(collectionPath)) {
                ZkNodeProps cProps = ZkNodeProps.load(zkClient.getData(collectionPath, null, null));
                if (cProps.containsKey(CONFIGNAME_PROP)) {
                  break;
                }
              }
              // if there is only one conf, use that
              List<String> configNames = zkClient.getChildren(CONFIGS_ZKNODE, null);
              if (configNames.size() == 1) {
                // no config set named, but there is only 1 - use it
                log.info("Only one config set found in zk - using it:" + configNames.get(0));
                collectionProps.put(CONFIGNAME_PROP,  configNames.get(0));
                break;
              }
              log.info("Could not find collection configName - pausing for 2 seconds and trying again - try: " + retry);
              Thread.sleep(2000);
            }
            if (retry == 6) {
              log.error("Could not find configName for collection " + collection);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR,
                  "Could not find configName for collection " + collection);
            }
          }
          
          collectionProps.put("num_shards", Integer.toString(numShards));
          ZkNodeProps zkProps = new ZkNodeProps(collectionProps);
          zkClient.makePath(collectionPath, zkProps.store(), CreateMode.PERSISTENT, null, true);
          try {
            // shards_lock node
            if (!zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/shards_lock")) {
              // makes shards_lock zkNode if it doesn't exist
              zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/shards_lock");
            }
          } catch (KeeperException e) {
            // its okay if another beats us creating the node
            if (e.code() != KeeperException.Code.NODEEXISTS) {
              throw e;
            }
          }
         
          // ping that there is a new collection
          zkClient.setData(ZkStateReader.COLLECTIONS_ZKNODE, (byte[])null);
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
  
  public ZkStateReader getZkStateReader() {
    return zkStateReader;
  }

  
  private void publishState(CloudDescriptor cloudDesc, String shardZkNodeName,
      Map<String,String> props) {
    CoreState coreState = new CoreState(shardZkNodeName,
        cloudDesc.getCollectionName(), props);
    coreStates.put(shardZkNodeName, coreState);
    final String nodePath = "/node_states/" + getNodeName();

    try {

      if (!zkClient.exists(nodePath)) {
        zkClient.makePath(nodePath);
      }
      
      log.info("publishing node state:" + coreStates.values());
      zkClient.setData(
          nodePath,
          CoreState.tobytes(coreStates.values().toArray(
              new CoreState[coreStates.size()])));
    } catch (IOException e) {
      throw new ZooKeeperException(
          SolrException.ErrorCode.SERVER_ERROR,
          "could not publish node state", e);
    } catch (KeeperException e) {
      throw new ZooKeeperException(
          SolrException.ErrorCode.SERVER_ERROR,
          "could not publish node state", e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(
          SolrException.ErrorCode.SERVER_ERROR,
          "could not publish node state", e);
    }
  }

  private String doGetShardIdProcess(String coreName, CloudDescriptor descriptor) {
    final String shardZkNodeName = getNodeName() + "_" + coreName;
    while (true) {
      synchronized (assignments) {
        CoreAssignment assignment = assignments.get(shardZkNodeName);
        if (assignment != null
            && assignment.getProperties().get("shard_name") != null) {
          return assignment.getProperties().get("shard_name");
        }
        
//        System.out.println("current assignments:" + assignments);
        try {
          assignments.wait(1000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
        }
      }
    }
  }

  /**
   * Atomic assignments update
   * @throws IOException 
   */
  private void processAssignmentsUpdate(byte[] assignments) throws IOException {

    if (assignments == null) {
      return;
    }

    HashMap<String, CoreAssignment> newAssignments = new HashMap<String, CoreAssignment>();
    CoreAssignment[] assignments2 = CoreAssignment.fromBytes(assignments);
    
    System.out.println("read assignments from controller:" + Arrays.asList(assignments2));
    for (CoreAssignment assignment : assignments2) {
      newAssignments.put(assignment.getCoreName(), assignment);
    }

    synchronized (this.assignments) {
      this.assignments.notifyAll();
      this.assignments = newAssignments;
    }
  }
}
