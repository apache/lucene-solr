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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
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
import org.apache.solr.core.CoreDescriptor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
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

  private SolrZkClient zkClient;
  
  private ZkStateReader zkStateReader;

  private SliceLeaderElector leaderElector;
  
  private String zkServerAddress;

  private String localHostPort;
  private String localHostContext;
  private String localHostName;
  private String localHost;

  private String hostName;

  private AssignShard assignShard;

  private int numShards;

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
                  register(descriptor.getName(), descriptor,
                      descriptor.getCloudDescriptor());
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
    assignShard = new AssignShard(zkClient);
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
      log.error("", e);
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
    ZkNodeProps props = new ZkNodeProps();
    
    if(data != null) {
      props.load(data);
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
  public String register(String coreName, final CoreDescriptor desc, final CloudDescriptor cloudDesc) throws Exception {
    String shardUrl = localHostName + ":" + localHostPort + "/" + localHostContext
        + "/" + coreName;
    
    final String collection = cloudDesc.getCollectionName();
    
    byte[] data = zkClient.getData(ZkStateReader.CLUSTER_STATE,
        null, null);
    log.info("Attempting to update " + ZkStateReader.CLUSTER_STATE + " version "
        + null);
    CloudState state = CloudState.load(data);
    
    boolean recover = false;
    Map<String,Slice> slices = state.getSlices(cloudDesc.getCollectionName());
    String shardZkNodeName = getNodeName() + "_" + coreName;
    if (slices != null) {
      Map<String,String> nodes = new HashMap<String,String>();

      for (Slice s : slices.values()) {
        System.out.println("add slice: "+ s.getName());
        for (String node : s.getShards().keySet()) {
          System.out.println("add node: "+ node);
          nodes.put(node, s.getName());
        }
      }
      System.out.println("print recovery:" + nodes + " name: " + shardZkNodeName);
      if (nodes.containsKey(shardZkNodeName)) {
        // TODO: we where already registered - go into recovery mode
        cloudDesc.setShardId(nodes.get(shardZkNodeName));
        recover = true;
      }
    }
    
    String shardId = cloudDesc.getShardId();
    if (shardId == null && !recover) {
      shardId = assignShard.assignShard(collection, numShards);
      cloudDesc.setShardId(shardId);
    }
    

    if (log.isInfoEnabled()) {
        log.info("Register shard - core:" + coreName + " address:"
            + shardUrl);
      }
    
    leaderElector.setupForSlice(shardId, collection);
    
    ZkNodeProps props = addToZk(collection, desc, cloudDesc, shardUrl, shardZkNodeName, recover);
    
    // leader election
    doLeaderElectionProcess(shardId, collection, shardZkNodeName, props);
    return shardId;
  }


  ZkNodeProps addToZk(String collection, final CoreDescriptor desc, final CloudDescriptor cloudDesc, String shardUrl,
      final String shardZkNodeName, boolean recover)
      throws Exception {
    ZkNodeProps props = new ZkNodeProps();
    props.put(ZkStateReader.URL_PROP, shardUrl);
    
    props.put(ZkStateReader.NODE_NAME, getNodeName());
    
    props.put("roles", cloudDesc.getRoles());

    Map<String, ZkNodeProps> shardProps = new HashMap<String, ZkNodeProps>();
    shardProps.put(shardZkNodeName, props);
		Slice slice = new Slice(cloudDesc.getShardId(), shardProps);
		
		boolean persisted = false;
		Stat stat = zkClient.exists(ZkStateReader.CLUSTER_STATE, null);
		if (stat == null) {
			log.info(ZkStateReader.CLUSTER_STATE + " does not exist, attempting to create");
			try {
				CloudState state = new CloudState();

				state.addSlice(cloudDesc.getCollectionName(), slice);

				zkClient.create(ZkStateReader.CLUSTER_STATE,
						CloudState.store(state), Ids.OPEN_ACL_UNSAFE,
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
			stat = new Stat();
			boolean updated = false;
			
			// TODO: we don't want to retry forever
			// give up at some point
			while (!updated) {

				byte[] data = zkClient.getData(ZkStateReader.CLUSTER_STATE,
						null, stat);
				log.info("Attempting to update " + ZkStateReader.CLUSTER_STATE + " version "
						+ stat.getVersion());
				CloudState state = CloudState.load(data);
				// our second state read - should only need one (see register)
        
				// we need a new copy to modify
        state = new CloudState(state.getLiveNodes(), state.getCollectionStates());
				state.addSlice(cloudDesc.getCollectionName(), slice);

				try {
					zkClient.setData(ZkStateReader.CLUSTER_STATE,
							CloudState.store(state), stat.getVersion());
					updated = true;
					if (recover) {
					  // nocommit: joke code
					  System.out.println("do recovery");
					  // start buffer updates to tran log
					  // and do recovery - either replay via realtime get 
					  // or full index replication
            System.out.println("RECOVERY");
            // seems we cannot do this here since we are not fully running - 
            // we need to trigger a recovery that happens later
            System.out.println("shard is:" + cloudDesc.getShardId());
            String leaderUrl = zkStateReader.getLeader(collection, cloudDesc.getShardId());
            System.out.println("leader url: "+ leaderUrl);
            System.out.println("shard url: "+ shardUrl);
            if (!leaderUrl.equals(shardUrl)) {
              // if we are the leader, either we are trying to recover faster
              // then our ephemeral timed out or we are the only node
              
              ModifiableSolrParams params = new ModifiableSolrParams();
              params.set("command", "fetchindex");
              params.set("force", true); // force replication regardless of
                                         // versions
              params.set("masterUrl", leaderUrl + "replication");
              QueryRequest req = new QueryRequest(params);
              req.setPath("/replication");
              System.out.println("Make replication call to:" + leaderUrl);
              System.out.println("params:" + params);
              
              // if we want to buffer updates while recovering, this
              // will have to trigger later - http is not yet up
              
              // we need to use embedded cause http is not up yet anyhow
              EmbeddedSolrServer server = new EmbeddedSolrServer(
                  desc.getCoreContainer(), desc.getName());
              server.request(req);
            }
					}
				} catch (KeeperException e) {
					if (e.code() != Code.BADVERSION) {
						throw e;
					}
					log.info("Failed to update " + ZkStateReader.CLUSTER_STATE + ", retrying");
				}

			}
		}
    return props;
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

  public void createCollectionZkNode(CloudDescriptor cd) throws KeeperException, InterruptedException, IOException {
    String collection = cd.getCollectionName();
    
    log.info("Check for collection zkNode:" + collection);
    String collectionPath = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
    
    try {
      if(!zkClient.exists(collectionPath)) {
        log.info("Creating collection in ZooKeeper:" + collection);
       SolrParams params = cd.getParams();

        try {
          ZkNodeProps collectionProps = new ZkNodeProps();
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
                collectionProps = new ZkNodeProps();
                collectionProps.load(zkClient.getData(collectionPath, null, null));
                if (collectionProps.containsKey(CONFIGNAME_PROP)) {
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
          
          zkClient.makePath(collectionPath, collectionProps.store(), CreateMode.PERSISTENT, null, true);
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

}
