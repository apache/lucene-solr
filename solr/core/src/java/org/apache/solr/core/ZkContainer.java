package org.apache.solr.core;

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

import org.apache.commons.lang.StringUtils;
import org.apache.solr.cloud.CurrentCoreDescriptorProvider;
import org.apache.solr.cloud.SolrZkServer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.util.SystemIdResolver;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class ZkContainer {
  protected static Logger log = LoggerFactory.getLogger(ZkContainer.class);
  
  /** @deprecated will be remove in Solr 5.0 (SOLR-4622) */
  public static final String DEFAULT_HOST_CONTEXT = "solr";
  /** @deprecated will be remove in Solr 5.0 (SOLR-4622) */
  public static final String DEFAULT_HOST_PORT = "8983";
  
  protected ZkController zkController;
  private SolrZkServer zkServer;
  private int zkClientTimeout;
  private String hostPort;
  private String hostContext;
  private String host;
  private int leaderVoteWait;
  private Boolean genericCoreNodeNames;
  
  public ZkContainer() {
    
  }

  public void initZooKeeper(final CoreContainer cc, String solrHome, ConfigSolr config) {

    if (config.getCoreLoadThreadCount() <= 1) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "SolrCloud requires a value of at least 2 in solr.xml for coreLoadThreads");
    }

    initZooKeeper(cc, solrHome,
        config.getZkHost(), config.getZkClientTimeout(), config.getZkHostPort(), config.getZkHostContext(),
        config.getHost(), config.getLeaderVoteWait(), config.getGenericCoreNodeNames());
  }
  // TODO: 5.0 remove this, it's only here for back-compat and only called from ConfigSolr.
  public static boolean isZkMode() {
    String test = System.getProperty("zkHost");
    if (StringUtils.isBlank(test)) {
      test = System.getProperty("zkRun");
    }
    return StringUtils.isNotBlank(test);
  }

  public void initZooKeeper(final CoreContainer cc, String solrHome, String zkHost, int zkClientTimeout, String hostPort,
                            String hostContext, String host, int leaderVoteWait, boolean genericCoreNodeNames) {
    ZkController zkController = null;
    
    // if zkHost sys property is not set, we are not using ZooKeeper
    String zookeeperHost;
    if(zkHost == null) {
      zookeeperHost = System.getProperty("zkHost");
    } else {
      zookeeperHost = zkHost;
    }

    String zkRun = System.getProperty("zkRun");
    
    this.zkClientTimeout = zkClientTimeout;
    this.hostPort = hostPort;
    this.hostContext = hostContext;
    this.host = host;
    this.leaderVoteWait = leaderVoteWait;
    this.genericCoreNodeNames = genericCoreNodeNames;
    
    if (zkRun == null && zookeeperHost == null)
        return;  // not in zk mode


    // BEGIN: SOLR-4622: deprecated hardcoded defaults for hostPort & hostContext
    if (null == hostPort) {
      // throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
      //               "'hostPort' must be configured to run SolrCloud");
      log.warn("Solr 'hostPort' has not be explicitly configured, using hardcoded default of " + DEFAULT_HOST_PORT + ".  This default has been deprecated and will be removed in future versions of Solr, please configure this value explicitly");
      hostPort = DEFAULT_HOST_PORT;
    }
    if (null == hostContext) {
      // throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
      //               "'hostContext' must be configured to run SolrCloud");
      log.warn("Solr 'hostContext' has not be explicitly configured, using hardcoded default of " + DEFAULT_HOST_CONTEXT + ".  This default has been deprecated and will be removed in future versions of Solr, please configure this value explicitly");
      hostContext = DEFAULT_HOST_CONTEXT;
    }
    // END: SOLR-4622

    // zookeeper in quorum mode currently causes a failure when trying to
    // register log4j mbeans.  See SOLR-2369
    // TODO: remove after updating to an slf4j based zookeeper
    System.setProperty("zookeeper.jmx.log4j.disable", "true");

    if (zkRun != null) {
      String zkDataHome = System.getProperty("zkServerDataDir", solrHome + "zoo_data");
      String zkConfHome = System.getProperty("zkServerConfDir", solrHome);
      zkServer = new SolrZkServer(zkRun, zookeeperHost, zkDataHome, zkConfHome, hostPort);
      zkServer.parseConfig();
      zkServer.start();
      
      // set client from server config if not already set
      if (zookeeperHost == null) {
        zookeeperHost = zkServer.getClientString();
      }
    }

    int zkClientConnectTimeout = 30000;

    if (zookeeperHost != null) {

      // we are ZooKeeper enabled
      try {
        // If this is an ensemble, allow for a long connect time for other servers to come up
        if (zkRun != null && zkServer.getServers().size() > 1) {
          zkClientConnectTimeout = 24 * 60 * 60 * 1000;  // 1 day for embedded ensemble
          log.info("Zookeeper client=" + zookeeperHost + "  Waiting for a quorum.");
        } else {
          log.info("Zookeeper client=" + zookeeperHost);          
        }
        String confDir = System.getProperty("bootstrap_confdir");
        boolean boostrapConf = Boolean.getBoolean("bootstrap_conf");  
        
        if(!ZkController.checkChrootPath(zookeeperHost, (confDir!=null) || boostrapConf)) {
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "A chroot was specified in ZkHost but the znode doesn't exist. ");
        }
        zkController = new ZkController(cc, zookeeperHost, zkClientTimeout,
            zkClientConnectTimeout, host, hostPort, hostContext,
            leaderVoteWait, genericCoreNodeNames,
            new CurrentCoreDescriptorProvider() {

              @Override
              public List<CoreDescriptor> getCurrentDescriptors() {
                List<CoreDescriptor> descriptors = new ArrayList<CoreDescriptor>(
                    cc.getCoreNames().size());
                Collection<SolrCore> cores = cc.getCores();
                for (SolrCore core : cores) {
                  descriptors.add(core.getCoreDescriptor());
                }
                return descriptors;
              }
            });


        if (zkRun != null && zkServer.getServers().size() > 1 && confDir == null && boostrapConf == false) {
          // we are part of an ensemble and we are not uploading the config - pause to give the config time
          // to get up
          Thread.sleep(10000);
        }
        
        if(confDir != null) {
          File dir = new File(confDir);
          if(!dir.isDirectory()) {
            throw new IllegalArgumentException("bootstrap_confdir must be a directory of configuration files");
          }
          String confName = System.getProperty(ZkController.COLLECTION_PARAM_PREFIX+ZkController.CONFIGNAME_PROP, "configuration1");
          zkController.uploadConfigDir(dir, confName);
        }


        
        if(boostrapConf) {
          ZkController.bootstrapConf(zkController.getZkClient(), cc, solrHome);
        }
        
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      } catch (TimeoutException e) {
        log.error("Could not connect to ZooKeeper", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      } catch (IOException e) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      } catch (KeeperException e) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      }
      

    }
    this.zkController = zkController;
  }
  
  // Helper method to separate out creating a core from ZK as opposed to the
  // "usual" way. See create()
  SolrCore createFromZk(String instanceDir, CoreDescriptor dcore, SolrResourceLoader loader) {
    try {
      SolrResourceLoader solrLoader = null;
      SolrConfig config = null;
      String zkConfigName = null;
      IndexSchema schema;
      String collection = dcore.getCloudDescriptor().getCollectionName();
      zkController.createCollectionZkNode(dcore.getCloudDescriptor());
      
      zkConfigName = zkController.getZkStateReader().readConfigName(collection);
      if (zkConfigName == null) {
        log.error("Could not find config name for collection:" + collection);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "Could not find config name for collection:" + collection);
      }
      solrLoader = new ZkSolrResourceLoader(instanceDir, zkConfigName,
          loader.getClassLoader(), dcore.getSubstitutableProperties(), zkController);
      config = getSolrConfigFromZk(zkConfigName, dcore.getConfigName(),
          solrLoader);
      schema = IndexSchemaFactory.buildIndexSchema(dcore.getSchemaName(),
          config);
      return new SolrCore(dcore.getName(), null, config, schema, dcore);
      
    } catch (KeeperException e) {
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }
  }
  
  public void registerInZk(SolrCore core) {
    if (zkController != null) {
      try {
        zkController.register(core.getName(), core.getCoreDescriptor());
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        SolrException.log(log, "", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      } catch (Exception e) {
        // if register fails, this is really bad - close the zkController to
        // minimize any damage we can cause
        try {
          zkController.publish(core.getCoreDescriptor(), ZkStateReader.DOWN);
        } catch (KeeperException e1) {
          log.error("", e);
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          log.error("", e);
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        }
        SolrException.log(log, "", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      }
    }
  }
  
  public SolrConfig getSolrConfigFromZk(String zkConfigName, String solrConfigFileName,
      SolrResourceLoader resourceLoader) {
    SolrConfig cfg = null;
    try {
      byte[] config = zkController.getConfigFileData(zkConfigName,
          solrConfigFileName);
      InputSource is = new InputSource(new ByteArrayInputStream(config));
      is.setSystemId(SystemIdResolver
          .createSystemIdFromResourceName(solrConfigFileName));
      cfg = solrConfigFileName == null ? new SolrConfig(resourceLoader,
          SolrConfig.DEFAULT_CONF_FILE, is) : new SolrConfig(resourceLoader,
          solrConfigFileName, is);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "getSolrConfigFromZK failed for " + zkConfigName + " "
              + solrConfigFileName, e);
    }
    return cfg;
  }
  
  public ZkController getZkController() {
    return zkController;
  }
  
  public void publishCoresAsDown(List<SolrCore> cores) {
    
    for (SolrCore core : cores) {
      try {
        zkController.publish(core.getCoreDescriptor(), ZkStateReader.DOWN);
      } catch (KeeperException e) {
        CoreContainer.log.error("", e);
      } catch (InterruptedException e) {
        CoreContainer.log.error("", e);
      }
    }
  }

  public void close() {
    if (zkController != null) {
      zkController.close();
    }
    
    if (zkServer != null) {
      zkServer.stop();
    }
  }
}
