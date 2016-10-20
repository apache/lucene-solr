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
package org.apache.solr.core;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import org.apache.solr.cloud.CurrentCoreDescriptorProvider;
import org.apache.solr.cloud.SolrZkServer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkContainer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  protected ZkController zkController;
  private SolrZkServer zkServer;

  private ExecutorService coreZkRegister = ExecutorUtil.newMDCAwareCachedThreadPool(
      new DefaultSolrThreadFactory("coreZkRegister") );
  
  // see ZkController.zkRunOnly
  private boolean zkRunOnly = Boolean.getBoolean("zkRunOnly"); // expert
  
  public ZkContainer() {
    
  }

  public void initZooKeeper(final CoreContainer cc, String solrHome, CloudConfig config) {

    ZkController zkController = null;

    String zkRun = System.getProperty("zkRun");

    if (zkRun != null && config == null)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Cannot start Solr in cloud mode - no cloud config provided");
    
    if (config == null)
        return;  // not in zk mode

    String zookeeperHost = config.getZkHost();

    // zookeeper in quorum mode currently causes a failure when trying to
    // register log4j mbeans.  See SOLR-2369
    // TODO: remove after updating to an slf4j based zookeeper
    System.setProperty("zookeeper.jmx.log4j.disable", "true");

    if (zkRun != null) {
      String zkDataHome = System.getProperty("zkServerDataDir", Paths.get(solrHome).resolve("zoo_data").toString());
      String zkConfHome = System.getProperty("zkServerConfDir", solrHome);
      zkServer = new SolrZkServer(stripChroot(zkRun), stripChroot(config.getZkHost()), zkDataHome, zkConfHome, config.getSolrHostPort());
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
        
        if(!ZkController.checkChrootPath(zookeeperHost, (confDir!=null) || boostrapConf || zkRunOnly)) {
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "A chroot was specified in ZkHost but the znode doesn't exist. " + zookeeperHost);
        }
        zkController = new ZkController(cc, zookeeperHost, zkClientConnectTimeout, config,
            new CurrentCoreDescriptorProvider() {

              @Override
              public List<CoreDescriptor> getCurrentDescriptors() {
                List<CoreDescriptor> descriptors = new ArrayList<>(
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
          Path configPath = Paths.get(confDir);
          if (!Files.isDirectory(configPath))
            throw new IllegalArgumentException("bootstrap_confdir must be a directory of configuration files");

          String confName = System.getProperty(ZkController.COLLECTION_PARAM_PREFIX+ZkController.CONFIGNAME_PROP, "configuration1");
          ZkConfigManager configManager = new ZkConfigManager(zkController.getZkClient());
          configManager.uploadConfigDir(configPath, confName);
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
      } catch (IOException | KeeperException e) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      }


    }
    this.zkController = zkController;
  }
  
  private String stripChroot(String zkRun) {
    if (zkRun == null || zkRun.trim().length() == 0 || zkRun.lastIndexOf('/') < 0) return zkRun;
    return zkRun.substring(0, zkRun.lastIndexOf('/'));
  }

  public void registerInZk(final SolrCore core, boolean background, boolean skipRecovery) {
    Runnable r = () -> {
      MDCLoggingContext.setCore(core);
      try {
        try {
          zkController.register(core.getName(), core.getCoreDescriptor(), skipRecovery);
        } catch (InterruptedException e) {
          // Restore the interrupted status
          Thread.currentThread().interrupt();
          SolrException.log(log, "", e);
        } catch (Exception e) {
          try {
            zkController.publish(core.getCoreDescriptor(), Replica.State.DOWN);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            log.error("", e1);
          } catch (Exception e1) {
            log.error("", e1);
          }
          SolrException.log(log, "", e);
        }
      } finally {
        MDCLoggingContext.clear();
      }
    };

    if (zkController != null) {
      if (background) {
        coreZkRegister.execute(r);
      } else {
        MDCLoggingContext.setCore(core);
        try {
          r.run();
        } finally {
          MDCLoggingContext.clear();
        }
      }
    }
  }
  
  public ZkController getZkController() {
    return zkController;
  }
  
  public void publishCoresAsDown(List<SolrCore> cores) {
    
    for (SolrCore core : cores) {
      try {
        zkController.publish(core.getCoreDescriptor(), Replica.State.DOWN);
      } catch (KeeperException e) {
        ZkContainer.log.error("", e);
      } catch (InterruptedException e) {
        Thread.interrupted();
        ZkContainer.log.error("", e);
      }
    }
  }

  public void close() {
    
    try {
      if (zkController != null) {
        zkController.close();
      }
    } finally {
      try {
        if (zkServer != null) {
          zkServer.stop();
        }
      } finally {
        ExecutorUtil.shutdownAndAwaitTermination(coreZkRegister);
      }
    }
    
  }

  public ExecutorService getCoreZkRegisterExecutorService() {
    return coreZkRegister;
  }
}
