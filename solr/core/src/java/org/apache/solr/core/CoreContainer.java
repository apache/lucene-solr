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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.CurrentCoreDescriptorProvider;
import org.apache.solr.cloud.SolrZkServer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.ExecutorUtil;

import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.logging.ListenerConfig;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.jul.JulWatcher;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.PropertiesUtil;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;


/**
 *
 * @since solr 1.3
 */
public class CoreContainer
{
  private static final String LEADER_VOTE_WAIT = "180000";  // 3 minutes
  private static final int CORE_LOAD_THREADS = 3;
  private static final String DEFAULT_HOST_CONTEXT = "solr";
  private static final String DEFAULT_HOST_PORT = "8983";
  private static final int DEFAULT_ZK_CLIENT_TIMEOUT = 15000;
  public static final String DEFAULT_DEFAULT_CORE_NAME = "collection1";
  private static final boolean DEFAULT_SHARE_SCHEMA = false;

  protected static Logger log = LoggerFactory.getLogger(CoreContainer.class);


  private final CoreMaps coreMaps = new CoreMaps(this);

  protected final Map<String,Exception> coreInitFailures =
    Collections.synchronizedMap(new LinkedHashMap<String,Exception>());
  
  protected boolean persistent = false;
  protected String adminPath = null;
  protected String managementPath = null;
  protected String hostPort;
  protected String hostContext;
  protected String host;
  protected CoreAdminHandler coreAdminHandler = null;
  protected CollectionsHandler collectionsHandler = null;
  protected File configFile = null;
  protected String libDir = null;
  protected ClassLoader libLoader = null;
  protected SolrResourceLoader loader = null;
  protected Properties containerProperties;
  protected Map<String ,IndexSchema> indexSchemaCache;
  protected String adminHandler;
  protected boolean shareSchema;
  protected Integer zkClientTimeout;
  protected String solrHome;
  protected String defaultCoreName = null;

  private ZkController zkController;
  private SolrZkServer zkServer;
  private ShardHandlerFactory shardHandlerFactory;
  protected LogWatcher logging = null;
  private String zkHost;

  private String leaderVoteWait = LEADER_VOTE_WAIT;
  private int distribUpdateConnTimeout = 0;
  private int distribUpdateSoTimeout = 0;
  protected int transientCacheSize = Integer.MAX_VALUE; // Use as a flag too, if transientCacheSize set in solr.xml this will be changed
  private int coreLoadThreads;
  private CloserThread backgroundCloser = null;
  
  {
    log.info("New CoreContainer " + System.identityHashCode(this));
  }

  /**
   * Deprecated
   * @deprecated use the single arg constructure with locateSolrHome()
   * @see SolrResourceLoader#locateSolrHome
   */
  @Deprecated
  public CoreContainer() {
    this(SolrResourceLoader.locateSolrHome());
  }

  /**
   * Initalize CoreContainer directly from the constructor
   */
  public CoreContainer(String dir, File configFile) throws FileNotFoundException {
    this(dir);
    this.load(dir, configFile);
  }

  /**
   * Minimal CoreContainer constructor.
   * @param loader the CoreContainer resource loader
   */
  public CoreContainer(SolrResourceLoader loader) {
    this(loader.getInstanceDir());
    this.loader = loader;
  }

  public CoreContainer(String solrHome) {
    this.solrHome = solrHome;
  }

  protected void initZooKeeper(String zkHost, int zkClientTimeout) {
    // if zkHost sys property is not set, we are not using ZooKeeper
    String zookeeperHost;
    if(zkHost == null) {
      zookeeperHost = System.getProperty("zkHost");
    } else {
      zookeeperHost = zkHost;
    }

    String zkRun = System.getProperty("zkRun");

    if (zkRun == null && zookeeperHost == null)
        return;  // not in zk mode

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

    int zkClientConnectTimeout = 15000;

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
        zkController = new ZkController(this, zookeeperHost, zkClientTimeout,
            zkClientConnectTimeout, host, hostPort, hostContext,
            leaderVoteWait, distribUpdateConnTimeout, distribUpdateSoTimeout,
            new CurrentCoreDescriptorProvider() {

              @Override
              public List<CoreDescriptor> getCurrentDescriptors() {
                List<CoreDescriptor> descriptors = new ArrayList<CoreDescriptor>(
                    getCoreNames().size());
                for (SolrCore core : getCores()) {
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
          ZkController.bootstrapConf(zkController.getZkClient(), cfg, solrHome);
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
    
  }

  public Properties getContainerProperties() {
    return containerProperties;
  }

  // Helper class to initialize the CoreContainer
  public static class Initializer {
    protected String containerConfigFilename = null;  // normally "solr.xml" becoming solr.properties in 5.0
    protected String dataDir = null; // override datadir for single core mode

    // core container instantiation
    public CoreContainer initialize() throws FileNotFoundException {
      CoreContainer cores = null;
      String solrHome = SolrResourceLoader.locateSolrHome();
      // ContainerConfigFilename could  could be a properties file
      File fconf = new File(solrHome, containerConfigFilename == null ? "solr.xml"
          : containerConfigFilename);

      log.info("looking for solr config file: " + fconf.getAbsolutePath());
      cores = new CoreContainer(solrHome);

      if (! fconf.exists()) {
        if (StringUtils.isBlank(containerConfigFilename) || containerConfigFilename.endsWith(".xml")) {
          fconf = new File(solrHome, SolrProperties.SOLR_PROPERTIES_FILE);
        }
      }
      // Either we have a config file or not. If it ends in .properties, assume new-style.
      
      if (fconf.exists()) {
        cores.load(solrHome, fconf);
      } else {
        log.info("no solr.xml or solr.properties file found - using default old-style solr.xml");
        try {
          cores.load(solrHome, new ByteArrayInputStream(ConfigSolrXmlBackCompat.DEF_SOLR_XML.getBytes("UTF-8")), true, null);
        } catch (Exception e) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "CoreContainer.Initialize failed when trying to load default solr.xml file", e);
        }
        cores.configFile = fconf;
      }
      
      containerConfigFilename = cores.getConfigFile().getName();
      
      return cores;
    }
  }


  //-------------------------------------------------------------------
  // Initialization / Cleanup
  //-------------------------------------------------------------------
  
  /**
   * Load a config file listing the available solr cores.
   * @param dir the home directory of all resources.
   * @param configFile the configuration file
   */
  public void load(String dir, File configFile) throws FileNotFoundException {
    this.configFile = configFile;
    InputStream in = new FileInputStream(configFile);
    try {
      this.load(dir, in, configFile.getName().endsWith(".xml"),  configFile.getName());
    } finally {
      IOUtils.closeQuietly(in);
    }
  } 

  /**
   * Load a config file listing the available solr cores.
   * 
   * @param dir the home directory of all resources.
   * @param is the configuration file InputStream. May be a properties file or an xml file
   */

  // Let's keep this ugly boolean out of public circulation.
  protected void load(String dir, InputStream is, boolean isXmlFile, String fileName)  {
    ThreadPoolExecutor coreLoadExecutor = null;
    if (null == dir) {
      // don't rely on SolrResourceLoader(), determine explicitly first
      dir = SolrResourceLoader.locateSolrHome();
    }
    log.info("Loading CoreContainer using Solr Home: '{}'", dir);
    
    this.loader = new SolrResourceLoader(dir);
    solrHome = loader.getInstanceDir();

    ConfigSolr cfg;
    
    // keep orig config for persist to consult
    //TODO 5.0: Remove this confusing junk, the properties file is so fast to read that there's no good reason
    //          to add this stuff. Furthermore, it would be good to persist comments when saving.....
    try {
      if (isXmlFile) {
        cfg = new ConfigSolrXmlBackCompat(loader, null, is, null, false);
        this.cfg = new ConfigSolrXmlBackCompat(loader, (ConfigSolrXmlBackCompat)cfg);
      } else {
        cfg = new SolrProperties(this, is, fileName);
        this.cfg = new SolrProperties(this, loader, (SolrProperties)cfg);
      }
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "", e);
    }
    // Since the cores var is now initialized to null, let's set it up right
    // now.
    cfg.substituteProperties();

    shardHandlerFactory = cfg.initShardHandler();

    coreMaps.allocateLazyCores(cfg, loader);

    // Initialize Logging
    if (cfg.getBool(ConfigSolr.ConfLevel.SOLR_LOGGING, "enabled", true)) {
      String slf4jImpl = null;
      String fname = cfg.get(ConfigSolr.ConfLevel.SOLR_LOGGING, "class", null);
      try {
        slf4jImpl = StaticLoggerBinder.getSingleton()
            .getLoggerFactoryClassStr();
        if (fname == null) {
          if (slf4jImpl.indexOf("Log4j") > 0) {
            log.warn("Log watching is not yet implemented for log4j");
          } else if (slf4jImpl.indexOf("JDK") > 0) {
            fname = "JUL";
          }
        }
      } catch (Throwable ex) {
        log.warn("Unable to read SLF4J version.  LogWatcher will be disabled: "
            + ex);
      }
      
      // Now load the framework
      if (fname != null) {
        if ("JUL".equalsIgnoreCase(fname)) {
          logging = new JulWatcher(slf4jImpl);
//      else if( "Log4j".equals(fname) ) {
//        logging = new Log4jWatcher(slf4jImpl);
//      }
        } else {
          try {
            logging = loader.newInstance(fname, LogWatcher.class);
          } catch (Throwable e) {
            log.warn("Unable to load LogWatcher", e);
          }
        }
        
        if (logging != null) {
          ListenerConfig v = new ListenerConfig();
          v.size = cfg.getInt(ConfigSolr.ConfLevel.SOLR_LOGGING_WATCHER, "size", 50);
          v.threshold = cfg.get(ConfigSolr.ConfLevel.SOLR_LOGGING_WATCHER, "threshold", null);
          if (v.size > 0) {
            log.info("Registering Log Listener");
            logging.registerListener(v, this);
          }
        }
      }
    }
    
    String dcoreName = cfg.get(ConfigSolr.ConfLevel.SOLR_CORES, "defaultCoreName", null);
    if (dcoreName != null && !dcoreName.isEmpty()) {
      defaultCoreName = dcoreName;
    }
    persistent = cfg.getBool(ConfigSolr.ConfLevel.SOLR, "persistent", false);
    libDir = cfg.get(ConfigSolr.ConfLevel.SOLR, "sharedLib", null);
    zkHost = cfg.get(ConfigSolr.ConfLevel.SOLR, "zkHost", null);
    coreLoadThreads = cfg.getInt(ConfigSolr.ConfLevel.SOLR, "coreLoadThreads", CORE_LOAD_THREADS);
    
    adminPath = cfg.get(ConfigSolr.ConfLevel.SOLR_CORES, "adminPath", null);
    shareSchema = cfg.getBool(ConfigSolr.ConfLevel.SOLR_CORES, "shareSchema", DEFAULT_SHARE_SCHEMA);
    zkClientTimeout = cfg.getInt(ConfigSolr.ConfLevel.SOLR_CORES, "zkClientTimeout", DEFAULT_ZK_CLIENT_TIMEOUT);
    
    distribUpdateConnTimeout = cfg.getInt(ConfigSolr.ConfLevel.SOLR_CORES, "distribUpdateConnTimeout", 0);
    distribUpdateSoTimeout = cfg.getInt(ConfigSolr.ConfLevel.SOLR_CORES, "distribUpdateSoTimeout", 0);
    
    hostPort = cfg.get(ConfigSolr.ConfLevel.SOLR_CORES, "hostPort", DEFAULT_HOST_PORT);

    hostContext = cfg.get(ConfigSolr.ConfLevel.SOLR_CORES, "hostContext", DEFAULT_HOST_CONTEXT);
    host = cfg.get(ConfigSolr.ConfLevel.SOLR_CORES, "host", null);
    
    leaderVoteWait = cfg.get(ConfigSolr.ConfLevel.SOLR_CORES, "leaderVoteWait", LEADER_VOTE_WAIT);
    
    if (shareSchema) {
      indexSchemaCache = new ConcurrentHashMap<String,IndexSchema>();
    }
    adminHandler = cfg.get(ConfigSolr.ConfLevel.SOLR_CORES, "adminHandler", null);
    managementPath = cfg.get(ConfigSolr.ConfLevel.SOLR_CORES, "managementPath", null);
    
    zkClientTimeout = Integer.parseInt(System.getProperty("zkClientTimeout",
        Integer.toString(zkClientTimeout)));
    initZooKeeper(zkHost, zkClientTimeout);
    
    if (isZooKeeperAware() && coreLoadThreads <= 1) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
          "SolrCloud requires a value of at least 2 in solr.xml for coreLoadThreads");
    }
    
    if (libDir != null) {
      File f = FileUtils.resolvePath(new File(dir), libDir);
      log.info("loading shared library: " + f.getAbsolutePath());
      libLoader = SolrResourceLoader.createClassLoader(f, null);
    }
    
    if (adminPath != null) {
      if (adminHandler == null) {
        coreAdminHandler = new CoreAdminHandler(this);
      } else {
        coreAdminHandler = this.createMultiCoreHandler(adminHandler);
      }
    }
    
    collectionsHandler = new CollectionsHandler(this);
    containerProperties = cfg.getSolrProperties(cfg, DEFAULT_HOST_CONTEXT);

    // setup executor to load cores in parallel
    coreLoadExecutor = new ThreadPoolExecutor(coreLoadThreads, coreLoadThreads, 1,
        TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
        new DefaultSolrThreadFactory("coreLoadExecutor"));
    try {
      CompletionService<SolrCore> completionService = new ExecutorCompletionService<SolrCore>(
          coreLoadExecutor);
      Set<Future<SolrCore>> pending = new HashSet<Future<SolrCore>>();

      List<String> allCores = cfg.getAllCoreNames();

      for (String oneCoreName : allCores) {

        try {
          String rawName = cfg.getProperty(oneCoreName, CoreDescriptor.CORE_NAME, null);

          if (null == rawName) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Each core in solr.xml must have a 'name'");
          }
          final String name = rawName;
          final CoreDescriptor p = new CoreDescriptor(this, name,
              cfg.getProperty(oneCoreName, CoreDescriptor.CORE_INSTDIR, null));
          
          // deal with optional settings
          String opt = cfg.getProperty(oneCoreName, CoreDescriptor.CORE_CONFIG, null);
          
          if (opt != null) {
            p.setConfigName(opt);
          }
          opt = cfg.getProperty(oneCoreName, CoreDescriptor.CORE_SCHEMA, null);
          if (opt != null) {
            p.setSchemaName(opt);
          }
          
          if (zkController != null) {
            opt = cfg.getProperty(oneCoreName, CoreDescriptor.CORE_SHARD, null);
            if (opt != null && opt.length() > 0) {
              p.getCloudDescriptor().setShardId(opt);
            }
            opt = cfg.getProperty(oneCoreName, CoreDescriptor.CORE_COLLECTION, null);
            if (opt != null) {
              p.getCloudDescriptor().setCollectionName(opt);
            }
            opt = cfg.getProperty(oneCoreName, CoreDescriptor.CORE_ROLES, null);
            if (opt != null) {
              p.getCloudDescriptor().setRoles(opt);
            }

            opt = cfg.getProperty(oneCoreName, CoreDescriptor.CORE_NODE_NAME, null);
            if (opt != null && opt.length() > 0) {
              p.getCloudDescriptor().setCoreNodeName(opt);
            }
          }
          opt = cfg.getProperty(oneCoreName, CoreDescriptor.CORE_PROPERTIES, null);
          if (opt != null) {
            p.setPropertiesName(opt);
          }
          opt = cfg.getProperty(oneCoreName, CoreDescriptor.CORE_DATADIR, null);
          if (opt != null) {
            p.setDataDir(opt);
          }
          
          p.setCoreProperties(cfg.readCoreProperties(oneCoreName));
          
          opt = cfg.getProperty(oneCoreName, CoreDescriptor.CORE_LOADONSTARTUP, null);
          if (opt != null) {
            p.setLoadOnStartup(("true".equalsIgnoreCase(opt) || "on"
                .equalsIgnoreCase(opt)) ? true : false);
          }
          
          opt = cfg.getProperty(oneCoreName, CoreDescriptor.CORE_TRANSIENT, null);
          if (opt != null) {
            p.setTransient(("true".equalsIgnoreCase(opt) || "on"
                .equalsIgnoreCase(opt)) ? true : false);
          }
          
          if (p.isLoadOnStartup()) { // The normal case

            Callable<SolrCore> task = new Callable<SolrCore>() {
              @Override
              public SolrCore call() {
                SolrCore c = null;
                try {
                  c = create(p);
                  registerCore(p.isTransient(), name, c, false);
                } catch (Throwable t) {
                  SolrException.log(log, null, t);
                  if (c != null) {
                    c.close();
                  }
                }
                return c;
              }
            };
            pending.add(completionService.submit(task));

          } else {
            // Store it away for later use. includes non-transient but not
            // loaded at startup cores.
            coreMaps.putDynamicDescriptor(rawName, p);
          }
        } catch (Throwable ex) {
          SolrException.log(log, null, ex);
        }
      }
      
      while (pending != null && pending.size() > 0) {
        try {

          Future<SolrCore> future = completionService.take();
          if (future == null) return;
          pending.remove(future);

          try {
            SolrCore c = future.get();
            // track original names
            if (c != null) {
              coreMaps.putCoreToOrigName(c, c.getName());
            }
          } catch (ExecutionException e) {
            SolrException.log(SolrCore.log, "Error loading core", e);
          }
          
        } catch (InterruptedException e) {
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "interrupted while loading core", e);
        }
      }

      // Start the background thread
      backgroundCloser = new CloserThread(this, coreMaps, cfg);
      backgroundCloser.start();

    } finally {
      if (coreLoadExecutor != null) {
        ExecutorUtil.shutdownNowAndAwaitTermination(coreLoadExecutor);
      }
    }
  }

  // To make this available to TestHarness.
  protected void initShardHandler() {
    if (cfg != null) {
      cfg.initShardHandler();
    } else {
      // Cough! Hack! But tests run this way.
      HttpShardHandlerFactory fac = new HttpShardHandlerFactory();
      shardHandlerFactory = fac;
    }
  }

  private volatile boolean isShutDown = false;

  private volatile ConfigSolr cfg;
  
  public boolean isShutDown() {
    return isShutDown;
  }

  /**
   * Stops all cores.
   */
  public void shutdown() {
    log.info("Shutting down CoreContainer instance="
        + System.identityHashCode(this));
    
    if (isZooKeeperAware()) {
      try {
        zkController.publishAndWaitForDownStates();
      } catch (KeeperException e) {
        log.error("", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("", e);
      }
    }
    isShutDown = true;

    if (isZooKeeperAware()) {
      coreMaps.publishCoresAsDown(zkController);
      cancelCoreRecoveries();
    }


    try {
      // First wake up the closer thread, it'll terminate almost immediately since it checks isShutDown.
      synchronized (coreMaps.getLocker()) {
        coreMaps.getLocker().notifyAll(); // wake up anyone waiting
      }
      if (backgroundCloser != null) { // Doesn't seem right, but tests get in here without initializing the core.
        try {
          backgroundCloser.join();
        } catch (InterruptedException e) {
          ; // Don't much care if this gets interrupted
        }
      }
      // Now clear all the cores that are being operated upon.
      coreMaps.clearMaps(cfg);

      // It's still possible that one of the pending dynamic load operation is waiting, so wake it up if so.
      // Since all the pending operations queues have been drained, there should be nothing to do.
      synchronized (coreMaps.getLocker()) {
        coreMaps.getLocker().notifyAll(); // wake up the thread
      }

    } finally {
      if (shardHandlerFactory != null) {
        shardHandlerFactory.close();
      }
      
      // we want to close zk stuff last
      if (zkController != null) {
        zkController.close();
      }
      if (zkServer != null) {
        zkServer.stop();
      }
    }
  }

  public void cancelCoreRecoveries() {
    ArrayList<SolrCoreState> coreStates = new ArrayList<SolrCoreState>();
    coreMaps.addCoresToList(coreStates);

    // we must cancel without holding the cores sync
    // make sure we wait for any recoveries to stop
    for (SolrCoreState coreState : coreStates) {
      try {
        coreState.cancelRecovery();
      } catch (Throwable t) {
        SolrException.log(log, "Error canceling recovery for core", t);
      }
    }
  }
  
  @Override
  protected void finalize() throws Throwable {
    try {
      if(!isShutDown){
        log.error("CoreContainer was not shutdown prior to finalize(), indicates a bug -- POSSIBLE RESOURCE LEAK!!!  instance=" + System.identityHashCode(this));
      }
    } finally {
      super.finalize();
    }
  }

  protected SolrCore registerCore(boolean isTransientCore, String name, SolrCore core, boolean returnPrevNotClosed) {
    if( core == null ) {
      throw new RuntimeException( "Can not register a null core." );
    }
    if( name == null ||
        name.indexOf( '/'  ) >= 0 ||
        name.indexOf( '\\' ) >= 0 ){
      throw new RuntimeException( "Invalid core name: "+name );
    }

    if (zkController != null) {
      // this happens before we can receive requests
      try {
        zkController.preRegister(core.getCoreDescriptor());
      } catch (KeeperException e) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      }
    }
    
    SolrCore old = null;

    if (isShutDown) {
      core.close();
      throw new IllegalStateException("This CoreContainer has been shutdown");
    }
    if (isTransientCore) {
      old = coreMaps.putTransientCore(cfg, name, core, loader);
    } else {
      old = coreMaps.putCore(name, core);
    }
      /*
      * set both the name of the descriptor and the name of the
      * core, since the descriptors name is used for persisting.
      */

    core.setName(name);
    core.getCoreDescriptor().putProperty(CoreDescriptor.CORE_NAME, name);

    synchronized (coreInitFailures) {
      coreInitFailures.remove(name);
    }

    if( old == null || old == core) {
      log.info( "registering core: "+name );
      registerInZk(core);
      return null;
    }
    else {
      log.info( "replacing core: "+name );
      if (!returnPrevNotClosed) {
        old.close();
      }
      registerInZk(core);
      return old;
    }
  }

  private void registerInZk(SolrCore core) {
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

  /**
   * Registers a SolrCore descriptor in the registry using the core's name.
   * If returnPrev==false, the old core, if different, is closed.
   * @return a previous core having the same name if it existed and returnPrev==true
   */
  public SolrCore register(SolrCore core, boolean returnPrev) {
    return registerCore(false, core.getName(), core, returnPrev);
  }

  public SolrCore register(String name, SolrCore core, boolean returnPrev) {
    return registerCore(false, name, core, returnPrev);
  }


  // Helper method to separate out creating a core from ZK as opposed to the "usual" way. See create()
  private SolrCore createFromZk(String instanceDir, CoreDescriptor dcore)
  {
    try {
      SolrResourceLoader solrLoader = null;
      SolrConfig config = null;
      String zkConfigName = null;
      IndexSchema schema;
      String collection = dcore.getCloudDescriptor().getCollectionName();
      zkController.createCollectionZkNode(dcore.getCloudDescriptor());

      zkConfigName = zkController.readConfigName(collection);
      if (zkConfigName == null) {
        log.error("Could not find config name for collection:" + collection);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "Could not find config name for collection:" + collection);
      }
      solrLoader = new ZkSolrResourceLoader(instanceDir, zkConfigName, libLoader, SolrProperties.getCoreProperties(instanceDir, dcore), zkController);
      config = getSolrConfigFromZk(zkConfigName, dcore.getConfigName(), solrLoader);
      schema = getSchemaFromZk(zkConfigName, dcore.getSchemaName(), config);
      return new SolrCore(dcore.getName(), null, config, schema, dcore);

    } catch (KeeperException e) {
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    }
  }

  // Helper method to separate out creating a core from local configuration files. See create()
  private SolrCore createFromLocal(String instanceDir, CoreDescriptor dcore) {
    SolrResourceLoader solrLoader = null;

    SolrConfig config = null;
    solrLoader = new SolrResourceLoader(instanceDir, libLoader, SolrProperties.getCoreProperties(instanceDir, dcore));
    try {
      config = new SolrConfig(solrLoader, dcore.getConfigName(), null);
    } catch (Exception e) {
      log.error("Failed to load file {}/{}", instanceDir, dcore.getConfigName());
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not load config for " + dcore.getConfigName(), e);
    }

    IndexSchema schema = null;
    if (indexSchemaCache != null) {
      File schemaFile = new File(dcore.getSchemaName());
      if (!schemaFile.isAbsolute()) {
        schemaFile = new File(solrLoader.getInstanceDir() + "conf"
            + File.separator + dcore.getSchemaName());
      }
      if (schemaFile.exists()) {
        String key = schemaFile.getAbsolutePath()
            + ":"
            + new SimpleDateFormat("yyyyMMddHHmmss", Locale.ROOT).format(new Date(
            schemaFile.lastModified()));
        schema = indexSchemaCache.get(key);
        if (schema == null) {
          log.info("creating new schema object for core: " + dcore.getProperty(CoreDescriptor.CORE_NAME));
          schema = new IndexSchema(config, dcore.getSchemaName(), null);
          indexSchemaCache.put(key, schema);
        } else {
          log.info("re-using schema object for core: " + dcore.getProperty(CoreDescriptor.CORE_NAME));
        }
      }
    }

    if (schema == null) {
      schema = new IndexSchema(config, dcore.getSchemaName(), null);
    }

    SolrCore core = new SolrCore(dcore.getName(), null, config, schema, dcore);

    if (core.getUpdateHandler().getUpdateLog() != null) {
      // always kick off recovery if we are in standalone mode.
      core.getUpdateHandler().getUpdateLog().recoverFromLog();
    }
    return core;
  }

  /**
   * Creates a new core based on a descriptor but does not register it.
   *
   * @param dcore a core descriptor
   * @return the newly created core
   */
  public SolrCore create(CoreDescriptor dcore) {

    final String name = dcore.getName();

    try {
      // Make the instanceDir relative to the cores instanceDir if not absolute
      File idir = new File(dcore.getInstanceDir());
      String instanceDir = idir.getPath();
      log.info("Creating SolrCore '{}' using instanceDir: {}",
               dcore.getName(), instanceDir);

      // Initialize the solr config
      if (zkController != null) {
        return createFromZk(instanceDir, dcore);
      } else {
        return createFromLocal(instanceDir, dcore);
      }

      // :TODO: Java7...
      // http://docs.oracle.com/javase/7/docs/technotes/guides/language/catch-multiple.html
    } catch (Exception ex) {
      throw recordAndThrow(name, "Unable to create core: " + name, ex);
    }
  }

  /**
   * @return a Collection of registered SolrCores
   */
  public Collection<SolrCore> getCores() {
    return coreMaps.getCores();
  }

  /**
   * @return a Collection of the names that cores are mapped to
   */
  public Collection<String> getCoreNames() {
    return coreMaps.getCoreNames();
  }

  /** This method is currently experimental.
   * @return a Collection of the names that a specific core is mapped to.
   */
  public Collection<String> getCoreNames(SolrCore core) {
    return coreMaps.getCoreNames(core);
  }

  /**
   * get a list of all the cores that are currently loaded
   * @return a list of al lthe available core names in either permanent or transient core lists.
   */
  public Collection<String> getAllCoreNames() {
    return coreMaps.getAllCoreNames();

  }

  /**
   * Returns an immutable Map of Exceptions that occured when initializing 
   * SolrCores (either at startup, or do to runtime requests to create cores) 
   * keyed off of the name (String) of the SolrCore that had the Exception 
   * during initialization.
   * <p>
   * While the Map returned by this method is immutable and will not change 
   * once returned to the client, the source data used to generate this Map 
   * can be changed as various SolrCore operations are performed:
   * </p>
   * <ul>
   *  <li>Failed attempts to create new SolrCores will add new Exceptions.</li>
   *  <li>Failed attempts to re-create a SolrCore using a name already contained in this Map will replace the Exception.</li>
   *  <li>Failed attempts to reload a SolrCore will cause an Exception to be added to this list -- even though the existing SolrCore with that name will continue to be available.</li>
   *  <li>Successful attempts to re-created a SolrCore using a name already contained in this Map will remove the Exception.</li>
   *  <li>Registering an existing SolrCore with a name already contained in this Map (ie: ALIAS or SWAP) will remove the Exception.</li>
   * </ul>
   */
  public Map<String,Exception> getCoreInitFailures() {
    synchronized ( coreInitFailures ) {
      return Collections.unmodifiableMap(new LinkedHashMap<String,Exception>
                                         (coreInitFailures));
    }
  }


  // ---------------- Core name related methods --------------- 
  /**
   * Recreates a SolrCore.
   * While the new core is loading, requests will continue to be dispatched to
   * and processed by the old core
   * 
   * @param name the name of the SolrCore to reload
   */
  public void reload(String name) {
    try {
      name = checkDefault(name);

      SolrCore core = coreMaps.getCore(name);
      if (core == null)
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "No such core: " + name );

      try {
        coreMaps.waitAddPendingCoreOps(name);
        CoreDescriptor cd = core.getCoreDescriptor();

        File instanceDir = new File(cd.getInstanceDir());

        log.info("Reloading SolrCore '{}' using instanceDir: {}",
                 cd.getName(), instanceDir.getAbsolutePath());
        SolrResourceLoader solrLoader;
        if(zkController == null) {
          solrLoader = new SolrResourceLoader(instanceDir.getAbsolutePath(), libLoader, SolrProperties.getCoreProperties(instanceDir.getAbsolutePath(), cd));
        } else {
          try {
            String collection = cd.getCloudDescriptor().getCollectionName();
            zkController.createCollectionZkNode(cd.getCloudDescriptor());

            String zkConfigName = zkController.readConfigName(collection);
            if (zkConfigName == null) {
              log.error("Could not find config name for collection:" + collection);
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                                           "Could not find config name for collection:" + collection);
            }
            solrLoader = new ZkSolrResourceLoader(instanceDir.getAbsolutePath(), zkConfigName, libLoader,
                SolrProperties.getCoreProperties(instanceDir.getAbsolutePath(), cd), zkController);
          } catch (KeeperException e) {
            log.error("", e);
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                                         "", e);
          } catch (InterruptedException e) {
            // Restore the interrupted status
            Thread.currentThread().interrupt();
            log.error("", e);
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                                         "", e);
          }
        }
        SolrCore newCore = core.reload(solrLoader, core);
        // keep core to orig name link
        coreMaps.removeCoreToOrigName(newCore, core);
        registerCore(false, name, newCore, false);
      } finally {
        coreMaps.removeFromPendingOps(name);
      }
      // :TODO: Java7...
      // http://docs.oracle.com/javase/7/docs/technotes/guides/language/catch-multiple.html
    } catch (Exception ex) {
      throw recordAndThrow(name, "Unable to reload core: " + name, ex);
    }
  }

  private String checkDefault(String name) {
    return (null == name || name.isEmpty()) ? defaultCoreName : name;
  } 

  /**
   * Swaps two SolrCore descriptors.
   */
  public void swap(String n0, String n1) {
    if( n0 == null || n1 == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Can not swap unnamed cores." );
    }
    n0 = checkDefault(n0);
    n1 = checkDefault(n1);
    coreMaps.swap(n0, n1);

    log.info("swapped: "+n0 + " with " + n1);
  }
  
  /** Removes and returns registered core w/o decrementing it's reference count */
  public SolrCore remove( String name ) {
    name = checkDefault(name);    

    return coreMaps.remove(name, true);

  }

  public void rename(String name, String toName) {
    SolrCore core = getCore(name);
    try {
      if (core != null) {
        registerCore(false, toName, core, false);
        name = checkDefault(name);
        coreMaps.remove(name, false);
      }
    } finally {
      if (core != null) {
        core.close();
      }
    }
  }
  /** Gets a core by name and increase its refcount.
   * @see SolrCore#close() 
   * @param name the core name
   * @return the core if found
   */
  public SolrCore getCore(String name) {

    name = checkDefault(name);
    // Do this in two phases since we don't want to lock access to the cores over a load.
    SolrCore core = coreMaps.getCoreFromAnyList(name);

    if (core != null) {
      core.open();
      return core;
    }

    // OK, it's not presently in any list, is it in the list of dynamic cores but not loaded yet? If so, load it.
    CoreDescriptor desc = coreMaps.getDynamicDescriptor(name);
    if (desc == null) { //Nope, no transient core with this name
      return null;
    }

    // This will put an entry in pending core ops if the core isn't loaded
    core = coreMaps.waitAddPendingCoreOps(name);

    if (isShutDown) return null; // We're quitting, so stop. This needs to be after the wait above since we may come off
                                 // the wait as a consequence of shutting down.
    try {
      if (core == null) {
        core = create(desc); // This should throw an error if it fails.
        core.open();
        registerCore(desc.isTransient(), name, core, false);
      } else {
        core.open();
      }
    } catch(Exception ex){
      throw recordAndThrow(name, "Unable to create core: " + name, ex);
    } finally {
      coreMaps.removeFromPendingOps(name);
    }

    return core;
  }

  // ---------------- Multicore self related methods ---------------
  /** 
   * Creates a CoreAdminHandler for this MultiCore.
   * @return a CoreAdminHandler
   */
  protected CoreAdminHandler createMultiCoreHandler(final String adminHandlerClass) {
    // :TODO: why create a new SolrResourceLoader? why not use this.loader ???
    SolrResourceLoader loader = new SolrResourceLoader(solrHome, libLoader, null);
    return loader.newAdminHandlerInstance(CoreContainer.this, adminHandlerClass);
  }

  public CoreAdminHandler getMultiCoreHandler() {
    return coreAdminHandler;
  }
  
  public CollectionsHandler getCollectionsHandler() {
    return collectionsHandler;
  }
  
  /**
   * the default core name, or null if there is no default core name
   */
  public String getDefaultCoreName() {
    return defaultCoreName;
  }
  
  // all of the following properties aren't synchronized
  // but this should be OK since they normally won't be changed rapidly
  public boolean isPersistent() {
    return persistent;
  }
  
  public void setPersistent(boolean persistent) {
    this.persistent = persistent;
  }
  
  public String getAdminPath() {
    return adminPath;
  }
  
  public void setAdminPath(String adminPath) {
      this.adminPath = adminPath;
  }
  

  public String getManagementPath() {
    return managementPath;
  }
  
  /**
   * Sets the alternate path for multicore handling:
   * This is used in case there is a registered unnamed core (aka name is "") to
   * declare an alternate way of accessing named cores.
   * This can also be used in a pseudo single-core environment so admins can prepare
   * a new version before swapping.
   */
  public void setManagementPath(String path) {
    this.managementPath = path;
  }
  
  public LogWatcher getLogging() {
    return logging;
  }
  public void setLogging(LogWatcher v) {
    logging = v;
  }
  
  public File getConfigFile() {
    return configFile;
  }

  /**
   * Determines whether the core is already loaded or not but does NOT load the core
   *
   */
  public boolean isLoaded(String name) {
    return coreMaps.isLoaded(name);
  }

  /** Persists the cores config file in cores.xml. */
  public void persist() {
    persistFile(null);
  }

  /**
   * Gets a solr core descriptor for a core that is not loaded. Note that if the caller calls this on a
   * loaded core, the unloaded descriptor will be returned.
   *
   * @param cname - name of the unloaded core descriptor to load. NOTE:
   * @return a coreDescriptor. May return null
   */
  public CoreDescriptor getUnloadedCoreDescriptor(String cname) {
    return coreMaps.getUnloadedCoreDescriptor(cname);
  }

  /** Persists the cores config file in a user provided file. */
  public void persistFile(File file) {
    log.info("Persisting cores config to " + (file == null ? configFile : file));

    
    // <solr attrib="value">
    Map<String,String> rootSolrAttribs = new HashMap<String,String>();
    if (libDir != null) rootSolrAttribs.put("sharedLib", libDir);
    rootSolrAttribs.put("persistent", Boolean.toString(isPersistent()));
    
    // <solr attrib="value"> <cores attrib="value">
    Map<String,String> coresAttribs = new HashMap<String,String>();
    addCoresAttrib(coresAttribs, "adminPath", this.adminPath, null);
    addCoresAttrib(coresAttribs, "adminHandler", this.adminHandler, null);
    addCoresAttrib(coresAttribs, "shareSchema",
        Boolean.toString(this.shareSchema),
        Boolean.toString(DEFAULT_SHARE_SCHEMA));
    addCoresAttrib(coresAttribs, "host", this.host, null);

    if (! (null == defaultCoreName || defaultCoreName.equals("")) ) {
      coresAttribs.put("defaultCoreName", defaultCoreName);
    }

    if (transientCacheSize != Integer.MAX_VALUE) {
      coresAttribs.put("transientCacheSize", Integer.toString(transientCacheSize));
    }
    
    addCoresAttrib(coresAttribs, "hostPort", this.hostPort, DEFAULT_HOST_PORT);
    addCoresAttrib(coresAttribs, "zkClientTimeout",
        intToString(this.zkClientTimeout),
        Integer.toString(DEFAULT_ZK_CLIENT_TIMEOUT));
    addCoresAttrib(coresAttribs, "hostContext", this.hostContext, DEFAULT_HOST_CONTEXT);
    addCoresAttrib(coresAttribs, "leaderVoteWait", this.leaderVoteWait, LEADER_VOTE_WAIT);
    addCoresAttrib(coresAttribs, "coreLoadThreads", Integer.toString(this.coreLoadThreads), Integer.toString(CORE_LOAD_THREADS));

    coreMaps.persistCores(cfg, containerProperties, rootSolrAttribs, coresAttribs, file, configFile, loader);

  }
  private String intToString(Integer integer) {
    if (integer == null) return null;
    return Integer.toString(integer);
  }

  private void addCoresAttrib(Map<String,String> coresAttribs, String attribName, String attribValue, String defaultValue) {
    if (cfg == null) {
      coresAttribs.put(attribName, attribValue);
      return;
    }
    
    if (attribValue != null) {
      String rawValue = cfg.get(ConfigSolr.ConfLevel.SOLR_CORES, attribName, null);
      if (rawValue == null && defaultValue != null && attribValue.equals(defaultValue)) return;

      if (attribValue.equals(PropertiesUtil.substituteProperty(rawValue, loader.getCoreProperties()))) {
        coresAttribs.put(attribName, rawValue);
      } else {
        coresAttribs.put(attribName, attribValue);
      }
    }
  }

  public String getSolrHome() {
    return solrHome;
  }
  
  public boolean isZooKeeperAware() {
    return zkController != null;
  }
  
  public ZkController getZkController() {
    return zkController;
  }
  
  public boolean isShareSchema() {
    return shareSchema;
  }

  /** The default ShardHandlerFactory used to communicate with other solr instances */
  public ShardHandlerFactory getShardHandlerFactory() {
    return shardHandlerFactory;
  }
  
  private SolrConfig getSolrConfigFromZk(String zkConfigName, String solrConfigFileName,
      SolrResourceLoader resourceLoader)
  {
    return cfg.getSolrConfigFromZk(zkController, zkConfigName, solrConfigFileName, resourceLoader);
  }
  // Just to tidy up the code where it did this in-line.
  private SolrException recordAndThrow(String name, String msg, Exception ex) {
    synchronized (coreInitFailures) {
      coreInitFailures.remove(name);
      coreInitFailures.put(name, ex);
    }
    log.error(msg, ex);
    return new SolrException(ErrorCode.SERVER_ERROR, msg, ex);
  }

  private IndexSchema getSchemaFromZk(String zkConfigName, String schemaName,
      SolrConfig config)
      throws KeeperException, InterruptedException {
    return cfg.getSchemaFromZk(zkController, zkConfigName, schemaName, config);
  }
}


// Introducing the two new maps (transientCores and dynamicDescriptors) introduced some locking complexities. Rather
// than try to keep them all straight in the code, use this class you need to access any of:
// cores
// transientCores
// dynamicDescriptors
//


class CoreMaps {

  private static Object locker = new Object(); // for locking around manipulating any of the core maps.
  private final Map<String, SolrCore> cores = new LinkedHashMap<String, SolrCore>(); // For "permanent" cores

  //WARNING! The _only_ place you put anything into the list of transient cores is with the putTransientCore method!
  private Map<String, SolrCore> transientCores = new LinkedHashMap<String, SolrCore>(); // For "lazily loaded" cores

  private final Map<String, CoreDescriptor> dynamicDescriptors = new LinkedHashMap<String, CoreDescriptor>();

  private int transientCacheSize = Integer.MAX_VALUE;

  private Map<SolrCore, String> coreToOrigName = new ConcurrentHashMap<SolrCore, String>();

  private final CoreContainer container;

  // This map will hold objects that are being currently operated on. The core (value) may be null in the case of
  // initial load. The rule is, never to any operation on a core that is currently being operated upon.
  private static final Set<String> pendingCoreOps = new HashSet<String>();

  // Due to the fact that closes happen potentially whenever anything is _added_ to the transient core list, we need
  // to essentially queue them up to be handled via pendingCoreOps.
  private static final List<SolrCore> pendingCloses = new ArrayList<SolrCore>();

  CoreMaps(CoreContainer container) {
    this.container = container;
  }

  // Trivial helper method for load, note it implements LRU on transient cores. Also note, if
  // there is no setting for max size, nothing is done and all cores go in the regular "cores" list
  protected void allocateLazyCores(final ConfigSolr cfg, final SolrResourceLoader loader) {
    transientCacheSize = cfg.getInt(ConfigSolr.ConfLevel.SOLR_CORES, "transientCacheSize", Integer.MAX_VALUE);
    if (transientCacheSize != Integer.MAX_VALUE) {
      CoreContainer.log.info("Allocating transient cache for {} transient cores", transientCacheSize);
      transientCores = new LinkedHashMap<String, SolrCore>(transientCacheSize, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, SolrCore> eldest) {
          if (size() > transientCacheSize) {
            synchronized (locker) {
              pendingCloses.add(eldest.getValue()); // Essentially just queue this core up for closing.
              locker.notifyAll(); // Wakes up closer thread too
            }
            return true;
          }
          return false;
        }
      };
    }
  }

  protected void putDynamicDescriptor(String rawName, CoreDescriptor p) {
    synchronized (locker) {
      dynamicDescriptors.put(rawName, p);
    }
  }

  // We are shutting down. We don't want to risk deadlock, so do this manipulation the expensive way. Note, I've
  // already deadlocked with closing/opening cores while keeping locks here....
  protected void clearMaps(ConfigSolr cfg) {
    List<String> coreNames;
    List<String> transientNames;
    List<SolrCore> pendingToClose;
    synchronized (locker) {
      coreNames = new ArrayList(cores.keySet());
      transientNames = new ArrayList(transientCores.keySet());
      pendingToClose = new ArrayList(pendingCloses);
    }
    for (String coreName : coreNames) {
      SolrCore core = cores.get(coreName);
      if (core != null) {
        try {
          addPersistOneCore(cfg, core, container.loader);

          core.close();
        } catch (Throwable t) {
          SolrException.log(CoreContainer.log, "Error shutting down core", t);
        }
      }
    }
    cores.clear();

    for (String coreName : transientNames) {
      SolrCore core = transientCores.get(coreName);
      if (core != null) {
        try {
          core.close();
        } catch (Throwable t) {
          SolrException.log(CoreContainer.log, "Error shutting down core", t);
        }
      }
    }
    transientCores.clear();

    // We might have some cores that we were _thinking_ about shutting down, so take care of those too.
    for (SolrCore core : pendingToClose) {
      try {
        core.close();
      } catch (Throwable t) {
        SolrException.log(CoreContainer.log, "Error shutting down core", t);
      }
    }

  }

  protected void addCoresToList(ArrayList<SolrCoreState> coreStates) {
    List<SolrCore> addCores;
    synchronized (locker) {
      addCores = new ArrayList<SolrCore>(cores.values());
    }
    for (SolrCore core : addCores) {
      coreStates.add(core.getUpdateHandler().getSolrCoreState());
    }
  }

  //WARNING! This should be the _only_ place you put anything into the list of transient cores!
  protected SolrCore putTransientCore(ConfigSolr cfg, String name, SolrCore core, SolrResourceLoader loader) {
    SolrCore retCore;
    CoreContainer.log.info("Opening transient core {}", name);
    synchronized (locker) {
      retCore = transientCores.put(name, core);
  }
    return retCore;
  }

  protected SolrCore putCore(String name, SolrCore core) {
    synchronized (locker) {
      return cores.put(name, core);
    }
  }

  List<SolrCore> getCores() {
    List<SolrCore> lst = new ArrayList<SolrCore>();

    synchronized (locker) {
      lst.addAll(cores.values());
      return lst;
    }
  }

  Set<String> getCoreNames() {
    Set<String> set = new TreeSet<String>();

    synchronized (locker) {
      set.addAll(cores.keySet());
      set.addAll(transientCores.keySet());
    }
    return set;
  }

  List<String> getCoreNames(SolrCore core) {
    List<String> lst = new ArrayList<String>();

    synchronized (locker) {
      for (Map.Entry<String, SolrCore> entry : cores.entrySet()) {
        if (core == entry.getValue()) {
          lst.add(entry.getKey());
        }
      }
      for (Map.Entry<String, SolrCore> entry : transientCores.entrySet()) {
        if (core == entry.getValue()) {
          lst.add(entry.getKey());
        }
      }
    }
    return lst;
  }

  /**
   * Gets a list of all cores, loaded and unloaded (dynamic)
   *
   * @return all cores names, whether loaded or unloaded.
   */
  public Collection<String> getAllCoreNames() {
    Set<String> set = new TreeSet<String>();
    synchronized (locker) {
      set.addAll(cores.keySet());
      set.addAll(transientCores.keySet());
      set.addAll(dynamicDescriptors.keySet());
    }
    return set;
  }

  SolrCore getCore(String name) {

    synchronized (locker) {
      return cores.get(name);
    }
  }

  protected void swap(String n0, String n1) {

    synchronized (locker) {
      SolrCore c0 = cores.get(n0);
      SolrCore c1 = cores.get(n1);
      if (c0 == null)
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such core: " + n0);
      if (c1 == null)
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such core: " + n1);
      cores.put(n0, c1);
      cores.put(n1, c0);

      c0.setName(n1);
      c0.getCoreDescriptor().putProperty(CoreDescriptor.CORE_NAME, n1);
      c1.setName(n0);
      c1.getCoreDescriptor().putProperty(CoreDescriptor.CORE_NAME, n0);
    }

  }

  protected SolrCore remove(String name, boolean removeOrig) {

    synchronized (locker) {
      SolrCore core = cores.remove(name);
      if (removeOrig && core != null) {
        coreToOrigName.remove(core);
      }

      return core;
    }
  }

  protected void putCoreToOrigName(SolrCore c, String name) {

    synchronized (locker) {
      coreToOrigName.put(c, name);
    }

  }

  protected void removeCoreToOrigName(SolrCore newCore, SolrCore core) {

    synchronized (locker) {
      String origName = coreToOrigName.remove(core);
      if (origName != null) {
        coreToOrigName.put(newCore, origName);
      }
    }
  }

  protected SolrCore getCoreFromAnyList(String name) {
    SolrCore core;

    synchronized (locker) {
      core = cores.get(name);
      if (core != null) {
        return core;
      }

      if (dynamicDescriptors.size() == 0) {
        return null; // Nobody even tried to define any transient cores, so we're done.
      }
      // Now look for already loaded transient cores.
      return transientCores.get(name);
    }
  }

  protected CoreDescriptor getDynamicDescriptor(String name) {
    synchronized (locker) {
      return dynamicDescriptors.get(name);
    }
  }

  protected boolean isLoaded(String name) {
    synchronized (locker) {
      if (cores.containsKey(name)) {
        return true;
      }
      if (transientCores.containsKey(name)) {
        return true;
      }
    }
    return false;

  }

  protected CoreDescriptor getUnloadedCoreDescriptor(String cname) {
    synchronized (locker) {
      CoreDescriptor desc = dynamicDescriptors.get(cname);
      if (desc == null) {
        return null;
      }
      return new CoreDescriptor(desc);
    }

  }

  protected String getCoreToOrigName(SolrCore solrCore) {
    synchronized (locker) {
      return coreToOrigName.get(solrCore);
    }
  }

  protected void publishCoresAsDown(ZkController zkController) {
    synchronized (locker) {
      for (SolrCore core : cores.values()) {
        try {
          zkController.publish(core.getCoreDescriptor(), ZkStateReader.DOWN);
        } catch (KeeperException e) {
          CoreContainer.log.error("", e);
        } catch (InterruptedException e) {
          CoreContainer.log.error("", e);
        }
      }
      for (SolrCore core : transientCores.values()) {
        try {
          zkController.publish(core.getCoreDescriptor(), ZkStateReader.DOWN);
        } catch (KeeperException e) {
          CoreContainer.log.error("", e);
        } catch (InterruptedException e) {
          CoreContainer.log.error("", e);
        }
      }
    }
  }

  // Irrepressably ugly bit of the transition in SOLR-4196, but there as at least one test case that follows
  // this path, presumably it's there for a reason.
  // This is really perverse, but all we need the here is to call a couple of static methods that for back-compat
  // purposes
  public void persistCores(ConfigSolr cfg, Properties containerProperties, Map<String, String> rootSolrAttribs,
                           Map<String, String> coresAttribs, File file, File configFile, SolrResourceLoader loader) {
    // This is expensive in the maximal case, but I think necessary. It should keep a reference open to all of the
    // current cores while they are saved. Remember that especially the transient core can come and go.
    //
    // Maybe the right thing to do is keep all the core descriptors NOT in the SolrCore, but keep all of the
    // core descriptors in SolrProperties exclusively.
    // TODO: 5.0 move coreDescriptors out of SolrCore and keep them only once in SolrProperties
    //
    synchronized (locker) {
      if (cfg == null) {
        ConfigSolrXmlBackCompat.initPersistStatic();
        persistCores(cfg, cores, loader);
        persistCores(cfg, transientCores, loader);
        ConfigSolrXmlBackCompat.addPersistAllCoresStatic(containerProperties, rootSolrAttribs, coresAttribs,
            (file == null ? configFile : file));
      } else {
        cfg.initPersist();
        persistCores(cfg, cores, loader);
        persistCores(cfg, transientCores, loader);
        cfg.addPersistAllCores(containerProperties, rootSolrAttribs, coresAttribs, (file == null ? configFile : file));
      }
    }
  }
  // Wait here until any pending operations (load, unload or reload) are completed on this core.
  protected SolrCore waitAddPendingCoreOps(String name) {

    // Keep multiple threads from operating on a core at one time.
    synchronized (locker) {
      boolean pending;
      do { // Are we currently doing anything to this core? Loading, unloading, reloading?
        pending = pendingCoreOps.contains(name); // wait for the core to be done being operated upon
        if (! pending) { // Linear list, but shouldn't be too long
          for (SolrCore core : pendingCloses) {
            if (core.getName().equals(name)) {
              pending = true;
              break;
            }
          }
        }
        if (container.isShutDown()) return null; // Just stop already.

        if (pending) {
          try {
            locker.wait();
          } catch (InterruptedException e) {
            return null; // Seems best not to do anything at all if the thread is interrupted
          }
        }
      } while (pending);
      // We _really_ need to do this within the synchronized block!
      if (! container.isShutDown()) {
        if (! pendingCoreOps.add(name)) {
          CoreContainer.log.warn("Replaced an entry in pendingCoreOps {}, we should not be doing this", name);
        }
        return getCoreFromAnyList(name); // we might have been _unloading_ the core, so return the core if it was loaded.
      }
    }
    return null;
  }

  // We should always be removing the first thing in the list with our name! The idea here is to NOT do anything n
  // any core while some other operation is working on that core.
  protected void removeFromPendingOps(String name) {
    synchronized (locker) {
      if (! pendingCoreOps.remove(name)) {
        CoreContainer.log.warn("Tried to remove core {} from pendingCoreOps and it wasn't there. ", name);
      }
      locker.notifyAll();
    }
  }


  protected void persistCores(ConfigSolr cfg, Map<String, SolrCore> whichCores, SolrResourceLoader loader) {
    for (SolrCore solrCore : whichCores.values()) {
      addPersistOneCore(cfg, solrCore, loader);
    }
  }

  private void addIfNotNull(Map<String, String> coreAttribs, String key, String value) {
    if (value == null) return;
    coreAttribs.put(key, value);
  }

  protected void addPersistOneCore(ConfigSolr cfg, SolrCore solrCore, SolrResourceLoader loader) {

    CoreDescriptor dcore = solrCore.getCoreDescriptor();

    String coreName = dcore.getProperty(CoreDescriptor.CORE_NAME);

    String origCoreName = null;

    Map<String, String> coreAttribs = new HashMap<String, String>();
    Properties persistProps = new Properties();
    CloudDescriptor cd = dcore.getCloudDescriptor();
    String collection = null;
    if (cd  != null) collection = cd.getCollectionName();
    String instDir = dcore.getRawInstanceDir();

    if (cfg == null) {
      addIfNotNull(coreAttribs, CoreDescriptor.CORE_NAME, coreName);
      addIfNotNull(coreAttribs, CoreDescriptor.CORE_CONFIG, dcore.getDefaultConfigName());
      addIfNotNull(coreAttribs, CoreDescriptor.CORE_SCHEMA, dcore.getDefaultSchemaName());
      addIfNotNull(coreAttribs, CoreDescriptor.CORE_DATADIR, dcore.getProperty(CoreDescriptor.CORE_DATADIR));
      addIfNotNull(coreAttribs, CoreDescriptor.CORE_ULOGDIR, dcore.getProperty(CoreDescriptor.CORE_ULOGDIR));
      addIfNotNull(coreAttribs, CoreDescriptor.CORE_TRANSIENT, dcore.getProperty(CoreDescriptor.CORE_TRANSIENT));
      addIfNotNull(coreAttribs, CoreDescriptor.CORE_LOADONSTARTUP, dcore.getProperty(CoreDescriptor.CORE_LOADONSTARTUP));
      // we don't try and preserve sys prop defs in these

      addIfNotNull(coreAttribs, CoreDescriptor.CORE_PROPERTIES, dcore.getPropertiesName());
      // Add in any non-standard bits of data
      Set<String> std = new TreeSet<String>();

      Properties allProps = dcore.getCoreProperties();

      std.addAll(Arrays.asList(CoreDescriptor.standardPropNames));

      for (String prop : allProps.stringPropertyNames()) {
        if (! std.contains(prop)) {
          persistProps.put(prop, dcore.getProperty(prop));
        }
      }
      if (StringUtils.isNotBlank(collection) && !collection.equals(coreName)) {
        coreAttribs.put(CoreDescriptor.CORE_COLLECTION, collection);
      }

    } else {

      origCoreName = getCoreToOrigName(solrCore);

      if (origCoreName == null) {
        origCoreName = coreName;
      }
      String tmp = cfg.getCoreNameFromOrig(origCoreName, loader, coreName);
      if (tmp != null) coreName = tmp;

      coreAttribs = cfg.readCoreAttributes(origCoreName);
      persistProps = cfg.readCoreProperties(origCoreName);
      if (coreAttribs != null) {
        coreAttribs.put(CoreDescriptor.CORE_NAME, coreName);
        if (coreAttribs.containsKey(CoreDescriptor.CORE_COLLECTION)) collection = coreAttribs.get(CoreDescriptor.CORE_COLLECTION);
        if (coreAttribs.containsKey(CoreDescriptor.CORE_INSTDIR)) instDir = coreAttribs.get(CoreDescriptor.CORE_INSTDIR);
      }
      addIfNotNull(coreAttribs, CoreDescriptor.CORE_INSTDIR, dcore.getRawInstanceDir());
      coreAttribs.put(CoreDescriptor.CORE_COLLECTION, StringUtils.isNotBlank(collection) ? collection : dcore.getName());

    }

    // Default value here is same as old code.
    addIfNotNull(coreAttribs, CoreDescriptor.CORE_INSTDIR, instDir);

    // Emulating the old code, just overwrite shard and roles if present in the cloud descriptor
    if (cd != null) {
      addIfNotNull(coreAttribs, CoreDescriptor.CORE_SHARD, cd.getShardId());
      addIfNotNull(coreAttribs, CoreDescriptor.CORE_ROLES, cd.getRoles());
    }
    coreAttribs.put(CoreDescriptor.CORE_LOADONSTARTUP, Boolean.toString(dcore.isLoadOnStartup()));
    coreAttribs.put(CoreDescriptor.CORE_TRANSIENT, Boolean.toString(dcore.isTransient()));

    // Now add back in any implicit properties that aren't in already. These are all "attribs" in this meaning
    Properties implicit = dcore.initImplicitProperties();

    if (! coreName.equals(container.getDefaultCoreName())) {
      for (String prop : implicit.stringPropertyNames()) {
        if (coreAttribs.get(prop) == null) {
          coreAttribs.put(prop, implicit.getProperty(prop));
        }
      }
    }
    if (cfg != null) {
      cfg.addPersistCore(coreName, persistProps, coreAttribs);
    } else {
      // Another awkward bit for back-compat for SOLR-4196
      ConfigSolrXmlBackCompat.addPersistCore(persistProps, coreAttribs);
    }
  }

  protected Object getLocker() { return locker; }

  // Be a little careful. We don't want to either open or close a core unless it's _not_ being opened or closed by
  // another thread. So within this lock we'll walk along the list of pending closes until we find something NOT in
  // the list of threads currently being loaded or reloaded. The "usual" case will probably return the very first
  // one anyway..
  protected SolrCore getCoreToClose() {
    synchronized (locker) {
      for (SolrCore core : pendingCloses) {
        if (! pendingCoreOps.contains(core.getName())) {
          pendingCoreOps.add(core.getName());
          pendingCloses.remove(core);
          return core;
        }
      }
    }
    return null;
  }


}

class CloserThread extends Thread {
  CoreContainer container;
  CoreMaps coreMaps;
  ConfigSolr cfg;


  CloserThread(CoreContainer container, CoreMaps coreMaps, ConfigSolr cfg) {
    this.container = container;
    this.coreMaps = coreMaps;
    this.cfg = cfg;
  }

  // It's important that this be the _only_ thread removing things from pendingDynamicCloses!
  // This is single-threaded, but I tried a multi-threaded approach and didn't see any performance gains, so
  // there's no good justification for the complexity. I suspect that the locking on things like DefaultSolrCoreState
  // essentially create a single-threaded process anyway.
  @Override
  public void run() {
    while (! container.isShutDown()) {
      synchronized (coreMaps.getLocker()) { // need this so we can wait and be awoken.
        try {
          coreMaps.getLocker().wait();
        } catch (InterruptedException e) {
          // Well, if we've been told to stop, we will. Otherwise, continue on and check to see if there are
          // any cores to close.
        }
      }
      for (SolrCore removeMe = coreMaps.getCoreToClose();
           removeMe != null && !container.isShutDown();
           removeMe = coreMaps.getCoreToClose()) {
        try {
          coreMaps.addPersistOneCore(cfg, removeMe, container.loader);
          removeMe.close();
        } finally {
          coreMaps.removeFromPendingOps(removeMe.getName());
        }
      }
    }
  }
}
