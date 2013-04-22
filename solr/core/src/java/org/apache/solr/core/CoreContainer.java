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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.IOUtils;
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
import org.apache.solr.logging.log4j.Log4jWatcher;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.PropertiesUtil;
import org.apache.solr.util.SystemIdResolver;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;


/**
 *
 * @since solr 1.3
 */
public class CoreContainer
{
  private static final String LEADER_VOTE_WAIT = "180000";  // 3 minutes
  private static final int CORE_LOAD_THREADS = 3;
  /** @deprecated will be remove in Solr 5.0 (SOLR-4622) */
  private static final String DEFAULT_HOST_CONTEXT = "solr";
  /** @deprecated will be remove in Solr 5.0 (SOLR-4622) */
  private static final String DEFAULT_HOST_PORT = "8983";
  private static final int DEFAULT_ZK_CLIENT_TIMEOUT = 15000;
  public static final String DEFAULT_DEFAULT_CORE_NAME = "collection1";
  private static final boolean DEFAULT_SHARE_SCHEMA = false;

  protected static Logger log = LoggerFactory.getLogger(CoreContainer.class);


  private final SolrCores solrCores = new SolrCores(this);

  protected final Map<String,Exception> coreInitFailures =
    Collections.synchronizedMap(new LinkedHashMap<String,Exception>());
  
  protected boolean persistent = false;
  protected String adminPath = null;
  protected volatile String managementPath = null;
  protected String hostPort;
  protected String hostContext;
  protected String host;
  protected CoreAdminHandler coreAdminHandler = null;
  protected CollectionsHandler collectionsHandler = null;
  protected File configFile = null;
  protected String libDir = null;
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
  private int transientCacheSize = Integer.MAX_VALUE;

  private String leaderVoteWait = LEADER_VOTE_WAIT;
  private int distribUpdateConnTimeout = 0;
  private int distribUpdateSoTimeout = 0;
  private int coreLoadThreads;
  private CloserThread backgroundCloser = null;
  protected volatile ConfigSolr cfg;
  private Config origCfg;
  
  {
    log.info("New CoreContainer " + System.identityHashCode(this));
  }

  /**
   * Deprecated
   * @deprecated use the single arg constructor with locateSolrHome()
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

    // core container instantiation
    public CoreContainer initialize() throws FileNotFoundException {
      CoreContainer cores = null;
      String solrHome = SolrResourceLoader.locateSolrHome();
      // ContainerConfigFilename could  could be a properties file
      File fconf = new File(solrHome, "solr.xml");

      log.info("looking for solr config file: " + fconf.getAbsolutePath());
      cores = new CoreContainer(solrHome);

      // Either we have a config file or not.
      
      if (fconf.exists()) {
        cores.load(solrHome, fconf);
      } else {
        // Back compart support
        log.info("no solr.xml found. using default old-style solr.xml");
        try {
          cores.load(solrHome, new ByteArrayInputStream(ConfigSolrXmlOld.DEF_SOLR_XML.getBytes("UTF-8")), null);
        } catch (Exception e) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "CoreContainer.Initialize failed when trying to load default solr.xml file", e);
        }
        cores.configFile = fconf;
      }
      
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
      this.load(dir, in,  configFile.getName());
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
  protected void load(String dir, InputStream is, String fileName)  {
    ThreadPoolExecutor coreLoadExecutor = null;
    if (null == dir) {
      // don't rely on SolrResourceLoader(), determine explicitly first
      dir = SolrResourceLoader.locateSolrHome();
    }
    log.info("Loading CoreContainer using Solr Home: '{}'", dir);
    
    this.loader = new SolrResourceLoader(dir);
    solrHome = loader.getInstanceDir();

    //ConfigSolr cfg;
    
    // keep orig config for persist to consult. TODO: Remove this silly stuff for 5.0, persistence not supported.
    try {
      Config config = new Config(loader, null, new InputSource(is), null, false);
      this.origCfg = new Config(loader, null, copyDoc(config.getDocument()));
      boolean oldStyle = (config.getNode("solr/cores", false) != null);
      
      if (oldStyle) {
        this.cfg = new ConfigSolrXmlOld(config, this);
      } else {
        this.cfg = new ConfigSolrXml(config, this);
      }
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "", e);
    }
    // Since the cores var is now initialized to null, let's set it up right
    // now.
    cfg.substituteProperties();

    // add the sharedLib to the shared resource loader before initializing cfg based plugins
    libDir = cfg.get(ConfigSolr.CfgProp.SOLR_SHAREDLIB , null);
    if (libDir != null) {
      File f = FileUtils.resolvePath(new File(dir), libDir);
      log.info("loading shared library: " + f.getAbsolutePath());
      loader.addToClassLoader(libDir);
      loader.reloadLuceneSPI();
    }

    shardHandlerFactory = initShardHandler(cfg);

    solrCores.allocateLazyCores(cfg, loader);

    // Initialize Logging
    if (cfg.getBool(ConfigSolr.CfgProp.SOLR_LOGGING_ENABLED, true)) {
      String slf4jImpl = null;
      String fname = cfg.get(ConfigSolr.CfgProp.SOLR_LOGGING_CLASS, null);
      try {
        slf4jImpl = StaticLoggerBinder.getSingleton()
            .getLoggerFactoryClassStr();
        if (fname == null) {
          if (slf4jImpl.indexOf("Log4j") > 0) {
            fname = "Log4j";
          } else if (slf4jImpl.indexOf("JDK") > 0) {
            fname = "JUL";
          }
        }
      } catch (Throwable ex) {
        log.warn("Unable to read SLF4J version.  LogWatcher will be disabled: " + ex);
      }
      
      // Now load the framework
      if (fname != null) {
        if ("JUL".equalsIgnoreCase(fname)) {
          logging = new JulWatcher(slf4jImpl);
        }
        else if( "Log4j".equals(fname) ) {
          logging = new Log4jWatcher(slf4jImpl);
        } else {
          try {
            logging = loader.newInstance(fname, LogWatcher.class);
          } catch (Throwable e) {
            log.warn("Unable to load LogWatcher", e);
          }
        }
        
        if (logging != null) {
          ListenerConfig v = new ListenerConfig();
          v.size = cfg.getInt(ConfigSolr.CfgProp.SOLR_LOGGING_WATCHER_SIZE, 50);
          v.threshold = cfg.get(ConfigSolr.CfgProp.SOLR_LOGGING_WATCHER_THRESHOLD, null);
          if (v.size > 0) {
            log.info("Registering Log Listener");
            logging.registerListener(v, this);
          }
        }
      }
    }


    if (cfg instanceof ConfigSolrXmlOld) { //TODO: Remove for 5.0
      String dcoreName = cfg.get(ConfigSolr.CfgProp.SOLR_CORES_DEFAULT_CORE_NAME, null);
      if (dcoreName != null && !dcoreName.isEmpty()) {
        defaultCoreName = dcoreName;
      }
      persistent = cfg.getBool(ConfigSolr.CfgProp.SOLR_PERSISTENT, false);
      adminPath = cfg.get(ConfigSolr.CfgProp.SOLR_ADMINPATH, "/admin/cores");
    } else {
      adminPath = "/admin/cores";
    }
    zkHost = cfg.get(ConfigSolr.CfgProp.SOLR_ZKHOST, null);
    coreLoadThreads = cfg.getInt(ConfigSolr.CfgProp.SOLR_CORELOADTHREADS, CORE_LOAD_THREADS);
    

    shareSchema = cfg.getBool(ConfigSolr.CfgProp.SOLR_SHARESCHEMA, DEFAULT_SHARE_SCHEMA);
    zkClientTimeout = cfg.getInt(ConfigSolr.CfgProp.SOLR_ZKCLIENTTIMEOUT, DEFAULT_ZK_CLIENT_TIMEOUT);
    
    distribUpdateConnTimeout = cfg.getInt(ConfigSolr.CfgProp.SOLR_DISTRIBUPDATECONNTIMEOUT, 0);
    distribUpdateSoTimeout = cfg.getInt(ConfigSolr.CfgProp.SOLR_DISTRIBUPDATESOTIMEOUT, 0);

    // Note: initZooKeeper will apply hardcoded default if cloud mode
    hostPort = cfg.get(ConfigSolr.CfgProp.SOLR_HOSTPORT, null);
    // Note: initZooKeeper will apply hardcoded default if cloud mode
    hostContext = cfg.get(ConfigSolr.CfgProp.SOLR_HOSTCONTEXT, null);

    host = cfg.get(ConfigSolr.CfgProp.SOLR_HOST, null);
    
    leaderVoteWait = cfg.get(ConfigSolr.CfgProp.SOLR_LEADERVOTEWAIT, LEADER_VOTE_WAIT);

    adminHandler = cfg.get(ConfigSolr.CfgProp.SOLR_ADMINHANDLER, null);
    managementPath = cfg.get(ConfigSolr.CfgProp.SOLR_MANAGEMENTPATH, null);

    transientCacheSize = cfg.getInt(ConfigSolr.CfgProp.SOLR_TRANSIENTCACHESIZE, Integer.MAX_VALUE);

    if (shareSchema) {
      indexSchemaCache = new ConcurrentHashMap<String,IndexSchema>();
    }

    zkClientTimeout = Integer.parseInt(System.getProperty("zkClientTimeout",
        Integer.toString(zkClientTimeout)));
    initZooKeeper(zkHost, zkClientTimeout);
    
    if (isZooKeeperAware() && coreLoadThreads <= 1) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
          "SolrCloud requires a value of at least 2 in solr.xml for coreLoadThreads");
    }
    
    if (adminPath != null) {
      if (adminHandler == null) {
        coreAdminHandler = new CoreAdminHandler(this);
      } else {
        coreAdminHandler = this.createMultiCoreHandler(adminHandler);
      }
    }
    
    collectionsHandler = new CollectionsHandler(this);
    containerProperties = cfg.getSolrProperties("solr");

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
            solrCores.putDynamicDescriptor(rawName, p);
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
              solrCores.putCoreToOrigName(c, c.getName());
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
      backgroundCloser = new CloserThread(this, solrCores, cfg);
      backgroundCloser.start();

    } finally {
      if (coreLoadExecutor != null) {
        ExecutorUtil.shutdownNowAndAwaitTermination(coreLoadExecutor);
      }
    }
  }
  
  private ShardHandlerFactory initShardHandler(ConfigSolr configSolr) {
    PluginInfo info = null;
    Node shfn = configSolr.getConfig().getNode("solr/cores/shardHandlerFactory", false);

    if (shfn != null) {
      info = new PluginInfo(shfn, "shardHandlerFactory", false, true);
    } else {
      Map m = new HashMap();
      m.put("class", HttpShardHandlerFactory.class.getName());
      info = new PluginInfo("shardHandlerFactory", m, null, Collections.<PluginInfo>emptyList());
    }

    ShardHandlerFactory fac;
    try {
       fac = configSolr.getConfig().getResourceLoader().findClass(info.className, ShardHandlerFactory.class).newInstance();
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                              "Error instantiating shardHandlerFactory class " + info.className);
    }
    if (fac instanceof PluginInfoInitialized) {
      ((PluginInfoInitialized) fac).init(info);
    }
    return fac;
  }

  // To make this available to TestHarness.
  protected void initShardHandler() {
    if (cfg != null) {
      initShardHandler(cfg);
    } else {
      // Cough! Hack! But tests run this way.
      HttpShardHandlerFactory fac = new HttpShardHandlerFactory();
      shardHandlerFactory = fac;
    }
  }

  private volatile boolean isShutDown = false;
  
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
      publishCoresAsDown();
      cancelCoreRecoveries();
    }


    try {
      // First wake up the closer thread, it'll terminate almost immediately since it checks isShutDown.
      synchronized (solrCores.getModifyLock()) {
        solrCores.getModifyLock().notifyAll(); // wake up anyone waiting
      }
      if (backgroundCloser != null) { // Doesn't seem right, but tests get in here without initializing the core.
        try {
          backgroundCloser.join();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          if (log.isDebugEnabled()) {
            log.debug("backgroundCloser thread was interrupted before finishing");
          }
        }
      }
      // Now clear all the cores that are being operated upon.
      solrCores.close();

      // It's still possible that one of the pending dynamic load operation is waiting, so wake it up if so.
      // Since all the pending operations queues have been drained, there should be nothing to do.
      synchronized (solrCores.getModifyLock()) {
        solrCores.getModifyLock().notifyAll(); // wake up the thread
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
    solrCores.addCoresToList(coreStates);

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
        zkController.preRegister(core);
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
      old = solrCores.putTransientCore(cfg, name, core, loader);
    } else {
      old = solrCores.putCore(name, core);
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
      solrLoader = new ZkSolrResourceLoader(instanceDir, zkConfigName, loader.getClassLoader(),
          ConfigSolrXml.getCoreProperties(instanceDir, dcore), zkController);
      config = getSolrConfigFromZk(zkConfigName, dcore.getConfigName(), solrLoader);
      schema = IndexSchemaFactory.buildIndexSchema(dcore.getSchemaName(), config);
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
    solrLoader = new SolrResourceLoader(instanceDir, loader.getClassLoader(), ConfigSolrXml.getCoreProperties(instanceDir, dcore));
    try {
      config = new SolrConfig(solrLoader, dcore.getConfigName(), null);
    } catch (Exception e) {
      log.error("Failed to load file {}", new File(instanceDir, dcore.getConfigName()).getAbsolutePath());
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not load config for " + dcore.getConfigName(), e);
    }

    IndexSchema schema = null;
    if (indexSchemaCache != null) {
      final String resourceNameToBeUsed = IndexSchemaFactory.getResourceNameToBeUsed(dcore.getSchemaName(), config);
      File schemaFile = new File(resourceNameToBeUsed);
      if (!schemaFile.isAbsolute()) {
        schemaFile = new File(solrLoader.getConfigDir(), schemaFile.getPath());
      }
      if (schemaFile.exists()) {
        String key = schemaFile.getAbsolutePath()
            + ":"
            + new SimpleDateFormat("yyyyMMddHHmmss", Locale.ROOT).format(new Date(
            schemaFile.lastModified()));
        schema = indexSchemaCache.get(key);
        if (schema == null) {
          log.info("creating new schema object for core: " + dcore.getProperty(CoreDescriptor.CORE_NAME));
          schema = IndexSchemaFactory.buildIndexSchema(dcore.getSchemaName(), config);
          indexSchemaCache.put(key, schema);
        } else {
          log.info("re-using schema object for core: " + dcore.getProperty(CoreDescriptor.CORE_NAME));
        }
      }
    }

    if (schema == null) {
      schema = IndexSchemaFactory.buildIndexSchema(dcore.getSchemaName(), config);
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

    if (isShutDown) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Solr has shutdown.");
    }
    
    final String name = dcore.getName();

    try {
      // Make the instanceDir relative to the cores instanceDir if not absolute
      File idir = new File(dcore.getInstanceDir());
      String instanceDir = idir.getPath();
      log.info("Creating SolrCore '{}' using instanceDir: {}",
               dcore.getName(), instanceDir);

      // Initialize the solr config
      SolrCore created = null;
      if (zkController != null) {
        created = createFromZk(instanceDir, dcore);
      } else {
        created = createFromLocal(instanceDir, dcore);
      }

      solrCores.addCreated(created); // For persisting newly-created cores.
      return created;

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
    return solrCores.getCores();
  }

  /**
   * @return a Collection of the names that cores are mapped to
   */
  public Collection<String> getCoreNames() {
    return solrCores.getCoreNames();
  }

  /** This method is currently experimental.
   * @return a Collection of the names that a specific core is mapped to.
   */
  public Collection<String> getCoreNames(SolrCore core) {
    return solrCores.getCoreNames(core);
  }

  /**
   * get a list of all the cores that are currently loaded
   * @return a list of al lthe available core names in either permanent or transient core lists.
   */
  public Collection<String> getAllCoreNames() {
    return solrCores.getAllCoreNames();

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

      SolrCore core = solrCores.getCore(name);
      if (core == null)
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "No such core: " + name );

      try {
        solrCores.waitAddPendingCoreOps(name);
        CoreDescriptor cd = core.getCoreDescriptor();

        File instanceDir = new File(cd.getInstanceDir());

        log.info("Reloading SolrCore '{}' using instanceDir: {}",
                 cd.getName(), instanceDir.getAbsolutePath());
        SolrResourceLoader solrLoader;
        if(zkController == null) {
          solrLoader = new SolrResourceLoader(instanceDir.getAbsolutePath(), loader.getClassLoader(), ConfigSolrXml.getCoreProperties(instanceDir.getAbsolutePath(), cd));
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
            solrLoader = new ZkSolrResourceLoader(instanceDir.getAbsolutePath(), zkConfigName, loader.getClassLoader(),
                ConfigSolrXml.getCoreProperties(instanceDir.getAbsolutePath(), cd), zkController);
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
        solrCores.removeCoreToOrigName(newCore, core);
        registerCore(false, name, newCore, false);
      } finally {
        solrCores.removeFromPendingOps(name);
      }
      // :TODO: Java7...
      // http://docs.oracle.com/javase/7/docs/technotes/guides/language/catch-multiple.html
    } catch (Exception ex) {
      throw recordAndThrow(name, "Unable to reload core: " + name, ex);
    }
  }

  //5.0 remove all checkDefaults?
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
    solrCores.swap(n0, n1);

    log.info("swapped: "+n0 + " with " + n1);
  }
  
  /** Removes and returns registered core w/o decrementing it's reference count */
  public SolrCore remove( String name ) {
    name = checkDefault(name);    

    return solrCores.remove(name, true);

  }

  public void rename(String name, String toName) {
    SolrCore core = getCore(name);
    try {
      if (core != null) {
        registerCore(false, toName, core, false);
        name = checkDefault(name);
        solrCores.remove(name, false);
      }
    } finally {
      if (core != null) {
        core.close();
      }
    }
  }
  /** 
   * Gets a core by name and increase its refcount.
   *
   * @see SolrCore#close() 
   * @param name the core name
   * @return the core if found, null if a SolrCore by this name does not exist
   * @exception SolrException if a SolrCore with this name failed to be initialized
   */
  public SolrCore getCore(String name) {

    name = checkDefault(name);

    // Do this in two phases since we don't want to lock access to the cores over a load.
    SolrCore core = solrCores.getCoreFromAnyList(name);

    if (core != null) {
      core.open();
      return core;
    }

    // OK, it's not presently in any list, is it in the list of dynamic cores but not loaded yet? If so, load it.
    CoreDescriptor desc = solrCores.getDynamicDescriptor(name);
    if (desc == null) { //Nope, no transient core with this name
      
      // if there was an error initalizing this core, throw a 500
      // error with the details for clients attempting to access it.
      Exception e = getCoreInitFailures().get(name);
      if (null != e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "SolrCore '" + name +
                                "' is not available due to init failure: " +
                                e.getMessage(), e);
      }
      // otherwise the user is simply asking for something that doesn't exist.
      return null;
    }

    // This will put an entry in pending core ops if the core isn't loaded
    core = solrCores.waitAddPendingCoreOps(name);

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
      solrCores.removeFromPendingOps(name);
    }

    return core;
  }

  // ---------------- Multicore self related methods ---------------
  /** 
   * Creates a CoreAdminHandler for this MultiCore.
   * @return a CoreAdminHandler
   */
  protected CoreAdminHandler createMultiCoreHandler(final String adminHandlerClass) {
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
  @Deprecated
  public boolean isPersistent() {
    return persistent;
  }
  
  @Deprecated
  public void setPersistent(boolean persistent) {
    this.persistent = persistent;
  }
  
  public String getAdminPath() {
    return adminPath;
  }

  public String getHostPort() {
    return hostPort;
  }

  public String getHostContext() {
    return hostContext;
  }

  public String getHost() {
    return host;
  }

  public int getZkClientTimeout() {
    return zkClientTimeout;
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
    return solrCores.isLoaded(name);
  }

  /** Persists the cores config file in cores.xml. */
  public void persist() {
    persistFile(configFile);
  }

  /**
   * Gets a solr core descriptor for a core that is not loaded. Note that if the caller calls this on a
   * loaded core, the unloaded descriptor will be returned.
   *
   * @param cname - name of the unloaded core descriptor to load. NOTE:
   * @return a coreDescriptor. May return null
   */
  public CoreDescriptor getUnloadedCoreDescriptor(String cname) {
    return solrCores.getUnloadedCoreDescriptor(cname);
  }

  /** Persists the cores config file in a user provided file. */
  //TODO: obsolete in SOLR 5.0
  public void persistFile(File file) {
    assert file != null;
    // only the old solrxml persists
    if (cfg != null && !(cfg instanceof ConfigSolrXmlOld)) return;

    log.info("Persisting cores config to " + (file == null ? configFile : file));

    
    // <solr attrib="value">
    Map<String,String> rootSolrAttribs = new HashMap<String,String>();
    if (libDir != null) rootSolrAttribs.put("sharedLib", libDir);
    rootSolrAttribs.put("persistent", Boolean.toString(isPersistent()));
    
    // <solr attrib="value"> <cores attrib="value">
    Map<String,String> coresAttribs = new HashMap<String,String>();
    addCoresAttrib(coresAttribs, ConfigSolr.CfgProp.SOLR_ADMINPATH, "adminPath", this.adminPath, null);
    addCoresAttrib(coresAttribs, ConfigSolr.CfgProp.SOLR_ADMINHANDLER, "adminHandler", this.adminHandler, null);
    addCoresAttrib(coresAttribs, ConfigSolr.CfgProp.SOLR_SHARESCHEMA,"shareSchema",
        Boolean.toString(this.shareSchema),
        Boolean.toString(DEFAULT_SHARE_SCHEMA));
    addCoresAttrib(coresAttribs, ConfigSolr.CfgProp.SOLR_HOST, "host", this.host, null);

    if (! (null == defaultCoreName || defaultCoreName.equals("")) ) {
      coresAttribs.put("defaultCoreName", defaultCoreName);
    }

    addCoresAttrib(coresAttribs, ConfigSolr.CfgProp.SOLR_HOSTPORT, "hostPort", this.hostPort, DEFAULT_HOST_PORT);
    addCoresAttrib(coresAttribs, ConfigSolr.CfgProp.SOLR_ZKCLIENTTIMEOUT, "zkClientTimeout",
        intToString(this.zkClientTimeout),
        Integer.toString(DEFAULT_ZK_CLIENT_TIMEOUT));
    addCoresAttrib(coresAttribs, ConfigSolr.CfgProp.SOLR_HOSTCONTEXT, "hostContext",
        this.hostContext, DEFAULT_HOST_CONTEXT);
    addCoresAttrib(coresAttribs, ConfigSolr.CfgProp.SOLR_LEADERVOTEWAIT, "leaderVoteWait",
        this.leaderVoteWait, LEADER_VOTE_WAIT);
    addCoresAttrib(coresAttribs, ConfigSolr.CfgProp.SOLR_CORELOADTHREADS, "coreLoadThreads",
        Integer.toString(this.coreLoadThreads), Integer.toString(CORE_LOAD_THREADS));
    if (transientCacheSize != Integer.MAX_VALUE) { // This test
    // is a consequence of testing. I really hate it.
      addCoresAttrib(coresAttribs, ConfigSolr.CfgProp.SOLR_TRANSIENTCACHESIZE, "transientCacheSize",
          Integer.toString(this.transientCacheSize), Integer.toString(Integer.MAX_VALUE));
    }

    try {
      solrCores.persistCores(origCfg, containerProperties, rootSolrAttribs, coresAttribs, file, configFile, loader);
    } catch (XPathExpressionException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, null, e);
    }

  }
  private String intToString(Integer integer) {
    if (integer == null) return null;
    return Integer.toString(integer);
  }

  //TODO: Obsolete in 5.0 Having to pass prop is a hack to get us to 5.0.
  private void addCoresAttrib(Map<String,String> coresAttribs, ConfigSolr.CfgProp prop,
                              String attribName, String attribValue, String defaultValue) {
    if (cfg == null) {
      coresAttribs.put(attribName, attribValue);
      return;
    }
    
    if (attribValue != null) {
      String origValue = cfg.getOrigProp(prop, null);
      
      if (origValue == null && defaultValue != null && attribValue.equals(defaultValue)) return;

      if (attribValue.equals(PropertiesUtil.substituteProperty(origValue, loader.getCoreProperties()))) {
        coresAttribs.put(attribName, origValue);
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
  
  // Just to tidy up the code where it did this in-line.
  private SolrException recordAndThrow(String name, String msg, Exception ex) {
    synchronized (coreInitFailures) {
      coreInitFailures.remove(name);
      coreInitFailures.put(name, ex);
    }
    log.error(msg, ex);
    return new SolrException(ErrorCode.SERVER_ERROR, msg, ex);
  }
  
  String getCoreToOrigName(SolrCore core) {
    return solrCores.getCoreToOrigName(core);
  }
  
  private Document copyDoc(Document document) throws TransformerException {
    TransformerFactory tfactory = TransformerFactory.newInstance();
    Transformer tx   = tfactory.newTransformer();
    DOMSource source = new DOMSource(document);
    DOMResult result = new DOMResult();
    tx.transform(source,result);
    return (Document)result.getNode();
  }

  protected void publishCoresAsDown() {
    List<SolrCore> cores = solrCores.getCores();
    
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
}

class CloserThread extends Thread {
  CoreContainer container;
  SolrCores solrCores;
  ConfigSolr cfg;


  CloserThread(CoreContainer container, SolrCores solrCores, ConfigSolr cfg) {
    this.container = container;
    this.solrCores = solrCores;
    this.cfg = cfg;
  }

  // It's important that this be the _only_ thread removing things from pendingDynamicCloses!
  // This is single-threaded, but I tried a multi-threaded approach and didn't see any performance gains, so
  // there's no good justification for the complexity. I suspect that the locking on things like DefaultSolrCoreState
  // essentially create a single-threaded process anyway.
  @Override
  public void run() {
    while (! container.isShutDown()) {
      synchronized (solrCores.getModifyLock()) { // need this so we can wait and be awoken.
        try {
          solrCores.getModifyLock().wait();
        } catch (InterruptedException e) {
          // Well, if we've been told to stop, we will. Otherwise, continue on and check to see if there are
          // any cores to close.
        }
      }
      for (SolrCore removeMe = solrCores.getCoreToClose();
           removeMe != null && !container.isShutDown();
           removeMe = solrCores.getCoreToClose()) {
        try {
          removeMe.close();
        } finally {
          solrCores.removeFromPendingOps(removeMe.getName());
        }
      }
    }
  }
  
}
