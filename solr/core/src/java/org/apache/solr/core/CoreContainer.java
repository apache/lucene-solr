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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.IOUtils;
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
import org.apache.solr.core.SolrXMLSerializer.SolrCoreXMLDef;
import org.apache.solr.core.SolrXMLSerializer.SolrXMLDef;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.logging.ListenerConfig;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.jul.JulWatcher;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.util.AdjustableSemaphore;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.SystemIdResolver;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


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
  
  // solr.xml node constants
  private static final String CORE_NAME = "name";
  private static final String CORE_CONFIG = "config";
  private static final String CORE_INSTDIR = "instanceDir";
  private static final String CORE_DATADIR = "dataDir";
  private static final String CORE_SCHEMA = "schema";
  private static final String CORE_SHARD = "shard";
  private static final String CORE_COLLECTION = "collection";
  private static final String CORE_ROLES = "roles";
  private static final String CORE_PROPERTIES = "properties";


  protected final Map<String, SolrCore> cores = new LinkedHashMap<String, SolrCore>();

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
  private SolrXMLSerializer solrXMLSerializer = new SolrXMLSerializer();
  private ZkController zkController;
  private SolrZkServer zkServer;
  private ShardHandlerFactory shardHandlerFactory;
  protected LogWatcher logging = null;
  private String zkHost;
  private Map<SolrCore,String> coreToOrigName = new ConcurrentHashMap<SolrCore,String>();
  private String leaderVoteWait;
  private int coreLoadThreads;
  
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
  public CoreContainer(String dir, File configFile) throws ParserConfigurationException, IOException, SAXException
  {
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
        zkController = new ZkController(this, zookeeperHost, zkClientTimeout, zkClientConnectTimeout, host, hostPort, hostContext, leaderVoteWait, new CurrentCoreDescriptorProvider() {
          
          @Override
          public List<CoreDescriptor> getCurrentDescriptors() {
            List<CoreDescriptor> descriptors = new ArrayList<CoreDescriptor>(getCoreNames().size());
            for (SolrCore core : getCores()) {
              descriptors.add(core.getCoreDescriptor());
            }
            return descriptors;
          }
        });        

        String confDir = System.getProperty("bootstrap_confdir");
        boolean boostrapConf = Boolean.getBoolean("bootstrap_conf");
        
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
    protected String containerConfigFilename = null;  // normally "solr.xml"
    protected String dataDir = null; // override datadir for single core mode

    // core container instantiation
    public CoreContainer initialize() throws IOException,
        ParserConfigurationException, SAXException {
      CoreContainer cores = null;
      String solrHome = SolrResourceLoader.locateSolrHome();
      File fconf = new File(solrHome, containerConfigFilename == null ? "solr.xml"
          : containerConfigFilename);
      log.info("looking for solr.xml: " + fconf.getAbsolutePath());
      cores = new CoreContainer(solrHome);
      
      if (fconf.exists()) {
        cores.load(solrHome, fconf);
      } else {
        log.info("no solr.xml file found - using default");
        cores.load(solrHome, new InputSource(new ByteArrayInputStream(DEF_SOLR_XML.getBytes("UTF-8"))));
        cores.configFile = fconf;
      }
      
      containerConfigFilename = cores.getConfigFile().getName();
      
      return cores;
    }
  }

  static Properties getCoreProps(String instanceDir, String file, Properties defaults) {
    if(file == null) file = "conf"+File.separator+ "solrcore.properties";
    File corePropsFile = new File(file);
    if(!corePropsFile.isAbsolute()){
      corePropsFile = new File(instanceDir, file);
    }
    Properties p = defaults;
    if (corePropsFile.exists() && corePropsFile.isFile()) {
      p = new Properties(defaults);
      InputStream is = null;
      try {
        is = new FileInputStream(corePropsFile);
        p.load(is);
      } catch (IOException e) {
        log.warn("Error loading properties ",e);
      } finally{
        IOUtils.closeQuietly(is);        
      }
    }
    return p;
  }



  //-------------------------------------------------------------------
  // Initialization / Cleanup
  //-------------------------------------------------------------------
  
  /**
   * Load a config file listing the available solr cores.
   * @param dir the home directory of all resources.
   * @param configFile the configuration file
   */
  public void load(String dir, File configFile ) throws ParserConfigurationException, IOException, SAXException {
    this.configFile = configFile;
    this.load(dir, new InputSource(configFile.toURI().toASCIIString()));
  } 

  /**
   * Load a config file listing the available solr cores.
   * 
   * @param dir the home directory of all resources.
   * @param cfgis the configuration file InputStream
   */
  public void load(String dir, InputSource cfgis)
      throws ParserConfigurationException, IOException, SAXException {
    ThreadPoolExecutor coreLoadExecutor = null;
    if (null == dir) {
      // don't rely on SolrResourceLoader(), determine explicitly first
      dir = SolrResourceLoader.locateSolrHome();
    }
    log.info("Loading CoreContainer using Solr Home: '{}'", dir);
    
    this.loader = new SolrResourceLoader(dir);
    solrHome = loader.getInstanceDir();
    
    Config cfg = new Config(loader, null, cfgis, null, false);
    
    // keep orig config for persist to consult
    try {
      this.cfg = new Config(loader, null, copyDoc(cfg.getDocument()));
    } catch (TransformerException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "", e);
    }
    
    cfg.substituteProperties();
    
    // Initialize Logging
    if (cfg.getBool("solr/logging/@enabled", true)) {
      String slf4jImpl = null;
      String fname = cfg.get("solr/logging/watcher/@class", null);
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
        }
        // else if( "Log4j".equals(fname) ) {
        // logging = new Log4jWatcher(slf4jImpl);
        // }
        else {
          try {
            logging = loader.newInstance(fname, LogWatcher.class);
          } catch (Throwable e) {
            log.warn("Unable to load LogWatcher", e);
          }
        }
        
        if (logging != null) {
          ListenerConfig v = new ListenerConfig();
          v.size = cfg.getInt("solr/logging/watcher/@size", 50);
          v.threshold = cfg.get("solr/logging/watcher/@threshold", null);
          if (v.size > 0) {
            log.info("Registering Log Listener");
            logging.registerListener(v, this);
          }
        }
      }
    }
    
    String dcoreName = cfg.get("solr/cores/@defaultCoreName", null);
    if (dcoreName != null && !dcoreName.isEmpty()) {
      defaultCoreName = dcoreName;
    }
    persistent = cfg.getBool("solr/@persistent", false);
    libDir = cfg.get("solr/@sharedLib", null);
    zkHost = cfg.get("solr/@zkHost", null);
    coreLoadThreads = cfg.getInt("solr/@coreLoadThreads", CORE_LOAD_THREADS);
    adminPath = cfg.get("solr/cores/@adminPath", null);
    shareSchema = cfg.getBool("solr/cores/@shareSchema", DEFAULT_SHARE_SCHEMA);
    zkClientTimeout = cfg.getInt("solr/cores/@zkClientTimeout",
        DEFAULT_ZK_CLIENT_TIMEOUT);
    
    hostPort = cfg.get("solr/cores/@hostPort", DEFAULT_HOST_PORT);
    
    hostContext = cfg.get("solr/cores/@hostContext", DEFAULT_HOST_CONTEXT);
    host = cfg.get("solr/cores/@host", null);
    
    leaderVoteWait = cfg.get("solr/cores/@leaderVoteWait", LEADER_VOTE_WAIT);
    
    if (shareSchema) {
      indexSchemaCache = new ConcurrentHashMap<String,IndexSchema>();
    }
    adminHandler = cfg.get("solr/cores/@adminHandler", null);
    managementPath = cfg.get("solr/cores/@managementPath", null);
    
    zkClientTimeout = Integer.parseInt(System.getProperty("zkClientTimeout",
        Integer.toString(zkClientTimeout)));
    initZooKeeper(zkHost, zkClientTimeout);
    
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
    
    try {
      containerProperties = readProperties(cfg, ((NodeList) cfg.evaluate(
          DEFAULT_HOST_CONTEXT, XPathConstants.NODESET)).item(0));
    } catch (Throwable e) {
      SolrException.log(log, null, e);
    }
    
    // setup executor to load cores in parallel
    coreLoadExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 5,
        TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new DefaultSolrThreadFactory("coreLoadExecutor"));
    try {
      // 4 threads at a time max
      final AdjustableSemaphore semaphore = new AdjustableSemaphore(
          coreLoadThreads);
      
      CompletionService<SolrCore> completionService = new ExecutorCompletionService<SolrCore>(
          coreLoadExecutor);
      Set<Future<SolrCore>> pending = new HashSet<Future<SolrCore>>();
      
      NodeList nodes = (NodeList) cfg.evaluate("solr/cores/core",
          XPathConstants.NODESET);
      
      for (int i = 0; i < nodes.getLength(); i++) {
        Node node = nodes.item(i);
        try {
          String rawName = DOMUtil.getAttr(node, CORE_NAME, null);
          if (null == rawName) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Each core in solr.xml must have a 'name'");
          }
          final String name = rawName;
          final CoreDescriptor p = new CoreDescriptor(this, name,
              DOMUtil.getAttr(node, CORE_INSTDIR, null));
          
          // deal with optional settings
          String opt = DOMUtil.getAttr(node, CORE_CONFIG, null);
          
          if (opt != null) {
            p.setConfigName(opt);
          }
          opt = DOMUtil.getAttr(node, CORE_SCHEMA, null);
          if (opt != null) {
            p.setSchemaName(opt);
          }
          if (zkController != null) {
            opt = DOMUtil.getAttr(node, CORE_SHARD, null);
            if (opt != null && opt.length() > 0) {
              p.getCloudDescriptor().setShardId(opt);
            }
            opt = DOMUtil.getAttr(node, CORE_COLLECTION, null);
            if (opt != null) {
              p.getCloudDescriptor().setCollectionName(opt);
            }
            opt = DOMUtil.getAttr(node, CORE_ROLES, null);
            if (opt != null) {
              p.getCloudDescriptor().setRoles(opt);
            }
          }
          opt = DOMUtil.getAttr(node, CORE_PROPERTIES, null);
          if (opt != null) {
            p.setPropertiesName(opt);
          }
          opt = DOMUtil.getAttr(node, CORE_DATADIR, null);
          if (opt != null) {
            p.setDataDir(opt);
          }
          
          p.setCoreProperties(readProperties(cfg, node));
          
          Callable<SolrCore> task = new Callable<SolrCore>() {
            public SolrCore call() {
              SolrCore c = null;
              try {
                c = create(p);
                register(name, c, false);
              } catch (Throwable t) {
                SolrException.log(log, null, t);
                if (c != null) {
                  c.close();
                }
              }
              semaphore.release();
              
              return c;
            }
          };
          
          try {
            semaphore.acquire();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SolrException(ErrorCode.SERVER_ERROR,
                "Interrupted while loading SolrCore(s)", e);
          }
          
          try {
            pending.add(completionService.submit(task));
          } catch (RejectedExecutionException e) {
            semaphore.release();
            throw e;
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
              coreToOrigName.put(c, c.getName());
            }
          } catch (ExecutionException e) {
            // shouldn't happen since we catch exceptions ourselves
            SolrException.log(SolrCore.log,
                "error sending update request to shard", e);
          }
        } catch (InterruptedException e) {
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "interrupted waiting for shard update response", e);
        }
      }
      
    } finally {
      if (coreLoadExecutor != null) {
        ExecutorUtil.shutdownNowAndAwaitTermination(coreLoadExecutor);
      }
    }
  }

  private Document copyDoc(Document document) throws TransformerException {
    TransformerFactory tfactory = TransformerFactory.newInstance();
    Transformer tx   = tfactory.newTransformer();
    DOMSource source = new DOMSource(document);
    DOMResult result = new DOMResult();
    tx.transform(source,result);
    return (Document)result.getNode();
  }

  private Properties readProperties(Config cfg, Node node) throws XPathExpressionException {
    XPath xpath = cfg.getXPath();
    NodeList props = (NodeList) xpath.evaluate("property", node, XPathConstants.NODESET);
    Properties properties = new Properties();
    for (int i=0; i<props.getLength(); i++) {
      Node prop = props.item(i);
      properties.setProperty(DOMUtil.getAttr(prop, "name"), DOMUtil.getAttr(prop, "value"));
    }
    return properties;
  }
  
  private volatile boolean isShutDown = false;

  private volatile Config cfg;
  
  public boolean isShutDown() {
    return isShutDown;
  }

  /**
   * Stops all cores.
   */
  public void shutdown() {
    log.info("Shutting down CoreContainer instance="
        + System.identityHashCode(this));
    isShutDown = true;
    
    if (isZooKeeperAware()) {
      cancelCoreRecoveries();
    }
    try {
      synchronized (cores) {
        
        for (SolrCore core : cores.values()) {
          try {
            core.close();
          } catch (Throwable t) {
            SolrException.log(log, "Error shutting down core", t);
          }
        }
        cores.clear();
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
    synchronized (cores) {
      for (SolrCore core : cores.values()) {
        coreStates.add(core.getUpdateHandler().getSolrCoreState());
      }
    }

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

  /**
   * Registers a SolrCore descriptor in the registry using the specified name.
   * If returnPrevNotClosed==false, the old core, if different, is closed. if true, it is returned w/o closing the core
   *
   * @return a previous core having the same name if it existed
   */
  public SolrCore register(String name, SolrCore core, boolean returnPrevNotClosed) {
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
    synchronized (cores) {
      if (isShutDown) {
        core.close();
        throw new IllegalStateException("This CoreContainer has been shutdown");
      }
      old = cores.put(name, core);
      coreInitFailures.remove(name);
      /*
      * set both the name of the descriptor and the name of the
      * core, since the descriptors name is used for persisting.
      */
      core.setName(name);
      core.getCoreDescriptor().name = name;
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
    return register(core.getName(), core, returnPrev);
  }

  /**
   * Creates a new core based on a descriptor but does not register it.
   *
   * @param dcore a core descriptor
   * @return the newly created core
   */
  public SolrCore create(CoreDescriptor dcore)  throws ParserConfigurationException, IOException, SAXException {

    // :TODO: would be really nice if this method wrapped any underlying errors and only threw SolrException

    final String name = dcore.getName();
    Exception failure = null;

    try {
      // Make the instanceDir relative to the cores instanceDir if not absolute
      File idir = new File(dcore.getInstanceDir());
      String instanceDir = idir.getPath();
      log.info("Creating SolrCore '{}' using instanceDir: {}", 
               dcore.getName(), instanceDir);
      // Initialize the solr config
      SolrResourceLoader solrLoader = null;
      
      SolrConfig config = null;
      String zkConfigName = null;
      if(zkController == null) {
        solrLoader = new SolrResourceLoader(instanceDir, libLoader, getCoreProps(instanceDir, dcore.getPropertiesName(),dcore.getCoreProperties()));
        config = new SolrConfig(solrLoader, dcore.getConfigName(), null);
      } else {
        try {
          String collection = dcore.getCloudDescriptor().getCollectionName();
          zkController.createCollectionZkNode(dcore.getCloudDescriptor());
          
          zkConfigName = zkController.readConfigName(collection);
          if (zkConfigName == null) {
            log.error("Could not find config name for collection:" + collection);
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                                         "Could not find config name for collection:" + collection);
          }
          solrLoader = new ZkSolrResourceLoader(instanceDir, zkConfigName, libLoader, getCoreProps(instanceDir, dcore.getPropertiesName(),dcore.getCoreProperties()), zkController);
          config = getSolrConfigFromZk(zkConfigName, dcore.getConfigName(), solrLoader);
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
    
      IndexSchema schema = null;
      if (indexSchemaCache != null) {
        if (zkController == null) {
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
              log.info("creating new schema object for core: " + dcore.name);
              schema = new IndexSchema(config, dcore.getSchemaName(), null);
              indexSchemaCache.put(key, schema);
            } else {
              log.info("re-using schema object for core: " + dcore.name);
            }
          }
        } else {
          // TODO: handle caching from ZooKeeper - perhaps using ZooKeepers versioning
          // Don't like this cache though - how does it empty as last modified changes?
        }
      }
      if(schema == null){
        if(zkController != null) {
          try {
            schema = getSchemaFromZk(zkConfigName, dcore.getSchemaName(), config, solrLoader);
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
        } else {
          schema = new IndexSchema(config, dcore.getSchemaName(), null);
        }
      }

      SolrCore core = new SolrCore(dcore.getName(), null, config, schema, dcore);

      if (zkController == null && core.getUpdateHandler().getUpdateLog() != null) {
        // always kick off recovery if we are in standalone mode.
        core.getUpdateHandler().getUpdateLog().recoverFromLog();
      }

      return core;

      // :TODO: Java7...
      // http://docs.oracle.com/javase/7/docs/technotes/guides/language/catch-multiple.html
    } catch (ParserConfigurationException e1) {
      failure = e1;
      throw e1;
    } catch (IOException e2) {
      failure = e2;
      throw e2;
    } catch (SAXException e3) {
      failure = e3;
      throw e3;
    } catch (RuntimeException e4) {
      failure = e4;
      throw e4;
    } finally {
      if (null != failure) {
        log.error("Unable to create core: " + name, failure);
      }
      synchronized (coreInitFailures) {
        // remove first so insertion order is updated and newest is last
        coreInitFailures.remove(name);
        if (null != failure) {
          coreInitFailures.put(name, failure);
        }
      }
    }
  }

  /**
   * @return a Collection of registered SolrCores
   */
  public Collection<SolrCore> getCores() {
    List<SolrCore> lst = new ArrayList<SolrCore>();
    synchronized (cores) {
      lst.addAll(this.cores.values());
    }
    return lst;
  }

  /**
   * @return a Collection of the names that cores are mapped to
   */
  public Collection<String> getCoreNames() {
    List<String> lst = new ArrayList<String>();
    synchronized (cores) {
      lst.addAll(this.cores.keySet());
    }
    return lst;
  }

  /** This method is currently experimental.
   * @return a Collection of the names that a specific core is mapped to.
   */
  public Collection<String> getCoreNames(SolrCore core) {
    List<String> lst = new ArrayList<String>();
    synchronized (cores) {
      for (Map.Entry<String,SolrCore> entry : cores.entrySet()) {
        if (core == entry.getValue()) {
          lst.add(entry.getKey());
        }
      }
    }
    return lst;
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
  public void reload(String name) throws ParserConfigurationException, IOException, SAXException {

    // :TODO: would be really nice if this method wrapped any underlying errors and only threw SolrException

    Exception failure = null;
    try {

      name= checkDefault(name);
      SolrCore core;
      synchronized(cores) {
        core = cores.get(name);
      }
      if (core == null)
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "No such core: " + name );

      CoreDescriptor cd = core.getCoreDescriptor();
  
      File instanceDir = new File(cd.getInstanceDir());

      log.info("Reloading SolrCore '{}' using instanceDir: {}", 
               cd.getName(), instanceDir.getAbsolutePath());
    
      SolrResourceLoader solrLoader;
      if(zkController == null) {
        solrLoader = new SolrResourceLoader(instanceDir.getAbsolutePath(), libLoader, getCoreProps(instanceDir.getAbsolutePath(), cd.getPropertiesName(),cd.getCoreProperties()));
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
          solrLoader = new ZkSolrResourceLoader(instanceDir.getAbsolutePath(), zkConfigName, libLoader, getCoreProps(instanceDir.getAbsolutePath(), cd.getPropertiesName(),cd.getCoreProperties()), zkController);
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
      String origName = coreToOrigName.remove(core);
      if (origName != null) {
        coreToOrigName.put(newCore, origName);
      }
      register(name, newCore, false);

      // :TODO: Java7...
      // http://docs.oracle.com/javase/7/docs/technotes/guides/language/catch-multiple.html
    } catch (ParserConfigurationException e1) {
      failure = e1;
      throw e1;
    } catch (IOException e2) {
      failure = e2;
      throw e2;
    } catch (SAXException e3) {
      failure = e3;
      throw e3;
    } catch (RuntimeException e4) {
      failure = e4;
      throw e4;
    } finally {
      if (null != failure) {
        log.error("Unable to reload core: " + name, failure);
      }
      synchronized (coreInitFailures) {
        // remove first so insertion order is updated and newest is last
        coreInitFailures.remove(name);
        if (null != failure) {
          coreInitFailures.put(name, failure);
        }
      }
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
    synchronized( cores ) {
      SolrCore c0 = cores.get(n0);
      SolrCore c1 = cores.get(n1);
      if (c0 == null)
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "No such core: " + n0 );
      if (c1 == null)
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "No such core: " + n1 );
      cores.put(n0, c1);
      cores.put(n1, c0);

      c0.setName(n1);
      c0.getCoreDescriptor().name = n1;
      c1.setName(n0);
      c1.getCoreDescriptor().name = n0;
    }


    log.info("swapped: "+n0 + " with " + n1);
  }
  
  /** Removes and returns registered core w/o decrementing it's reference count */
  public SolrCore remove( String name ) {
    name = checkDefault(name);    

    synchronized(cores) {
      SolrCore core = cores.remove( name );
      if (core != null) {
        coreToOrigName.remove(core);
      }
      return core;
    }

  }

  public void rename(String name, String toName) {
    SolrCore core = getCore(name);
    try {
      if (core != null) {
        register(toName, core, false);
        name = checkDefault(name);
        
        synchronized (cores) {
          cores.remove(name);
        }
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
    name= checkDefault(name);
    synchronized(cores) {
      SolrCore core = cores.get(name);
      if (core != null)
        core.open();  // increment the ref count while still synchronized
      return core;
    }
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
  
/** Persists the cores config file in cores.xml. */
  public void persist() {
    persistFile(null);
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
    
    addCoresAttrib(coresAttribs, "hostPort", this.hostPort, DEFAULT_HOST_PORT);
    addCoresAttrib(coresAttribs, "zkClientTimeout",
        intToString(this.zkClientTimeout),
        Integer.toString(DEFAULT_ZK_CLIENT_TIMEOUT));
    addCoresAttrib(coresAttribs, "hostContext", this.hostContext, DEFAULT_HOST_CONTEXT);
    addCoresAttrib(coresAttribs, "leaderVoteWait", this.leaderVoteWait, LEADER_VOTE_WAIT);
    
    List<SolrCoreXMLDef> solrCoreXMLDefs = new ArrayList<SolrCoreXMLDef>();
    
    synchronized (cores) {
      for (SolrCore solrCore : cores.values()) {
        Map<String,String> coreAttribs = new HashMap<String,String>();
        CoreDescriptor dcore = solrCore.getCoreDescriptor();

        String coreName = dcore.name;
        Node coreNode = null;
        
        if (cfg != null) {
          NodeList nodes = (NodeList) cfg.evaluate("solr/cores/core",
              XPathConstants.NODESET);
          
          String origCoreName = coreToOrigName.get(solrCore);

          if (origCoreName == null) {
            origCoreName = coreName;
          }
          
          // look for an existing node
          
          // first look for an exact match
          for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);
            
            String name = DOMUtil.getAttr(node, CORE_NAME, null);
            if (origCoreName.equals(name)) {
              coreNode = node;
              if (coreName.equals(origCoreName)) {
                coreName = name;
              }
              break;
            }
          }
          
          if (coreNode == null) {
            // see if we match with substitution
            for (int i = 0; i < nodes.getLength(); i++) {
              Node node = nodes.item(i);
              String name = DOMUtil.getAttr(node, CORE_NAME, null);
              if (origCoreName.equals(DOMUtil.substituteProperty(name,
                  loader.getCoreProperties()))) {
                coreNode = node;
                if (coreName.equals(origCoreName)) {
                  coreName = name;
                }
                break;
              }
            }
          }
        }

        coreAttribs.put(CORE_NAME, coreName);
        
        String instanceDir = dcore.getRawInstanceDir();
        addCoreProperty(coreAttribs, coreNode, CORE_INSTDIR, instanceDir, null);
        
        // write config 
        String configName = dcore.getConfigName();
        addCoreProperty(coreAttribs, coreNode, CORE_CONFIG, configName, dcore.getDefaultConfigName());
        
        // write schema
        String schema = dcore.getSchemaName();
        addCoreProperty(coreAttribs, coreNode, CORE_SCHEMA, schema, dcore.getDefaultSchemaName());
        
        String dataDir = dcore.dataDir;
        addCoreProperty(coreAttribs, coreNode, CORE_DATADIR, dataDir, null);
        
        CloudDescriptor cd = dcore.getCloudDescriptor();
        String shard = null;
        String roles = null;
        if (cd != null) {
          shard = cd.getShardId();
          roles = cd.getRoles();
        }
        addCoreProperty(coreAttribs, coreNode, CORE_SHARD, shard, null);
        
        addCoreProperty(coreAttribs, coreNode, CORE_ROLES, roles, null);
        
        String collection = null;
        // only write out the collection name if it's not the default (the
        // core
        // name)
        if (cd != null) {
          collection = cd.getCollectionName();
        }
        
        addCoreProperty(coreAttribs, coreNode, CORE_COLLECTION, collection, dcore.name);
        
        // we don't try and preserve sys prop defs in these
        String opt = dcore.getPropertiesName();
        if (opt != null) {
          coreAttribs.put(CORE_PROPERTIES, opt);
        }
        
        SolrCoreXMLDef solrCoreXMLDef = new SolrCoreXMLDef();
        solrCoreXMLDef.coreAttribs = coreAttribs;
        solrCoreXMLDef.coreProperties = dcore.getCoreProperties();
        solrCoreXMLDefs.add(solrCoreXMLDef);
      }
      
      SolrXMLDef solrXMLDef = new SolrXMLDef();
      solrXMLDef.coresDefs = solrCoreXMLDefs;
      solrXMLDef.containerProperties = containerProperties;
      solrXMLDef.solrAttribs = rootSolrAttribs;
      solrXMLDef.coresAttribs = coresAttribs;
      solrXMLSerializer.persistFile(file == null ? configFile : file,
          solrXMLDef);
    }
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
      String rawValue = cfg.get("solr/cores/@" + attribName, null);
      if (rawValue == null && defaultValue != null && attribValue.equals(defaultValue)) return;
      if (attribValue.equals(DOMUtil.substituteProperty(rawValue, loader.getCoreProperties()))) {
        coresAttribs.put(attribName, rawValue);
      } else {
        coresAttribs.put(attribName, attribValue);
      }
    }
  }

  private void addCoreProperty(Map<String,String> coreAttribs, Node node, String name,
      String value, String defaultValue) {
    if (node == null) {
      coreAttribs.put(name, value);
      return;
    }
    
    if (node != null) {
      String rawAttribValue = DOMUtil.getAttr(node, name, null);
      if (value == null) {
        coreAttribs.put(name, rawAttribValue);
        return;
      }
      if (rawAttribValue == null && defaultValue != null && value.equals(defaultValue)) {
        return;
      }
      if (rawAttribValue != null && value.equals(DOMUtil.substituteProperty(rawAttribValue, loader.getCoreProperties()))){
        coreAttribs.put(name, rawAttribValue);
      } else {
        coreAttribs.put(name, value);
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
    synchronized (this) {
      if (shardHandlerFactory == null) {
        Map m = new HashMap();
        m.put("class",HttpShardHandlerFactory.class.getName());
        PluginInfo info = new PluginInfo("shardHandlerFactory", m,null,Collections.<PluginInfo>emptyList());

        HttpShardHandlerFactory fac = new HttpShardHandlerFactory();
        fac.init(info);
        shardHandlerFactory = fac;
      }
      return shardHandlerFactory;
    }
  }
  
  private SolrConfig getSolrConfigFromZk(String zkConfigName, String solrConfigFileName,
      SolrResourceLoader resourceLoader) throws IOException,
      ParserConfigurationException, SAXException, KeeperException,
      InterruptedException {
    byte[] config = zkController.getConfigFileData(zkConfigName, solrConfigFileName);
    InputSource is = new InputSource(new ByteArrayInputStream(config));
    is.setSystemId(SystemIdResolver.createSystemIdFromResourceName(solrConfigFileName));
    SolrConfig cfg = solrConfigFileName == null ? new SolrConfig(
        resourceLoader, SolrConfig.DEFAULT_CONF_FILE, is) : new SolrConfig(
        resourceLoader, solrConfigFileName, is);

    return cfg;
  }
  
  private IndexSchema getSchemaFromZk(String zkConfigName, String schemaName,
      SolrConfig config, SolrResourceLoader resourceLoader)
      throws KeeperException, InterruptedException {
    byte[] configBytes = zkController.getConfigFileData(zkConfigName, schemaName);
    InputSource is = new InputSource(new ByteArrayInputStream(configBytes));
    is.setSystemId(SystemIdResolver.createSystemIdFromResourceName(schemaName));
    IndexSchema schema = new IndexSchema(config, schemaName, is);
    return schema;
  }
  
  private static final String DEF_SOLR_XML ="<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
          "<solr persistent=\"false\">\n" +
          "  <cores adminPath=\"/admin/cores\" defaultCoreName=\"" + DEFAULT_DEFAULT_CORE_NAME + "\">\n" +
          "    <core name=\""+ DEFAULT_DEFAULT_CORE_NAME + "\" shard=\"${shard:}\" instanceDir=\"collection1\" />\n" +
          "  </cores>\n" +
          "</solr>";
}
