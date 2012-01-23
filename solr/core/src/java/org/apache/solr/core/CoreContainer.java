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

package org.apache.solr.core;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.text.SimpleDateFormat;

import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.CurrentCoreDescriptorProvider;
import org.apache.solr.cloud.SolrZkServer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.FileUtils;
import org.apache.solr.common.util.SystemIdResolver;
import org.apache.solr.core.SolrXMLSerializer.SolrCoreXMLDef;
import org.apache.solr.core.SolrXMLSerializer.SolrXMLDef;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.schema.IndexSchema;
import org.apache.zookeeper.KeeperException;
import org.apache.commons.io.IOUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;


/**
 *
 * @since solr 1.3
 */
public class CoreContainer 
{
  private static final String DEFAULT_DEFAULT_CORE_NAME = "collection1";

  protected static Logger log = LoggerFactory.getLogger(CoreContainer.class);
  
  protected final Map<String, SolrCore> cores = new LinkedHashMap<String, SolrCore>();
  protected boolean persistent = false;
  protected String adminPath = null;
  protected String managementPath = null;
  protected String hostPort;
  protected String hostContext;
  protected String host;
  protected CoreAdminHandler coreAdminHandler = null;
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
  protected String defaultCoreName = "";
  private SolrXMLSerializer solrXMLSerializer = new SolrXMLSerializer();
  private ZkController zkController;
  private SolrZkServer zkServer;
  private ShardHandlerFactory shardHandlerFactory;

  private String zkHost;

  {
    log.info("New CoreContainer " + System.identityHashCode(this));
  }

  public CoreContainer() {
    solrHome = SolrResourceLoader.locateSolrHome();
  }

  /**
   * Initalize CoreContainer directly from the constructor
   *
   * @param dir
   * @param configFile
   * @throws ParserConfigurationException
   * @throws IOException
   * @throws SAXException
   */
  public CoreContainer(String dir, File configFile) throws ParserConfigurationException, IOException, SAXException
  {
    this.load(dir, configFile);
  }

  /**
   * Minimal CoreContainer constructor.
   * @param loader the CoreContainer resource loader
   */
  public CoreContainer(SolrResourceLoader loader) {
    this.loader = loader;
    this.solrHome = loader.getInstanceDir();
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

    zkServer = new SolrZkServer(zkRun, zookeeperHost, solrHome, hostPort);
    zkServer.parseConfig();
    zkServer.start();

    // set client from server config if not already set
    if (zookeeperHost == null) {
      zookeeperHost = zkServer.getClientString();
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
        zkController = new ZkController(this, zookeeperHost, zkClientTimeout, zkClientConnectTimeout, host, hostPort, hostContext, new CurrentCoreDescriptorProvider() {
          
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
        if(confDir != null) {
          File dir = new File(confDir);
          if(!dir.isDirectory()) {
            throw new IllegalArgumentException("bootstrap_confdir must be a directory of configuration files");
          }
          String confName = System.getProperty(ZkController.COLLECTION_PARAM_PREFIX+ZkController.CONFIGNAME_PROP, "configuration1");
          zkController.uploadConfigDir(dir, confName);
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
      cores = new CoreContainer();
      
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
   * @throws javax.xml.parsers.ParserConfigurationException
   * @throws java.io.IOException
   * @throws org.xml.sax.SAXException
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
   * @throws ParserConfigurationException
   * @throws IOException
   * @throws SAXException
   */
  public void load(String dir, InputSource cfgis)
      throws ParserConfigurationException, IOException, SAXException {
    this.loader = new SolrResourceLoader(dir);
    solrHome = loader.getInstanceDir();
    Config cfg = new Config(loader, null, cfgis, null);
    String dcoreName = cfg.get("solr/cores/@defaultCoreName", null);
    if(dcoreName != null) {
      defaultCoreName = dcoreName;
    }
    persistent = cfg.getBool("solr/@persistent", false);
    libDir = cfg.get("solr/@sharedLib", null);
    zkHost = cfg.get("solr/@zkHost" , null);
    adminPath = cfg.get("solr/cores/@adminPath", null);
    shareSchema = cfg.getBool("solr/cores/@shareSchema", false);
    zkClientTimeout = cfg.getInt("solr/cores/@zkClientTimeout", 10000);

    hostPort = cfg.get("solr/cores/@hostPort", "8983");

    hostContext = cfg.get("solr/cores/@hostContext", "solr");
    host = cfg.get("solr/cores/@host", null);

    if(shareSchema){
      indexSchemaCache = new ConcurrentHashMap<String ,IndexSchema>();
    }
    adminHandler  = cfg.get("solr/cores/@adminHandler", null );
    managementPath  = cfg.get("solr/cores/@managementPath", null );
    
    zkClientTimeout = Integer.parseInt(System.getProperty("zkClientTimeout", Integer.toString(zkClientTimeout)));
    initZooKeeper(zkHost, zkClientTimeout);

    if (libDir != null) {
      File f = FileUtils.resolvePath(new File(dir), libDir);
      log.info( "loading shared library: "+f.getAbsolutePath() );
      libLoader = SolrResourceLoader.createClassLoader(f, null);
    }

    if (adminPath != null) {
      if (adminHandler == null) {
        coreAdminHandler = new CoreAdminHandler(this);
      } else {
        coreAdminHandler = this.createMultiCoreHandler(adminHandler);
      }
    }

    try {
      containerProperties = readProperties(cfg, ((NodeList) cfg.evaluate("solr", XPathConstants.NODESET)).item(0));
    } catch (Throwable e) {
      SolrException.log(log,null,e);
    }

    NodeList nodes = (NodeList)cfg.evaluate("solr/cores/core", XPathConstants.NODESET);

    for (int i=0; i<nodes.getLength(); i++) {
      Node node = nodes.item(i);
      try {
        String name = DOMUtil.getAttr(node, "name", null);
        if (null == name) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                                  "Each core in solr.xml must have a 'name'");
        }
        if (name.equals(defaultCoreName)){
          // for the default core we use a blank name,
          // later on attempts to access it by it's full name will 
          // be mapped to this.
          name="";
        }
        CoreDescriptor p = new CoreDescriptor(this, name, DOMUtil.getAttr(node, "instanceDir", null));

        // deal with optional settings
        String opt = DOMUtil.getAttr(node, "config", null);

        if (opt != null) {
          p.setConfigName(opt);
        }
        opt = DOMUtil.getAttr(node, "schema", null);
        if (opt != null) {
          p.setSchemaName(opt);
        }
        if (zkController != null) {
          opt = DOMUtil.getAttr(node, "shard", null);
          if (opt != null && opt.length() > 0) {
            p.getCloudDescriptor().setShardId(opt);
          }
          opt = DOMUtil.getAttr(node, "collection", null);
          if (opt != null) {
            p.getCloudDescriptor().setCollectionName(opt);
          }
          opt = DOMUtil.getAttr(node, "roles", null);
          if(opt != null){
        	  p.getCloudDescriptor().setRoles(opt);
          }
        }
        opt = DOMUtil.getAttr(node, "properties", null);
        if (opt != null) {
          p.setPropertiesName(opt);
        }
        opt = DOMUtil.getAttr(node, CoreAdminParams.DATA_DIR, null);
        if (opt != null) {
          p.setDataDir(opt);
        }

        p.setCoreProperties(readProperties(cfg, node));

        SolrCore core = create(p);
        register(name, core, false);
      }
      catch (Throwable ex) {
        SolrException.log(log,null,ex);
      }
    }
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
  
  public boolean isShutDown() {
    return isShutDown;
  }

  /**
   * Stops all cores.
   */
  public void shutdown() {
    log.info("Shutting down CoreContainer instance="+System.identityHashCode(this));    
    synchronized(cores) {
      try {
        for (SolrCore core : cores.values()) {
          try {
            if (!core.isClosed()) {
              core.close();
            }
          } catch (Throwable t) {
            SolrException.log(log, "Error shutting down core", t);
          }
        }
        cores.clear();
      } finally {
        if(zkController != null) {
          zkController.close();
        }
        if (zkServer != null) {
          zkServer.stop();
        }
        if (shardHandlerFactory != null) {
          shardHandlerFactory.close();
        }
        isShutDown = true;
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
      // before becoming available, make sure we are not live and active
      // this also gets us our assigned shard id if it was not specified
      zkController.publish(core, ZkStateReader.DOWN);
    }
    
    SolrCore old = null;
    synchronized (cores) {
      old = cores.put(name, core);
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
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      } catch (Exception e) {
        // if register fails, this is really bad - close the zkController to
        // minimize any damage we can cause
        zkController.publish(core, ZkStateReader.DOWN);
        zkController.close();
        log.error("", e);
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
   * @throws javax.xml.parsers.ParserConfigurationException
   * @throws java.io.IOException
   * @throws org.xml.sax.SAXException
   */
  public SolrCore create(CoreDescriptor dcore)  throws ParserConfigurationException, IOException, SAXException {
    // Make the instanceDir relative to the cores instanceDir if not absolute
    File idir = new File(dcore.getInstanceDir());
    if (!idir.isAbsolute()) {
      idir = new File(solrHome, dcore.getInstanceDir());
    }
    String instanceDir = idir.getPath();
    
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
      if (zkController != null) {
        File schemaFile = new File(dcore.getSchemaName());
        if (!schemaFile.isAbsolute()) {
          schemaFile = new File(solrLoader.getInstanceDir() + "conf"
              + File.separator + dcore.getSchemaName());
        }
        if (schemaFile.exists()) {
          String key = schemaFile.getAbsolutePath()
              + ":"
              + new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(new Date(
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

  // ---------------- Core name related methods --------------- 
  /**
   * Recreates a SolrCore.
   * While the new core is loading, requests will continue to be dispatched to
   * and processed by the old core
   * 
   * @param name the name of the SolrCore to reload
   * @throws ParserConfigurationException
   * @throws IOException
   * @throws SAXException
   */

  public void reload(String name) throws ParserConfigurationException, IOException, SAXException {
    name= checkDefault(name);
    SolrCore core;
    synchronized(cores) {
      core = cores.get(name);
    }
    if (core == null)
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "No such core: " + name );

    CoreDescriptor cd = core.getCoreDescriptor();
  
    File instanceDir = new File(cd.getInstanceDir());
    if (!instanceDir.isAbsolute()) {
      instanceDir = new File(getSolrHome(), cd.getInstanceDir());
    }
    
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
    
    SolrCore newCore = core.reload(solrLoader);
    register(name, newCore, false);
  }

  private String checkDefault(String name) {
    return name.length() == 0  || defaultCoreName.equals(name) || name.trim().length() == 0 ? "" : name;
  } 

  /**
   * Swaps two SolrCore descriptors.
   * @param n0
   * @param n1
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


    log.info("swaped: "+n0 + " with " + n1);
  }
  
  /** Removes and returns registered core w/o decrementing it's reference count */
  public SolrCore remove( String name ) {
    name = checkDefault(name);    
    synchronized(cores) {
      return cores.remove( name );
    }
  }

  
  /** Gets a core by name and increase its refcount.
   * @see SolrCore#open() 
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
    SolrResourceLoader loader = new SolrResourceLoader(null, libLoader, null);
    Object obj = loader.newAdminHandlerInstance(CoreContainer.this, adminHandlerClass);
    if ( !(obj instanceof CoreAdminHandler))
    {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "adminHandlerClass is not of type "+ CoreAdminHandler.class );
      
    }
    return (CoreAdminHandler) obj;
  }

  public CoreAdminHandler getMultiCoreHandler() {
    return coreAdminHandler;
  }
  
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
   * @param path
   */
  public void setManagementPath(String path) {
    this.managementPath = path;
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
    coresAttribs.put("adminPath", adminPath);
    if (adminHandler != null) coresAttribs.put("adminHandler", adminHandler);
    if (shareSchema) coresAttribs.put("shareSchema", "true");
    if (!defaultCoreName.equals("")) coresAttribs.put("defaultCoreName",
        defaultCoreName);
    if (host != null) coresAttribs.put("host", host);
    if (hostPort != null) coresAttribs.put("hostPort", hostPort);
    if (zkClientTimeout != null) coresAttribs.put("zkClientTimeout", Integer.toString(zkClientTimeout));
    if (hostContext != null) coresAttribs.put("hostContext", hostContext);
    
    List<SolrCoreXMLDef> solrCoreXMLDefs = new ArrayList<SolrCoreXMLDef>();
    
    synchronized (cores) {
      for (SolrCore solrCore : cores.values()) {
        Map<String,String> coreAttribs = new HashMap<String,String>();
        CoreDescriptor dcore = solrCore.getCoreDescriptor();
        
        coreAttribs.put("name", dcore.name.equals("") ? defaultCoreName
            : dcore.name);
        coreAttribs.put("instanceDir", dcore.getInstanceDir());
        // write config (if not default)
        String opt = dcore.getConfigName();
        if (opt != null && !opt.equals(dcore.getDefaultConfigName())) {
          coreAttribs.put("config", opt);
        }
        // write schema (if not default)
        opt = dcore.getSchemaName();
        if (opt != null && !opt.equals(dcore.getDefaultSchemaName())) {
          coreAttribs.put("schema", opt);
        }
        opt = dcore.getPropertiesName();
        if (opt != null) {
          coreAttribs.put("properties", opt);
        }
        opt = dcore.dataDir;
        if (opt != null) coreAttribs.put("dataDir", opt);
        
        CloudDescriptor cd = dcore.getCloudDescriptor();
        if (cd != null) {
          opt = cd.getShardId();
          if (opt != null) coreAttribs.put("shard", opt);
          // only write out the collection name if it's not the default (the
          // core
          // name)
          opt = cd.getCollectionName();
          if (opt != null && !opt.equals(dcore.name)) coreAttribs.put(
              "collection", opt);
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


  public String getSolrHome() {
    return solrHome;
  }
  
  public boolean isZooKeeperAware() {
    return zkController != null;
  }
  
  public ZkController getZkController() {
    return zkController;
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
          "    <core name=\""+ DEFAULT_DEFAULT_CORE_NAME + "\" shard=\"${shard:}\" instanceDir=\".\" />\n" +
          "  </cores>\n" +
          "</solr>";
}
