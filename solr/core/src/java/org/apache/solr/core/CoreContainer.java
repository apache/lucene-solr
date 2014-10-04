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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.InfoHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;


/**
 *
 * @since solr 1.3
 */
public class CoreContainer {

  protected static final Logger log = LoggerFactory.getLogger(CoreContainer.class);

  private final SolrCores solrCores = new SolrCores(this);

  public static class CoreLoadFailure {

    public final CoreDescriptor cd;
    public final Exception exception;

    public CoreLoadFailure(CoreDescriptor cd, Exception loadFailure) {
      this.cd = cd;
      this.exception = loadFailure;
    }
  }

  protected final Map<String, CoreLoadFailure> coreInitFailures = new ConcurrentHashMap<>();

  protected CoreAdminHandler coreAdminHandler = null;
  protected CollectionsHandler collectionsHandler = null;
  private InfoHandler infoHandler;

  protected Properties containerProperties;

  private ConfigSetService coreConfigService;
  
  protected ZkContainer zkSys = new ZkContainer();
  protected ShardHandlerFactory shardHandlerFactory;
  
  private UpdateShardHandler updateShardHandler;

  protected LogWatcher logging = null;

  private CloserThread backgroundCloser = null;
  protected final ConfigSolr cfg;
  protected final SolrResourceLoader loader;

  protected final String solrHome;

  protected final CoresLocator coresLocator;
  
  private String hostName;
  
  private Map<String ,SolrRequestHandler> containerHandlers = new HashMap<>();

  public SolrRequestHandler getRequestHandler(String path) {
    return RequestHandlerBase.getRequestHandler(path, containerHandlers);
  }

  public Map<String, SolrRequestHandler> getRequestHandlers(){
    return this.containerHandlers;

  }

 // private ClientConnectionManager clientConnectionManager = new PoolingClientConnectionManager();

  {
    log.info("New CoreContainer " + System.identityHashCode(this));
  }

  /**
   * Create a new CoreContainer using system properties to detect the solr home
   * directory.  The container's cores are not loaded.
   * @see #load()
   */
  public CoreContainer() {
    this(new SolrResourceLoader(SolrResourceLoader.locateSolrHome()));
  }

  /**
   * Create a new CoreContainer using the given SolrResourceLoader.  The container's
   * cores are not loaded.
   * @param loader the SolrResourceLoader
   * @see #load()
   */
  public CoreContainer(SolrResourceLoader loader) {
    this(loader, ConfigSolr.fromSolrHome(loader, loader.getInstanceDir()));
  }

  /**
   * Create a new CoreContainer using the given solr home directory.  The container's
   * cores are not loaded.
   * @param solrHome a String containing the path to the solr home directory
   * @see #load()
   */
  public CoreContainer(String solrHome) {
    this(new SolrResourceLoader(solrHome));
  }

  /**
   * Create a new CoreContainer using the given SolrResourceLoader,
   * configuration and CoresLocator.  The container's cores are
   * not loaded.
   * @param loader the SolrResourceLoader
   * @param config a ConfigSolr representation of this container's configuration
   * @see #load()
   */
  public CoreContainer(SolrResourceLoader loader, ConfigSolr config) {
    this.loader = checkNotNull(loader);
    this.solrHome = loader.getInstanceDir();
    this.cfg = checkNotNull(config);
    this.coresLocator = config.getCoresLocator();
  }

  public CoreContainer(SolrResourceLoader loader, ConfigSolr config, CoresLocator locator) {
    this.loader = checkNotNull(loader);
    this.solrHome = loader.getInstanceDir();
    this.cfg = checkNotNull(config);
    this.coresLocator = locator;
  }

  /**
   * This method allows subclasses to construct a CoreContainer
   * without any default init behavior.
   * 
   * @param testConstructor pass (Object)null.
   * @lucene.experimental
   */
  protected CoreContainer(Object testConstructor) {
    solrHome = null;
    loader = null;
    coresLocator = null;
    cfg = null;
  }
  
  /**
   * Create a new CoreContainer and load its cores
   * @param solrHome the solr home directory
   * @param configFile the file containing this container's configuration
   * @return a loaded CoreContainer
   */
  public static CoreContainer createAndLoad(String solrHome, File configFile) {
    SolrResourceLoader loader = new SolrResourceLoader(solrHome);
    CoreContainer cc = new CoreContainer(loader, ConfigSolr.fromFile(loader, configFile));
    cc.load();
    return cc;
  }
  
  public Properties getContainerProperties() {
    return containerProperties;
  }

  //-------------------------------------------------------------------
  // Initialization / Cleanup
  //-------------------------------------------------------------------

  /**
   * Load the cores defined for this CoreContainer
   */
  public void load()  {

    log.info("Loading cores into CoreContainer [instanceDir={}]", loader.getInstanceDir());

    // add the sharedLib to the shared resource loader before initializing cfg based plugins
    String libDir = cfg.getSharedLibDirectory();
    if (libDir != null) {
      File f = FileUtils.resolvePath(new File(solrHome), libDir);
      log.info("loading shared library: " + f.getAbsolutePath());
      loader.addToClassLoader(libDir, null, false);
      loader.reloadLuceneSPI();
    }


    shardHandlerFactory = ShardHandlerFactory.newInstance(cfg.getShardHandlerFactoryPluginInfo(), loader);

    updateShardHandler = new UpdateShardHandler(cfg);

    solrCores.allocateLazyCores(cfg.getTransientCacheSize(), loader);

    logging = LogWatcher.newRegisteredLogWatcher(cfg.getLogWatcherConfig(), loader);

    hostName = cfg.getHost();
    log.info("Host Name: " + hostName);

    zkSys.initZooKeeper(this, solrHome, cfg);

    collectionsHandler = createHandler(cfg.getCollectionsHandlerClass(), CollectionsHandler.class);
    containerHandlers.put("/admin/collections" , collectionsHandler);
    infoHandler        = createHandler(cfg.getInfoHandlerClass(), InfoHandler.class);
    containerHandlers.put("/admin/info" , infoHandler);
    coreAdminHandler   = createHandler(cfg.getCoreAdminHandlerClass(), CoreAdminHandler.class);
    containerHandlers.put(cfg.getAdminPath() , coreAdminHandler);

    coreConfigService = cfg.createCoreConfigService(loader, zkSys.getZkController());

    containerProperties = cfg.getSolrProperties("solr");

    // setup executor to load cores in parallel
    // do not limit the size of the executor in zk mode since cores may try and wait for each other.
    ExecutorService coreLoadExecutor = Executors.newFixedThreadPool(
        ( zkSys.getZkController() == null ? cfg.getCoreLoadThreadCount() : Integer.MAX_VALUE ),
        new DefaultSolrThreadFactory("coreLoadExecutor") );

    try {

      List<CoreDescriptor> cds = coresLocator.discover(this);
      checkForDuplicateCoreNames(cds);

      List<Callable<SolrCore>> creators = new ArrayList<>();
      for (final CoreDescriptor cd : cds) {
        if (cd.isTransient() || !cd.isLoadOnStartup()) {
          solrCores.putDynamicDescriptor(cd.getName(), cd);
        }
        if (cd.isLoadOnStartup()) {
          creators.add(new Callable<SolrCore>() {
            @Override
            public SolrCore call() throws Exception {
              if (zkSys.getZkController() != null) {
                zkSys.getZkController().throwErrorIfReplicaReplaced(cd);
              }
              return create(cd, false);   
            }
          });
        }
      }

      try {
        coreLoadExecutor.invokeAll(creators);
      }
      catch (InterruptedException e) {
        throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Interrupted while loading cores");
      }

      // Start the background thread
      backgroundCloser = new CloserThread(this, solrCores, cfg);
      backgroundCloser.start();

    } finally {
      ExecutorUtil.shutdownNowAndAwaitTermination(coreLoadExecutor);
    }
    
    if (isZooKeeperAware()) {
      // register in zk in background threads
      Collection<SolrCore> cores = getCores();
      if (cores != null) {
        for (SolrCore core : cores) {
          try {
            zkSys.registerInZk(core, true);
          } catch (Throwable t) {
            SolrException.log(log, "Error registering SolrCore", t);
          }
        }
      }
      zkSys.getZkController().checkOverseerDesignate();
    }
  }

  private static void checkForDuplicateCoreNames(List<CoreDescriptor> cds) {
    Map<String, String> addedCores = Maps.newHashMap();
    for (CoreDescriptor cd : cds) {
      final String name = cd.getName();
      if (addedCores.containsKey(name))
        throw new SolrException(ErrorCode.SERVER_ERROR,
            String.format(Locale.ROOT, "Found multiple cores with the name [%s], with instancedirs [%s] and [%s]",
                name, addedCores.get(name), cd.getInstanceDir()));
      addedCores.put(name, cd.getInstanceDir());
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
    
    isShutDown = true;
    
    if (isZooKeeperAware()) {
      cancelCoreRecoveries();
      zkSys.publishCoresAsDown(solrCores.getCores());
    }

    try {
      coreAdminHandler.shutdown();
    } catch (Exception e) {
      log.warn("Error shutting down CoreAdminHandler. Continuing to close CoreContainer.");
      e.printStackTrace();
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
      try {
        if (shardHandlerFactory != null) {
          shardHandlerFactory.close();
        }
      } finally {
        try {
          if (updateShardHandler != null) {
            updateShardHandler.close();
          }
        } finally {
          // we want to close zk stuff last
          zkSys.close();
        }
      }
    }
    org.apache.lucene.util.IOUtils.closeWhileHandlingException(loader); // best effort
  }

  public void cancelCoreRecoveries() {

    List<SolrCore> cores = solrCores.getCores();

    // we must cancel without holding the cores sync
    // make sure we wait for any recoveries to stop
    for (SolrCore core : cores) {
      try {
        core.getSolrCoreState().cancelRecovery();
      } catch (Exception e) {
        SolrException.log(log, "Error canceling recovery for core", e);
      }
    }
  }
  
  @Override
  protected void finalize() throws Throwable {
    try {
      if(!isShutDown){
        log.error("CoreContainer was not close prior to finalize(), indicates a bug -- POSSIBLE RESOURCE LEAK!!!  instance=" + System.identityHashCode(this));
      }
    } finally {
      super.finalize();
    }
  }

  public CoresLocator getCoresLocator() {
    return coresLocator;
  }
  
  protected SolrCore registerCore(String name, SolrCore core, boolean registerInZk) {
    if( core == null ) {
      throw new RuntimeException( "Can not register a null core." );
    }
    if( name == null ||
        name.indexOf( '/'  ) >= 0 ||
        name.indexOf( '\\' ) >= 0 ){
      throw new RuntimeException( "Invalid core name: "+name );
    }
    // We can register a core when creating them via the admin UI, so we need to insure that the dynamic descriptors
    // are up to date
    CoreDescriptor cd = core.getCoreDescriptor();
    if ((cd.isTransient() || ! cd.isLoadOnStartup())
        && solrCores.getDynamicDescriptor(name) == null) {
      // Store it away for later use. includes non-transient but not
      // loaded at startup cores.
      solrCores.putDynamicDescriptor(name, cd);
    }

    SolrCore old = null;

    if (isShutDown) {
      core.close();
      throw new IllegalStateException("This CoreContainer has been close");
    }
    if (cd.isTransient()) {
      old = solrCores.putTransientCore(cfg, name, core, loader);
    } else {
      old = solrCores.putCore(name, core);
    }
      /*
      * set both the name of the descriptor and the name of the
      * core, since the descriptors name is used for persisting.
      */

    core.setName(name);

    coreInitFailures.remove(name);

    if( old == null || old == core) {
      log.info( "registering core: "+name );
      if (registerInZk) {
        zkSys.registerInZk(core, false);
      }
      return null;
    }
    else {
      log.info( "replacing core: "+name );
      old.close();
      if (registerInZk) {
        zkSys.registerInZk(core, false);
      }
      return old;
    }
  }

  /**
   * Creates a new core based on a CoreDescriptor, publishing the core state to the cluster
   * @param cd the CoreDescriptor
   * @return the newly created core
   */
  public SolrCore create(CoreDescriptor cd) {
    return create(cd, true);
  }

  /**
   * Creates a new core based on a CoreDescriptor.
   *
   * @param dcore        a core descriptor
   * @param publishState publish core state to the cluster if true
   *
   * @return the newly created core
   */
  public SolrCore create(CoreDescriptor dcore, boolean publishState) {

    if (isShutDown) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Solr has close.");
    }

    try {

      if (zkSys.getZkController() != null) {
        zkSys.getZkController().preRegister(dcore);
      }

      ConfigSet coreConfig = coreConfigService.getConfig(dcore);
      log.info("Creating SolrCore '{}' using configuration from {}", dcore.getName(), coreConfig.getName());
      SolrCore core = new SolrCore(dcore, coreConfig);
      solrCores.addCreated(core);

      // always kick off recovery if we are in non-Cloud mode
      if (!isZooKeeperAware() && core.getUpdateHandler().getUpdateLog() != null) {
        core.getUpdateHandler().getUpdateLog().recoverFromLog();
      }

      registerCore(dcore.getName(), core, publishState);

      return core;

    }
    catch (Exception e) {
      coreInitFailures.put(dcore.getName(), new CoreLoadFailure(dcore, e));
      log.error("Error creating core [{}]: {}", dcore.getName(), e.getMessage(), e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to create core [" + dcore.getName() + "]", e);
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
  public Map<String, CoreLoadFailure> getCoreInitFailures() {
    return ImmutableMap.copyOf(coreInitFailures);
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

    name = checkDefault(name);

    SolrCore core = solrCores.getCoreFromAnyList(name, false);
    if (core == null)
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "No such core: " + name );

    CoreDescriptor cd = core.getCoreDescriptor();
    try {
      solrCores.waitAddPendingCoreOps(name);
      ConfigSet coreConfig = coreConfigService.getConfig(cd);
      log.info("Reloading SolrCore '{}' using configuration from {}", cd.getName(), coreConfig.getName());
      SolrCore newCore = core.reload(coreConfig, core);
      registerCore(name, newCore, false);
    }
    catch (Exception e) {
      coreInitFailures.put(cd.getName(), new CoreLoadFailure(cd, e));
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to reload core [" + cd.getName() + "]", e);
    }
    finally {
      solrCores.removeFromPendingOps(name);
    }

  }

  //5.0 remove all checkDefaults?
  private String checkDefault(String name) {
    return (null == name || name.isEmpty()) ? getDefaultCoreName() : name;
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

    coresLocator.swap(this, solrCores.getCoreDescriptor(n0), solrCores.getCoreDescriptor(n1));

    log.info("swapped: "+n0 + " with " + n1);
  }

  /**
   * Unload a core from this container, leaving all files on disk
   * @param name the name of the core to unload
   */
  public void unload(String name) {
    unload(name, false, false, false);
  }

  /**
   * Unload a core from this container, optionally removing the core's data and configuration
   *
   * @param name the name of the core to unload
   * @param deleteIndexDir if true, delete the core's index on close
   * @param deleteDataDir if true, delete the core's data directory on close
   * @param deleteInstanceDir if true, delete the core's instance directory on close
   */
  public void unload(String name, boolean deleteIndexDir, boolean deleteDataDir, boolean deleteInstanceDir) {

    name = checkDefault(name);

    // check for core-init errors first
    CoreLoadFailure loadFailure = coreInitFailures.remove(name);
    if (loadFailure != null) {
      // getting the index directory requires opening a DirectoryFactory with a SolrConfig, etc,
      // which we may not be able to do because of the init error.  So we just go with what we
      // can glean from the CoreDescriptor - datadir and instancedir
      SolrCore.deleteUnloadedCore(loadFailure.cd, deleteDataDir, deleteInstanceDir);
      return;
    }

    CoreDescriptor cd = solrCores.getCoreDescriptor(name);
    if (cd == null)
      throw new SolrException(ErrorCode.BAD_REQUEST, "Cannot unload non-existent core [" + name + "]");

    boolean close = solrCores.isLoadedNotPendingClose(name);
    SolrCore core = solrCores.remove(name);
    coresLocator.delete(this, cd);

    if (core == null) {
      // transient core
      SolrCore.deleteUnloadedCore(cd, deleteDataDir, deleteInstanceDir);
      return;
    }

    if (zkSys.getZkController() != null) {
      // cancel recovery in cloud mode
      core.getSolrCoreState().cancelRecovery();
    }

    core.unloadOnClose(deleteIndexDir, deleteDataDir, deleteInstanceDir);
    if (close)
      core.close();

    if (zkSys.getZkController() != null) {
      try {
        zkSys.getZkController().unregister(name, cd);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted while unregistering core [" + name + "] from cloud state");
      } catch (KeeperException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error unregistering core [" + name + "] from cloud state", e);
      }
    }

  }

  public void rename(String name, String toName) {
    try (SolrCore core = getCore(name)) {
      if (core != null) {
        registerCore(toName, core, true);
        name = checkDefault(name);
        SolrCore old = solrCores.remove(name);
        coresLocator.rename(this, old.getCoreDescriptor(), core.getCoreDescriptor());
      }
    }
  }

  /**
   * Get the CoreDescriptors for all cores managed by this container
   * @return a List of CoreDescriptors
   */
  public List<CoreDescriptor> getCoreDescriptors() {
    return solrCores.getCoreDescriptors();
  }

  public CoreDescriptor getCoreDescriptor(String coreName) {
    // TODO make this less hideous!
    for (CoreDescriptor cd : getCoreDescriptors()) {
      if (cd.getName().equals(coreName))
        return cd;
    }
    return null;
  }

  public String getCoreRootDirectory() {
    return cfg.getCoreRootDirectory();
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
    SolrCore core = solrCores.getCoreFromAnyList(name, true);

    if (core != null) {
      return core;
    }

    // OK, it's not presently in any list, is it in the list of dynamic cores but not loaded yet? If so, load it.
    CoreDescriptor desc = solrCores.getDynamicDescriptor(name);
    if (desc == null) { //Nope, no transient core with this name

      // if there was an error initalizing this core, throw a 500
      // error with the details for clients attempting to access it.
      CoreLoadFailure loadFailure = getCoreInitFailures().get(name);
      if (null != loadFailure) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "SolrCore '" + name +
                                "' is not available due to init failure: " +
                                loadFailure.exception.getMessage(), loadFailure.exception);
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
        if (zkSys.getZkController() != null) {
          zkSys.getZkController().throwErrorIfReplicaReplaced(desc);
        }
        core = create(desc); // This should throw an error if it fails.
      }
      core.open();
    }
    finally {
      solrCores.removeFromPendingOps(name);
    }

    return core;
  }

  // ---------------- CoreContainer request handlers --------------

  protected <T> T createHandler(String handlerClass, Class<T> clazz) {
    return loader.newInstance(handlerClass, clazz, null, new Class[] { CoreContainer.class }, new Object[] { this });
  }

  public CoreAdminHandler getMultiCoreHandler() {
    return coreAdminHandler;
  }

  public CollectionsHandler getCollectionsHandler() {
    return collectionsHandler;
  }

  public InfoHandler getInfoHandler() {
    return infoHandler;
  }

  // ---------------- Multicore self related methods ---------------

  /**
   * the default core name, or null if there is no default core name
   */
  public String getDefaultCoreName() {
    return cfg.getDefaultCoreName();
  }

  // all of the following properties aren't synchronized
  // but this should be OK since they normally won't be changed rapidly
  @Deprecated
  public boolean isPersistent() {
    return cfg.isPersistent();
  }
  
  public String getAdminPath() {
    return cfg.getAdminPath();
  }
  
  public String getHostName() {
    return this.hostName;
  }

  /**
   * Gets the alternate path for multicore handling:
   * This is used in case there is a registered unnamed core (aka name is "") to
   * declare an alternate way of accessing named cores.
   * This can also be used in a pseudo single-core environment so admins can prepare
   * a new version before swapping.
   */
  public String getManagementPath() {
    return cfg.getManagementPath();
  }

  public LogWatcher getLogging() {
    return logging;
  }

  /**
   * Determines whether the core is already loaded or not but does NOT load the core
   *
   */
  public boolean isLoaded(String name) {
    return solrCores.isLoaded(name);
  }

  public boolean isLoadedNotPendingClose(String name) {
    return solrCores.isLoadedNotPendingClose(name);
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

  public String getSolrHome() {
    return solrHome;
  }

  public boolean isZooKeeperAware() {
    return zkSys.getZkController() != null;
  }
  
  public ZkController getZkController() {
    return zkSys.getZkController();
  }
  
  public ConfigSolr getConfig() {
    return cfg;
  }

  /** The default ShardHandlerFactory used to communicate with other solr instances */
  public ShardHandlerFactory getShardHandlerFactory() {
    return shardHandlerFactory;
  }
  
  public UpdateShardHandler getUpdateShardHandler() {
    return updateShardHandler;
  }

  public SolrResourceLoader getResourceLoader() {
    return loader;
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
