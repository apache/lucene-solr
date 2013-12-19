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

import com.google.common.collect.Maps;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.InfoHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 *
 * @since solr 1.3
 */
public class CoreContainer {

  protected static final Logger log = LoggerFactory.getLogger(CoreContainer.class);

  private final SolrCores solrCores = new SolrCores(this);

  protected final Map<String,Exception> coreInitFailures =
    Collections.synchronizedMap(new LinkedHashMap<String,Exception>());

  protected CoreAdminHandler coreAdminHandler = null;
  protected CollectionsHandler collectionsHandler = null;
  private InfoHandler infoHandler;

  protected Properties containerProperties;

  protected Map<String ,IndexSchema> indexSchemaCache;
  protected boolean shareSchema;

  protected ZkContainer zkSys = new ZkContainer();
  private ShardHandlerFactory shardHandlerFactory;
  
  private UpdateShardHandler updateShardHandler;

  protected LogWatcher logging = null;

  private CloserThread backgroundCloser = null;
  protected final ConfigSolr cfg;
  protected final SolrResourceLoader loader;

  protected final String solrHome;

  protected final CoresLocator coresLocator;
  
  private String hostName;
  
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

    shareSchema = cfg.hasSchemaCache();

    if (shareSchema) {
      indexSchemaCache = new ConcurrentHashMap<String,IndexSchema>();
    }
    
    hostName = cfg.getHost();
    log.info("Host Name: " + hostName);

    zkSys.initZooKeeper(this, solrHome, cfg);

    collectionsHandler = createHandler(cfg.getCollectionsHandlerClass(), CollectionsHandler.class);
    infoHandler        = createHandler(cfg.getInfoHandlerClass(), InfoHandler.class);
    coreAdminHandler   = createHandler(cfg.getCoreAdminHandlerClass(), CoreAdminHandler.class);

    containerProperties = cfg.getSolrProperties("solr");

    // setup executor to load cores in parallel
    // do not limit the size of the executor in zk mode since cores may try and wait for each other.
    ExecutorService coreLoadExecutor = Executors.newFixedThreadPool(
        ( zkSys.getZkController() == null ? cfg.getCoreLoadThreadCount() : Integer.MAX_VALUE ),
        new DefaultSolrThreadFactory("coreLoadExecutor") );

    try {
      CompletionService<SolrCore> completionService = new ExecutorCompletionService<SolrCore>(
          coreLoadExecutor);

      Set<Future<SolrCore>> pending = new HashSet<Future<SolrCore>>();

      List<CoreDescriptor> cds = coresLocator.discover(this);
      checkForDuplicateCoreNames(cds);

      for (final CoreDescriptor cd : cds) {

        final String name = cd.getName();
        try {

          if (cd.isTransient() || ! cd.isLoadOnStartup()) {
            // Store it away for later use. includes non-transient but not
            // loaded at startup cores.
            solrCores.putDynamicDescriptor(name, cd);
          }
          if (cd.isLoadOnStartup()) { // The normal case

            Callable<SolrCore> task = new Callable<SolrCore>() {
              @Override
              public SolrCore call() {
                SolrCore c = null;
                try {
                  if (zkSys.getZkController() != null) {
                    preRegisterInZk(cd);
                  }
                  c = create(cd);
                  registerCore(cd.isTransient(), name, c, false);
                } catch (Throwable t) {
              /*    if (isZooKeeperAware()) {
                    try {
                      zkSys.zkController.unregister(name, cd);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      SolrException.log(log, null, e);
                    } catch (KeeperException e) {
                      SolrException.log(log, null, e);
                    }
                  }*/
                  SolrException.log(log, null, t);
                  if (c != null) {
                    c.close();
                  }
                }
                return c;
              }
            };
            pending.add(completionService.submit(task));

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
    
    if (isZooKeeperAware()) {
      try {
        zkSys.getZkController().publishAndWaitForDownStates();
      } catch (KeeperException e) {
        log.error("", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("", e);
      }
    }
    isShutDown = true;

    if (isZooKeeperAware()) {
      zkSys.publishCoresAsDown(solrCores.getCores());
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

  public CoresLocator getCoresLocator() {
    return coresLocator;
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

    synchronized (coreInitFailures) {
      coreInitFailures.remove(name);
    }

    if( old == null || old == core) {
      log.info( "registering core: "+name );
      zkSys.registerInZk(core);
      return null;
    }
    else {
      log.info( "replacing core: "+name );
      if (!returnPrevNotClosed) {
        old.close();
      }
      zkSys.registerInZk(core);
      return old;
    }
  }

  /**
   * Registers a SolrCore descriptor in the registry using the core's name.
   * If returnPrev==false, the old core, if different, is closed.
   * @return a previous core having the same name if it existed and returnPrev==true
   */
  public SolrCore register(SolrCore core, boolean returnPrev) {
    return registerCore(core.getCoreDescriptor().isTransient(), core.getName(), core, returnPrev);
  }

  public SolrCore register(String name, SolrCore core, boolean returnPrev) {
    return registerCore(core.getCoreDescriptor().isTransient(), name, core, returnPrev);
  }

  // Helper method to separate out creating a core from local configuration files. See create()
  private SolrCore createFromLocal(String instanceDir, CoreDescriptor dcore) {
    SolrResourceLoader solrLoader = null;

    SolrConfig config = null;
    solrLoader = new SolrResourceLoader(instanceDir, loader.getClassLoader(), dcore.getSubstitutableProperties());
    try {
      config = new SolrConfig(solrLoader, dcore.getConfigName(), null);
    } catch (Exception e) {
      log.error("Failed to load file {}", new File(instanceDir, dcore.getConfigName()).getAbsolutePath());
      throw new SolrException(ErrorCode.SERVER_ERROR,
          "Could not load config file " + new File(instanceDir, dcore.getConfigName()).getAbsolutePath(),
          e);
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
          log.info("creating new schema object for core: " + dcore.getName());
          schema = IndexSchemaFactory.buildIndexSchema(dcore.getSchemaName(), config);
          indexSchemaCache.put(key, schema);
        } else {
          log.info("re-using schema object for core: " + dcore.getName());
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
      if (zkSys.getZkController() != null) {
        created = zkSys.createFromZk(instanceDir, dcore, loader);
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

      SolrCore core = solrCores.getCoreFromAnyList(name, false);
      if (core == null)
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "No such core: " + name );

      try {
        solrCores.waitAddPendingCoreOps(name);
        CoreDescriptor cd = core.getCoreDescriptor();

        File instanceDir = new File(cd.getInstanceDir());

        log.info("Reloading SolrCore '{}' using instanceDir: {}",
                 cd.getName(), instanceDir.getAbsolutePath());
        SolrResourceLoader solrLoader;
        if(zkSys.getZkController() == null) {
          solrLoader = new SolrResourceLoader(instanceDir.getAbsolutePath(), loader.getClassLoader(),
                                                cd.getSubstitutableProperties());
        } else {
          try {
            String collection = cd.getCloudDescriptor().getCollectionName();
            zkSys.getZkController().createCollectionZkNode(cd.getCloudDescriptor());

            String zkConfigName = zkSys.getZkController().getZkStateReader().readConfigName(collection);
            if (zkConfigName == null) {
              log.error("Could not find config name for collection:" + collection);
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                                           "Could not find config name for collection:" + collection);
            }
            solrLoader = new ZkSolrResourceLoader(instanceDir.getAbsolutePath(), zkConfigName, loader.getClassLoader(),
                cd.getSubstitutableProperties(), zkSys.getZkController());
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
  
  /** Removes and returns registered core w/o decrementing it's reference count */
  public SolrCore remove( String name ) {
    name = checkDefault(name);
    CoreDescriptor cd = solrCores.getCoreDescriptor(name);
    SolrCore removed = solrCores.remove(name, true);
    coresLocator.delete(this, cd);
    return removed;
  }

  public void rename(String name, String toName) {
    SolrCore core = getCore(name);
    try {
      if (core != null) {
        registerCore(false, toName, core, false);
        name = checkDefault(name);
        SolrCore old = solrCores.remove(name, false);
        coresLocator.rename(this, old.getCoreDescriptor(), core.getCoreDescriptor());
      }
    } finally {
      if (core != null) {
        core.close();
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
        if (zkSys.getZkController() != null) {
          preRegisterInZk(desc);
        }
        core = create(desc); // This should throw an error if it fails.
        core.open();
        registerCore(desc.isTransient(), name, core, false);
      } else {
        core.open();
      }
    } catch(Exception ex){
      // remains to be seen how transient cores and such
      // will work in SolrCloud mode, but just to be future
      // proof...
      /*if (isZooKeeperAware()) {
        try {
          getZkController().unregister(name, desc);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          SolrException.log(log, null, e);
        } catch (KeeperException e) {
          SolrException.log(log, null, e);
        }
      }*/
      throw recordAndThrow(name, "Unable to create core: " + name, ex);
    } finally {
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

  public void preRegisterInZk(final CoreDescriptor p) {
    zkSys.getZkController().preRegister(p);
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
  
  public boolean isShareSchema() {
    return shareSchema;
  }

  /** The default ShardHandlerFactory used to communicate with other solr instances */
  public ShardHandlerFactory getShardHandlerFactory() {
    return shardHandlerFactory;
  }
  
  public UpdateShardHandler getUpdateShardHandler() {
    return updateShardHandler;
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
