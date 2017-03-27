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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.config.Lookup;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientContextBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientContextBuilder.AuthSchemeRegistryProvider;
import org.apache.solr.client.solrj.impl.SolrHttpClientContextBuilder.CredentialsProviderProvider;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.BackupRepositoryFactory;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.SnapShooter;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.InfoHandler;
import org.apache.solr.handler.admin.MetricsCollectorHandler;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.handler.admin.SecurityConfHandlerLocal;
import org.apache.solr.handler.admin.SecurityConfHandlerZk;
import org.apache.solr.handler.admin.ZookeeperInfoHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationPlugin;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.security.SecurityPluginHolder;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.stats.MetricUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static org.apache.solr.common.params.CommonParams.AUTHC_PATH;
import static org.apache.solr.common.params.CommonParams.AUTHZ_PATH;
import static org.apache.solr.common.params.CommonParams.COLLECTIONS_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.CONFIGSETS_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.CORES_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.INFO_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.METRICS_PATH;
import static org.apache.solr.common.params.CommonParams.ZK_PATH;
import static org.apache.solr.security.AuthenticationPlugin.AUTHENTICATION_PLUGIN_PROP;

/**
 *
 * @since solr 1.3
 */
public class CoreContainer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final SolrCores solrCores = new SolrCores(this);

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
  protected ConfigSetsHandler configSetsHandler = null;

  private PKIAuthenticationPlugin pkiAuthenticationPlugin;

  protected Properties containerProperties;

  private ConfigSetService coreConfigService;

  protected ZkContainer zkSys = new ZkContainer();
  protected ShardHandlerFactory shardHandlerFactory;

  private UpdateShardHandler updateShardHandler;

  private ExecutorService coreContainerWorkExecutor = ExecutorUtil.newMDCAwareCachedThreadPool(
      new DefaultSolrThreadFactory("coreContainerWorkExecutor") );

  protected LogWatcher logging = null;

  private CloserThread backgroundCloser = null;
  protected final NodeConfig cfg;
  protected final SolrResourceLoader loader;

  protected final String solrHome;

  protected final CoresLocator coresLocator;

  private String hostName;

  private final BlobRepository blobRepository = new BlobRepository(this);

  private PluginBag<SolrRequestHandler> containerHandlers = new PluginBag<>(SolrRequestHandler.class, null);

  private boolean asyncSolrCoreLoad;

  protected SecurityConfHandler securityConfHandler;

  private SecurityPluginHolder<AuthorizationPlugin> authorizationPlugin;

  private SecurityPluginHolder<AuthenticationPlugin> authenticationPlugin;

  private BackupRepositoryFactory backupRepoFactory;

  protected SolrMetricManager metricManager;

  protected MetricsHandler metricsHandler;

  protected MetricsCollectorHandler metricsCollectorHandler;

  private enum CoreInitFailedAction { fromleader, none }

  /**
   * This method instantiates a new instance of {@linkplain BackupRepository}.
   *
   * @param repositoryName The name of the backup repository (Optional).
   *                       If not specified, a default implementation is used.
   * @return a new instance of {@linkplain BackupRepository}.
   */
  public BackupRepository newBackupRepository(Optional<String> repositoryName) {
    BackupRepository repository;
    if (repositoryName.isPresent()) {
      repository = backupRepoFactory.newInstance(getResourceLoader(), repositoryName.get());
    } else {
      repository = backupRepoFactory.newInstance(getResourceLoader());
    }
    return repository;
  }

  public ExecutorService getCoreZkRegisterExecutorService() {
    return zkSys.getCoreZkRegisterExecutorService();
  }

  public SolrRequestHandler getRequestHandler(String path) {
    return RequestHandlerBase.getRequestHandler(path, containerHandlers);
  }

  public PluginBag<SolrRequestHandler> getRequestHandlers() {
    return this.containerHandlers;
  }

 // private ClientConnectionManager clientConnectionManager = new PoolingClientConnectionManager();

  {
    log.debug("New CoreContainer " + System.identityHashCode(this));
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
    this(SolrXmlConfig.fromSolrHome(loader, loader.getInstancePath()));
  }

  /**
   * Create a new CoreContainer using the given solr home directory.  The container's
   * cores are not loaded.
   * @param solrHome a String containing the path to the solr home directory
   * @see #load()
   */
  public CoreContainer(String solrHome) {
    this(new SolrResourceLoader(Paths.get(solrHome)));
  }

  /**
   * Create a new CoreContainer using the given SolrResourceLoader,
   * configuration and CoresLocator.  The container's cores are
   * not loaded.
   * @param config a ConfigSolr representation of this container's configuration
   * @see #load()
   */
  public CoreContainer(NodeConfig config) {
    this(config, new Properties());
  }

  public CoreContainer(NodeConfig config, Properties properties) {
    this(config, properties, new CorePropertiesLocator(config.getCoreRootDirectory()));
  }

  public CoreContainer(NodeConfig config, Properties properties, boolean asyncSolrCoreLoad) {
    this(config, properties, new CorePropertiesLocator(config.getCoreRootDirectory()), asyncSolrCoreLoad);
  }

  public CoreContainer(NodeConfig config, Properties properties, CoresLocator locator) {
    this(config, properties, locator, false);
  }

  public CoreContainer(NodeConfig config, Properties properties, CoresLocator locator, boolean asyncSolrCoreLoad) {
    this.loader = config.getSolrResourceLoader();
    this.solrHome = loader.getInstancePath().toString();
    this.cfg = requireNonNull(config);
    this.coresLocator = locator;
    this.containerProperties = new Properties(properties);
    this.asyncSolrCoreLoad = asyncSolrCoreLoad;
  }

  private synchronized void initializeAuthorizationPlugin(Map<String, Object> authorizationConf) {
    authorizationConf = Utils.getDeepCopy(authorizationConf, 4);
    //Initialize the Authorization module
    SecurityPluginHolder<AuthorizationPlugin> old = authorizationPlugin;
    SecurityPluginHolder<AuthorizationPlugin> authorizationPlugin = null;
    if (authorizationConf != null) {
      String klas = (String) authorizationConf.get("class");
      if (klas == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "class is required for authorization plugin");
      }
      if (old != null && old.getZnodeVersion() == readVersion(authorizationConf)) {
        return;
      }
      log.info("Initializing authorization plugin: " + klas);
      authorizationPlugin = new SecurityPluginHolder<>(readVersion(authorizationConf),
          getResourceLoader().newInstance(klas, AuthorizationPlugin.class));

      // Read and pass the authorization context to the plugin
      authorizationPlugin.plugin.init(authorizationConf);
    } else {
      log.debug("Security conf doesn't exist. Skipping setup for authorization module.");
    }
    this.authorizationPlugin = authorizationPlugin;
    if (old != null) {
      try {
        old.plugin.close();
      } catch (Exception e) {
      }
    }
  }

  private synchronized void initializeAuthenticationPlugin(Map<String, Object> authenticationConfig) {
    authenticationConfig = Utils.getDeepCopy(authenticationConfig, 4);
    String pluginClassName = null;
    if (authenticationConfig != null) {
      if (authenticationConfig.containsKey("class")) {
        pluginClassName = String.valueOf(authenticationConfig.get("class"));
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, "No 'class' specified for authentication in ZK.");
      }
    }

    if (pluginClassName != null) {
      log.debug("Authentication plugin class obtained from security.json: "+pluginClassName);
    } else if (System.getProperty(AUTHENTICATION_PLUGIN_PROP) != null) {
      pluginClassName = System.getProperty(AUTHENTICATION_PLUGIN_PROP);
      log.debug("Authentication plugin class obtained from system property '" +
          AUTHENTICATION_PLUGIN_PROP + "': " + pluginClassName);
    } else {
      log.debug("No authentication plugin used.");
    }
    SecurityPluginHolder<AuthenticationPlugin> old = authenticationPlugin;
    SecurityPluginHolder<AuthenticationPlugin> authenticationPlugin = null;

    // Initialize the plugin
    if (pluginClassName != null) {
      log.info("Initializing authentication plugin: " + pluginClassName);
      authenticationPlugin = new SecurityPluginHolder<>(readVersion(authenticationConfig),
          getResourceLoader().newInstance(pluginClassName,
              AuthenticationPlugin.class,
              null,
              new Class[]{CoreContainer.class},
              new Object[]{this}));
    }
    if (authenticationPlugin != null) {
      authenticationPlugin.plugin.init(authenticationConfig);
      setupHttpClientForAuthPlugin(authenticationPlugin.plugin);
    }
    this.authenticationPlugin = authenticationPlugin;
    try {
      if (old != null) old.plugin.close();
    } catch (Exception e) {/*do nothing*/ }

  }

  private void setupHttpClientForAuthPlugin(Object authcPlugin) {
    if (authcPlugin instanceof HttpClientBuilderPlugin) {
      // Setup HttpClient for internode communication
      SolrHttpClientBuilder builder = ((HttpClientBuilderPlugin) authcPlugin).getHttpClientBuilder(HttpClientUtil.getHttpClientBuilder());
      
      // The default http client of the core container's shardHandlerFactory has already been created and
      // configured using the default httpclient configurer. We need to reconfigure it using the plugin's
      // http client configurer to set it up for internode communication.
      log.debug("Reconfiguring HttpClient settings.");

      SolrHttpClientContextBuilder httpClientBuilder = new SolrHttpClientContextBuilder();
      if (builder.getCredentialsProviderProvider() != null) {
        httpClientBuilder.setDefaultCredentialsProvider(new CredentialsProviderProvider() {
          
          @Override
          public CredentialsProvider getCredentialsProvider() {
            return builder.getCredentialsProviderProvider().getCredentialsProvider();
          }
        });
      }
      if (builder.getAuthSchemeRegistryProvider() != null) {
        httpClientBuilder.setAuthSchemeRegistryProvider(new AuthSchemeRegistryProvider() {

          @Override
          public Lookup<AuthSchemeProvider> getAuthSchemeRegistry() {
            return builder.getAuthSchemeRegistryProvider().getAuthSchemeRegistry();
          }
        });
      }

      HttpClientUtil.setHttpClientRequestContextBuilder(httpClientBuilder);

    } else {
      if (pkiAuthenticationPlugin != null) {
        //this happened due to an authc plugin reload. no need to register the pkiAuthc plugin again
        if(pkiAuthenticationPlugin.isInterceptorRegistered()) return;
        log.info("PKIAuthenticationPlugin is managing internode requests");
        setupHttpClientForAuthPlugin(pkiAuthenticationPlugin);
        pkiAuthenticationPlugin.setInterceptorRegistered();
      }
    }
  }

  private static int readVersion(Map<String, Object> conf) {
    if (conf == null) return -1;
    Map meta = (Map) conf.get("");
    if (meta == null) return -1;
    Number v = (Number) meta.get("v");
    return v == null ? -1 : v.intValue();
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
    containerProperties = null;
  }

  public static CoreContainer createAndLoad(Path solrHome) {
    return createAndLoad(solrHome, solrHome.resolve(SolrXmlConfig.SOLR_XML_FILE));
  }

  /**
   * Create a new CoreContainer and load its cores
   * @param solrHome the solr home directory
   * @param configFile the file containing this container's configuration
   * @return a loaded CoreContainer
   */
  public static CoreContainer createAndLoad(Path solrHome, Path configFile) {
    SolrResourceLoader loader = new SolrResourceLoader(solrHome);
    CoreContainer cc = new CoreContainer(SolrXmlConfig.fromFile(loader, configFile));
    try {
      cc.load();
    } catch (Exception e) {
      cc.shutdown();
      throw e;
    }
    return cc;
  }

  public Properties getContainerProperties() {
    return containerProperties;
  }

  public PKIAuthenticationPlugin getPkiAuthenticationPlugin() {
    return pkiAuthenticationPlugin;
  }

  public SolrMetricManager getMetricManager() {
    return metricManager;
  }

  //-------------------------------------------------------------------
  // Initialization / Cleanup
  //-------------------------------------------------------------------

  /**
   * Load the cores defined for this CoreContainer
   */
  public void load()  {
    log.debug("Loading cores into CoreContainer [instanceDir={}]", loader.getInstancePath());

    // add the sharedLib to the shared resource loader before initializing cfg based plugins
    String libDir = cfg.getSharedLibDirectory();
    if (libDir != null) {
      Path libPath = loader.getInstancePath().resolve(libDir);
      try {
        loader.addToClassLoader(SolrResourceLoader.getURLs(libPath));
        loader.reloadLuceneSPI();
      } catch (IOException e) {
        if (!libDir.equals("lib")) { // Don't complain if default "lib" dir does not exist
          log.warn("Couldn't add files from {} to classpath: {}", libPath, e.getMessage());
        }
      }
    }

    metricManager = new SolrMetricManager();

    coreContainerWorkExecutor = MetricUtils.instrumentedExecutorService(
        coreContainerWorkExecutor,
        metricManager.registry(SolrMetricManager.getRegistryName(SolrInfoMBean.Group.node)),
        SolrMetricManager.mkName("coreContainerWorkExecutor", SolrInfoMBean.Category.CONTAINER.toString(), "threadPool"));

    shardHandlerFactory = ShardHandlerFactory.newInstance(cfg.getShardHandlerFactoryPluginInfo(), loader);
    if (shardHandlerFactory instanceof SolrMetricProducer) {
      SolrMetricProducer metricProducer = (SolrMetricProducer) shardHandlerFactory;
      metricProducer.initializeMetrics(metricManager, SolrInfoMBean.Group.node.toString(), "httpShardHandler");
    }

    updateShardHandler = new UpdateShardHandler(cfg.getUpdateShardHandlerConfig());
    updateShardHandler.initializeMetrics(metricManager, SolrInfoMBean.Group.node.toString(), "updateShardHandler");

    solrCores.allocateLazyCores(cfg.getTransientCacheSize(), loader);

    logging = LogWatcher.newRegisteredLogWatcher(cfg.getLogWatcherConfig(), loader);

    hostName = cfg.getNodeName();

    zkSys.initZooKeeper(this, solrHome, cfg.getCloudConfig());
    if(isZooKeeperAware())  pkiAuthenticationPlugin = new PKIAuthenticationPlugin(this, zkSys.getZkController().getNodeName());

    MDCLoggingContext.setNode(this);

    securityConfHandler = isZooKeeperAware() ? new SecurityConfHandlerZk(this) : new SecurityConfHandlerLocal(this);
    reloadSecurityProperties();
    this.backupRepoFactory = new BackupRepositoryFactory(cfg.getBackupRepositoryPlugins());

    createHandler(ZK_PATH, ZookeeperInfoHandler.class.getName(), ZookeeperInfoHandler.class);
    collectionsHandler = createHandler(COLLECTIONS_HANDLER_PATH, cfg.getCollectionsHandlerClass(), CollectionsHandler.class);
    infoHandler        = createHandler(INFO_HANDLER_PATH, cfg.getInfoHandlerClass(), InfoHandler.class);
    coreAdminHandler   = createHandler(CORES_HANDLER_PATH, cfg.getCoreAdminHandlerClass(), CoreAdminHandler.class);
    configSetsHandler = createHandler(CONFIGSETS_HANDLER_PATH, cfg.getConfigSetsHandlerClass(), ConfigSetsHandler.class);
    metricsHandler = createHandler(METRICS_PATH, MetricsHandler.class.getName(), MetricsHandler.class);
    metricsCollectorHandler = createHandler(MetricsCollectorHandler.HANDLER_PATH, MetricsCollectorHandler.class.getName(), MetricsCollectorHandler.class);
    // may want to add some configuration here in the future
    metricsCollectorHandler.init(null);
    containerHandlers.put(AUTHZ_PATH, securityConfHandler);
    securityConfHandler.initializeMetrics(metricManager, SolrInfoMBean.Group.node.toString(), AUTHZ_PATH);
    containerHandlers.put(AUTHC_PATH, securityConfHandler);
    if(pkiAuthenticationPlugin != null)
      containerHandlers.put(PKIAuthenticationPlugin.PATH, pkiAuthenticationPlugin.getRequestHandler());

    metricManager.loadReporters(cfg.getMetricReporterPlugins(), loader, null, SolrInfoMBean.Group.node);
    metricManager.loadReporters(cfg.getMetricReporterPlugins(), loader, null, SolrInfoMBean.Group.jvm);
    metricManager.loadReporters(cfg.getMetricReporterPlugins(), loader, null, SolrInfoMBean.Group.jetty);

    coreConfigService = ConfigSetService.createConfigSetService(cfg, loader, zkSys.zkController);

    containerProperties.putAll(cfg.getSolrProperties());

    // initialize gauges for reporting the number of cores and disk total/free

    String registryName = SolrMetricManager.getRegistryName(SolrInfoMBean.Group.node);
    metricManager.registerGauge(registryName, () -> solrCores.getCores().size(),
        true, "loaded", SolrInfoMBean.Category.CONTAINER.toString(), "cores");
    metricManager.registerGauge(registryName, () -> solrCores.getCoreNames().size() - solrCores.getCores().size(),
        true, "lazy",SolrInfoMBean.Category.CONTAINER.toString(), "cores");
    metricManager.registerGauge(registryName, () -> solrCores.getAllCoreNames().size() - solrCores.getCoreNames().size(),
        true, "unloaded",SolrInfoMBean.Category.CONTAINER.toString(), "cores");
    metricManager.registerGauge(registryName, () -> cfg.getCoreRootDirectory().toFile().getTotalSpace(),
        true, "totalSpace", SolrInfoMBean.Category.CONTAINER.toString(), "fs");
    metricManager.registerGauge(registryName, () -> cfg.getCoreRootDirectory().toFile().getUsableSpace(),
        true, "usableSpace", SolrInfoMBean.Category.CONTAINER.toString(), "fs");

    if (isZooKeeperAware()) {
      metricManager.loadClusterReporters(cfg.getMetricReporterPlugins(), this);
    }

    // setup executor to load cores in parallel
    ExecutorService coreLoadExecutor = MetricUtils.instrumentedExecutorService(
        ExecutorUtil.newMDCAwareFixedThreadPool(
            cfg.getCoreLoadThreadCount(isZooKeeperAware()),
            new DefaultSolrThreadFactory("coreLoadExecutor")),
        metricManager.registry(SolrMetricManager.getRegistryName(SolrInfoMBean.Group.node)),
        SolrMetricManager.mkName("coreLoadExecutor",SolrInfoMBean.Category.CONTAINER.toString(), "threadPool"));
    final List<Future<SolrCore>> futures = new ArrayList<>();
    try {
      List<CoreDescriptor> cds = coresLocator.discover(this);
      if (isZooKeeperAware()) {
        //sort the cores if it is in SolrCloud. In standalone node the order does not matter
        CoreSorter coreComparator = new CoreSorter().init(this);
        cds = new ArrayList<>(cds);//make a copy
        Collections.sort(cds, coreComparator::compare);
      }
      checkForDuplicateCoreNames(cds);

      for (final CoreDescriptor cd : cds) {
        if (cd.isTransient() || !cd.isLoadOnStartup()) {
          solrCores.putDynamicDescriptor(cd.getName(), cd);
        } else if (asyncSolrCoreLoad) {
          solrCores.markCoreAsLoading(cd);
        }
        if (cd.isLoadOnStartup()) {
          futures.add(coreLoadExecutor.submit(() -> {
            SolrCore core;
            try {
              if (zkSys.getZkController() != null) {
                zkSys.getZkController().throwErrorIfReplicaReplaced(cd);
              }

              core = create(cd, false, false);
            } finally {
              if (asyncSolrCoreLoad) {
                solrCores.markCoreAsNotLoading(cd);
              }
            }
            try {
              zkSys.registerInZk(core, true, false);
            } catch (RuntimeException e) {
              SolrException.log(log, "Error registering SolrCore", e);
            }
            return core;
          }));
        }
      }


      // Start the background thread
      backgroundCloser = new CloserThread(this, solrCores, cfg);
      backgroundCloser.start();

    } finally {
      if (asyncSolrCoreLoad && futures != null) {

        coreContainerWorkExecutor.submit((Runnable) () -> {
          try {
            for (Future<SolrCore> future : futures) {
              try {
                future.get();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } catch (ExecutionException e) {
                log.error("Error waiting for SolrCore to be created", e);
              }
            }
          } finally {
            ExecutorUtil.shutdownAndAwaitTermination(coreLoadExecutor);
          }
        });
      } else {
        ExecutorUtil.shutdownAndAwaitTermination(coreLoadExecutor);
      }
    }

    if (isZooKeeperAware()) {
      zkSys.getZkController().checkOverseerDesignate();
    }
  }

  public void securityNodeChanged() {
    log.info("Security node changed, reloading security.json");
    reloadSecurityProperties();
  }

  /**
   * Make sure securityConfHandler is initialized
   */
  private void reloadSecurityProperties() {
    SecurityConfHandler.SecurityConfig securityConfig = securityConfHandler.getSecurityConfig(false);
    initializeAuthorizationPlugin((Map<String, Object>) securityConfig.getData().get("authorization"));
    initializeAuthenticationPlugin((Map<String, Object>) securityConfig.getData().get("authentication"));
  }

  private static void checkForDuplicateCoreNames(List<CoreDescriptor> cds) {
    Map<String, Path> addedCores = Maps.newHashMap();
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

    ExecutorUtil.shutdownAndAwaitTermination(coreContainerWorkExecutor);
    if (metricManager != null) {
      metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoMBean.Group.node));
    }

    if (isZooKeeperAware()) {
      cancelCoreRecoveries();
      zkSys.zkController.publishNodeAsDown(zkSys.zkController.getNodeName());
      if (metricManager != null) {
        metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoMBean.Group.cluster));
      }
    }

    try {
      if (coreAdminHandler != null) coreAdminHandler.shutdown();
    } catch (Exception e) {
      log.warn("Error shutting down CoreAdminHandler. Continuing to close CoreContainer.", e);
    }

    try {
      // First wake up the closer thread, it'll terminate almost immediately since it checks isShutDown.
      synchronized (solrCores.getModifyLock()) {
        solrCores.getModifyLock().notifyAll(); // wake up anyone waiting
      }
      if (backgroundCloser != null) { // Doesn't seem right, but tests get in here without initializing the core.
        try {
          while (true) {
            backgroundCloser.join(15000);
            if (backgroundCloser.isAlive()) {
              synchronized (solrCores.getModifyLock()) {
                solrCores.getModifyLock().notifyAll(); // there is a race we have to protect against
              }
            } else {
              break;
            }
          }
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

    // It should be safe to close the authorization plugin at this point.
    try {
      if(authorizationPlugin != null) {
        authorizationPlugin.plugin.close();
      }
    } catch (IOException e) {
      log.warn("Exception while closing authorization plugin.", e);
    }

    // It should be safe to close the authentication plugin at this point.
    try {
      if(authenticationPlugin != null) {
        authenticationPlugin.plugin.close();
        authenticationPlugin = null;
      }
    } catch (Exception e) {
      log.warn("Exception while closing authentication plugin.", e);
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

  protected SolrCore registerCore(String name, SolrCore core, boolean registerInZk, boolean skipRecovery) {
    if( core == null ) {
      throw new RuntimeException( "Can not register a null core." );
    }

    // We can register a core when creating them via the admin UI, so we need to ensure that the dynamic descriptors
    // are up to date
    CoreDescriptor cd = core.getCoreDescriptor();
    if ((cd.isTransient() || ! cd.isLoadOnStartup())
        && solrCores.getDynamicDescriptor(name) == null) {
      // Store it away for later use. includes non-transient but not
      // loaded at startup cores.
      solrCores.putDynamicDescriptor(name, cd);
    }

    SolrCore old;

    if (isShutDown) {
      core.close();
      throw new IllegalStateException("This CoreContainer has been closed");
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
      log.debug( "registering core: "+name );
      if (registerInZk) {
        zkSys.registerInZk(core, false, skipRecovery);
      }
      return null;
    }
    else {
      log.debug( "replacing core: "+name );
      old.close();
      if (registerInZk) {
        zkSys.registerInZk(core, false, skipRecovery);
      }
      return old;
    }
  }

  /**
   * Creates a new core, publishing the core state to the cluster
   * @param coreName the core name
   * @param parameters the core parameters
   * @return the newly created core
   */
  public SolrCore create(String coreName, Map<String, String> parameters) {
    return create(coreName, cfg.getCoreRootDirectory().resolve(coreName), parameters, false);
  }

  /**
   * Creates a new core in a specified instance directory, publishing the core state to the cluster
   * @param coreName the core name
   * @param instancePath the instance directory
   * @param parameters the core parameters
   * @return the newly created core
   */
  public SolrCore create(String coreName, Path instancePath, Map<String, String> parameters, boolean newCollection) {

    CoreDescriptor cd = new CoreDescriptor(this, coreName, instancePath, parameters);

    // TODO: There's a race here, isn't there?
    if (getAllCoreNames().contains(coreName)) {
      log.warn("Creating a core with existing name is not allowed");
      // TODO: Shouldn't this be a BAD_REQUEST?
      throw new SolrException(ErrorCode.SERVER_ERROR, "Core with name '" + coreName + "' already exists.");
    }

    boolean preExisitingZkEntry = false;
    try {
      if (getZkController() != null) {
        if (!Overseer.isLegacy(getZkController().getZkStateReader())) {
          if (cd.getCloudDescriptor().getCoreNodeName() == null) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "non legacy mode coreNodeName missing " + parameters.toString());

          }
        }
        preExisitingZkEntry = getZkController().checkIfCoreNodeNameAlreadyExists(cd);
      }

      SolrCore core = create(cd, true, newCollection);

      // only write out the descriptor if the core is successfully created
      coresLocator.create(this, cd);

      return core;
    }
    catch (Exception ex) {
      if (isZooKeeperAware() && !preExisitingZkEntry) {
        try {
          getZkController().unregister(coreName, cd);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          SolrException.log(log, null, e);
        } catch (KeeperException e) {
          SolrException.log(log, null, e);
        }
      }

      Throwable tc = ex;
      Throwable c = null;
      do {
        tc = tc.getCause();
        if (tc != null) {
          c = tc;
        }
      } while (tc != null);

      String rootMsg = "";
      if (c != null) {
        rootMsg = " Caused by: " + c.getMessage();
      }

      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Error CREATEing SolrCore '" + coreName + "': " + ex.getMessage() + rootMsg, ex);
    }

  }

  /**
   * Creates a new core based on a CoreDescriptor.
   *
   * @param dcore        a core descriptor
   * @param publishState publish core state to the cluster if true
   *
   * @return the newly created core
   */
  private SolrCore create(CoreDescriptor dcore, boolean publishState, boolean newCollection) {

    if (isShutDown) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Solr has been shutdown.");
    }

    SolrCore core = null;
    try {
      MDCLoggingContext.setCoreDescriptor(dcore);
      SolrIdentifierValidator.validateCoreName(dcore.getName());
      if (zkSys.getZkController() != null) {
        zkSys.getZkController().preRegister(dcore);
      }

      ConfigSet coreConfig = coreConfigService.getConfig(dcore);
      log.info("Creating SolrCore '{}' using configuration from {}", dcore.getName(), coreConfig.getName());
      try {
        core = new SolrCore(dcore, coreConfig);
      } catch (SolrException e) {
        core = processCoreCreateException(e, dcore, coreConfig);
      }

      // always kick off recovery if we are in non-Cloud mode
      if (!isZooKeeperAware() && core.getUpdateHandler().getUpdateLog() != null) {
        core.getUpdateHandler().getUpdateLog().recoverFromLog();
      }

      registerCore(dcore.getName(), core, publishState, newCollection);

      return core;
    } catch (Exception e) {
      coreInitFailures.put(dcore.getName(), new CoreLoadFailure(dcore, e));
      final SolrException solrException = new SolrException(ErrorCode.SERVER_ERROR, "Unable to create core [" + dcore.getName() + "]", e);
      if(core != null && !core.isClosed())
        IOUtils.closeQuietly(core);
      throw solrException;
    } catch (Throwable t) {
      SolrException e = new SolrException(ErrorCode.SERVER_ERROR, "JVM Error creating core [" + dcore.getName() + "]: " + t.getMessage(), t);
      coreInitFailures.put(dcore.getName(), new CoreLoadFailure(dcore, e));
      if(core != null && !core.isClosed())
        IOUtils.closeQuietly(core);
      throw t;
    } finally {
      MDCLoggingContext.clear();
    }
  }
  
  /**
   * Take action when we failed to create a SolrCore. If error is due to corrupt index, try to recover. Various recovery
   * strategies can be specified via system properties "-DCoreInitFailedAction={fromleader, none}"
   *
   * @see CoreInitFailedAction
   *
   * @param original
   *          the problem seen when loading the core the first time.
   * @param dcore
   *          core descriptor for the core to create
   * @param coreConfig
   *          core config for the core to create
   * @return if possible
   * @throws SolrException
   *           rethrows the original exception if we will not attempt to recover, throws a new SolrException with the
   *           original exception as a suppressed exception if there is a second problem creating the solr core.
   */
  private SolrCore processCoreCreateException(SolrException original, CoreDescriptor dcore, ConfigSet coreConfig) {
    // Traverse full chain since CIE may not be root exception
    Throwable cause = original;
    while ((cause = cause.getCause()) != null) {
      if (cause instanceof CorruptIndexException) {
        break;
      }
    }
    
    // If no CorruptIndexExeption, nothing we can try here
    if (cause == null) throw original;
    
    CoreInitFailedAction action = CoreInitFailedAction.valueOf(System.getProperty(CoreInitFailedAction.class.getSimpleName(), "none"));
    log.debug("CorruptIndexException while creating core, will attempt to repair via {}", action);
    
    switch (action) {
      case fromleader: // Recovery from leader on a CorruptedIndexException
        if (isZooKeeperAware()) {
          CloudDescriptor desc = dcore.getCloudDescriptor();
          try {
            Replica leader = getZkController().getClusterState()
                .getCollection(desc.getCollectionName())
                .getSlice(desc.getShardId())
                .getLeader();
            if (leader != null && leader.getState() == State.ACTIVE) {
              log.info("Found active leader, will attempt to create fresh core and recover.");
              resetIndexDirectory(dcore, coreConfig);
              return new SolrCore(dcore, coreConfig);
            }
          } catch (SolrException se) {
            se.addSuppressed(original);
            throw se;
          }
        }
        throw original;
      case none:
        throw original;
      default:
        log.warn("Failed to create core, and did not recognize specified 'CoreInitFailedAction': [{}]. Valid options are {}.",
            action, Arrays.asList(CoreInitFailedAction.values()));
        throw original;
    }
  }

  /**
   * Write a new index directory for the a SolrCore, but do so without loading it.
   */
  private void resetIndexDirectory(CoreDescriptor dcore, ConfigSet coreConfig) {
    SolrConfig config = coreConfig.getSolrConfig();

    String registryName = SolrMetricManager.getRegistryName(SolrInfoMBean.Group.core, dcore.getName());
    DirectoryFactory df = DirectoryFactory.loadDirectoryFactory(config, this, registryName);
    String dataDir = SolrCore.findDataDir(df, null, config, dcore);

    String tmpIdxDirName = "index." + new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).format(new Date());
    SolrCore.modifyIndexProps(df, dataDir, config, tmpIdxDirName);

    // Free the directory object that we had to create for this
    Directory dir = null;
    try {
      dir = df.get(dataDir, DirContext.META_DATA, config.indexConfig.lockType);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      try {
        df.release(dir);
        df.doneWithDirectory(dir);
      } catch (IOException e) {
        SolrException.log(log, e);
      }
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
    SolrCore core = solrCores.getCoreFromAnyList(name, false);
    if (core != null) {
      CoreDescriptor cd = core.getCoreDescriptor();
      try {
        solrCores.waitAddPendingCoreOps(cd.getName());
        ConfigSet coreConfig = coreConfigService.getConfig(cd);
        log.info("Reloading SolrCore '{}' using configuration from {}", cd.getName(), coreConfig.getName());
        SolrCore newCore = core.reload(coreConfig);
        registerCore(cd.getName(), newCore, false, false);
        if (getZkController() != null) {
          boolean onlyLeaderIndexes = getZkController().getClusterState().getCollection(cd.getCollectionName()).getRealtimeReplicas() == 1;
          if (onlyLeaderIndexes && !cd.getCloudDescriptor().isLeader()) {
            getZkController().stopReplicationFromLeader(core.getName());
            getZkController().startReplicationFromLeader(newCore.getName());
          }
        }
      } catch (SolrCoreState.CoreIsClosedException e) {
        throw e;
      } catch (Exception e) {
        coreInitFailures.put(cd.getName(), new CoreLoadFailure(cd, e));
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to reload core [" + cd.getName() + "]", e);
      }
      finally {
        solrCores.removeFromPendingOps(cd.getName());
      }
    } else {
      CoreLoadFailure clf = coreInitFailures.get(name);
      if (clf != null) {
        create(clf.cd, true, false);
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such core: " + name );
      }
    }
  }

  /**
   * Swaps two SolrCore descriptors.
   */
  public void swap(String n0, String n1) {
    if( n0 == null || n1 == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Can not swap unnamed cores." );
    }
    solrCores.swap(n0, n1);

    coresLocator.swap(this, solrCores.getCoreDescriptor(n0), solrCores.getCoreDescriptor(n1));

    log.info("swapped: " + n0 + " with " + n1);
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

    if (name != null) {
      // check for core-init errors first
      CoreLoadFailure loadFailure = coreInitFailures.remove(name);
      if (loadFailure != null) {
        // getting the index directory requires opening a DirectoryFactory with a SolrConfig, etc,
        // which we may not be able to do because of the init error.  So we just go with what we
        // can glean from the CoreDescriptor - datadir and instancedir
        SolrCore.deleteUnloadedCore(loadFailure.cd, deleteDataDir, deleteInstanceDir);
        return;
      }
    }

    CoreDescriptor cd = solrCores.getCoreDescriptor(name);
    if (cd == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Cannot unload non-existent core [" + name + "]");
    }

    boolean close = solrCores.isLoadedNotPendingClose(name);
    SolrCore core = solrCores.remove(name);
    coresLocator.delete(this, cd);

    if (core == null) {
      // transient core
      SolrCore.deleteUnloadedCore(cd, deleteDataDir, deleteInstanceDir);
      return;
    }

    // delete metrics specific to this core
    metricManager.removeRegistry(core.getCoreMetricManager().getRegistryName());

    if (zkSys.getZkController() != null) {
      // cancel recovery in cloud mode
      core.getSolrCoreState().cancelRecovery();
    }
    
    core.unloadOnClose(deleteIndexDir, deleteDataDir, deleteInstanceDir);
    if (close)
      core.closeAndWait();

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
    SolrIdentifierValidator.validateCoreName(toName);
    try (SolrCore core = getCore(name)) {
      if (core != null) {
        String oldRegistryName = core.getCoreMetricManager().getRegistryName();
        String newRegistryName = SolrCoreMetricManager.createRegistryName(core, toName);
        metricManager.swapRegistries(oldRegistryName, newRegistryName);
        registerCore(toName, core, true, false);
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

  public Path getCoreRootDirectory() {
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

    // Do this in two phases since we don't want to lock access to the cores over a load.
    SolrCore core = solrCores.getCoreFromAnyList(name, true);

    if (core != null) {
      return core;
    }

    // OK, it's not presently in any list, is it in the list of dynamic cores but not loaded yet? If so, load it.
    CoreDescriptor desc = solrCores.getDynamicDescriptor(name);
    if (desc == null) { //Nope, no transient core with this name

      // if there was an error initializing this core, throw a 500
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
        core = create(desc, true, false); // This should throw an error if it fails.
      }
      core.open();
    }
    finally {
      solrCores.removeFromPendingOps(name);
    }

    return core;
  }

  public BlobRepository getBlobRepository(){
    return blobRepository;
  }
  
  /**
   * If using asyncSolrCoreLoad=true, calling this after {@link #load()} will
   * not return until all cores have finished loading.
   * 
   * @param timeoutMs timeout, upon which method simply returns
   */
  public void waitForLoadingCoresToFinish(long timeoutMs) {
    solrCores.waitForLoadingCoresToFinish(timeoutMs);
  }
  
  public void waitForLoadingCore(String name, long timeoutMs) {
    solrCores.waitForLoadingCoreToFinish(name, timeoutMs);
  }

  // ---------------- CoreContainer request handlers --------------

  protected <T> T createHandler(String path, String handlerClass, Class<T> clazz) {
    T handler = loader.newInstance(handlerClass, clazz, null, new Class[] { CoreContainer.class }, new Object[] { this });
    if (handler instanceof SolrRequestHandler) {
      containerHandlers.put(path, (SolrRequestHandler)handler);
    }
    if (handler instanceof SolrMetricProducer) {
      ((SolrMetricProducer)handler).initializeMetrics(metricManager, SolrInfoMBean.Group.node.toString(), path);
    }
    return handler;
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

  public ConfigSetsHandler getConfigSetsHandler() {
    return configSetsHandler;
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
  
  public NodeConfig getConfig() {
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
  
  public boolean isCoreLoading(String name) {
    return solrCores.isCoreLoading(name);
  }

  public AuthorizationPlugin getAuthorizationPlugin() {
    return authorizationPlugin == null ? null : authorizationPlugin.plugin;
  }

  public AuthenticationPlugin getAuthenticationPlugin() {
    return authenticationPlugin == null ? null : authenticationPlugin.plugin;
  }

  public NodeConfig getNodeConfig() {
    return cfg;
  }

}

class CloserThread extends Thread {
  CoreContainer container;
  SolrCores solrCores;
  NodeConfig cfg;


  CloserThread(CoreContainer container, SolrCores solrCores, NodeConfig cfg) {
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
