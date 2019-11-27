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

import java.io.Closeable;
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
import java.util.HashMap;
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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientContextBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientContextBuilder.AuthSchemeRegistryProvider;
import org.apache.solr.client.solrj.impl.SolrHttpClientContextBuilder.CredentialsProviderProvider;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerTaskQueue;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.autoscaling.AutoScalingHandler;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.BackupRepositoryFactory;
import org.apache.solr.filestore.PackageStoreAPI;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.SnapShooter;
import org.apache.solr.handler.admin.AutoscalingHistoryHandler;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.HealthCheckHandler;
import org.apache.solr.handler.admin.InfoHandler;
import org.apache.solr.handler.admin.MetricsCollectorHandler;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.handler.admin.MetricsHistoryHandler;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.handler.admin.SecurityConfHandlerLocal;
import org.apache.solr.handler.admin.SecurityConfHandlerZk;
import org.apache.solr.handler.admin.ZookeeperInfoHandler;
import org.apache.solr.handler.admin.ZookeeperStatusHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.pkg.PackageLoader;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.SolrFieldCacheBean;
import org.apache.solr.security.AuditLoggerPlugin;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationPlugin;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.security.PublicKeyHandler;
import org.apache.solr.security.SecurityPluginHolder;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.OrderedExecutor;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.stats.MetricUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static org.apache.solr.common.params.CommonParams.AUTHC_PATH;
import static org.apache.solr.common.params.CommonParams.AUTHZ_PATH;
import static org.apache.solr.common.params.CommonParams.AUTOSCALING_HISTORY_PATH;
import static org.apache.solr.common.params.CommonParams.COLLECTIONS_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.CONFIGSETS_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.CORES_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.INFO_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.METRICS_HISTORY_PATH;
import static org.apache.solr.common.params.CommonParams.METRICS_PATH;
import static org.apache.solr.common.params.CommonParams.ZK_PATH;
import static org.apache.solr.common.params.CommonParams.ZK_STATUS_PATH;
import static org.apache.solr.core.CorePropertiesLocator.PROPERTIES_FILENAME;
import static org.apache.solr.security.AuthenticationPlugin.AUTHENTICATION_PLUGIN_PROP;

/**
 * @since solr 1.3
 */
public class CoreContainer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final SolrCores solrCores = new SolrCores(this);

  public static class CoreLoadFailure {

    public final CoreDescriptor cd;
    public final Exception exception;

    public CoreLoadFailure(CoreDescriptor cd, Exception loadFailure) {
      this.cd = new CoreDescriptor(cd.getName(), cd);
      this.exception = loadFailure;
    }
  }

  protected final Map<String, CoreLoadFailure> coreInitFailures = new ConcurrentHashMap<>();

  protected volatile CoreAdminHandler coreAdminHandler = null;
  protected volatile CollectionsHandler collectionsHandler = null;
  protected volatile HealthCheckHandler healthCheckHandler = null;

  private volatile InfoHandler infoHandler;
  protected volatile ConfigSetsHandler configSetsHandler = null;

  private volatile PKIAuthenticationPlugin pkiAuthenticationPlugin;

  protected volatile Properties containerProperties;

  private volatile ConfigSetService coreConfigService;

  protected final ZkContainer zkSys = new ZkContainer();
  protected volatile ShardHandlerFactory shardHandlerFactory;

  private volatile UpdateShardHandler updateShardHandler;

  private volatile ExecutorService coreContainerWorkExecutor = ExecutorUtil.newMDCAwareCachedThreadPool(
      new DefaultSolrThreadFactory("coreContainerWorkExecutor"));

  private final OrderedExecutor replayUpdatesExecutor;

  protected volatile LogWatcher logging = null;

  private volatile CloserThread backgroundCloser = null;
  protected final NodeConfig cfg;
  protected final SolrResourceLoader loader;

  protected final String solrHome;

  protected final CoresLocator coresLocator;

  private volatile String hostName;

  private final BlobRepository blobRepository = new BlobRepository(this);

  private volatile PluginBag<SolrRequestHandler> containerHandlers = new PluginBag<>(SolrRequestHandler.class, null);

  private volatile boolean asyncSolrCoreLoad;

  protected volatile SecurityConfHandler securityConfHandler;

  private volatile SecurityPluginHolder<AuthorizationPlugin> authorizationPlugin;

  private volatile SecurityPluginHolder<AuthenticationPlugin> authenticationPlugin;

  private volatile SecurityPluginHolder<AuditLoggerPlugin> auditloggerPlugin;

  private volatile BackupRepositoryFactory backupRepoFactory;

  protected volatile SolrMetricManager metricManager;

  protected volatile String metricTag = SolrMetricProducer.getUniqueMetricTag(this, null);

  protected volatile SolrMetricsContext solrMetricsContext;

  protected MetricsHandler metricsHandler;

  protected volatile MetricsHistoryHandler metricsHistoryHandler;

  protected volatile MetricsCollectorHandler metricsCollectorHandler;

  protected volatile AutoscalingHistoryHandler autoscalingHistoryHandler;

  private PackageStoreAPI packageStoreAPI;
  private PackageLoader packageLoader;


  // Bits for the state variable.
  public final static long LOAD_COMPLETE = 0x1L;
  public final static long CORE_DISCOVERY_COMPLETE = 0x2L;
  public final static long INITIAL_CORE_LOAD_COMPLETE = 0x4L;
  private volatile long status = 0L;

  protected volatile AutoScalingHandler autoScalingHandler;

  private ExecutorService coreContainerAsyncTaskExecutor = ExecutorUtil.newMDCAwareCachedThreadPool("Core Container Async Task");

  private enum CoreInitFailedAction {fromleader, none}

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

  {
    log.debug("New CoreContainer " + System.identityHashCode(this));
  }

  /**
   * Create a new CoreContainer using system properties to detect the solr home
   * directory.  The container's cores are not loaded.
   *
   * @see #load()
   */
  public CoreContainer() {
    this(new SolrResourceLoader(SolrResourceLoader.locateSolrHome()));
  }

  /**
   * Create a new CoreContainer using the given SolrResourceLoader.  The container's
   * cores are not loaded.
   *
   * @param loader the SolrResourceLoader
   * @see #load()
   */
  public CoreContainer(SolrResourceLoader loader) {
    this(SolrXmlConfig.fromSolrHome(loader, loader.getInstancePath()));
  }

  /**
   * Create a new CoreContainer using the given solr home directory.  The container's
   * cores are not loaded.
   *
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
   *
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
    containerHandlers.put(PublicKeyHandler.PATH, new PublicKeyHandler());
    this.cfg = requireNonNull(config);
    if (null != this.cfg.getBooleanQueryMaxClauseCount()) {
      IndexSearcher.setMaxClauseCount(this.cfg.getBooleanQueryMaxClauseCount());
    }
    this.coresLocator = locator;
    this.containerProperties = new Properties(properties);
    this.asyncSolrCoreLoad = asyncSolrCoreLoad;
    this.replayUpdatesExecutor = new OrderedExecutor(
        cfg.getReplayUpdatesThreads(),
        ExecutorUtil.newMDCAwareCachedThreadPool(
            cfg.getReplayUpdatesThreads(),
            new DefaultSolrThreadFactory("replayUpdatesExecutor")));
  }

  private synchronized void initializeAuthorizationPlugin(Map<String, Object> authorizationConf) {
    authorizationConf = Utils.getDeepCopy(authorizationConf, 4);
    int newVersion = readVersion(authorizationConf);
    //Initialize the Authorization module
    SecurityPluginHolder<AuthorizationPlugin> old = authorizationPlugin;
    SecurityPluginHolder<AuthorizationPlugin> authorizationPlugin = null;
    if (authorizationConf != null) {
      String klas = (String) authorizationConf.get("class");
      if (klas == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "class is required for authorization plugin");
      }
      if (old != null && old.getZnodeVersion() == newVersion && newVersion > 0) {
        log.debug("Authorization config not modified");
        return;
      }
      log.info("Initializing authorization plugin: " + klas);
      authorizationPlugin = new SecurityPluginHolder<>(newVersion,
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
        log.error("Exception while attempting to close old authorization plugin", e);
      }
    }
  }

  private void initializeAuditloggerPlugin(Map<String, Object> auditConf) {
    auditConf = Utils.getDeepCopy(auditConf, 4);
    int newVersion = readVersion(auditConf);
    //Initialize the Auditlog module
    SecurityPluginHolder<AuditLoggerPlugin> old = auditloggerPlugin;
    SecurityPluginHolder<AuditLoggerPlugin> newAuditloggerPlugin = null;
    if (auditConf != null) {
      String klas = (String) auditConf.get("class");
      if (klas == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "class is required for auditlogger plugin");
      }
      if (old != null && old.getZnodeVersion() == newVersion && newVersion > 0) {
        log.debug("Auditlogger config not modified");
        return;
      }
      log.info("Initializing auditlogger plugin: " + klas);
      newAuditloggerPlugin = new SecurityPluginHolder<>(newVersion,
          getResourceLoader().newInstance(klas, AuditLoggerPlugin.class));

      newAuditloggerPlugin.plugin.init(auditConf);
      newAuditloggerPlugin.plugin.initializeMetrics(solrMetricsContext, "/auditlogging");
    } else {
      log.debug("Security conf doesn't exist. Skipping setup for audit logging module.");
    }
    this.auditloggerPlugin = newAuditloggerPlugin;
    if (old != null) {
      try {
        old.plugin.close();
      } catch (Exception e) {
        log.error("Exception while attempting to close old auditlogger plugin", e);
      }
    }
  }


  private synchronized void initializeAuthenticationPlugin(Map<String, Object> authenticationConfig) {
    authenticationConfig = Utils.getDeepCopy(authenticationConfig, 4);
    int newVersion = readVersion(authenticationConfig);
    String pluginClassName = null;
    if (authenticationConfig != null) {
      if (authenticationConfig.containsKey("class")) {
        pluginClassName = String.valueOf(authenticationConfig.get("class"));
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, "No 'class' specified for authentication in ZK.");
      }
    }

    if (pluginClassName != null) {
      log.debug("Authentication plugin class obtained from security.json: " + pluginClassName);
    } else if (System.getProperty(AUTHENTICATION_PLUGIN_PROP) != null) {
      pluginClassName = System.getProperty(AUTHENTICATION_PLUGIN_PROP);
      log.debug("Authentication plugin class obtained from system property '" +
          AUTHENTICATION_PLUGIN_PROP + "': " + pluginClassName);
    } else {
      log.debug("No authentication plugin used.");
    }
    SecurityPluginHolder<AuthenticationPlugin> old = authenticationPlugin;
    SecurityPluginHolder<AuthenticationPlugin> authenticationPlugin = null;

    if (old != null && old.getZnodeVersion() == newVersion && newVersion > 0) {
      log.debug("Authentication config not modified");
      return;
    }

    // Initialize the plugin
    if (pluginClassName != null) {
      log.info("Initializing authentication plugin: " + pluginClassName);
      authenticationPlugin = new SecurityPluginHolder<>(newVersion,
          getResourceLoader().newInstance(pluginClassName,
              AuthenticationPlugin.class,
              null,
              new Class[]{CoreContainer.class},
              new Object[]{this}));
    }
    if (authenticationPlugin != null) {
      authenticationPlugin.plugin.init(authenticationConfig);
      setupHttpClientForAuthPlugin(authenticationPlugin.plugin);
      authenticationPlugin.plugin.initializeMetrics(solrMetricsContext, "/authentication");
    }
    this.authenticationPlugin = authenticationPlugin;
    try {
      if (old != null) old.plugin.close();
    } catch (Exception e) {
      log.error("Exception while attempting to close old authentication plugin", e);
    }

  }

  private void setupHttpClientForAuthPlugin(Object authcPlugin) {
    if (authcPlugin instanceof HttpClientBuilderPlugin) {
      // Setup HttpClient for internode communication
      HttpClientBuilderPlugin builderPlugin = ((HttpClientBuilderPlugin) authcPlugin);
      SolrHttpClientBuilder builder = builderPlugin.getHttpClientBuilder(HttpClientUtil.getHttpClientBuilder());
      shardHandlerFactory.setSecurityBuilder(builderPlugin);
      updateShardHandler.setSecurityBuilder(builderPlugin);

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
    }
    // Always register PKI auth interceptor, which will then delegate the decision of who should secure
    // each request to the configured authentication plugin.
    if (pkiAuthenticationPlugin != null && !pkiAuthenticationPlugin.isInterceptorRegistered()) {
      pkiAuthenticationPlugin.getHttpClientBuilder(HttpClientUtil.getHttpClientBuilder());
      shardHandlerFactory.setSecurityBuilder(pkiAuthenticationPlugin);
      updateShardHandler.setSecurityBuilder(pkiAuthenticationPlugin);
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
    replayUpdatesExecutor = null;
  }

  public static CoreContainer createAndLoad(Path solrHome) {
    return createAndLoad(solrHome, solrHome.resolve(SolrXmlConfig.SOLR_XML_FILE));
  }

  /**
   * Create a new CoreContainer and load its cores
   *
   * @param solrHome   the solr home directory
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

  public MetricsHandler getMetricsHandler() {
    return metricsHandler;
  }

  public MetricsHistoryHandler getMetricsHistoryHandler() {
    return metricsHistoryHandler;
  }

  public OrderedExecutor getReplayUpdatesExecutor() {
    return replayUpdatesExecutor;
  }

  public PackageLoader getPackageLoader() {
    return packageLoader;
  }

  public PackageStoreAPI getPackageStoreAPI() {
    return packageStoreAPI;
  }
  //-------------------------------------------------------------------
  // Initialization / Cleanup
  //-------------------------------------------------------------------

  /**
   * Load the cores defined for this CoreContainer
   */
  public void load() {
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

    packageStoreAPI = new PackageStoreAPI(this);
    containerHandlers.getApiBag().register(new AnnotatedApi(packageStoreAPI.readAPI), Collections.EMPTY_MAP);
    containerHandlers.getApiBag().register(new AnnotatedApi(packageStoreAPI.writeAPI), Collections.EMPTY_MAP);

    metricManager = new SolrMetricManager(loader, cfg.getMetricsConfig());
    String registryName = SolrMetricManager.getRegistryName(SolrInfoBean.Group.node);
    solrMetricsContext = new SolrMetricsContext(metricManager, registryName, metricTag);

    coreContainerWorkExecutor = MetricUtils.instrumentedExecutorService(
        coreContainerWorkExecutor, null,
        metricManager.registry(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node)),
        SolrMetricManager.mkName("coreContainerWorkExecutor", SolrInfoBean.Category.CONTAINER.toString(), "threadPool"));

    shardHandlerFactory = ShardHandlerFactory.newInstance(cfg.getShardHandlerFactoryPluginInfo(), loader);
    if (shardHandlerFactory instanceof SolrMetricProducer) {
      SolrMetricProducer metricProducer = (SolrMetricProducer) shardHandlerFactory;
      metricProducer.initializeMetrics(solrMetricsContext, "httpShardHandler");
    }

    updateShardHandler = new UpdateShardHandler(cfg.getUpdateShardHandlerConfig());
    updateShardHandler.initializeMetrics(solrMetricsContext, "updateShardHandler");

    solrCores.load(loader);


    logging = LogWatcher.newRegisteredLogWatcher(cfg.getLogWatcherConfig(), loader);

    hostName = cfg.getNodeName();

    zkSys.initZooKeeper(this, solrHome, cfg.getCloudConfig());
    if (isZooKeeperAware()) {
      pkiAuthenticationPlugin = new PKIAuthenticationPlugin(this, zkSys.getZkController().getNodeName(),
          (PublicKeyHandler) containerHandlers.get(PublicKeyHandler.PATH));
      // use deprecated API for back-compat, remove in 9.0
      pkiAuthenticationPlugin.initializeMetrics(solrMetricsContext, "/authentication/pki");
      TracerConfigurator.loadTracer(loader, cfg.getTracerConfiguratorPluginInfo(), getZkController().getZkStateReader());
    }

    MDCLoggingContext.setNode(this);

    securityConfHandler = isZooKeeperAware() ? new SecurityConfHandlerZk(this) : new SecurityConfHandlerLocal(this);
    reloadSecurityProperties();
    this.backupRepoFactory = new BackupRepositoryFactory(cfg.getBackupRepositoryPlugins());

    createHandler(ZK_PATH, ZookeeperInfoHandler.class.getName(), ZookeeperInfoHandler.class);
    createHandler(ZK_STATUS_PATH, ZookeeperStatusHandler.class.getName(), ZookeeperStatusHandler.class);
    collectionsHandler = createHandler(COLLECTIONS_HANDLER_PATH, cfg.getCollectionsHandlerClass(), CollectionsHandler.class);
    infoHandler = createHandler(INFO_HANDLER_PATH, cfg.getInfoHandlerClass(), InfoHandler.class);
    coreAdminHandler = createHandler(CORES_HANDLER_PATH, cfg.getCoreAdminHandlerClass(), CoreAdminHandler.class);
    configSetsHandler = createHandler(CONFIGSETS_HANDLER_PATH, cfg.getConfigSetsHandlerClass(), ConfigSetsHandler.class);

    // metricsHistoryHandler uses metricsHandler, so create it first
    metricsHandler = new MetricsHandler(this);
    containerHandlers.put(METRICS_PATH, metricsHandler);
    metricsHandler.initializeMetrics(solrMetricsContext, METRICS_PATH);

    createMetricsHistoryHandler();

    autoscalingHistoryHandler = createHandler(AUTOSCALING_HISTORY_PATH, AutoscalingHistoryHandler.class.getName(), AutoscalingHistoryHandler.class);
    metricsCollectorHandler = createHandler(MetricsCollectorHandler.HANDLER_PATH, MetricsCollectorHandler.class.getName(), MetricsCollectorHandler.class);
    // may want to add some configuration here in the future
    metricsCollectorHandler.init(null);

    containerHandlers.put(AUTHZ_PATH, securityConfHandler);
    securityConfHandler.initializeMetrics(solrMetricsContext, AUTHZ_PATH);
    containerHandlers.put(AUTHC_PATH, securityConfHandler);


    PluginInfo[] metricReporters = cfg.getMetricsConfig().getMetricReporters();
    metricManager.loadReporters(metricReporters, loader, this, null, null, SolrInfoBean.Group.node);
    metricManager.loadReporters(metricReporters, loader, this, null, null, SolrInfoBean.Group.jvm);
    metricManager.loadReporters(metricReporters, loader, this, null, null, SolrInfoBean.Group.jetty);

    coreConfigService = ConfigSetService.createConfigSetService(cfg, loader, zkSys.zkController);

    containerProperties.putAll(cfg.getSolrProperties());

    // initialize gauges for reporting the number of cores and disk total/free

    solrMetricsContext.gauge(() -> solrCores.getCores().size(),
        true, "loaded", SolrInfoBean.Category.CONTAINER.toString(), "cores");
    solrMetricsContext.gauge(() -> solrCores.getLoadedCoreNames().size() - solrCores.getCores().size(),
        true, "lazy", SolrInfoBean.Category.CONTAINER.toString(), "cores");
    solrMetricsContext.gauge(() -> solrCores.getAllCoreNames().size() - solrCores.getLoadedCoreNames().size(),
        true, "unloaded", SolrInfoBean.Category.CONTAINER.toString(), "cores");
    Path dataHome = cfg.getSolrDataHome() != null ? cfg.getSolrDataHome() : cfg.getCoreRootDirectory();
    solrMetricsContext.gauge(() -> dataHome.toFile().getTotalSpace(),
        true, "totalSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs");
    solrMetricsContext.gauge(() -> dataHome.toFile().getUsableSpace(),
        true, "usableSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs");
    solrMetricsContext.gauge(() -> dataHome.toAbsolutePath().toString(),
        true, "path", SolrInfoBean.Category.CONTAINER.toString(), "fs");
    solrMetricsContext.gauge(() -> {
          try {
            return org.apache.lucene.util.IOUtils.spins(dataHome.toAbsolutePath());
          } catch (IOException e) {
            // default to spinning
            return true;
          }
        },
        true, "spins", SolrInfoBean.Category.CONTAINER.toString(), "fs");
    solrMetricsContext.gauge(() -> cfg.getCoreRootDirectory().toFile().getTotalSpace(),
        true, "totalSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs", "coreRoot");
    solrMetricsContext.gauge(() -> cfg.getCoreRootDirectory().toFile().getUsableSpace(),
        true, "usableSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs", "coreRoot");
    solrMetricsContext.gauge(() -> cfg.getCoreRootDirectory().toAbsolutePath().toString(),
        true, "path", SolrInfoBean.Category.CONTAINER.toString(), "fs", "coreRoot");
    solrMetricsContext.gauge(() -> {
          try {
            return org.apache.lucene.util.IOUtils.spins(cfg.getCoreRootDirectory().toAbsolutePath());
          } catch (IOException e) {
            // default to spinning
            return true;
          }
        },
        true, "spins", SolrInfoBean.Category.CONTAINER.toString(), "fs", "coreRoot");
    // add version information
    solrMetricsContext.gauge(() -> this.getClass().getPackage().getSpecificationVersion(),
        true, "specification", SolrInfoBean.Category.CONTAINER.toString(), "version");
    solrMetricsContext.gauge(() -> this.getClass().getPackage().getImplementationVersion(),
        true, "implementation", SolrInfoBean.Category.CONTAINER.toString(), "version");

    SolrFieldCacheBean fieldCacheBean = new SolrFieldCacheBean();
    fieldCacheBean.initializeMetrics(solrMetricsContext, null);

    if (isZooKeeperAware()) {
      metricManager.loadClusterReporters(metricReporters, this);
      packageLoader = new PackageLoader(this);
      containerHandlers.getApiBag().register(new AnnotatedApi(packageLoader.getPackageAPI().editAPI), Collections.EMPTY_MAP);
      containerHandlers.getApiBag().register(new AnnotatedApi(packageLoader.getPackageAPI().readAPI), Collections.EMPTY_MAP);
    }


    // setup executor to load cores in parallel
    ExecutorService coreLoadExecutor = MetricUtils.instrumentedExecutorService(
        ExecutorUtil.newMDCAwareFixedThreadPool(
            cfg.getCoreLoadThreadCount(isZooKeeperAware()),
            new DefaultSolrThreadFactory("coreLoadExecutor")), null,
        metricManager.registry(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node)),
        SolrMetricManager.mkName("coreLoadExecutor", SolrInfoBean.Category.CONTAINER.toString(), "threadPool"));
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
      status |= CORE_DISCOVERY_COMPLETE;

      for (final CoreDescriptor cd : cds) {
        if (cd.isTransient() || !cd.isLoadOnStartup()) {
          solrCores.addCoreDescriptor(cd);
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
              solrCores.waitAddPendingCoreOps(cd.getName());
              core = createFromDescriptor(cd, false, false);
            } finally {
              solrCores.removeFromPendingOps(cd.getName());
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

        coreContainerWorkExecutor.submit(() -> {
          try {
            for (Future<SolrCore> future : futures) {
              try {
                future.get();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } catch (ExecutionException e) {
                log.error("Error waiting for SolrCore to be loaded on startup", e.getCause());
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
      // initialize this handler here when SolrCloudManager is ready
      autoScalingHandler = new AutoScalingHandler(getZkController().getSolrCloudManager(), loader);
      containerHandlers.put(AutoScalingHandler.HANDLER_PATH, autoScalingHandler);
      autoScalingHandler.initializeMetrics(solrMetricsContext, AutoScalingHandler.HANDLER_PATH);
    }
    // This is a bit redundant but these are two distinct concepts for all they're accomplished at the same time.
    status |= LOAD_COMPLETE | INITIAL_CORE_LOAD_COMPLETE;
  }

  // MetricsHistoryHandler supports both cloud and standalone configs
  private void createMetricsHistoryHandler() {
    PluginInfo plugin = cfg.getMetricsConfig().getHistoryHandler();
    Map<String, Object> initArgs;
    if (plugin != null && plugin.initArgs != null) {
      initArgs = plugin.initArgs.asMap(5);
      initArgs.put(MetricsHistoryHandler.ENABLE_PROP, plugin.isEnabled());
    } else {
      initArgs = new HashMap<>();
    }
    String name;
    SolrCloudManager cloudManager;
    SolrClient client;
    if (isZooKeeperAware()) {
      name = getZkController().getNodeName();
      cloudManager = getZkController().getSolrCloudManager();
      client = new CloudSolrClient.Builder(Collections.singletonList(getZkController().getZkServerAddress()), Optional.empty())
          .withSocketTimeout(30000).withConnectionTimeout(15000)
          .withHttpClient(updateShardHandler.getDefaultHttpClient()).build();
    } else {
      name = getNodeConfig().getNodeName();
      if (name == null || name.isEmpty()) {
        name = "localhost";
      }
      cloudManager = null;
      client = new EmbeddedSolrServer(this, null) {
        @Override
        public void close() throws IOException {
          // do nothing - we close the container ourselves
        }
      };
      // enable local metrics unless specifically set otherwise
      if (!initArgs.containsKey(MetricsHistoryHandler.ENABLE_NODES_PROP)) {
        initArgs.put(MetricsHistoryHandler.ENABLE_NODES_PROP, true);
      }
      if (!initArgs.containsKey(MetricsHistoryHandler.ENABLE_REPLICAS_PROP)) {
        initArgs.put(MetricsHistoryHandler.ENABLE_REPLICAS_PROP, true);
      }
    }
    metricsHistoryHandler = new MetricsHistoryHandler(name, metricsHandler,
        client, cloudManager, initArgs);
    containerHandlers.put(METRICS_HISTORY_PATH, metricsHistoryHandler);
    metricsHistoryHandler.initializeMetrics(solrMetricsContext, METRICS_HISTORY_PATH);
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
    initializeAuditloggerPlugin((Map<String, Object>) securityConfig.getData().get("auditlogging"));
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

  public void shutdown() {

    ZkController zkController = getZkController();
    if (zkController != null) {
      OverseerTaskQueue overseerCollectionQueue = zkController.getOverseerCollectionQueue();
      overseerCollectionQueue.allowOverseerPendingTasksToComplete();
    }
    log.info("Shutting down CoreContainer instance=" + System.identityHashCode(this));

    ExecutorUtil.shutdownAndAwaitTermination(coreContainerAsyncTaskExecutor);
    ExecutorService customThreadPool = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("closeThreadPool"));

    isShutDown = true;
    try {
      if (isZooKeeperAware()) {
        cancelCoreRecoveries();
        zkSys.zkController.preClose();
      }

      ExecutorUtil.shutdownAndAwaitTermination(coreContainerWorkExecutor);

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

      customThreadPool.submit(() -> {
        replayUpdatesExecutor.shutdownAndAwaitTermination();
      });

      if (metricsHistoryHandler != null) {
        metricsHistoryHandler.close();
        IOUtils.closeQuietly(metricsHistoryHandler.getSolrClient());
      }

      if (metricManager != null) {
        metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node));
        metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm));
        metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jetty));

        metricManager.unregisterGauges(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node), metricTag);
        metricManager.unregisterGauges(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm), metricTag);
        metricManager.unregisterGauges(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jetty), metricTag);
      }

      if (isZooKeeperAware()) {
        cancelCoreRecoveries();

        if (metricManager != null) {
          metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.cluster));
        }
      }

      try {
        if (coreAdminHandler != null) {
          customThreadPool.submit(() -> {
            coreAdminHandler.shutdown();
          });
        }
      } catch (Exception e) {
        log.warn("Error shutting down CoreAdminHandler. Continuing to close CoreContainer.", e);
      }

    } finally {
      try {
        if (shardHandlerFactory != null) {
          customThreadPool.submit(() -> {
            shardHandlerFactory.close();
          });
        }
      } finally {
        try {
          if (updateShardHandler != null) {
            customThreadPool.submit(() -> Collections.singleton(shardHandlerFactory).parallelStream().forEach(c -> {
              updateShardHandler.close();
            }));
          }
        } finally {
          try {
            // we want to close zk stuff last
            zkSys.close();
          } finally {
            ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);
          }
        }

      }
    }

    // It should be safe to close the authorization plugin at this point.
    try {
      if (authorizationPlugin != null) {
        authorizationPlugin.plugin.close();
      }
    } catch (IOException e) {
      log.warn("Exception while closing authorization plugin.", e);
    }

    // It should be safe to close the authentication plugin at this point.
    try {
      if (authenticationPlugin != null) {
        authenticationPlugin.plugin.close();
        authenticationPlugin = null;
      }
    } catch (Exception e) {
      log.warn("Exception while closing authentication plugin.", e);
    }

    // It should be safe to close the auditlogger plugin at this point.
    try {
      if (auditloggerPlugin != null) {
        auditloggerPlugin.plugin.close();
        auditloggerPlugin = null;
      }
    } catch (Exception e) {
      log.warn("Exception while closing auditlogger plugin.", e);
    }

    if(packageLoader != null){
      org.apache.lucene.util.IOUtils.closeWhileHandlingException(packageLoader);
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

  public CoresLocator getCoresLocator() {
    return coresLocator;
  }

  protected SolrCore registerCore(CoreDescriptor cd, SolrCore core, boolean registerInZk, boolean skipRecovery) {
    if (core == null) {
      throw new RuntimeException("Can not register a null core.");
    }

    if (isShutDown) {
      core.close();
      throw new IllegalStateException("This CoreContainer has been closed");
    }
    SolrCore old = solrCores.putCore(cd, core);
    /*
     * set both the name of the descriptor and the name of the
     * core, since the descriptors name is used for persisting.
     */

    core.setName(cd.getName());

    coreInitFailures.remove(cd.getName());

    if (old == null || old == core) {
      log.debug("registering core: " + cd.getName());
      if (registerInZk) {
        zkSys.registerInZk(core, false, skipRecovery);
      }
      return null;
    } else {
      log.debug("replacing core: " + cd.getName());
      old.close();
      if (registerInZk) {
        zkSys.registerInZk(core, false, skipRecovery);
      }
      return old;
    }
  }

  /**
   * Creates a new core, publishing the core state to the cluster
   *
   * @param coreName   the core name
   * @param parameters the core parameters
   * @return the newly created core
   */
  public SolrCore create(String coreName, Map<String, String> parameters) {
    return create(coreName, cfg.getCoreRootDirectory().resolve(coreName), parameters, false);
  }

  /**
   * Creates a new core in a specified instance directory, publishing the core state to the cluster
   *
   * @param coreName     the core name
   * @param instancePath the instance directory
   * @param parameters   the core parameters
   * @return the newly created core
   */
  public SolrCore create(String coreName, Path instancePath, Map<String, String> parameters, boolean newCollection) {

    CoreDescriptor cd = new CoreDescriptor(coreName, instancePath, parameters, getContainerProperties(), isZooKeeperAware());

    // TODO: There's a race here, isn't there?
    // Since the core descriptor is removed when a core is unloaded, it should never be anywhere when a core is created.
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

      // Much of the logic in core handling pre-supposes that the core.properties file already exists, so create it
      // first and clean it up if there's an error.
      coresLocator.create(this, cd);

      SolrCore core = null;
      try {
        solrCores.waitAddPendingCoreOps(cd.getName());
        core = createFromDescriptor(cd, true, newCollection);
        coresLocator.persist(this, cd); // Write out the current core properties in case anything changed when the core was created
      } finally {
        solrCores.removeFromPendingOps(cd.getName());
      }

      return core;
    } catch (Exception ex) {
      // First clean up any core descriptor, there should never be an existing core.properties file for any core that
      // failed to be created on-the-fly.
      coresLocator.delete(this, cd);
      if (isZooKeeperAware() && !preExisitingZkEntry) {
        try {
          getZkController().unregister(coreName, cd);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          SolrException.log(log, null, e);
        } catch (KeeperException e) {
          SolrException.log(log, null, e);
        } catch (Exception e) {
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
   *                     <p>
   *                     WARNING: Any call to this method should be surrounded by a try/finally block
   *                     that calls solrCores.waitAddPendingCoreOps(...) and solrCores.removeFromPendingOps(...)
   *
   *                     <pre>
   *                                                               <code>
   *                                                               try {
   *                                                                  solrCores.waitAddPendingCoreOps(dcore.getName());
   *                                                                  createFromDescriptor(...);
   *                                                               } finally {
   *                                                                  solrCores.removeFromPendingOps(dcore.getName());
   *                                                               }
   *                                                               </code>
   *                                                             </pre>
   *                     <p>
   *                     Trying to put the waitAddPending... in this method results in Bad Things Happening due to race conditions.
   *                     getCore() depends on getting the core returned _if_ it's in the pending list due to some other thread opening it.
   *                     If the core is not in the pending list and not loaded, then getCore() calls this method. Anything that called
   *                     to check if the core was loaded _or_ in pending ops and, based on the return called createFromDescriptor would
   *                     introduce a race condition, see getCore() for the place it would be a problem
   * @return the newly created core
   */
  @SuppressWarnings("resource")
  private SolrCore createFromDescriptor(CoreDescriptor dcore, boolean publishState, boolean newCollection) {

    if (isShutDown) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Solr has been shutdown.");
    }

    SolrCore core = null;
    try {
      MDCLoggingContext.setCoreDescriptor(this, dcore);
      SolrIdentifierValidator.validateCoreName(dcore.getName());
      if (zkSys.getZkController() != null) {
        zkSys.getZkController().preRegister(dcore, publishState);
      }

      ConfigSet coreConfig = getConfigSet(dcore);
      dcore.setConfigSetTrusted(coreConfig.isTrusted());
      log.info("Creating SolrCore '{}' using configuration from {}, trusted={}", dcore.getName(), coreConfig.getName(), dcore.isConfigSetTrusted());
      try {
        core = new SolrCore(this, dcore, coreConfig);
      } catch (SolrException e) {
        core = processCoreCreateException(e, dcore, coreConfig);
      }

      // always kick off recovery if we are in non-Cloud mode
      if (!isZooKeeperAware() && core.getUpdateHandler().getUpdateLog() != null) {
        core.getUpdateHandler().getUpdateLog().recoverFromLog();
      }

      registerCore(dcore, core, publishState, newCollection);

      return core;
    } catch (Exception e) {
      coreInitFailures.put(dcore.getName(), new CoreLoadFailure(dcore, e));
      if (e instanceof ZkController.NotInClusterStateException && !newCollection) {
        // this mostly happen when the core is deleted when this node is down
        unload(dcore.getName(), true, true, true);
        throw e;
      }
      solrCores.removeCoreDescriptor(dcore);
      final SolrException solrException = new SolrException(ErrorCode.SERVER_ERROR, "Unable to create core [" + dcore.getName() + "]", e);
      if (core != null && !core.isClosed())
        IOUtils.closeQuietly(core);
      throw solrException;
    } catch (Throwable t) {
      SolrException e = new SolrException(ErrorCode.SERVER_ERROR, "JVM Error creating core [" + dcore.getName() + "]: " + t.getMessage(), t);
      coreInitFailures.put(dcore.getName(), new CoreLoadFailure(dcore, e));
      solrCores.removeCoreDescriptor(dcore);
      if (core != null && !core.isClosed())
        IOUtils.closeQuietly(core);
      throw t;
    } finally {
      MDCLoggingContext.clear();
    }
  }

  public boolean isSharedFs(CoreDescriptor cd) {
    try (SolrCore core = this.getCore(cd.getName())) {
      if (core != null) {
        return core.getDirectoryFactory().isSharedStorage();
      } else {
        ConfigSet configSet = getConfigSet(cd);
        return DirectoryFactory.loadDirectoryFactory(configSet.getSolrConfig(), this, null).isSharedStorage();
      }
    }
  }

  private ConfigSet getConfigSet(CoreDescriptor cd) {
    return coreConfigService.getConfig(cd);
  }

  /**
   * Take action when we failed to create a SolrCore. If error is due to corrupt index, try to recover. Various recovery
   * strategies can be specified via system properties "-DCoreInitFailedAction={fromleader, none}"
   *
   * @param original   the problem seen when loading the core the first time.
   * @param dcore      core descriptor for the core to create
   * @param coreConfig core config for the core to create
   * @return if possible
   * @throws SolrException rethrows the original exception if we will not attempt to recover, throws a new SolrException with the
   *                       original exception as a suppressed exception if there is a second problem creating the solr core.
   * @see CoreInitFailedAction
   */
  private SolrCore processCoreCreateException(SolrException original, CoreDescriptor dcore, ConfigSet coreConfig) {
    // Traverse full chain since CIE may not be root exception
    Throwable cause = original;
    while ((cause = cause.getCause()) != null) {
      if (cause instanceof CorruptIndexException) {
        break;
      }
    }

    // If no CorruptIndexException, nothing we can try here
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
              // the index of this core is emptied, its term should be set to 0
              getZkController().getShardTerms(desc.getCollectionName(), desc.getShardId()).setTermToZero(desc.getCoreNodeName());
              return new SolrCore(this, dcore, coreConfig);
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

    String registryName = SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, dcore.getName());
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
   * Gets the cores that are currently loaded, i.e. cores that have
   * 1: loadOnStartup=true and are either not-transient or, if transient, have been loaded and have not been aged out
   * 2: loadOnStartup=false and have been loaded but are either non-transient or have not been aged out.
   * <p>
   * Put another way, this will not return any names of cores that are lazily loaded but have not been called for yet
   * or are transient and either not loaded or have been swapped out.
   */
  public Collection<String> getLoadedCoreNames() {
    return solrCores.getLoadedCoreNames();
  }

  /**
   * This method is currently experimental.
   *
   * @return a Collection of the names that a specific core object is mapped to, there are more than one.
   */
  public Collection<String> getNamesForCore(SolrCore core) {
    return solrCores.getNamesForCore(core);
  }

  /**
   * get a list of all the cores that are currently known, whether currently loaded or not
   *
   * @return a list of all the available core names in either permanent or transient cores
   */
  public Collection<String> getAllCoreNames() {
    return solrCores.getAllCoreNames();

  }

  /**
   * Returns an immutable Map of Exceptions that occurred when initializing
   * SolrCores (either at startup, or do to runtime requests to create cores)
   * keyed off of the name (String) of the SolrCore that had the Exception
   * during initialization.
   * <p>
   * While the Map returned by this method is immutable and will not change
   * once returned to the client, the source data used to generate this Map
   * can be changed as various SolrCore operations are performed:
   * </p>
   * <ul>
   * <li>Failed attempts to create new SolrCores will add new Exceptions.</li>
   * <li>Failed attempts to re-create a SolrCore using a name already contained in this Map will replace the Exception.</li>
   * <li>Failed attempts to reload a SolrCore will cause an Exception to be added to this list -- even though the existing SolrCore with that name will continue to be available.</li>
   * <li>Successful attempts to re-created a SolrCore using a name already contained in this Map will remove the Exception.</li>
   * <li>Registering an existing SolrCore with a name already contained in this Map (ie: ALIAS or SWAP) will remove the Exception.</li>
   * </ul>
   */
  public Map<String, CoreLoadFailure> getCoreInitFailures() {
    return ImmutableMap.copyOf(coreInitFailures);
  }


  // ---------------- Core name related methods ---------------

  private CoreDescriptor reloadCoreDescriptor(CoreDescriptor oldDesc) {
    if (oldDesc == null) {
      return null;
    }

    CorePropertiesLocator cpl = new CorePropertiesLocator(null);
    CoreDescriptor ret = cpl.buildCoreDescriptor(oldDesc.getInstanceDir().resolve(PROPERTIES_FILENAME), this);

    // Ok, this little jewel is all because we still create core descriptors on the fly from lists of properties
    // in tests particularly. Theoretically, there should be _no_ way to create a CoreDescriptor in the new world
    // of core discovery without writing the core.properties file out first.
    //
    // TODO: remove core.properties from the conf directory in test files, it's in a bad place there anyway.
    if (ret == null) {
      oldDesc.loadExtraProperties(); // there may be changes to extra properties that we need to pick up.
      return oldDesc;

    }
    // The CloudDescriptor bit here is created in a very convoluted way, requiring access to private methods
    // in ZkController. When reloading, this behavior is identical to what used to happen where a copy of the old
    // CoreDescriptor was just re-used.

    if (ret.getCloudDescriptor() != null) {
      ret.getCloudDescriptor().reload(oldDesc.getCloudDescriptor());
    }

    return ret;
  }

  /**
   * Recreates a SolrCore.
   * While the new core is loading, requests will continue to be dispatched to
   * and processed by the old core
   *
   * @param name the name of the SolrCore to reload
   */
  public void reload(String name) {
    if (isShutDown) {
      throw new AlreadyClosedException();
    }
    SolrCore newCore = null;
    SolrCore core = solrCores.getCoreFromAnyList(name, false);
    if (core != null) {

      // The underlying core properties files may have changed, we don't really know. So we have a (perhaps) stale
      // CoreDescriptor and we need to reload it from the disk files
      CoreDescriptor cd = reloadCoreDescriptor(core.getCoreDescriptor());
      solrCores.addCoreDescriptor(cd);
      Closeable oldCore = null;
      boolean success = false;
      try {
        solrCores.waitAddPendingCoreOps(cd.getName());
        ConfigSet coreConfig = coreConfigService.getConfig(cd);
        log.info("Reloading SolrCore '{}' using configuration from {}", cd.getName(), coreConfig.getName());
        newCore = core.reload(coreConfig);

        DocCollection docCollection = null;
        if (getZkController() != null) {
          docCollection = getZkController().getClusterState().getCollection(cd.getCollectionName());
          // turn off indexing now, before the new core is registered
          if (docCollection.getBool(ZkStateReader.READ_ONLY, false)) {
            newCore.readOnly = true;
          }
        }

        registerCore(cd, newCore, false, false);

        // force commit on old core if the new one is readOnly and prevent any new updates
        if (newCore.readOnly) {
          RefCounted<IndexWriter> iwRef = core.getSolrCoreState().getIndexWriter(null);
          if (iwRef != null) {
            IndexWriter iw = iwRef.get();
            // switch old core to readOnly
            core.readOnly = true;
            try {
              if (iw != null) {
                iw.commit();
              }
            } finally {
              iwRef.decref();
            }
          }
        }


        if (docCollection != null) {
          Replica replica = docCollection.getReplica(cd.getCloudDescriptor().getCoreNodeName());
          assert replica != null;
          if (replica.getType() == Replica.Type.TLOG) { // TODO: needed here?
            getZkController().stopReplicationFromLeader(core.getName());
            if (!cd.getCloudDescriptor().isLeader()) {
              getZkController().startReplicationFromLeader(newCore.getName(), true);
            }

          } else if (replica.getType() == Replica.Type.PULL) {
            getZkController().stopReplicationFromLeader(core.getName());
            getZkController().startReplicationFromLeader(newCore.getName(), false);
          }
        }
        success = true;
      } catch (SolrCoreState.CoreIsClosedException e) {
        throw e;
      } catch (Exception e) {
        coreInitFailures.put(cd.getName(), new CoreLoadFailure(cd, (Exception) e));
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to reload core [" + cd.getName() + "]", e);
      } finally {
        if (!success && newCore != null && newCore.getOpenCount() > 0) {
          IOUtils.closeQuietly(newCore);
        }
        solrCores.removeFromPendingOps(cd.getName());
      }
    } else {
      CoreLoadFailure clf = coreInitFailures.get(name);
      if (clf != null) {
        try {
          solrCores.waitAddPendingCoreOps(clf.cd.getName());
          createFromDescriptor(clf.cd, true, false);
        } finally {
          solrCores.removeFromPendingOps(clf.cd.getName());
        }
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such core: " + name);
      }
    }
  }

  /**
   * Swaps two SolrCore descriptors.
   */
  public void swap(String n0, String n1) {
    if (n0 == null || n1 == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not swap unnamed cores.");
    }
    solrCores.swap(n0, n1);

    coresLocator.swap(this, solrCores.getCoreDescriptor(n0), solrCores.getCoreDescriptor(n1));

    log.info("swapped: " + n0 + " with " + n1);
  }

  /**
   * Unload a core from this container, leaving all files on disk
   *
   * @param name the name of the core to unload
   */
  public void unload(String name) {
    unload(name, false, false, false);
  }

  /**
   * Unload a core from this container, optionally removing the core's data and configuration
   *
   * @param name              the name of the core to unload
   * @param deleteIndexDir    if true, delete the core's index on close
   * @param deleteDataDir     if true, delete the core's data directory on close
   * @param deleteInstanceDir if true, delete the core's instance directory on close
   */
  public void unload(String name, boolean deleteIndexDir, boolean deleteDataDir, boolean deleteInstanceDir) {

    CoreDescriptor cd = solrCores.getCoreDescriptor(name);

    if (name != null) {
      // check for core-init errors first
      CoreLoadFailure loadFailure = coreInitFailures.remove(name);
      if (loadFailure != null) {
        // getting the index directory requires opening a DirectoryFactory with a SolrConfig, etc,
        // which we may not be able to do because of the init error.  So we just go with what we
        // can glean from the CoreDescriptor - datadir and instancedir
        SolrCore.deleteUnloadedCore(loadFailure.cd, deleteDataDir, deleteInstanceDir);
        // If last time around we didn't successfully load, make sure that all traces of the coreDescriptor are gone.
        if (cd != null) {
          solrCores.removeCoreDescriptor(cd);
          coresLocator.delete(this, cd);
        }
        return;
      }
    }

    if (cd == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Cannot unload non-existent core [" + name + "]");
    }

    boolean close = solrCores.isLoadedNotPendingClose(name);
    SolrCore core = solrCores.remove(name);

    solrCores.removeCoreDescriptor(cd);
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
      if (cd.getCloudDescriptor().getReplicaType() == Replica.Type.PULL
          || cd.getCloudDescriptor().getReplicaType() == Replica.Type.TLOG) {
        // Stop replication if this is part of a pull/tlog replica before closing the core
        zkSys.getZkController().stopReplicationFromLeader(name);
      }
    }

    core.unloadOnClose(cd, deleteIndexDir, deleteDataDir, deleteInstanceDir);
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
      } catch (Exception e) {
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
        // The old coreDescriptor is obsolete, so remove it. registerCore will put it back.
        CoreDescriptor cd = core.getCoreDescriptor();
        solrCores.removeCoreDescriptor(cd);
        cd.setProperty("name", toName);
        solrCores.addCoreDescriptor(cd);
        core.setName(toName);
        registerCore(cd, core, true, false);
        SolrCore old = solrCores.remove(name);

        coresLocator.rename(this, old.getCoreDescriptor(), core.getCoreDescriptor());
      }
    }
  }

  /**
   * Get the CoreDescriptors for all cores managed by this container
   *
   * @return a List of CoreDescriptors
   */
  public List<CoreDescriptor> getCoreDescriptors() {
    return solrCores.getCoreDescriptors();
  }

  public CoreDescriptor getCoreDescriptor(String coreName) {
    return solrCores.getCoreDescriptor(coreName);
  }

  public Path getCoreRootDirectory() {
    return cfg.getCoreRootDirectory();
  }

  /**
   * Gets a core by name and increase its refcount.
   *
   * @param name the core name
   * @return the core if found, null if a SolrCore by this name does not exist
   * @throws SolrCoreInitializationException if a SolrCore with this name failed to be initialized
   * @see SolrCore#close()
   */
  public SolrCore getCore(String name) {

    // Do this in two phases since we don't want to lock access to the cores over a load.
    SolrCore core = solrCores.getCoreFromAnyList(name, true);

    // If a core is loaded, we're done just return it.
    if (core != null) {
      return core;
    }

    // If it's not yet loaded, we can check if it's had a core init failure and "do the right thing"
    CoreDescriptor desc = solrCores.getCoreDescriptor(name);

    // if there was an error initializing this core, throw a 500
    // error with the details for clients attempting to access it.
    CoreLoadFailure loadFailure = getCoreInitFailures().get(name);
    if (null != loadFailure) {
      throw new SolrCoreInitializationException(name, loadFailure.exception);
    }
    // This is a bit of awkwardness where SolrCloud and transient cores don't play nice together. For transient cores,
    // we have to allow them to be created at any time there hasn't been a core load failure (use reload to cure that).
    // But for TestConfigSetsAPI.testUploadWithScriptUpdateProcessor, this needs to _not_ try to load the core if
    // the core is null and there was an error. If you change this, be sure to run both TestConfiSetsAPI and
    // TestLazyCores
    if (desc == null || zkSys.getZkController() != null) return null;

    // This will put an entry in pending core ops if the core isn't loaded. Here's where moving the
    // waitAddPendingCoreOps to createFromDescriptor would introduce a race condition.
    core = solrCores.waitAddPendingCoreOps(name);

    if (isShutDown) return null; // We're quitting, so stop. This needs to be after the wait above since we may come off
    // the wait as a consequence of shutting down.
    try {
      if (core == null) {
        if (zkSys.getZkController() != null) {
          zkSys.getZkController().throwErrorIfReplicaReplaced(desc);
        }
        core = createFromDescriptor(desc, true, false); // This should throw an error if it fails.
      }
      core.open();
    } finally {
      solrCores.removeFromPendingOps(name);
    }

    return core;
  }

  public BlobRepository getBlobRepository() {
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
    T handler = loader.newInstance(handlerClass, clazz, null, new Class[]{CoreContainer.class}, new Object[]{this});
    if (handler instanceof SolrRequestHandler) {
      containerHandlers.put(path, (SolrRequestHandler) handler);
    }
    if (handler instanceof SolrMetricProducer) {
      ((SolrMetricProducer) handler).initializeMetrics(solrMetricsContext, path);
    }
    return handler;
  }

  public CoreAdminHandler getMultiCoreHandler() {
    return coreAdminHandler;
  }

  public CollectionsHandler getCollectionsHandler() {
    return collectionsHandler;
  }

  public HealthCheckHandler getHealthCheckHandler() {
    return healthCheckHandler;
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
   */
  public boolean isLoaded(String name) {
    return solrCores.isLoaded(name);
  }

  public boolean isLoadedNotPendingClose(String name) {
    return solrCores.isLoadedNotPendingClose(name);
  }

  // Primarily for transient cores when a core is aged out.
  public void queueCoreToClose(SolrCore coreToClose) {
    solrCores.queueCoreToClose(coreToClose);
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

  /**
   * The default ShardHandlerFactory used to communicate with other solr instances
   */
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

  public AuditLoggerPlugin getAuditLoggerPlugin() {
    return auditloggerPlugin == null ? null : auditloggerPlugin.plugin;
  }

  public NodeConfig getNodeConfig() {
    return cfg;
  }

  public long getStatus() {
    return status;
  }

  // Occasionally we need to access the transient cache handler in places other than coreContainer.
  public TransientSolrCoreCache getTransientCache() {
    return solrCores.getTransientCacheHandler();
  }


  /**
   * @param cd   CoreDescriptor, presumably a deficient one
   * @param prop The property that needs to be repaired.
   * @return true if we were able to successfuly perisist the repaired coreDescriptor, false otherwise.
   * <p>
   * See SOLR-11503, This can be removed when there's no chance we'll need to upgrade a
   * Solr installation created with legacyCloud=true from 6.6.1 through 7.1
   */
  public boolean repairCoreProperty(CoreDescriptor cd, String prop) {
    // So far, coreNodeName is the only property that we need to repair, this may get more complex as other properties
    // are added.

    if (CoreDescriptor.CORE_NODE_NAME.equals(prop) == false) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
          String.format(Locale.ROOT, "The only supported property for repair is currently [%s]",
              CoreDescriptor.CORE_NODE_NAME));
    }

    // Try to read the coreNodeName from the cluster state.

    String coreName = cd.getName();
    DocCollection coll = getZkController().getZkStateReader().getClusterState().getCollection(cd.getCollectionName());
    for (Replica rep : coll.getReplicas()) {
      if (coreName.equals(rep.getCoreName())) {
        log.warn("Core properties file for node {} found with no coreNodeName, attempting to repair with value {}. See SOLR-11503. " +
                "This message should only appear if upgrading from collections created Solr 6.6.1 through 7.1.",
            rep.getCoreName(), rep.getName());
        cd.getCloudDescriptor().setCoreNodeName(rep.getName());
        coresLocator.persist(this, cd);
        return true;
      }
    }
    log.error("Could not repair coreNodeName in core.properties file for core {}", coreName);
    return false;
  }

  /**
   * @param solrCore the core against which we check if there has been a tragic exception
   * @return whether this Solr core has tragic exception
   * @see org.apache.lucene.index.IndexWriter#getTragicException()
   */
  public boolean checkTragicException(SolrCore solrCore) {
    Throwable tragicException;
    try {
      tragicException = solrCore.getSolrCoreState().getTragicException();
    } catch (IOException e) {
      // failed to open an indexWriter
      tragicException = e;
    }

    if (tragicException != null && isZooKeeperAware()) {
      getZkController().giveupLeadership(solrCore.getCoreDescriptor(), tragicException);
    }

    return tragicException != null;
  }

  static {
    ExecutorUtil.addThreadLocalProvider(SolrRequestInfo.getInheritableThreadLocalProvider());
  }

  /**
   * Run an arbitrary task in it's own thread. This is an expert option and is
   * a method you should use with great care. It would be bad to run something that never stopped
   * or run something that took a very long time. Typically this is intended for actions that take
   * a few seconds, and therefore would be bad to wait for within a request, or actions that need to happen
   * when a core has zero references, but but would not pose a significant hindrance to server shut down times.
   * It is not intended for long running tasks and if you are using a Runnable with a loop in it, you are
   * almost certainly doing it wrong.
   * <p><br>
   * WARNING: Solr wil not be able to shut down gracefully until this task completes!
   * <p><br>
   * A significant upside of using this method vs creating your own ExecutorService is that your code
   * does not have to properly shutdown executors which typically is risky from a unit testing
   * perspective since the test framework will complain if you don't carefully ensure the executor
   * shuts down before the end of the test. Also the threads running this task are sure to have
   * a proper MDC for logging.
   * <p><br>
   * Normally, one uses {@link SolrCore#runAsync(Runnable)} if possible, but in some cases
   * you might need to execute a task asynchronously when you could be running on a node with no
   * cores, and then use of this method is indicated.
   *
   * @param r the task to run
   */
  public void runAsync(Runnable r) {
    coreContainerAsyncTaskExecutor.submit(r);
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
    while (!container.isShutDown()) {
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
