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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.spec.InvalidKeySpecException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.config.Lookup;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientContextBuilder;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerTaskQueue;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.PerThreadExecService;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.OrderedExecutor;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.BackupRepositoryFactory;
import org.apache.solr.filestore.PackageStoreAPI;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.SnapShooter;
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
import org.apache.solr.handler.admin.ZookeeperReadAPI;
import org.apache.solr.handler.admin.ZookeeperStatusHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.sql.CalciteSolrDriver;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.pkg.PackageLoader;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.rest.schema.FieldTypeXmlAdapter;
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
import org.apache.solr.util.RefCounted;
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
import static org.apache.solr.common.params.CommonParams.METRICS_HISTORY_PATH;
import static org.apache.solr.common.params.CommonParams.METRICS_PATH;
import static org.apache.solr.common.params.CommonParams.ZK_PATH;
import static org.apache.solr.common.params.CommonParams.ZK_STATUS_PATH;
import static org.apache.solr.core.CorePropertiesLocator.PROPERTIES_FILENAME;
import static org.apache.solr.security.AuthenticationPlugin.AUTHENTICATION_PLUGIN_PROP;

/**
 * @since solr 1.3
 */
public class CoreContainer implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static {
    log.warn("expected pre init of xml factories {} {} {} {}", XmlConfigFile.xpathFactory,
        FieldTypeXmlAdapter.dbf, XMLResponseParser.inputFactory, XMLResponseParser.saxFactory);
  }

  final SolrCores solrCores = new SolrCores(this);
  private final boolean isZkAware;
  private volatile boolean startedLoadingCores;
  private volatile boolean loaded;

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

  private CloseTracker closeTracker;

  private volatile ConfigSetService coreConfigService;

  protected volatile ZkContainer zkSys = null;
  protected volatile ShardHandlerFactory shardHandlerFactory;

  private volatile UpdateShardHandler updateShardHandler;

  public volatile ExecutorService solrCoreLoadExecutor;

  private final OrderedExecutor replayUpdatesExecutor;

  @SuppressWarnings({"rawtypes"})
  protected volatile LogWatcher logging = null;

  protected final NodeConfig cfg;
  protected final SolrResourceLoader loader;

  protected final Path solrHome;

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

  protected volatile MetricsHandler metricsHandler;

  protected volatile MetricsHistoryHandler metricsHistoryHandler;

  protected volatile MetricsCollectorHandler metricsCollectorHandler;

  private volatile SolrClientCache solrClientCache;

  private final ObjectCache objectCache = new ObjectCache();

  private PackageStoreAPI packageStoreAPI;
  private PackageLoader packageLoader;

  // private Set<Future> zkRegFutures = zkRegFutures = ConcurrentHashMap.newKeySet();


  // Bits for the state variable.
  public final static long LOAD_COMPLETE = 0x1L;
  public final static long CORE_DISCOVERY_COMPLETE = 0x2L;
  public final static long INITIAL_CORE_LOAD_COMPLETE = 0x4L;
  private volatile long status = 0L;

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

  public SolrRequestHandler getRequestHandler(String path) {
    return RequestHandlerBase.getRequestHandler(path, containerHandlers);
  }

  public PluginBag<SolrRequestHandler> getRequestHandlers() {
    return this.containerHandlers;
  }

  {
    if (log.isDebugEnabled()) {
      log.debug("New CoreContainer {}", System.identityHashCode(this));
    }
  }

  /**
   * Create a new CoreContainer using the given solr home directory.  The container's
   * cores are not loaded.
   *
   * @param solrHome a String containing the path to the solr home directory
   * @param properties substitutable properties (alternative to Sys props)
   * @see #load()
   */
  public CoreContainer(Path solrHome, Properties properties) {
    this(SolrXmlConfig.fromSolrHome(solrHome, properties));
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
    this(config, new CorePropertiesLocator(config.getCoreRootDirectory()));
  }

  public CoreContainer(NodeConfig config, boolean asyncSolrCoreLoad) {
    this(config, new CorePropertiesLocator(config.getCoreRootDirectory()), asyncSolrCoreLoad);
  }

  public CoreContainer(NodeConfig config, CoresLocator locator) {
    this(config, locator, config.getCloudConfig() != null);
  }

  public CoreContainer(NodeConfig config, CoresLocator locator, boolean asyncSolrCoreLoad) {
    this(null, config, locator, asyncSolrCoreLoad);
  }
  public CoreContainer(SolrZkClient zkClient, NodeConfig config, CoresLocator locator, boolean asyncSolrCoreLoad) {
    assert ObjectReleaseTracker.track(this);
    assert (closeTracker = new CloseTracker()) != null;
    this.containerProperties = new Properties(config.getSolrProperties());
    String zkHost = System.getProperty("zkHost");
    if (!StringUtils.isEmpty(zkHost)) {
      zkSys = new ZkContainer(zkClient);
      isZkAware = true;
    } else {
      isZkAware = false;
    }

    this.loader = config.getSolrResourceLoader();
    this.solrHome = config.getSolrHome();
    this.cfg = requireNonNull(config);

    if (null != this.cfg.getBooleanQueryMaxClauseCount()) {
      IndexSearcher.setMaxClauseCount(this.cfg.getBooleanQueryMaxClauseCount());
    }
    this.coresLocator = locator;

    this.asyncSolrCoreLoad = asyncSolrCoreLoad;

    this.replayUpdatesExecutor = new OrderedExecutor(cfg.getReplayUpdatesThreads(),
        ParWork.getParExecutorService("replayUpdatesExecutor", cfg.getReplayUpdatesThreads(), cfg.getReplayUpdatesThreads(),
            0, new LinkedBlockingQueue<>(cfg.getReplayUpdatesThreads())));

    metricManager = new SolrMetricManager(loader, cfg.getMetricsConfig());
    String registryName = SolrMetricManager.getRegistryName(SolrInfoBean.Group.node);
    solrMetricsContext = new SolrMetricsContext(metricManager, registryName, metricTag);
    try (ParWork work = new ParWork(this)) {

      work.collect("", () -> {
        try {
          containerHandlers.put(PublicKeyHandler.PATH, new PublicKeyHandler(cfg.getCloudConfig()));
        } catch (IOException | InvalidKeySpecException e) {
          throw new RuntimeException("Bad PublicKeyHandler configuration.", e);
        }
      });

      work.collect("",() -> {
        updateShardHandler = new UpdateShardHandler(cfg.getUpdateShardHandlerConfig());
        updateShardHandler.initializeMetrics(solrMetricsContext, "updateShardHandler");
      });

      work.addCollect();

      work.collect("",() -> {
        shardHandlerFactory = ShardHandlerFactory.newInstance(cfg.getShardHandlerFactoryPluginInfo(),
            loader, updateShardHandler);
        if (shardHandlerFactory instanceof SolrMetricProducer) {
          SolrMetricProducer metricProducer = (SolrMetricProducer) shardHandlerFactory;
          metricProducer.initializeMetrics(solrMetricsContext, "httpShardHandler");
        }
      });
    }
    if (zkClient != null) {
      zkSys.initZooKeeper(this, cfg.getCloudConfig());
    }
    coreConfigService = ConfigSetService.createConfigSetService(cfg, loader, zkSys == null ? null : zkSys.zkController);

    containerProperties.putAll(cfg.getSolrProperties());


    solrCoreLoadExecutor = new PerThreadExecService(ParWork.getRootSharedExecutor(), Math.max(32, Runtime.getRuntime().availableProcessors()),
        false, false);
  }

  @SuppressWarnings({"unchecked"})
  private void initializeAuthorizationPlugin(Map<String, Object> authorizationConf) {
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
      log.info("Initializing authorization plugin: {}", klas);
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
        ParWork.propagateInterrupt(e);
        log.error("Exception while attempting to close old authorization plugin", e);
      }
    }
  }

  public boolean startedLoadingCores() {
    return startedLoadingCores;
  }

  @SuppressWarnings({"unchecked"})
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
      log.info("Initializing auditlogger plugin: {}", klas);
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
        ParWork.propagateInterrupt(e);
        log.error("Exception while attempting to close old auditlogger plugin", e);
      }
    }
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  private void initializeAuthenticationPlugin(Map<String, Object> authenticationConfig) {
    log.info("Initialize authenitcation plugin ..");
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
      log.debug("Authentication plugin class obtained from security.json: {}", pluginClassName);
    } else if (System.getProperty(AUTHENTICATION_PLUGIN_PROP) != null) {
      pluginClassName = System.getProperty(AUTHENTICATION_PLUGIN_PROP);
      log.debug("Authentication plugin class obtained from system property '{}': {}"
          , AUTHENTICATION_PLUGIN_PROP, pluginClassName);
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
      log.info("Initializing authentication plugin: {}", pluginClassName);
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
      ParWork.propagateInterrupt(e);
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
        httpClientBuilder.setDefaultCredentialsProvider(new CredentialsProviderProvider(builder));
      }
      if (builder.getAuthSchemeRegistryProvider() != null) {
        httpClientBuilder.setAuthSchemeRegistryProvider(new AuthSchemeRegistryProvider(builder));
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

  @SuppressWarnings({"rawtypes"})
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
    assert (closeTracker = new CloseTracker()) != null;
    solrHome = null;
    loader = null;
    coresLocator = null;
    cfg = null;
    containerProperties = null;
    replayUpdatesExecutor = null;
    isZkAware = false;
  }

  public static CoreContainer createAndLoad(Path solrHome) {
    return createAndLoad(solrHome, solrHome.resolve(SolrXmlConfig.SOLR_XML_FILE));
  }

  public static CoreContainer createAndLoad(Path solrHome, Path configFile) {
    return createAndLoad(solrHome, configFile, null);
  }
  /**
   * Create a new CoreContainer and load its cores
   *
   * @param solrHome   the solr home directory
   * @param configFile the file containing this container's configuration
   * @return a loaded CoreContainer
   */
  public static CoreContainer createAndLoad(Path solrHome, Path configFile, SolrZkClient zkClient) {
    NodeConfig config = SolrXmlConfig.fromFile(solrHome, configFile, new Properties());
    CoreContainer cc = new CoreContainer(zkClient, config, new CorePropertiesLocator(config.getCoreRootDirectory()), true);
    try {
      cc.load();
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
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

  public SolrClientCache getSolrClientCache() {
    return solrClientCache;
  }

  public ObjectCache getObjectCache() {
    return objectCache;
  }

  //-------------------------------------------------------------------
  // Initialization / Cleanup
  //-------------------------------------------------------------------

  /**
   * Load the cores defined for this CoreContainer
   */
  public synchronized void load() {
    if (log.isDebugEnabled()) {
      log.debug("Loading cores into CoreContainer [instanceDir={}]", getSolrHome());
    }

    if (loaded) {
      throw new IllegalStateException("CoreContainer already loaded");
    }

    loaded = true;

    if (isZooKeeperAware()) {
      try {
        zkSys.start(this);
      } catch (IOException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      } catch (KeeperException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    // Always add $SOLR_HOME/lib to the shared resource loader
    Set<String> libDirs = new LinkedHashSet<>();
    libDirs.add("lib");

    if (!StringUtils.isBlank(cfg.getSharedLibDirectory())) {
      List<String> sharedLibs = Arrays.asList(cfg.getSharedLibDirectory().split("\\s*,\\s*"));
      libDirs.addAll(sharedLibs);
    }

    boolean modified = false;
    // add the sharedLib to the shared resource loader before initializing cfg based plugins
    for (String libDir : libDirs) {
      Path libPath = Paths.get(getSolrHome()).resolve(libDir);
      if (Files.exists(libPath)) {
        try {
          loader.addToClassLoader(SolrResourceLoader.getURLs(libPath));
          modified = true;
        } catch (IOException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Couldn't load libs: " + e, e);
        }
      }
    }
    if (modified) {
      loader.reloadLuceneSPI();
    }

    packageStoreAPI = new PackageStoreAPI(this);
    containerHandlers.getApiBag().registerObject(packageStoreAPI.readAPI);
    containerHandlers.getApiBag().registerObject(packageStoreAPI.writeAPI);

    solrClientCache = new SolrClientCache(isZooKeeperAware() ? zkSys.getZkController().getZkStateReader() : null, updateShardHandler.getTheSharedHttpClient());

    // initialize CalciteSolrDriver instance to use this solrClientCache
    CalciteSolrDriver.INSTANCE.setSolrClientCache(solrClientCache);

    try (ParWork work = new ParWork(this)) {

      work.collect("", () -> {
        solrCores.load(loader);

        logging = LogWatcher.newRegisteredLogWatcher(cfg.getLogWatcherConfig(), loader);

        hostName = cfg.getNodeName();

        if (isZooKeeperAware()) {
          pkiAuthenticationPlugin = new PKIAuthenticationPlugin(this, zkSys.getZkController().getNodeName(), (PublicKeyHandler) containerHandlers.get(PublicKeyHandler.PATH));
          // use deprecated API for back-compat, remove in 9.0
          pkiAuthenticationPlugin.initializeMetrics(solrMetricsContext, "/authentication/pki");
          TracerConfigurator.loadTracer(loader, cfg.getTracerConfiguratorPluginInfo(), getZkController().getZkStateReader());
          packageLoader = new PackageLoader(this);
          containerHandlers.getApiBag().registerObject(packageLoader.getPackageAPI().editAPI);
          containerHandlers.getApiBag().registerObject(packageLoader.getPackageAPI().readAPI);
          ZookeeperReadAPI zookeeperReadAPI = new ZookeeperReadAPI(this);
          containerHandlers.getApiBag().registerObject(zookeeperReadAPI);
        }
      });

      work.collect("", () -> {
        MDCLoggingContext.setNode(this);

        securityConfHandler = isZooKeeperAware() ? new SecurityConfHandlerZk(this) : new SecurityConfHandlerLocal(this);
        reloadSecurityProperties();
        warnUsersOfInsecureSettings();
        this.backupRepoFactory = new BackupRepositoryFactory(cfg.getBackupRepositoryPlugins());
      });

      work.collect("", () -> {
        createHandler(ZK_PATH, ZookeeperInfoHandler.class.getName(), ZookeeperInfoHandler.class);
        createHandler(ZK_STATUS_PATH, ZookeeperStatusHandler.class.getName(), ZookeeperStatusHandler.class);
      });

      work.collect("", () -> {
        collectionsHandler = createHandler(COLLECTIONS_HANDLER_PATH, cfg.getCollectionsHandlerClass(), CollectionsHandler.class);
        infoHandler = createHandler(INFO_HANDLER_PATH, cfg.getInfoHandlerClass(), InfoHandler.class);
      });

      work.collect("", () -> {
        // metricsHistoryHandler uses metricsHandler, so create it first
        metricsHandler = new MetricsHandler(this);
        containerHandlers.put(METRICS_PATH, metricsHandler);
        metricsHandler.initializeMetrics(solrMetricsContext, METRICS_PATH);
      });

      work.collect("", () -> {
        metricsCollectorHandler = createHandler(MetricsCollectorHandler.HANDLER_PATH, MetricsCollectorHandler.class.getName(), MetricsCollectorHandler.class);
        // may want to add some configuration here in the future
        metricsCollectorHandler.init(null);
      });

      work.collect("", () -> {
        if (securityConfHandler != null) {
          containerHandlers.put(AUTHZ_PATH, securityConfHandler);
          securityConfHandler.initializeMetrics(solrMetricsContext, AUTHZ_PATH);
          containerHandlers.put(AUTHC_PATH, securityConfHandler);
        }
      });

      work.collect("", () -> {
        PluginInfo[] metricReporters = cfg.getMetricsConfig().getMetricReporters();
        metricManager.loadReporters(metricReporters, loader, this, null, null, SolrInfoBean.Group.node);
        metricManager.loadReporters(metricReporters, loader, this, null, null, SolrInfoBean.Group.jvm);
        metricManager.loadReporters(metricReporters, loader, this, null, null, SolrInfoBean.Group.jetty);
      });

      work.addCollect();

      if (!Boolean.getBoolean("solr.disableMetricsHistoryHandler")) {
        work.collect("", () -> {
          createMetricsHistoryHandler();
        });
      }

      //  work.addCollect();
      work.collect("", () -> {
        coreAdminHandler = createHandler(CORES_HANDLER_PATH, cfg.getCoreAdminHandlerClass(), CoreAdminHandler.class);
        configSetsHandler = createHandler(CONFIGSETS_HANDLER_PATH, cfg.getConfigSetsHandlerClass(), ConfigSetsHandler.class);
      });

    }

    // initialize gauges for reporting the number of cores and disk total/free

    solrMetricsContext.gauge(() -> solrCores.getCores().size(), true, "loaded", SolrInfoBean.Category.CONTAINER.toString(), "cores");
    solrMetricsContext.gauge(() -> solrCores.getLoadedCoreNames().size() - solrCores.getCores().size(), true, "lazy", SolrInfoBean.Category.CONTAINER.toString(), "cores");
    solrMetricsContext.gauge(() -> solrCores.getAllCoreNames().size() - solrCores.getLoadedCoreNames().size(), true, "unloaded", SolrInfoBean.Category.CONTAINER.toString(), "cores");
    Path dataHome = cfg.getSolrDataHome() != null ? cfg.getSolrDataHome() : cfg.getCoreRootDirectory();
    solrMetricsContext.gauge(() -> dataHome.toFile().getTotalSpace(), true, "totalSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs");
    solrMetricsContext.gauge(() -> dataHome.toFile().getUsableSpace(), true, "usableSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs");
    solrMetricsContext.gauge(() -> dataHome.toAbsolutePath().toString(), true, "path", SolrInfoBean.Category.CONTAINER.toString(), "fs");
    solrMetricsContext.gauge(() -> {
      try {
        return org.apache.lucene.util.IOUtils.spins(dataHome.toAbsolutePath());
      } catch (IOException e) {
        // default to spinning
        return true;
      }
    }, true, "spins", SolrInfoBean.Category.CONTAINER.toString(), "fs");
    solrMetricsContext.gauge(() -> cfg.getCoreRootDirectory().toFile().getTotalSpace(), true, "totalSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs", "coreRoot");
    solrMetricsContext.gauge(() -> cfg.getCoreRootDirectory().toFile().getUsableSpace(), true, "usableSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs", "coreRoot");
    solrMetricsContext.gauge(() -> cfg.getCoreRootDirectory().toAbsolutePath().toString(), true, "path", SolrInfoBean.Category.CONTAINER.toString(), "fs", "coreRoot");
    solrMetricsContext.gauge(() -> {
      try {
        return org.apache.lucene.util.IOUtils.spins(cfg.getCoreRootDirectory().toAbsolutePath());
      } catch (IOException e) {
        // default to spinning
        return true;
      }
    }, true, "spins", SolrInfoBean.Category.CONTAINER.toString(), "fs", "coreRoot");
    // add version information
    solrMetricsContext.gauge(() -> this.getClass().getPackage().getSpecificationVersion(), true, "specification", SolrInfoBean.Category.CONTAINER.toString(), "version");
    solrMetricsContext.gauge(() -> this.getClass().getPackage().getImplementationVersion(), true, "implementation", SolrInfoBean.Category.CONTAINER.toString(), "version");

    SolrFieldCacheBean fieldCacheBean = new SolrFieldCacheBean();
    fieldCacheBean.initializeMetrics(solrMetricsContext, null);

    if (isZooKeeperAware()) {
      metricManager.loadClusterReporters(cfg.getMetricsConfig().getMetricReporters(), this);
    }

    List<Future<SolrCore>> coreLoadFutures = null;
    try {
      List<CoreDescriptor> cds = coresLocator.discover(this);
      coreLoadFutures = new ArrayList<>(cds.size());
      if (isZooKeeperAware()) {
        cds = CoreSorter.sortCores(this, cds);
      }
      checkForDuplicateCoreNames(cds);
      status |= CORE_DISCOVERY_COMPLETE;

      for (final CoreDescriptor cd : cds) {
        if (cd.isTransient() || !cd.isLoadOnStartup()) {
          solrCores.addCoreDescriptor(cd);
        } else {
          solrCores.markCoreAsLoading(cd);
        }
        if (cd.isLoadOnStartup()) {
          coreLoadFutures.add(solrCoreLoadExecutor.submit(() -> {
            SolrCore core;
            try {
              if (isZooKeeperAware()) {
                zkSys.getZkController().throwErrorIfReplicaReplaced(cd);
              }
              core = createFromDescriptor(cd, false);
            } finally {
              solrCores.markCoreAsNotLoading(cd);
            }

            return core;
          }));
        }
      }

    } finally {

      startedLoadingCores = true;
      if (coreLoadFutures != null) {

        for (Future<SolrCore> future : coreLoadFutures) {
          try {
            future.get();
          } catch (InterruptedException e) {
            ParWork.propagateInterrupt(e);
          } catch (ExecutionException e) {
            log.error("Error waiting for SolrCore to be loaded on startup", e.getCause());
          }
        }
      }
    }
    if (isZooKeeperAware()) {

     // zkSys.getZkController().checkOverseerDesignate();
      // initialize this handler here when SolrCloudManager is ready
    }
    // This is a bit redundant but these are two distinct concepts for all they're accomplished at the same time.
    status |= LOAD_COMPLETE | INITIAL_CORE_LOAD_COMPLETE;
  }

  // MetricsHistoryHandler supports both cloud and standalone configs
  @SuppressWarnings({"unchecked"})
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
      client = new CloudHttp2SolrClient.Builder(getZkController().getZkStateReader())
          .withHttpClient(updateShardHandler.getTheSharedHttpClient()).build();
      ((CloudHttp2SolrClient)client).connect();
    } else {
      name = getNodeConfig().getNodeName();
      if (name == null || name.isEmpty()) {
        name = "localhost";
      }
      cloudManager = null;
      client = new EmbeddedSolrServer();
      // enable local metrics unless specifically set otherwise
      if (!initArgs.containsKey(MetricsHistoryHandler.ENABLE_NODES_PROP)) {
        initArgs.put(MetricsHistoryHandler.ENABLE_NODES_PROP, true);
      }
      if (!initArgs.containsKey(MetricsHistoryHandler.ENABLE_REPLICAS_PROP)) {
        initArgs.put(MetricsHistoryHandler.ENABLE_REPLICAS_PROP, true);
      }
    }
    metricsHistoryHandler = new MetricsHistoryHandler(name, metricsHandler,
        client, cloudManager, initArgs, isZooKeeperAware() ? zkSys.getZkController().getOverseer() : null);
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
  @SuppressWarnings({"unchecked"})
  private void reloadSecurityProperties() {
    SecurityConfHandler.SecurityConfig securityConfig = securityConfHandler.getSecurityConfig(false);
    initializeAuthorizationPlugin((Map<String, Object>) securityConfig.getData().get("authorization"));
    initializeAuthenticationPlugin((Map<String, Object>) securityConfig.getData().get("authentication"));
    initializeAuditloggerPlugin((Map<String, Object>) securityConfig.getData().get("auditlogging"));
  }

  private void warnUsersOfInsecureSettings() {
    if (authenticationPlugin == null || authorizationPlugin == null) {
      log.warn("Not all security plugins configured!  authentication={} authorization={}.  Solr is only as secure as " +
              "you make it. Consider configuring authentication/authorization before exposing Solr to users internal or " +
              "external.  See https://s.apache.org/solrsecurity for more info",
          (authenticationPlugin != null) ? "enabled" : "disabled",
          (authorizationPlugin != null) ? "enabled" : "disabled");
    }

    if (authenticationPlugin !=null && StringUtils.isNotEmpty(System.getProperty("solr.jetty.https.port"))) {
      log.warn("Solr authentication is enabled, but SSL is off.  Consider enabling SSL to protect user credentials and data with encryption.");
    }
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

  @Override
  public void close() throws IOException {
    assert closeTracker.close();
    log.info("Closing CoreContainer");
    isShutDown = true;

    if (solrCores != null) {
      solrCores.closing();
    }

    log.info("Shutting down CoreContainer instance=" + System.identityHashCode(this));

    if (isZooKeeperAware() && zkSys != null && zkSys.getZkController() != null && !zkSys.getZkController().isDcCalled()) {
      zkSys.zkController.disconnect();
    }

    // must do before isShutDown=true
    if (isZooKeeperAware()) {
      try {
        cancelCoreRecoveries(false, true);
      } catch (Exception e) {

        ParWork.propagateInterrupt(e);
        log.error("Exception trying to cancel recoveries on shutdown", e);
      }
    }


    try (ParWork closer = new ParWork(this, true, true)) {

      ZkController zkController = getZkController();
      if (zkController != null) {
        OverseerTaskQueue overseerCollectionQueue = zkController.getOverseerCollectionQueue();
        overseerCollectionQueue.allowOverseerPendingTasksToComplete();
      }

      closer.collect("replayUpdateExec", () -> {
        replayUpdatesExecutor.shutdownAndAwaitTermination();
      });

      closer.collect("WaitForSolrCores", solrCores);
      closer.addCollect();
      List<Callable<?>> callables = new ArrayList<>();

      if (metricManager != null) {
        callables.add(() -> {
          metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node));
          return metricManager.getClass().getName() + ":REP:NODE";
        });
        callables.add(() -> {
          metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm));
          return metricManager.getClass().getName() + ":REP:JVM";
        });
        callables.add(() -> {
          metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jetty));
          return metricManager.getClass().getName() + ":REP:JETTY";
        });

        callables.add(() -> {
          metricManager.unregisterGauges(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node), metricTag);
          return metricManager.getClass().getName() + ":GA:NODE";
        });
        callables.add(() -> {
          metricManager.unregisterGauges(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm), metricTag);
          return metricManager.getClass().getName() + ":GA:JVM";
        });
        callables.add(() -> {
          metricManager.unregisterGauges(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jetty), metricTag);
          return metricManager.getClass().getName() + ":GA:JETTY";
        });
      }

      closer.collect("SolrCoreInternals", callables);

      callables = new ArrayList<>();
      if (isZooKeeperAware()) {
        if (metricManager != null) {
          callables.add(() -> {
            metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.cluster));
            return metricManager.getClass().getName() + ":REP:CLUSTER";
          });
        }
      }

      if (coreAdminHandler != null) {
        callables.add(() -> {
          coreAdminHandler.shutdown();
          return coreAdminHandler;
        });
      }
      AuthorizationPlugin authPlugin = null;
      if (authorizationPlugin != null) {
        authPlugin = authorizationPlugin.plugin;
      }
      AuthenticationPlugin authenPlugin = null;
      if (authenticationPlugin != null) {
        authenPlugin = authenticationPlugin.plugin;
      }
      AuditLoggerPlugin auditPlugin = null;
      if (auditloggerPlugin != null) {
        auditPlugin = auditloggerPlugin.plugin;
      }

      closer.collect(authPlugin);
      closer.collect(authenPlugin);
      closer.collect(auditPlugin);
      closer.collect(callables);
      closer.collect(metricsHistoryHandler);


      closer.addCollect();

      closer.collect(shardHandlerFactory);
      closer.collect(updateShardHandler);

      closer.addCollect();

      closer.collect(solrClientCache);

      closer.collect(zkSys);

      closer.addCollect();

      closer.collect(loader);
    }
    log.info("CoreContainer closed");
    assert ObjectReleaseTracker.release(this);
  }

  public void shutdown() {
    try {
      close();
    } catch (IOException e) {
      log.error("", e);
    }
  }

  public void waitForCoresToFinish() {
    solrCores.waitForLoadingCoresToFinish(30000);
  }

  public void cancelCoreRecoveries(boolean wait, boolean prepForClose) {

    List<SolrCore> cores = solrCores.getCores();

    // we must cancel without holding the cores sync
    // make sure we wait for any recoveries to stop
    try (ParWork work = new ParWork(this, true)) {
      for (SolrCore core : cores) {
        work.collect("cancelRecoveryFor-" + core.getName(), () -> {
          try {
            core.getSolrCoreState().cancelRecovery(wait, prepForClose);
          } catch (Exception e) {
            SolrException.log(log, "Error canceling recovery for core", e);
          }
        });
      }
    }
  }

  public CoresLocator getCoresLocator() {
    return coresLocator;
  }

  protected SolrCore registerCore(CoreDescriptor cd, SolrCore core, boolean registerInZk, boolean skipRecovery) {

    log.info("registerCore name={}, registerInZk={}, skipRecovery={}", cd.getName(), registerInZk, skipRecovery);

    if (core == null) {
      throw new RuntimeException("Can not register a null core.");
    }

    //    if (isShutDown) {
    //      core.close();
    //      throw new IllegalStateException("This CoreContainer has been closed");
    //    }
    SolrCore old = solrCores.putCore(cd, core);
    /*
     * set both the name of the descriptor and the name of the
     * core, since the descriptors name is used for persisting.
     */

    core.setName(cd.getName());

    coreInitFailures.remove(cd.getName());

    if (old == null || old == core) {
      log.info("registering core: " + cd.getName());
      if (registerInZk) {
        zkSys.registerInZk(core, skipRecovery);
      }
      return null;
    } else {
      log.info("replacing core: " + cd.getName());
      old.close();
      if (registerInZk) {
        zkSys.registerInZk(core, skipRecovery);
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
    if (isShutDown) {
      throw new AlreadyClosedException();
    }
    SolrCore core = null;
    CoreDescriptor cd = new CoreDescriptor(coreName, instancePath, parameters, getContainerProperties(), getZkController());

    // TODO: There's a race here, isn't there?
    // Since the core descriptor is removed when a core is unloaded, it should never be anywhere when a core is created.
    // nocommit
//    if (getAllCoreNames().contains(coreName)) {
//      log.warn("Creating a core with existing name is not allowed");
//      // TODO: Shouldn't this be a BAD_REQUEST?
//      throw new SolrException(ErrorCode.SERVER_ERROR, "Core with name '" + coreName + "' already exists.");
//    }

    boolean preExisitingZkEntry = false;
    try {
      if (getZkController() != null) {
        // in some rare case in TestTolerantUpdateProcessorRandomCloud, this happens with
        // non legacy and picking and removing this check makes it pass
        //        if (!Overseer.isLegacy(getZkController().getZkStateReader())) {
        //          if (cd.getCloudDescriptor().getCoreNodeName() == null) {
        //            throw new SolrException(ErrorCode.SERVER_ERROR, "non legacy mode coreNodeName missing " + parameters.toString());
        //
        //          }
        //        }
        preExisitingZkEntry = getZkController().checkIfCoreNodeNameAlreadyExists(cd);
      }

      // Much of the logic in core handling pre-supposes that the core.properties file already exists, so create it
      // first and clean it up if there's an error.
      coresLocator.create(this, cd);


      core = createFromDescriptor(cd, newCollection);
      coresLocator.persist(this, cd); // Write out the current core properties in case anything changed when the core was created


      return core;
    } catch (Exception ex) {
      ParWork.propagateInterrupt(ex);
      // First clean up any core descriptor, there should never be an existing core.properties file for any core that
      // failed to be created on-the-fly.
      coresLocator.delete(this, cd);
      if (isZooKeeperAware() && !preExisitingZkEntry) {
        try {
          getZkController().unregister(coreName, cd);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          SolrException.log(log, null, e);
        } catch (KeeperException e) {
          SolrException.log(log, null, e);
        } catch (Exception e) {
          SolrException.log(log, null, e);
        }
      }

      ParWork.close(core);
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
  private SolrCore createFromDescriptor(CoreDescriptor dcore, boolean newCollection) {

    log.info("createFromDescriptor {} {}", dcore, newCollection);

    if (isShutDown) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Solr has been shutdown.");
    }

    SolrCore core = null;
    boolean registered = false;
    try {
      MDCLoggingContext.setCoreDescriptor(this, dcore);
      SolrIdentifierValidator.validateCoreName(dcore.getName());

      ConfigSet coreConfig = coreConfigService.loadConfigSet(dcore);
      dcore.setConfigSetTrusted(coreConfig.isTrusted());
      if (log.isInfoEnabled()) {
        log.info("Creating SolrCore '{}' using configuration from {}, trusted={}", dcore.getName(), coreConfig.getName(), dcore.isConfigSetTrusted());
      }
      try {
        if (isShutDown) {
          throw new AlreadyClosedException("Solr has been shutdown.");
        }
        core = new SolrCore(this, dcore, coreConfig);
      } catch (Exception e) {
        core = processCoreCreateException(e, dcore, coreConfig);
      }


      registerCore(dcore, core, isZooKeeperAware(), false);
      registered = true;

      // always kick off recovery if we are in non-Cloud mode
      if (!isZooKeeperAware() && core.getUpdateHandler().getUpdateLog() != null) {
        core.getUpdateHandler().getUpdateLog().recoverFromLog();
      }

      return core;
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.error("Unable to create SolrCore", e);
      coreInitFailures.put(dcore.getName(), new CoreLoadFailure(dcore, e));
      if (e instanceof ZkController.NotInClusterStateException && !newCollection) {
        // this mostly happen when the core is deleted when this node is down
        unload(dcore.getName(), true, true, true);
        throw e;
      }
      if (!registered) {
        solrCores.removeCoreDescriptor(dcore);
      }
      final SolrException solrException = new SolrException(ErrorCode.SERVER_ERROR, "Unable to create core [" + dcore.getName() + "]", e);
      if (core != null && !core.isClosed())
        ParWork.close(core);
      throw solrException;
    } catch (Throwable t) {
      log.error("Unable to create SolrCore", t);
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
        ConfigSet configSet = coreConfigService.loadConfigSet(cd);
        return DirectoryFactory.loadDirectoryFactory(configSet.getSolrConfig(), this, null).isSharedStorage();
      }
    }
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
  private SolrCore processCoreCreateException(Exception original, CoreDescriptor dcore, ConfigSet coreConfig) {
    log.error("Error creating SolrCore", original);

    // Traverse full chain since CIE may not be root exception
    Throwable cause = original;
    if (!(cause instanceof  CorruptIndexException)) {
      while ((cause = cause.getCause()) != null) {
        if (cause instanceof CorruptIndexException) {
          break;
        }
      }
    }

    // If no CorruptIndexException, nothing we can try here
    if (cause == null) {
      if (original instanceof RuntimeException) {
        throw (RuntimeException) original;
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, original);
      }
    }

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
              getZkController().getShardTerms(desc.getCollectionName(), desc.getShardId()).setTermToZero(dcore.getName());
              return new SolrCore(this, dcore, coreConfig);
            }
          } catch (SolrException se) {
            se.addSuppressed(original);
            throw se;
          }
        }
        if (original instanceof RuntimeException) {
          throw (RuntimeException) original;
        } else {
          throw new SolrException(ErrorCode.SERVER_ERROR, original);
        }
      case none:
        if (original instanceof RuntimeException) {
          throw (RuntimeException) original;
        } else {
          throw new SolrException(ErrorCode.SERVER_ERROR, original);
        }
      default:
        log.warn("Failed to create core, and did not recognize specified 'CoreInitFailedAction': [{}]. Valid options are {}.",
            action, Arrays.asList(CoreInitFailedAction.values()));
        if (original instanceof RuntimeException) {
          throw (RuntimeException) original;
        } else {
          throw new SolrException(ErrorCode.SERVER_ERROR, original);
        }
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
        df.doneWithDirectory(dir);
        df.release(dir);
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
    try (SolrCore core = solrCores.getCoreFromAnyList(name, true)) {
      if (core != null) {

        // The underlying core properties files may have changed, we don't really know. So we have a (perhaps) stale
        // CoreDescriptor and we need to reload it from the disk files
        CoreDescriptor cd = core.getCoreDescriptor();
        //        if (core.getDirectoryFactory().isPersistent()) {
        //          cd = reloadCoreDescriptor(core.getCoreDescriptor());
        //        } else {
        //          cd = core.getCoreDescriptor();
        //        }
        //        solrCores.addCoreDescriptor(cd);
        Closeable oldCore = null;
        boolean success = false;
        try {
          solrCores.waitForLoadingCoreToFinish(name, 15000);
          ConfigSet coreConfig = coreConfigService.loadConfigSet(cd);
          log.info("Reloading SolrCore '{}' using configuration from {}", name, coreConfig.getName());
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
            Replica replica = docCollection.getReplica(cd.getName());
            assert replica != null;
            if (replica.getType() == Replica.Type.TLOG) { // TODO: needed here?
              getZkController().stopReplicationFromLeader(core.getName());
              if (!cd.getCloudDescriptor().isLeader()) {
                getZkController().startReplicationFromLeader(newCore.getName(), true);
              }

            } else if (replica.getType() == Replica.Type.PULL) {
              getZkController().startReplicationFromLeader(newCore.getName(), false);
            }
          }
          success = true;
        } catch (SolrCoreState.CoreIsClosedException e) {
          throw e;
        } catch (Exception e) {
          ParWork.propagateInterrupt("Exception reloading SolrCore", e);
          SolrException exp = new SolrException(ErrorCode.SERVER_ERROR, "Unable to reload core [" + cd.getName() + "]", e);
          try {
            coreInitFailures.put(cd.getName(), new CoreLoadFailure(cd, e));

          } catch (Exception e1) {
            ParWork.propagateInterrupt(e1);
            exp.addSuppressed(e1);
          }
          throw exp;
        } finally {
          if (!success) {
            ParWork.close(newCore);
          }
        }
      } else {
        CoreLoadFailure clf = coreInitFailures.get(name);
        if (clf != null) {
          createFromDescriptor(clf.cd, false);
        } else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such core: " + name);
        }
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

    log.info("swapped: {} with {}", n0, n1);
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
    log.info("Unload SolrCore {} deleteIndexDir={} deleteDataDir={} deleteInstanceDir={}", name, deleteIndexDir, deleteDataDir, deleteInstanceDir);
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

    if (isZooKeeperAware()) {
      if (cd != null) {
        getZkController().closeLeaderContext(cd);
      }
      getZkController().stopReplicationFromLeader(name);
    }

    SolrCore core = null;
    try {

      core = solrCores.remove(name);
      if (core != null) {
        core.closing = true;
      }

      if (cd == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Cannot unload non-existent core [" + name + "]");
      }
      if (cd != null) {
        SolrCore.deleteUnloadedCore(cd, deleteDataDir, deleteInstanceDir);
        solrCores.removeCoreDescriptor(cd);
        coresLocator.delete(this, cd);
        if (core == null) {
          // transient core
          SolrCore.deleteUnloadedCore(cd, deleteDataDir, deleteInstanceDir);
          return;
        }
      }
    } finally {
      if (isZooKeeperAware()) {
        // cancel recovery in cloud mode
        if (core != null) {
          core.getSolrCoreState().cancelRecovery(true, true);
        }
        if (cd != null &&  cd.getCloudDescriptor() != null && (cd.getCloudDescriptor().getReplicaType() ==
            Replica.Type.PULL || cd.getCloudDescriptor().getReplicaType() == Replica.Type.TLOG)) {
          // Stop replication if this is part of a pull/tlog replica before closing the core
          zkSys.getZkController().stopReplicationFromLeader(name);

        }
        if (cd != null && zkSys.zkController.getZkClient().isConnected()) {
          try {
            zkSys.getZkController().unregister(name, cd);
          } catch (InterruptedException e) {
            ParWork.propagateInterrupt(e);
            throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted while unregistering core [" + name + "] from cloud state");
          } catch (KeeperException e) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "Error unregistering core [" + name + "] from cloud state", e);
          } catch (AlreadyClosedException e) {

          } catch (Exception e) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "Error unregistering core [" + name + "] from cloud state", e);
          }
        }

      }
    }

    // delete metrics specific to this core
    if (metricManager != null && core != null) {
      metricManager.removeRegistry(core.getCoreMetricManager().getRegistryName());
    }

    if (cd != null && core != null) {
      core.unloadOnClose(cd, deleteIndexDir, deleteDataDir);
    }

    try {
      if (core != null) {
        core.closeAndWait();
      }
    } catch (TimeoutException e) {
      log.error("Timeout waiting for SolrCore close on unload", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Timeout waiting for SolrCore close on unload", e);
    } finally {
      if (deleteInstanceDir && cd != null) {
        try {
          FileUtils.deleteDirectory(cd.getInstanceDir().toFile());
        } catch (IOException e) {
          SolrException.log(log, "Failed to delete instance dir for core:" + cd.getName() + " dir:" + cd.getInstanceDir());
        }
      }
    }
  }

  void deleteCoreNode(String collectionName, String nodeName, String baseUrl, String core) throws Exception {
    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
        ZkStateReader.CORE_NAME_PROP, core,
        ZkStateReader.NODE_NAME_PROP, nodeName,
        ZkStateReader.COLLECTION_PROP, collectionName,
        ZkStateReader.BASE_URL_PROP, baseUrl);
    getZkController().getOverseer().offerStateUpdate(Utils.toJSON(m));
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
        registerCore(cd, core, isZooKeeperAware(), false);
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

  /** Where cores are created (absolute). */
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
    return getCore(name, true);
  }

  /**
   * Gets a core by name and increase its refcount.
   *
   * @param name the core name
   * @return the core if found, null if a SolrCore by this name does not exist
   * @throws SolrCoreInitializationException if a SolrCore with this name failed to be initialized
   * @see SolrCore#close()
   */
  public SolrCore getCore(String name, boolean incRefCount) {
    SolrCore core = null;
    CoreDescriptor desc = null;
    for (int i = 0; i < 2; i++) {
      // Do this in two phases since we don't want to lock access to the cores over a load.
      core = solrCores.getCoreFromAnyList(name, incRefCount);

      // If a core is loaded, we're done just return it.
      if (core != null) {
        return core;
      }

      // If it's not yet loaded, we can check if it's had a core init failure and "do the right thing"
      desc = solrCores.getCoreDescriptor(name);

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
      if (isZooKeeperAware()) {
        solrCores.waitForLoadingCoreToFinish(name, 15000);
      } else {
        break;
      }
    }

    if (desc == null || isZooKeeperAware()) return null;

    // This will put an entry in pending core ops if the core isn't loaded. Here's where moving the
    // waitAddPendingCoreOps to createFromDescriptor would introduce a race condition.

    // todo: ensure only transient?
    if (core == null) {
      // nocommit - this does not seem right - should stop a core from loading on startup, before zk reg, not from getCore ...
      //      if (isZooKeeperAware()) {
      //        zkSys.getZkController().throwErrorIfReplicaReplaced(desc);
      //      }
      core = createFromDescriptor(desc, false); // This should throw an error if it fails.
    }



    core.open();


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

  @SuppressWarnings({"rawtypes"})
  protected <T> T createHandler(String path, String handlerClass, Class<T> clazz) {
    T handler = loader.newInstance(handlerClass, clazz, new String[]{"handler.admin."}, new Class[]{CoreContainer.class}, new Object[]{this});
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

  @SuppressWarnings({"rawtypes"})
  public LogWatcher getLogging() {
    return logging;
  }

  /**
   * Determines whether the core is already loaded or not but does NOT load the core
   */
  public boolean isLoaded(String name) {
    return solrCores.isLoaded(name);
  }

  // Primarily for transient cores when a core is aged out.
  //  public void queueCoreToClose(SolrCore coreToClose) {
  //    solrCores.queueCoreToClose(coreToClose);
  //  }

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

  /** The primary path of a Solr server's config, cores, and misc things. Absolute. */
  //TODO return Path
  public String getSolrHome() {
    return solrHome.toString();
  }

  public boolean isZooKeeperAware() {
    return isZkAware && zkSys != null && zkSys.zkController != null;
  }

  public ZkController getZkController() {
    return zkSys == null ? null : zkSys.getZkController();
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

  private static class CredentialsProviderProvider extends SolrHttpClientContextBuilder.CredentialsProviderProvider {

    private final SolrHttpClientBuilder builder;

    public CredentialsProviderProvider(SolrHttpClientBuilder builder) {
      this.builder = builder;
    }

    @Override
    public CredentialsProvider getCredentialsProvider() {
      return builder.getCredentialsProviderProvider().getCredentialsProvider();
    }
  }

  private static class AuthSchemeRegistryProvider extends SolrHttpClientContextBuilder.AuthSchemeRegistryProvider {

    private final SolrHttpClientBuilder builder;

    public AuthSchemeRegistryProvider(SolrHttpClientBuilder builder) {
      this.builder = builder;
    }

    @Override
    public Lookup<AuthSchemeProvider> getAuthSchemeRegistry() {
      return builder.getAuthSchemeRegistryProvider().getAuthSchemeRegistry();
    }
  }

  private class EmbeddedSolrServer extends org.apache.solr.client.solrj.embedded.EmbeddedSolrServer {
    public EmbeddedSolrServer() {
      super(CoreContainer.this, null);
    }

    @Override
    public void close() throws IOException {
      // do nothing - we close the container ourselves
    }
  }
}

