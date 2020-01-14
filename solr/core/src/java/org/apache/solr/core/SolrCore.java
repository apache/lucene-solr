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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.google.common.collect.Iterators;
import com.google.common.collect.MapMaker;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.RecoveryStrategy;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CommonParams.EchoParamStyle;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager.SnapshotMetaData;
import org.apache.solr.handler.IndexFetcher;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.handler.component.HighlightComponent;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.pkg.PackageListeners;
import org.apache.solr.pkg.PackageLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.BinaryResponseWriter;
import org.apache.solr.response.CSVResponseWriter;
import org.apache.solr.response.GeoJSONResponseWriter;
import org.apache.solr.response.GraphMLResponseWriter;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.solr.response.PHPResponseWriter;
import org.apache.solr.response.PHPSerializedResponseWriter;
import org.apache.solr.response.PythonResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.RubyResponseWriter;
import org.apache.solr.response.SchemaXmlResponseWriter;
import org.apache.solr.response.SmileResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.XMLResponseWriter;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.rest.ManagedResourceStorage;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.apache.solr.rest.RestManager;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrFieldCacheBean;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.search.stats.LocalStatsCache;
import org.apache.solr.search.stats.StatsCache;
import org.apache.solr.update.DefaultSolrCoreState;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.IndexFingerprint;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.update.SolrCoreState.IndexWriterCloser;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.VersionInfo;
import org.apache.solr.update.processor.DistributedUpdateProcessorFactory;
import org.apache.solr.update.processor.LogUpdateProcessorFactory;
import org.apache.solr.update.processor.NestedUpdateProcessorFactory;
import org.apache.solr.update.processor.RunUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.update.processor.UpdateRequestProcessorChain.ProcessorInfo;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.IOFunction;
import org.apache.solr.util.NumberUtils;
import org.apache.solr.util.PropertiesInputStream;
import org.apache.solr.util.PropertiesOutputStream;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.PATH;

/**
 * SolrCore got its name because it represents the "core" of Solr -- one index and everything needed to make it work.
 * When multi-core support was added to Solr way back in version 1.3, this class was required so that the core
 * functionality could be re-used multiple times.
 */
public final class SolrCore implements SolrInfoBean, Closeable {

  public static final String version = "1.0";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Logger requestLog = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getName() + ".Request");
  private static final Logger slowLog = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getName() + ".SlowRequest");

  private String name;
  private String logid; // used to show what name is set

  private boolean isReloaded = false;

  private final SolrConfig solrConfig;
  private final SolrResourceLoader resourceLoader;
  private volatile IndexSchema schema;
  private final NamedList configSetProperties;
  private final String dataDir;
  private final String ulogDir;
  private final UpdateHandler updateHandler;
  private final SolrCoreState solrCoreState;

  private final Date startTime = new Date();
  private final long startNanoTime = System.nanoTime();
  private final RequestHandlers reqHandlers;
  private final PluginBag<SearchComponent> searchComponents = new PluginBag<>(SearchComponent.class, this);
  private final PluginBag<UpdateRequestProcessorFactory> updateProcessors = new PluginBag<>(UpdateRequestProcessorFactory.class, this, true);
  private final Map<String, UpdateRequestProcessorChain> updateProcessorChains;
  private final SolrCoreMetricManager coreMetricManager;
  private final Map<String, SolrInfoBean> infoRegistry = new ConcurrentHashMap<>();
  private final IndexDeletionPolicyWrapper solrDelPolicy;
  private final SolrSnapshotMetaDataManager snapshotMgr;
  private final DirectoryFactory directoryFactory;
  private final RecoveryStrategy.Builder recoveryStrategyBuilder;
  private IndexReaderFactory indexReaderFactory;
  private final Codec codec;
  private final MemClassLoader memClassLoader;

  private final List<Runnable> confListeners = new CopyOnWriteArrayList<>();

  private final ReentrantLock ruleExpiryLock;
  private final ReentrantLock snapshotDelLock; // A lock instance to guard against concurrent deletions.

  private Timer newSearcherTimer;
  private Timer newSearcherWarmupTimer;
  private Counter newSearcherCounter;
  private Counter newSearcherMaxReachedCounter;
  private Counter newSearcherOtherErrorsCounter;
  private final CoreContainer coreContainer;

  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private final String metricTag = SolrMetricProducer.getUniqueMetricTag(this, null);
  private final SolrMetricsContext solrMetricsContext;

  public volatile boolean searchEnabled = true;
  public volatile boolean indexEnabled = true;
  public volatile boolean readOnly = false;

  private PackageListeners packageListeners = new PackageListeners(this);

  public Set<String> getMetricNames() {
    return metricNames;
  }

  public Date getStartTimeStamp() {
    return startTime;
  }

  private final Map<IndexReader.CacheKey, IndexFingerprint> perSegmentFingerprintCache = new MapMaker().weakKeys().makeMap();

  public long getStartNanoTime() {
    return startNanoTime;
  }

  public long getUptimeMs() {
    return TimeUnit.MILLISECONDS.convert(System.nanoTime() - startNanoTime, TimeUnit.NANOSECONDS);
  }

  private final RestManager restManager;

  public RestManager getRestManager() {
    return restManager;
  }

  public PackageListeners getPackageListeners() {
    return packageListeners;
  }

  static int boolean_query_max_clause_count = Integer.MIN_VALUE;

  private ExecutorService coreAsyncTaskExecutor = ExecutorUtil.newMDCAwareCachedThreadPool("Core Async Task");

  /**
   * The SolrResourceLoader used to load all resources for this core.
   *
   * @since solr 1.3
   */
  public SolrResourceLoader getResourceLoader() {
    return resourceLoader;
  }

  /** Gets the SolrResourceLoader for a given package
   * @param pkg The package name
   */
  public SolrResourceLoader getResourceLoader(String pkg) {
    if (pkg == null) {
      return resourceLoader;
    }
    PackageLoader.Package aPackage = coreContainer.getPackageLoader().getPackage(pkg);
    PackageLoader.Package.Version latest = aPackage.getLatest();
    return latest.getLoader();
  }

  /**
   * Gets the configuration resource name used by this core instance.
   *
   * @since solr 1.3
   */
  public String getConfigResource() {
    return solrConfig.getResourceName();
  }

  /**
   * Gets the configuration object used by this core instance.
   */
  public SolrConfig getSolrConfig() {
    return solrConfig;
  }

  /**
   * Gets the schema resource name used by this core instance.
   *
   * @since solr 1.3
   */
  public String getSchemaResource() {
    return getLatestSchema().getResourceName();
  }

  /**
   * @return the latest snapshot of the schema used by this core instance.
   * @see #setLatestSchema
   */
  public IndexSchema getLatestSchema() {
    return schema;
  }

  /**
   * Sets the latest schema snapshot to be used by this core instance.
   * If the specified <code>replacementSchema</code> uses a {@link SimilarityFactory} which is
   * {@link SolrCoreAware} then this method will {@link SolrCoreAware#inform} that factory about
   * this SolrCore prior to using the <code>replacementSchema</code>
   *
   * @see #getLatestSchema
   */
  public void setLatestSchema(IndexSchema replacementSchema) {
    // 1) For a newly instantiated core, the Similarity needs SolrCore before inform() is called on
    // any registered SolrCoreAware listeners (which will likeley need to use the SolrIndexSearcher.
    //
    // 2) If a new IndexSchema is assigned to an existing live SolrCore (ie: managed schema
    // replacement via SolrCloud) then we need to explicitly inform() the similarity because
    // we can't rely on the normal SolrResourceLoader lifecycle because the sim was instantiated
    // after the SolrCore was already live (see: SOLR-8311 + SOLR-8280)
    final SimilarityFactory similarityFactory = replacementSchema.getSimilarityFactory();
    if (similarityFactory instanceof SolrCoreAware) {
      ((SolrCoreAware) similarityFactory).inform(this);
    }
    this.schema = replacementSchema;
  }

  public NamedList getConfigSetProperties() {
    return configSetProperties;
  }

  public String getDataDir() {
    return dataDir;
  }

  public String getUlogDir() {
    return ulogDir;
  }

  public String getIndexDir() {
    synchronized (searcherLock) {
      if (_searcher == null) return getNewIndexDir();
      SolrIndexSearcher searcher = _searcher.get();
      return searcher.getPath() == null ? dataDir + "index/" : searcher
          .getPath();
    }
  }


  /**
   * Returns the indexdir as given in index.properties. If index.properties exists in dataDir and
   * there is a property <i>index</i> available and it points to a valid directory
   * in dataDir that is returned. Else dataDir/index is returned. Only called for creating new indexSearchers
   * and indexwriters. Use the getIndexDir() method to know the active index directory
   *
   * @return the indexdir as given in index.properties
   * @throws SolrException if for any reason the a reasonable index directory cannot be determined.
   */
  public String getNewIndexDir() {
    Directory dir = null;
    try {
      dir = getDirectoryFactory().get(getDataDir(), DirContext.META_DATA, getSolrConfig().indexConfig.lockType);
      String result = getIndexPropertyFromPropFile(dir);
      if (!result.equals(lastNewIndexDir)) {
        log.debug("New index directory detected: old={} new={}", lastNewIndexDir, result);
      }
      lastNewIndexDir = result;
      return result;
    } catch (IOException e) {
      SolrException.log(log, "", e);
      // See SOLR-11687. It is inadvisable to assume we can do the right thing for any but a small
      // number of exceptions that ware caught and swallowed in getIndexProperty.
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error in getNewIndexDir, exception: ", e);
    } finally {
      if (dir != null) {
        try {
          getDirectoryFactory().release(dir);
        } catch (IOException e) {
          SolrException.log(log, "", e);
        }
      }
    }
  }

  // This is guaranteed to return a string or throw an exception.
  //
  // NOTE: Not finding the index.properties file is normal.
  //
  // We return dataDir/index if there is an index.properties file with no value for "index"
  // See SOLR-11687
  //

  private String getIndexPropertyFromPropFile(Directory dir) throws IOException {
    IndexInput input;
    try {
      input = dir.openInput(IndexFetcher.INDEX_PROPERTIES, IOContext.DEFAULT);
    } catch (FileNotFoundException | NoSuchFileException e) {
      // Swallow this error, dataDir/index is the right thing to return
      // if there is no index.properties file
      // All other exceptions are will propagate to caller.
      return dataDir + "index/";
    }
    final InputStream is = new PropertiesInputStream(input); // c'tor just assigns a variable here, no exception thrown.
    try {
      Properties p = new Properties();
      p.load(new InputStreamReader(is, StandardCharsets.UTF_8));

      String s = p.getProperty("index");
      if (s != null && s.trim().length() > 0) {
        return dataDir + s.trim();
      }

      // We'll return dataDir/index/ if the properties file has an "index" property with
      // no associated value or does not have an index property at all.
      return dataDir + "index/";
    } finally {
      IOUtils.closeQuietly(is);
    }
  }

  private String lastNewIndexDir; // for debugging purposes only... access not synchronized, but that's ok


  public DirectoryFactory getDirectoryFactory() {
    return directoryFactory;
  }

  public IndexReaderFactory getIndexReaderFactory() {
    return indexReaderFactory;
  }

  public long getIndexSize() {
    Directory dir;
    long size = 0;
    try {
      if (directoryFactory.exists(getIndexDir())) {
        dir = directoryFactory.get(getIndexDir(), DirContext.DEFAULT, solrConfig.indexConfig.lockType);
        try {
          size = DirectoryFactory.sizeOfDirectory(dir);
        } finally {
          directoryFactory.release(dir);
        }
      }
    } catch (IOException e) {
      SolrException.log(log, "IO error while trying to get the size of the Directory", e);
    }
    return size;
  }

  @Override
  public String getName() {
    return name;
  }

  public void setName(String v) {
    this.name = v;
    this.logid = (v == null) ? "" : ("[" + v + "] ");
    if (coreMetricManager != null) {
      coreMetricManager.afterCoreSetName();
    }
  }

  public String getLogId() {
    return this.logid;
  }

  /**
   * Returns the {@link SolrCoreMetricManager} for this core.
   *
   * @return the {@link SolrCoreMetricManager} for this core
   */
  public SolrCoreMetricManager getCoreMetricManager() {
    return coreMetricManager;
  }

  /**
   * Returns a Map of name vs SolrInfoBean objects. The returned map is an instance of
   * a ConcurrentHashMap and therefore no synchronization is needed for putting, removing
   * or iterating over it.
   *
   * @return the Info Registry map which contains SolrInfoBean objects keyed by name
   * @since solr 1.3
   */
  public Map<String, SolrInfoBean> getInfoRegistry() {
    return infoRegistry;
  }

  private IndexDeletionPolicyWrapper initDeletionPolicy(IndexDeletionPolicyWrapper delPolicyWrapper) {
    if (delPolicyWrapper != null) {
      return delPolicyWrapper;
    }

    final PluginInfo info = solrConfig.getPluginInfo(IndexDeletionPolicy.class.getName());
    final IndexDeletionPolicy delPolicy;
    if (info != null) {
      delPolicy = createInstance(info.className, IndexDeletionPolicy.class, "Deletion Policy for SOLR", this, getResourceLoader());
      if (delPolicy instanceof NamedListInitializedPlugin) {
        ((NamedListInitializedPlugin) delPolicy).init(info.initArgs);
      }
    } else {
      delPolicy = new SolrDeletionPolicy();
    }

    return new IndexDeletionPolicyWrapper(delPolicy, snapshotMgr);
  }

  private SolrSnapshotMetaDataManager initSnapshotMetaDataManager() {
    try {
      String dirName = getDataDir() + SolrSnapshotMetaDataManager.SNAPSHOT_METADATA_DIR + "/";
      Directory snapshotDir = directoryFactory.get(dirName, DirContext.DEFAULT,
          getSolrConfig().indexConfig.lockType);
      return new SolrSnapshotMetaDataManager(this, snapshotDir);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * This method deletes the snapshot with the specified name. If the directory
   * storing the snapshot is not the same as the *current* core index directory,
   * then delete the files corresponding to this snapshot. Otherwise we leave the
   * index files related to snapshot as is (assuming the underlying Solr IndexDeletionPolicy
   * will clean them up appropriately).
   *
   * @param commitName The name of the snapshot to be deleted.
   * @throws IOException in case of I/O error.
   */
  public void deleteNamedSnapshot(String commitName) throws IOException {
    // Note this lock is required to prevent multiple snapshot deletions from
    // opening multiple IndexWriter instances simultaneously.
    this.snapshotDelLock.lock();
    try {
      Optional<SnapshotMetaData> metadata = snapshotMgr.release(commitName);
      if (metadata.isPresent()) {
        long gen = metadata.get().getGenerationNumber();
        String indexDirPath = metadata.get().getIndexDirPath();

        if (!indexDirPath.equals(getIndexDir())) {
          Directory d = getDirectoryFactory().get(indexDirPath, DirContext.DEFAULT, "none");
          try {
            Collection<SnapshotMetaData> snapshots = snapshotMgr.listSnapshotsInIndexDir(indexDirPath);
            log.info("Following snapshots exist in the index directory {} : {}", indexDirPath, snapshots);
            if (snapshots.isEmpty()) {// No snapshots remain in this directory. Can be cleaned up!
              log.info("Removing index directory {} since all named snapshots are deleted.", indexDirPath);
              getDirectoryFactory().remove(d);
            } else {
              SolrSnapshotManager.deleteSnapshotIndexFiles(this, d, gen);
            }
          } finally {
            getDirectoryFactory().release(d);
          }
        }
      }
    } finally {
      snapshotDelLock.unlock();
    }
  }

  /**
   * This method deletes the index files not associated with any named snapshot only
   * if the specified indexDirPath is not the *current* index directory.
   *
   * @param indexDirPath The path of the directory
   * @throws IOException In case of I/O error.
   */
  public void deleteNonSnapshotIndexFiles(String indexDirPath) throws IOException {
    // Skip if the specified indexDirPath is the *current* index directory.
    if (getIndexDir().equals(indexDirPath)) {
      return;
    }

    // Note this lock is required to prevent multiple snapshot deletions from
    // opening multiple IndexWriter instances simultaneously.
    this.snapshotDelLock.lock();
    Directory dir = getDirectoryFactory().get(indexDirPath, DirContext.DEFAULT, "none");
    try {
      Collection<SnapshotMetaData> snapshots = snapshotMgr.listSnapshotsInIndexDir(indexDirPath);
      log.info("Following snapshots exist in the index directory {} : {}", indexDirPath, snapshots);
      // Delete the old index directory only if no snapshot exists in that directory.
      if (snapshots.isEmpty()) {
        log.info("Removing index directory {} since all named snapshots are deleted.", indexDirPath);
        getDirectoryFactory().remove(dir);
      } else {
        SolrSnapshotManager.deleteNonSnapshotIndexFiles(this, dir, snapshots);
      }
    } finally {
      snapshotDelLock.unlock();
      if (dir != null) {
        getDirectoryFactory().release(dir);
      }
    }
  }


  private void initListeners() {
    final Class<SolrEventListener> clazz = SolrEventListener.class;
    final String label = "Event Listener";
    for (PluginInfo info : solrConfig.getPluginInfos(SolrEventListener.class.getName())) {
      final String event = info.attributes.get("event");
      if ("firstSearcher".equals(event)) {
        SolrEventListener obj = createInitInstance(info, clazz, label, null);
        firstSearcherListeners.add(obj);
        log.debug("[{}] Added SolrEventListener for firstSearcher: [{}]", logid, obj);
      } else if ("newSearcher".equals(event)) {
        SolrEventListener obj = createInitInstance(info, clazz, label, null);
        newSearcherListeners.add(obj);
        log.debug("[{}] Added SolrEventListener for newSearcher: [{}]", logid, obj);
      }
    }
  }

  final List<SolrEventListener> firstSearcherListeners = new ArrayList<>();
  final List<SolrEventListener> newSearcherListeners = new ArrayList<>();

  /**
   * NOTE: this function is not thread safe.  However, it is safe to call within the
   * <code>inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes.
   * Outside <code>inform</code>, this could potentially throw a ConcurrentModificationException
   *
   * @see SolrCoreAware
   */
  public void registerFirstSearcherListener(SolrEventListener listener) {
    firstSearcherListeners.add(listener);
  }

  /**
   * NOTE: this function is not thread safe.  However, it is safe to call within the
   * <code>inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes.
   * Outside <code>inform</code>, this could potentially throw a ConcurrentModificationException
   *
   * @see SolrCoreAware
   */
  public void registerNewSearcherListener(SolrEventListener listener) {
    newSearcherListeners.add(listener);
  }

  /**
   * NOTE: this function is not thread safe.  However, it is safe to call within the
   * <code>inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes.
   * Outside <code>inform</code>, this could potentially throw a ConcurrentModificationException
   *
   * @see SolrCoreAware
   */
  public QueryResponseWriter registerResponseWriter(String name, QueryResponseWriter responseWriter) {
    return responseWriters.put(name, responseWriter);
  }

  public SolrCore reload(ConfigSet coreConfig) throws IOException {
    // only one reload at a time
    synchronized (getUpdateHandler().getSolrCoreState().getReloadLock()) {
      solrCoreState.increfSolrCoreState();
      final SolrCore currentCore;
      if (!getNewIndexDir().equals(getIndexDir())) {
        // the directory is changing, don't pass on state
        currentCore = null;
      } else {
        currentCore = this;
      }

      boolean success = false;
      SolrCore core = null;
      try {
        CoreDescriptor cd = new CoreDescriptor(name, getCoreDescriptor());
        cd.loadExtraProperties(); //Reload the extra properties
        core = new SolrCore(coreContainer, getName(), getDataDir(), coreConfig.getSolrConfig(),
            coreConfig.getIndexSchema(), coreConfig.getProperties(),
            cd, updateHandler, solrDelPolicy, currentCore, true);

        // we open a new IndexWriter to pick up the latest config
        core.getUpdateHandler().getSolrCoreState().newIndexWriter(core, false);

        core.getSearcher(true, false, null, true);
        success = true;
        return core;
      } finally {
        // close the new core on any errors that have occurred.
        if (!success && core != null && core.getOpenCount() > 0) {
          IOUtils.closeQuietly(core);
        }
      }
    }
  }

  private DirectoryFactory initDirectoryFactory() {
    return DirectoryFactory.loadDirectoryFactory(solrConfig, coreContainer, coreMetricManager.getRegistryName());
  }

  private RecoveryStrategy.Builder initRecoveryStrategyBuilder() {
    final PluginInfo info = solrConfig.getPluginInfo(RecoveryStrategy.Builder.class.getName());
    final RecoveryStrategy.Builder rsBuilder;
    if (info != null && info.className != null) {
      log.info(info.className);
      rsBuilder = getResourceLoader().newInstance(info.className, RecoveryStrategy.Builder.class);
    } else {
      log.debug("solr.RecoveryStrategy.Builder");
      rsBuilder = new RecoveryStrategy.Builder();
    }
    if (info != null) {
      rsBuilder.init(info.initArgs);
    }
    return rsBuilder;
  }

  private void initIndexReaderFactory() {
    IndexReaderFactory indexReaderFactory;
    PluginInfo info = solrConfig.getPluginInfo(IndexReaderFactory.class.getName());
    if (info != null) {
      indexReaderFactory = resourceLoader.newInstance(info.className, IndexReaderFactory.class);
      indexReaderFactory.init(info.initArgs);
    } else {
      indexReaderFactory = new StandardIndexReaderFactory();
    }
    this.indexReaderFactory = indexReaderFactory;
  }

  // protect via synchronized(SolrCore.class)
  private static Set<String> dirs = new HashSet<>();

  /**
   * Returns <code>true</code> iff the index in the named directory is
   * currently locked.
   *
   * @param directory the directory to check for a lock
   * @throws IOException if there is a low-level IO error
   * @deprecated Use of this method can only lead to race conditions. Try
   * to actually obtain a lock instead.
   */
  @Deprecated
  private static boolean isWriterLocked(Directory directory) throws IOException {
    try {
      directory.obtainLock(IndexWriter.WRITE_LOCK_NAME).close();
      return false;
    } catch (LockObtainFailedException failed) {
      return true;
    }
  }

  void initIndex(boolean passOnPreviousState, boolean reload) throws IOException {
    String indexDir = getNewIndexDir();
    boolean indexExists = getDirectoryFactory().exists(indexDir);
    boolean firstTime;
    synchronized (SolrCore.class) {
      firstTime = dirs.add(getDirectoryFactory().normalize(indexDir));
    }

    initIndexReaderFactory();

    if (indexExists && firstTime && !passOnPreviousState) {
      final String lockType = getSolrConfig().indexConfig.lockType;
      Directory dir = directoryFactory.get(indexDir, DirContext.DEFAULT, lockType);
      try {
        if (isWriterLocked(dir)) {
          log.error("{}Solr index directory '{}' is locked (lockType={}).  Throwing exception.", logid,
              indexDir, lockType);
          throw new LockObtainFailedException(
              "Index dir '" + indexDir + "' of core '" + name + "' is already locked. " +
                  "The most likely cause is another Solr server (or another solr core in this server) " +
                  "also configured to use this directory; other possible causes may be specific to lockType: " +
                  lockType);
        }
      } finally {
        directoryFactory.release(dir);
      }
    }

    // Create the index if it doesn't exist.
    if (!indexExists) {
      log.debug("{}Solr index directory '{}' doesn't exist. Creating new index...", logid, indexDir);
      SolrIndexWriter writer = null;
      try {
        writer = SolrIndexWriter.create(this, "SolrCore.initIndex", indexDir, getDirectoryFactory(), true,
            getLatestSchema(), solrConfig.indexConfig, solrDelPolicy, codec);
      } finally {
        IOUtils.closeQuietly(writer);
      }

    }

    cleanupOldIndexDirectories(reload);
  }


  /**
   * Creates an instance by trying a constructor that accepts a SolrCore before
   * trying the default (no arg) constructor.
   *
   * @param className the instance class to create
   * @param cast      the class or interface that the instance should extend or implement
   * @param msg       a message helping compose the exception error if any occurs.
   * @param core      The SolrCore instance for which this object needs to be loaded
   * @return the desired instance
   * @throws SolrException if the object could not be instantiated
   */
  public static <T> T createInstance(String className, Class<T> cast, String msg, SolrCore core, ResourceLoader resourceLoader) {
    Class<? extends T> clazz = null;
    if (msg == null) msg = "SolrCore Object";
    try {
      clazz = resourceLoader.findClass(className, cast);
      //most of the classes do not have constructors which takes SolrCore argument. It is recommended to obtain SolrCore by implementing SolrCoreAware.
      // So invariably always it will cause a  NoSuchMethodException. So iterate though the list of available constructors
      Constructor<?>[] cons = clazz.getConstructors();
      for (Constructor<?> con : cons) {
        Class<?>[] types = con.getParameterTypes();
        if (types.length == 1 && types[0] == SolrCore.class) {
          return cast.cast(con.newInstance(core));
        }
      }
      return resourceLoader.newInstance(className, cast);//use the empty constructor
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      // The JVM likes to wrap our helpful SolrExceptions in things like
      // "InvocationTargetException" that have no useful getMessage
      if (null != e.getCause() && e.getCause() instanceof SolrException) {
        SolrException inner = (SolrException) e.getCause();
        throw inner;
      }

      throw new SolrException(ErrorCode.SERVER_ERROR, "Error Instantiating " + msg + ", " + className + " failed to instantiate " + cast.getName(), e);
    }
  }

  private UpdateHandler createReloadedUpdateHandler(String className, String msg, UpdateHandler updateHandler) {
    Class<? extends UpdateHandler> clazz = null;
    if (msg == null) msg = "SolrCore Object";
    try {
      clazz = getResourceLoader().findClass(className, UpdateHandler.class);
      //most of the classes do not have constructors which takes SolrCore argument. It is recommended to obtain SolrCore by implementing SolrCoreAware.
      // So invariably always it will cause a  NoSuchMethodException. So iterate though the list of available constructors
      Constructor<?>[] cons = clazz.getConstructors();
      for (Constructor<?> con : cons) {
        Class<?>[] types = con.getParameterTypes();
        if (types.length == 2 && types[0] == SolrCore.class && types[1] == UpdateHandler.class) {
          return UpdateHandler.class.cast(con.newInstance(this, updateHandler));
        }
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error Instantiating " + msg + ", " + className + " could not find proper constructor for " + UpdateHandler.class.getName());
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      // The JVM likes to wrap our helpful SolrExceptions in things like
      // "InvocationTargetException" that have no useful getMessage
      if (null != e.getCause() && e.getCause() instanceof SolrException) {
        SolrException inner = (SolrException) e.getCause();
        throw inner;
      }

      throw new SolrException(ErrorCode.SERVER_ERROR, "Error Instantiating " + msg + ", " + className + " failed to instantiate " + UpdateHandler.class.getName(), e);
    }
  }

  public <T extends Object> T createInitInstance(PluginInfo info, Class<T> cast, String msg, String defClassName) {
    if (info == null) return null;
    T o = createInstance(info.className == null ? defClassName : info.className, cast, msg, this, getResourceLoader(info.pkgName));
    if (o instanceof PluginInfoInitialized) {
      ((PluginInfoInitialized) o).init(info);
    } else if (o instanceof NamedListInitializedPlugin) {
      ((NamedListInitializedPlugin) o).init(info.initArgs);
    }
    if (o instanceof SearchComponent) {
      ((SearchComponent) o).setName(info.name);
    }
    return o;
  }

  private UpdateHandler createUpdateHandler(String className) {
    return createInstance(className, UpdateHandler.class, "Update Handler", this, getResourceLoader());
  }

  private UpdateHandler createUpdateHandler(String className, UpdateHandler updateHandler) {
    return createReloadedUpdateHandler(className, "Update Handler", updateHandler);
  }

  public SolrCore(CoreContainer coreContainer, CoreDescriptor cd, ConfigSet coreConfig) {
    this(coreContainer, cd.getName(), null, coreConfig.getSolrConfig(), coreConfig.getIndexSchema(), coreConfig.getProperties(),
        cd, null, null, null, false);
  }

  public CoreContainer getCoreContainer() {
    return coreContainer;
  }


  /**
   * Creates a new core and register it in the list of cores. If a core with the
   * same name already exists, it will be stopped and replaced by this one.
   *
   * @param dataDir the index directory
   * @param config  a solr config instance
   * @param schema  a solr schema instance
   * @since solr 1.3
   */
  public SolrCore(CoreContainer coreContainer, String name, String dataDir, SolrConfig config,
                  IndexSchema schema, NamedList configSetProperties,
                  CoreDescriptor coreDescriptor, UpdateHandler updateHandler,
                  IndexDeletionPolicyWrapper delPolicy, SolrCore prev, boolean reload) {

    assert ObjectReleaseTracker.track(searcherExecutor); // ensure that in unclean shutdown tests we still close this

    this.coreContainer = coreContainer;

    final CountDownLatch latch = new CountDownLatch(1);

    try {

      CoreDescriptor cd = Objects.requireNonNull(coreDescriptor, "coreDescriptor cannot be null");
      coreContainer.solrCores.addCoreDescriptor(cd);

      setName(name);
      MDCLoggingContext.setCore(this);

      resourceLoader = config.getResourceLoader();
      this.solrConfig = config;
      this.configSetProperties = configSetProperties;
      // Initialize the metrics manager
      this.coreMetricManager = initCoreMetricManager(config);
      solrMetricsContext = coreMetricManager.getSolrMetricsContext();
      this.coreMetricManager.loadReporters();

      if (updateHandler == null) {
        directoryFactory = initDirectoryFactory();
        recoveryStrategyBuilder = initRecoveryStrategyBuilder();
        solrCoreState = new DefaultSolrCoreState(directoryFactory, recoveryStrategyBuilder);
      } else {
        solrCoreState = updateHandler.getSolrCoreState();
        directoryFactory = solrCoreState.getDirectoryFactory();
        recoveryStrategyBuilder = solrCoreState.getRecoveryStrategyBuilder();
        isReloaded = true;
      }

      this.dataDir = initDataDir(dataDir, config, coreDescriptor);
      this.ulogDir = initUpdateLogDir(coreDescriptor);

      log.info("[{}] Opening new SolrCore at [{}], dataDir=[{}]", logid, resourceLoader.getInstancePath(),
          this.dataDir);

      checkVersionFieldExistsInSchema(schema, coreDescriptor);

      // initialize core metrics
      initializeMetrics(solrMetricsContext, null);

      SolrFieldCacheBean solrFieldCacheBean = new SolrFieldCacheBean();
      // this is registered at the CONTAINER level because it's not core-specific - for now we
      // also register it here for back-compat
      solrFieldCacheBean.initializeMetrics(solrMetricsContext, "core");
      infoRegistry.put("fieldCache", solrFieldCacheBean);

      initSchema(config, schema);

      this.maxWarmingSearchers = config.maxWarmingSearchers;
      this.slowQueryThresholdMillis = config.slowQueryThresholdMillis;

      initListeners();

      this.snapshotMgr = initSnapshotMetaDataManager();
      this.solrDelPolicy = initDeletionPolicy(delPolicy);

      this.codec = initCodec(solrConfig, this.schema);

      memClassLoader = new MemClassLoader(
          PluginBag.RuntimeLib.getLibObjects(this, solrConfig.getPluginInfos(PluginBag.RuntimeLib.class.getName())),
          getResourceLoader());
      initIndex(prev != null, reload);

      initWriters();
      qParserPlugins.init(QParserPlugin.standardPlugins, this);
      valueSourceParsers.init(ValueSourceParser.standardValueSourceParsers, this);
      transformerFactories.init(TransformerFactory.defaultFactories, this);
      loadSearchComponents();
      updateProcessors.init(Collections.emptyMap(), this);

      // Processors initialized before the handlers
      updateProcessorChains = loadUpdateProcessorChains();
      reqHandlers = new RequestHandlers(this);
      reqHandlers.initHandlersFromConfig(solrConfig);

      // cause the executor to stall so firstSearcher events won't fire
      // until after inform() has been called for all components.
      // searchExecutor must be single-threaded for this to work
      searcherExecutor.submit(() -> {
        latch.await();
        return null;
      });

      this.updateHandler = initUpdateHandler(updateHandler);

      initSearcher(prev);

      // Initialize the RestManager
      restManager = initRestManager();

      // at this point we can load jars loaded from remote urls.
      memClassLoader.loadRemoteJars();

      // Finally tell anyone who wants to know
      resourceLoader.inform(resourceLoader);
      resourceLoader.inform(this); // last call before the latch is released.
      this.updateHandler.informEventListeners(this);

      infoRegistry.put("core", this);

      // register any SolrInfoMBeans SolrResourceLoader initialized
      //
      // this must happen after the latch is released, because a JMX server impl may
      // choose to block on registering until properties can be fetched from an MBean,
      // and a SolrCoreAware MBean may have properties that depend on getting a Searcher
      // from the core.
      resourceLoader.inform(infoRegistry);

      // Allow the directory factory to report metrics
      if (directoryFactory instanceof SolrMetricProducer) {
        ((SolrMetricProducer) directoryFactory).initializeMetrics(solrMetricsContext, "directoryFactory");
      }

      // seed version buckets with max from index during core initialization ... requires a searcher!
      seedVersionBuckets();

      bufferUpdatesIfConstructing(coreDescriptor);

      this.ruleExpiryLock = new ReentrantLock();
      this.snapshotDelLock = new ReentrantLock();

      registerConfListener();

    } catch (Throwable e) {
      // release the latch, otherwise we block trying to do the close. This
      // should be fine, since counting down on a latch of 0 is still fine
      latch.countDown();
      if (e instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) e;
      }

      try {
        // close down the searcher and any other resources, if it exists, as this
        // is not recoverable
        close();
      } catch (Throwable t) {
        if (t instanceof OutOfMemoryError) {
          throw (OutOfMemoryError) t;
        }
        log.error("Error while closing", t);
      }

      throw new SolrException(ErrorCode.SERVER_ERROR, e.getMessage(), e);
    } finally {
      // allow firstSearcher events to fire and make sure it is released
      latch.countDown();
    }

    assert ObjectReleaseTracker.track(this);
  }

  public void seedVersionBuckets() {
    UpdateHandler uh = getUpdateHandler();
    if (uh != null && uh.getUpdateLog() != null) {
      RefCounted<SolrIndexSearcher> newestSearcher = getRealtimeSearcher();
      if (newestSearcher != null) {
        try {
          uh.getUpdateLog().seedBucketsWithHighestVersion(newestSearcher.get());
        } finally {
          newestSearcher.decref();
        }
      } else {
        log.warn("No searcher available! Cannot seed version buckets with max from index.");
      }
    }
  }

  /**
   * Set UpdateLog to buffer updates if the slice is in construction.
   */
  private void bufferUpdatesIfConstructing(CoreDescriptor coreDescriptor) {

    if (coreContainer != null && coreContainer.isZooKeeperAware()) {
      if (reqHandlers.get("/get") == null) {
        log.warn("WARNING: RealTimeGetHandler is not registered at /get. " +
            "SolrCloud will always use full index replication instead of the more efficient PeerSync method.");
      }

      // ZK pre-register would have already happened so we read slice properties now
      final ClusterState clusterState = coreContainer.getZkController().getClusterState();
      final DocCollection collection = clusterState.getCollection(coreDescriptor.getCloudDescriptor().getCollectionName());
      final Slice slice = collection.getSlice(coreDescriptor.getCloudDescriptor().getShardId());
      if (slice.getState() == Slice.State.CONSTRUCTION) {
        // set update log to buffer before publishing the core
        getUpdateHandler().getUpdateLog().bufferUpdates();
      }
    }
  }

  private void initSearcher(SolrCore prev) throws IOException {
    // use the (old) writer to open the first searcher
    RefCounted<IndexWriter> iwRef = null;
    if (prev != null) {
      iwRef = prev.getUpdateHandler().getSolrCoreState().getIndexWriter(null);
      if (iwRef != null) {
        final IndexWriter iw = iwRef.get();
        final SolrCore core = this;
        newReaderCreator = () -> indexReaderFactory.newReader(iw, core);
      }
    }

    try {
      getSearcher(false, false, null, true);
    } finally {
      newReaderCreator = null;
      if (iwRef != null) {
        iwRef.decref();
      }
    }
  }

  private UpdateHandler initUpdateHandler(UpdateHandler updateHandler) {
    String updateHandlerClass = solrConfig.getUpdateHandlerInfo().className;
    if (updateHandlerClass == null) {
      updateHandlerClass = DirectUpdateHandler2.class.getName();
    }

    final UpdateHandler newUpdateHandler;
    if (updateHandler == null) {
      newUpdateHandler = createUpdateHandler(updateHandlerClass);
    } else {
      newUpdateHandler = createUpdateHandler(updateHandlerClass, updateHandler);
    }
    if (newUpdateHandler instanceof SolrMetricProducer) {
      coreMetricManager.registerMetricProducer("updateHandler", (SolrMetricProducer) newUpdateHandler);
    }
    infoRegistry.put("updateHandler", newUpdateHandler);
    return newUpdateHandler;
  }

  /**
   * Initializes the "Latest Schema" for this SolrCore using either the provided <code>schema</code>
   * if non-null, or a new instance build via the factory identified in the specified <code>config</code>
   *
   * @see IndexSchemaFactory
   * @see #setLatestSchema
   */
  private void initSchema(SolrConfig config, IndexSchema schema) {
    if (schema == null) {
      schema = IndexSchemaFactory.buildIndexSchema(IndexSchema.DEFAULT_SCHEMA_FILE, config);
    }
    setLatestSchema(schema);
  }

  /**
   * Initializes the core's {@link SolrCoreMetricManager} with a given configuration.
   * If metric reporters are configured, they are also initialized for this core.
   *
   * @param config the given configuration
   * @return an instance of {@link SolrCoreMetricManager}
   */
  private SolrCoreMetricManager initCoreMetricManager(SolrConfig config) {
    SolrCoreMetricManager coreMetricManager = new SolrCoreMetricManager(this);
    return coreMetricManager;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    newSearcherCounter = parentContext.counter("new", Category.SEARCHER.toString());
    newSearcherTimer = parentContext.timer("time", Category.SEARCHER.toString(), "new");
    newSearcherWarmupTimer = parentContext.timer("warmup", Category.SEARCHER.toString(), "new");
    newSearcherMaxReachedCounter = parentContext.counter("maxReached", Category.SEARCHER.toString(), "new");
    newSearcherOtherErrorsCounter = parentContext.counter("errors", Category.SEARCHER.toString(), "new");

    parentContext.gauge(() -> name == null ? "(null)" : name, true, "coreName", Category.CORE.toString());
    parentContext.gauge(() -> startTime, true, "startTime", Category.CORE.toString());
    parentContext.gauge(() -> getOpenCount(), true, "refCount", Category.CORE.toString());
    parentContext.gauge(() -> resourceLoader.getInstancePath().toString(), true, "instanceDir", Category.CORE.toString());
    parentContext.gauge(() -> isClosed() ? "(closed)" : getIndexDir(), true, "indexDir", Category.CORE.toString());
    parentContext.gauge(() -> isClosed() ? 0 : getIndexSize(), true, "sizeInBytes", Category.INDEX.toString());
    parentContext.gauge(() -> isClosed() ? "(closed)" : NumberUtils.readableSize(getIndexSize()), true, "size", Category.INDEX.toString());
    if (coreContainer != null) {
      parentContext.gauge(() -> coreContainer.getNamesForCore(this), true, "aliases", Category.CORE.toString());
      final CloudDescriptor cd = getCoreDescriptor().getCloudDescriptor();
      if (cd != null) {
        parentContext.gauge(() -> {
          if (cd.getCollectionName() != null) {
            return cd.getCollectionName();
          } else {
            return "_notset_";
          }
        }, true, "collection", Category.CORE.toString());

        parentContext.gauge(() -> {
          if (cd.getShardId() != null) {
            return cd.getShardId();
          } else {
            return "_auto_";
          }
        }, true, "shard", Category.CORE.toString());
      }
    }
    // initialize disk total / free metrics
    Path dataDirPath = Paths.get(dataDir);
    File dataDirFile = dataDirPath.toFile();
    parentContext.gauge(() -> dataDirFile.getTotalSpace(), true, "totalSpace", Category.CORE.toString(), "fs");
    parentContext.gauge(() -> dataDirFile.getUsableSpace(), true, "usableSpace", Category.CORE.toString(), "fs");
    parentContext.gauge(() -> dataDirPath.toAbsolutePath().toString(), true, "path", Category.CORE.toString(), "fs");
    parentContext.gauge(() -> {
      try {
        return org.apache.lucene.util.IOUtils.spins(dataDirPath.toAbsolutePath());
      } catch (IOException e) {
        // default to spinning
        return true;
      }
    }, true, "spins", Category.CORE.toString(), "fs");
  }

  public String getMetricTag() {
    return metricTag;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  private void checkVersionFieldExistsInSchema(IndexSchema schema, CoreDescriptor coreDescriptor) {
    if (null != coreDescriptor.getCloudDescriptor()) {
      // we are evidently running in cloud mode.  
      //
      // In cloud mode, version field is required for correct consistency
      // ideally this check would be more fine grained, and individual features
      // would assert it when they initialize, but DistributedUpdateProcessor
      // is currently a big ball of wax that does more then just distributing
      // updates (ie: partial document updates), so it needs to work in no cloud
      // mode as well, and can't assert version field support on init.

      try {
        VersionInfo.getAndCheckVersionField(schema);
      } catch (SolrException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Schema will not work with SolrCloud mode: " +
                e.getMessage(), e);
      }
    }
  }

  private String initDataDir(String dataDir, SolrConfig config, CoreDescriptor coreDescriptor) {
    return findDataDir(getDirectoryFactory(), dataDir, config, coreDescriptor);
  }

  /**
   * Locate the data directory for a given config and core descriptor.
   *
   * @param directoryFactory The directory factory to use if necessary to calculate an absolute path. Should be the same as what will
   *                         be used to open the data directory later.
   * @param dataDir          An optional hint to the data directory location. Will be normalized and used if not null.
   * @param config           A solr config to retrieve the default data directory location, if used.
   * @param coreDescriptor   descriptor to load the actual data dir from, if not using the defualt.
   * @return a normalized data directory name
   * @throws SolrException if the data directory cannot be loaded from the core descriptor
   */
  static String findDataDir(DirectoryFactory directoryFactory, String dataDir, SolrConfig config, CoreDescriptor coreDescriptor) {
    if (dataDir == null) {
      if (coreDescriptor.usingDefaultDataDir()) {
        dataDir = config.getDataDir();
      }
      if (dataDir == null) {
        try {
          dataDir = coreDescriptor.getDataDir();
          if (!directoryFactory.isAbsolute(dataDir)) {
            dataDir = directoryFactory.getDataHome(coreDescriptor);
          }
        } catch (IOException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
      }
    }
    return SolrResourceLoader.normalizeDir(dataDir);
  }


  public boolean modifyIndexProps(String tmpIdxDirName) {
    return SolrCore.modifyIndexProps(getDirectoryFactory(), getDataDir(), getSolrConfig(), tmpIdxDirName);
  }

  /**
   * Update the index.properties file with the new index sub directory name
   */
  // package private
  static boolean modifyIndexProps(DirectoryFactory directoryFactory, String dataDir, SolrConfig solrConfig, String tmpIdxDirName) {
    log.info("Updating index properties... index={}", tmpIdxDirName);
    Directory dir = null;
    try {
      dir = directoryFactory.get(dataDir, DirContext.META_DATA, solrConfig.indexConfig.lockType);
      String tmpIdxPropName = IndexFetcher.INDEX_PROPERTIES + "." + System.nanoTime();
      writeNewIndexProps(dir, tmpIdxPropName, tmpIdxDirName);
      directoryFactory.renameWithOverwrite(dir, tmpIdxPropName, IndexFetcher.INDEX_PROPERTIES);
      return true;
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    } finally {
      if (dir != null) {
        try {
          directoryFactory.release(dir);
        } catch (IOException e) {
          SolrException.log(log, "", e);
        }
      }
    }
  }

  /**
   * Write the index.properties file with the new index sub directory name
   *
   * @param dir           a data directory (containing an index.properties file)
   * @param tmpFileName   the file name to write the new index.properties to
   * @param tmpIdxDirName new index directory name
   */
  private static void writeNewIndexProps(Directory dir, String tmpFileName, String tmpIdxDirName) {
    if (tmpFileName == null) {
      tmpFileName = IndexFetcher.INDEX_PROPERTIES;
    }
    final Properties p = new Properties();

    // Read existing properties
    try {
      final IndexInput input = dir.openInput(IndexFetcher.INDEX_PROPERTIES, DirectoryFactory.IOCONTEXT_NO_CACHE);
      final InputStream is = new PropertiesInputStream(input);
      try {
        p.load(new InputStreamReader(is, StandardCharsets.UTF_8));
      } catch (Exception e) {
        log.error("Unable to load {}", IndexFetcher.INDEX_PROPERTIES, e);
      } finally {
        IOUtils.closeQuietly(is);
      }
    } catch (IOException e) {
      // ignore; file does not exist
    }

    p.put("index", tmpIdxDirName);

    // Write new properties
    Writer os = null;
    try {
      IndexOutput out = dir.createOutput(tmpFileName, DirectoryFactory.IOCONTEXT_NO_CACHE);
      os = new OutputStreamWriter(new PropertiesOutputStream(out), StandardCharsets.UTF_8);
      p.store(os, IndexFetcher.INDEX_PROPERTIES);
      dir.sync(Collections.singleton(tmpFileName));
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to write " + IndexFetcher.INDEX_PROPERTIES, e);
    } finally {
      IOUtils.closeQuietly(os);
    }
  }

  private String initUpdateLogDir(CoreDescriptor coreDescriptor) {
    String updateLogDir = coreDescriptor.getUlogDir();
    if (updateLogDir == null) {
      updateLogDir = coreDescriptor.getInstanceDir().resolve(dataDir).normalize().toAbsolutePath().toString();
    }
    return updateLogDir;
  }

  /**
   * Close the core, if it is still in use waits until is no longer in use.
   *
   * @see #close()
   * @see #isClosed()
   */
  public void closeAndWait() {
    close();
    while (!isClosed()) {
      final long milliSleep = 100;
      log.info("Core {} is not yet closed, waiting {} ms before checking again.", getName(), milliSleep);
      try {
        Thread.sleep(milliSleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Caught InterruptedException whilst waiting for core " + getName() + " to close: "
                + e.getMessage(), e);
      }
    }
  }

  private Codec initCodec(SolrConfig solrConfig, final IndexSchema schema) {
    final PluginInfo info = solrConfig.getPluginInfo(CodecFactory.class.getName());
    final CodecFactory factory;
    if (info != null) {
      factory = resourceLoader.newInstance(info.className, CodecFactory.class);
      factory.init(info.initArgs);
    } else {
      factory = new CodecFactory() {
        @Override
        public Codec getCodec() {
          return Codec.getDefault();
        }
      };
    }
    if (factory instanceof SolrCoreAware) {
      // CodecFactory needs SolrCore before inform() is called on all registered
      // SolrCoreAware listeners, at the end of the SolrCore constructor
      ((SolrCoreAware) factory).inform(this);
    } else {
      for (FieldType ft : schema.getFieldTypes().values()) {
        if (null != ft.getPostingsFormat()) {
          String msg = "FieldType '" + ft.getTypeName() + "' is configured with a postings format, but the codec does not support it: " + factory.getClass();
          log.error(msg);
          throw new SolrException(ErrorCode.SERVER_ERROR, msg);
        }
        if (null != ft.getDocValuesFormat()) {
          String msg = "FieldType '" + ft.getTypeName() + "' is configured with a docValues format, but the codec does not support it: " + factory.getClass();
          log.error(msg);
          throw new SolrException(ErrorCode.SERVER_ERROR, msg);
        }
      }
    }
    return factory.getCodec();
  }

  /**
   * Create an instance of {@link StatsCache} using configured parameters.
   */
  public StatsCache createStatsCache() {
    final StatsCache cache;
    PluginInfo pluginInfo = solrConfig.getPluginInfo(StatsCache.class.getName());
    if (pluginInfo != null && pluginInfo.className != null && pluginInfo.className.length() > 0) {
      cache = createInitInstance(pluginInfo, StatsCache.class, null,
          LocalStatsCache.class.getName());
      log.debug("Using statsCache impl: {}", cache.getClass().getName());
    } else {
      log.debug("Using default statsCache cache: {}", LocalStatsCache.class.getName());
      cache = new LocalStatsCache();
    }
    return cache;
  }

  /**
   * Load the request processors
   */
  private Map<String, UpdateRequestProcessorChain> loadUpdateProcessorChains() {
    Map<String, UpdateRequestProcessorChain> map = new HashMap<>();
    UpdateRequestProcessorChain def = initPlugins(map, UpdateRequestProcessorChain.class, UpdateRequestProcessorChain.class.getName());
    if (def == null) {
      def = map.get(null);
    }
    if (def == null) {
      log.debug("no updateRequestProcessorChain defined as default, creating implicit default");
      // construct the default chain
      UpdateRequestProcessorFactory[] factories = new UpdateRequestProcessorFactory[]{
          new LogUpdateProcessorFactory(),
          new DistributedUpdateProcessorFactory(),
          new RunUpdateProcessorFactory()
      };
      def = new UpdateRequestProcessorChain(Arrays.asList(factories), this);
    }
    map.put(null, def);
    map.put("", def);

    map.computeIfAbsent(RunUpdateProcessorFactory.PRE_RUN_CHAIN_NAME,
        k -> new UpdateRequestProcessorChain(Collections.singletonList(new NestedUpdateProcessorFactory()), this));

    return map;
  }

  public SolrCoreState getSolrCoreState() {
    return solrCoreState;
  }

  /**
   * @return an update processor registered to the given name.  Throw an exception if this chain is undefined
   */
  public UpdateRequestProcessorChain getUpdateProcessingChain(final String name) {
    UpdateRequestProcessorChain chain = updateProcessorChains.get(name);
    if (chain == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "unknown UpdateRequestProcessorChain: " + name);
    }
    return chain;
  }

  public UpdateRequestProcessorChain getUpdateProcessorChain(SolrParams params) {
    String chainName = params.get(UpdateParams.UPDATE_CHAIN);
    UpdateRequestProcessorChain defaultUrp = getUpdateProcessingChain(chainName);
    ProcessorInfo processorInfo = new ProcessorInfo(params);
    if (processorInfo.isEmpty()) return defaultUrp;
    return UpdateRequestProcessorChain.constructChain(defaultUrp, processorInfo, this);
  }

  public PluginBag<UpdateRequestProcessorFactory> getUpdateProcessors() {
    return updateProcessors;
  }

  // this core current usage count
  private final AtomicInteger refCount = new AtomicInteger(1);

  /**
   * expert: increments the core reference count
   */
  public void open() {
    refCount.incrementAndGet();
  }

  /**
   * Close all resources allocated by the core if it is no longer in use...
   * <ul>
   * <li>searcher</li>
   * <li>updateHandler</li>
   * <li>all CloseHooks will be notified</li>
   * <li>All MBeans will be unregistered from MBeanServer if JMX was enabled
   * </li>
   * </ul>
   * <p>
   * The behavior of this method is determined by the result of decrementing
   * the core's reference count (A core is created with a reference count of 1)...
   * </p>
   * <ul>
   * <li>If reference count is &gt; 0, the usage count is decreased by 1 and no
   * resources are released.
   * </li>
   * <li>If reference count is == 0, the resources are released.
   * <li>If reference count is &lt; 0, and error is logged and no further action
   * is taken.
   * </li>
   * </ul>
   *
   * @see #isClosed()
   */
  @Override
  public void close() {
    int count = refCount.decrementAndGet();
    if (count > 0) return; // close is called often, and only actually closes if nothing is using it.
    if (count < 0) {
      log.error("Too many close [count:{}] on {}. Please report this exception to solr-user@lucene.apache.org", count, this);
      assert false : "Too many closes on SolrCore";
      return;
    }
    log.info("{} CLOSING SolrCore {}", logid, this);

    ExecutorUtil.shutdownAndAwaitTermination(coreAsyncTaskExecutor);

    // stop reporting metrics
    try {
      coreMetricManager.close();
    } catch (Throwable e) {
      SolrException.log(log, e);
      if (e instanceof Error) {
        throw (Error) e;
      }
    }

    if (closeHooks != null) {
      for (CloseHook hook : closeHooks) {
        try {
          hook.preClose(this);
        } catch (Throwable e) {
          SolrException.log(log, e);
          if (e instanceof Error) {
            throw (Error) e;
          }
        }
      }
    }

    if (reqHandlers != null) reqHandlers.close();
    responseWriters.close();
    searchComponents.close();
    qParserPlugins.close();
    valueSourceParsers.close();
    transformerFactories.close();

    if (memClassLoader != null) {
      try {
        memClassLoader.close();
      } catch (Exception e) {
      }
    }


    try {
      if (null != updateHandler) {
        updateHandler.close();
      }
    } catch (Throwable e) {
      SolrException.log(log, e);
      if (e instanceof Error) {
        throw (Error) e;
      }
    }

    boolean coreStateClosed = false;
    try {
      if (solrCoreState != null) {
        if (updateHandler instanceof IndexWriterCloser) {
          coreStateClosed = solrCoreState.decrefSolrCoreState((IndexWriterCloser) updateHandler);
        } else {
          coreStateClosed = solrCoreState.decrefSolrCoreState(null);
        }
      }
    } catch (Throwable e) {
      SolrException.log(log, e);
      if (e instanceof Error) {
        throw (Error) e;
      }
    }

    try {
      ExecutorUtil.shutdownAndAwaitTermination(searcherExecutor);
    } catch (Throwable e) {
      SolrException.log(log, e);
      if (e instanceof Error) {
        throw (Error) e;
      }
    }
    assert ObjectReleaseTracker.release(searcherExecutor);

    try {
      // Since we waited for the searcherExecutor to shut down,
      // there should be no more searchers warming in the background
      // that we need to take care of.
      //
      // For the case that a searcher was registered *before* warming
      // then the searchExecutor will throw an exception when getSearcher()
      // tries to use it, and the exception handling code should close it.
      closeSearcher();
    } catch (Throwable e) {
      SolrException.log(log, e);
      if (e instanceof Error) {
        throw (Error) e;
      }
    }

    if (coreStateClosed) {
      try {
        cleanupOldIndexDirectories(false);
      } catch (Exception e) {
        SolrException.log(log, e);
      }
    }

    try {
      infoRegistry.clear();
    } catch (Throwable e) {
      SolrException.log(log, e);
      if (e instanceof Error) {
        throw (Error) e;
      }
    }

    // Close the snapshots meta-data directory.
    Directory snapshotsDir = snapshotMgr.getSnapshotsDir();
    try {
      this.directoryFactory.release(snapshotsDir);
    } catch (Throwable e) {
      SolrException.log(log, e);
      if (e instanceof Error) {
        throw (Error) e;
      }
    }

    if (coreStateClosed) {

      try {
        directoryFactory.close();
      } catch (Throwable e) {
        SolrException.log(log, e);
        if (e instanceof Error) {
          throw (Error) e;
        }
      }
    }

    if (closeHooks != null) {
      for (CloseHook hook : closeHooks) {
        try {
          hook.postClose(this);
        } catch (Throwable e) {
          SolrException.log(log, e);
          if (e instanceof Error) {
            throw (Error) e;
          }
        }
      }
    }

    assert ObjectReleaseTracker.release(this);
  }

  /**
   * Current core usage count.
   */
  public int getOpenCount() {
    return refCount.get();
  }

  /**
   * Whether this core is closed.
   */
  public boolean isClosed() {
    return refCount.get() <= 0;
  }

  private Collection<CloseHook> closeHooks = null;

  /**
   * Add a close callback hook
   */
  public void addCloseHook(CloseHook hook) {
    if (closeHooks == null) {
      closeHooks = new ArrayList<>();
    }
    closeHooks.add(hook);
  }

  /**
   * @lucene.internal Debugging aid only.  No non-test code should be released with uncommented verbose() calls.
   */
  public static boolean VERBOSE = Boolean.parseBoolean(System.getProperty("tests.verbose", "false"));

  public static void verbose(Object... args) {
    if (!VERBOSE) return;
    StringBuilder sb = new StringBuilder("VERBOSE:");
//    sb.append(Thread.currentThread().getName());
//    sb.append(':');
    for (Object o : args) {
      sb.append(' ');
      sb.append(o == null ? "(null)" : o.toString());
    }
    // System.out.println(sb.toString());
    log.info(sb.toString());
  }


  ////////////////////////////////////////////////////////////////////////////////
  // Request Handler
  ////////////////////////////////////////////////////////////////////////////////

  /**
   * Get the request handler registered to a given name.
   * <p>
   * This function is thread safe.
   */
  public SolrRequestHandler getRequestHandler(String handlerName) {
    return RequestHandlerBase.getRequestHandler(RequestHandlers.normalize(handlerName), reqHandlers.handlers);
  }

  /**
   * Returns an unmodifiable Map containing the registered handlers
   */
  public PluginBag<SolrRequestHandler> getRequestHandlers() {
    return reqHandlers.handlers;
  }


  /**
   * Registers a handler at the specified location.  If one exists there, it will be replaced.
   * To remove a handler, register <code>null</code> at its path
   * <p>
   * Once registered the handler can be accessed through:
   * <pre>
   *   http://${host}:${port}/${context}/${handlerName}
   * or:
   *   http://${host}:${port}/${context}/select?qt=${handlerName}
   * </pre>
   * <p>
   * Handlers <em>must</em> be initialized before getting registered.  Registered
   * handlers can immediately accept requests.
   * <p>
   * This call is thread safe.
   *
   * @return the previous <code>SolrRequestHandler</code> registered to this name <code>null</code> if none.
   */
  public SolrRequestHandler registerRequestHandler(String handlerName, SolrRequestHandler handler) {
    return reqHandlers.register(handlerName, handler);
  }

  /**
   * Register the default search components
   */
  private void loadSearchComponents() {
    Map<String, SearchComponent> instances = createInstances(SearchComponent.standard_components);
    for (Map.Entry<String, SearchComponent> e : instances.entrySet()) e.getValue().setName(e.getKey());
    searchComponents.init(instances, this);

    for (String name : searchComponents.keySet()) {
      if (searchComponents.isLoaded(name) && searchComponents.get(name) instanceof HighlightComponent) {
        if (!HighlightComponent.COMPONENT_NAME.equals(name)) {
          searchComponents.put(HighlightComponent.COMPONENT_NAME, searchComponents.getRegistry().get(name));
        }
        break;
      }
    }
  }

  /**
   * @return a Search Component registered to a given name.  Throw an exception if the component is undefined
   */
  public SearchComponent getSearchComponent(String name) {
    return searchComponents.get(name);
  }

  /**
   * Accessor for all the Search Components
   *
   * @return An unmodifiable Map of Search Components
   */
  public PluginBag<SearchComponent> getSearchComponents() {
    return searchComponents;
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Update Handler
  ////////////////////////////////////////////////////////////////////////////////

  /**
   * RequestHandlers need access to the updateHandler so they can all talk to the
   * same RAM indexer.
   */
  public UpdateHandler getUpdateHandler() {
    return updateHandler;
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Searcher Control
  ////////////////////////////////////////////////////////////////////////////////

  // The current searcher used to service queries.
  // Don't access this directly!!!! use getSearcher() to
  // get it (and it will increment the ref count at the same time).
  // This reference is protected by searcherLock.
  private RefCounted<SolrIndexSearcher> _searcher;

  // All of the normal open searchers.  Don't access this directly.
  // protected by synchronizing on searcherLock.
  private final LinkedList<RefCounted<SolrIndexSearcher>> _searchers = new LinkedList<>();
  private final LinkedList<RefCounted<SolrIndexSearcher>> _realtimeSearchers = new LinkedList<>();

  final ExecutorService searcherExecutor = ExecutorUtil.newMDCAwareSingleThreadExecutor(
      new DefaultSolrThreadFactory("searcherExecutor"));
  private int onDeckSearchers;  // number of searchers preparing
  // Lock ordering: one can acquire the openSearcherLock and then the searcherLock, but not vice-versa.
  private Object searcherLock = new Object();  // the sync object for the searcher
  private ReentrantLock openSearcherLock = new ReentrantLock(true);     // used to serialize opens/reopens for absolute ordering
  private final int maxWarmingSearchers;  // max number of on-deck searchers allowed
  private final int slowQueryThresholdMillis;  // threshold above which a query is considered slow

  private RefCounted<SolrIndexSearcher> realtimeSearcher;
  private Callable<DirectoryReader> newReaderCreator;

  // For testing
  boolean areAllSearcherReferencesEmpty() {
    boolean isEmpty;
    synchronized (searcherLock) {
      isEmpty = _searchers.isEmpty();
      isEmpty = isEmpty && _realtimeSearchers.isEmpty();
      isEmpty = isEmpty && (_searcher == null);
      isEmpty = isEmpty && (realtimeSearcher == null);
    }
    return isEmpty;
  }

  /**
   * Return a registered {@link RefCounted}&lt;{@link SolrIndexSearcher}&gt; with
   * the reference count incremented.  It <b>must</b> be decremented when no longer needed.
   * This method should not be called from SolrCoreAware.inform() since it can result
   * in a deadlock if useColdSearcher==false.
   * If handling a normal request, the searcher should be obtained from
   * {@link org.apache.solr.request.SolrQueryRequest#getSearcher()} instead.
   * If you still think you need to call this, consider {@link #withSearcher(IOFunction)} instead which is easier to
   * use.
   *
   * @see SolrQueryRequest#getSearcher()
   * @see #withSearcher(IOFunction)
   */
  public RefCounted<SolrIndexSearcher> getSearcher() {
    if (searchEnabled) {
      return getSearcher(false, true, null);
    }
    throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Search is temporarily disabled");
  }

  /**
   * Executes the lambda with the {@link SolrIndexSearcher}.  This is more convenient than using
   * {@link #getSearcher()} since there is no ref-counting business to worry about.
   * Example:
   * <pre class="prettyprint">
   *   IndexReader reader = h.getCore().withSearcher(SolrIndexSearcher::getIndexReader);
   * </pre>
   * Warning: although a lambda is concise, it may be inappropriate to simply return the IndexReader because it might
   * be closed soon after this method returns; it really depends.
   */
  @SuppressWarnings("unchecked")
  public <R> R withSearcher(IOFunction<SolrIndexSearcher, R> lambda) throws IOException {
    final RefCounted<SolrIndexSearcher> refCounted = getSearcher();
    try {
      return lambda.apply(refCounted.get());
    } finally {
      refCounted.decref();
    }
  }

  /**
   * Computes fingerprint of a segment and caches it only if all the version in segment are included in the fingerprint.
   * We can't use computeIfAbsent as caching is conditional (as described above)
   * There is chance that two threads may compute fingerprint on the same segment. It might be OK to do so rather than locking entire map.
   *
   * @param searcher   searcher that includes specified LeaderReaderContext
   * @param ctx        LeafReaderContext of a segment to compute fingerprint of
   * @param maxVersion maximum version number to consider for fingerprint computation
   * @return IndexFingerprint of the segment
   * @throws IOException Can throw IOException
   */
  public IndexFingerprint getIndexFingerprint(SolrIndexSearcher searcher, LeafReaderContext ctx, long maxVersion)
      throws IOException {
    IndexReader.CacheHelper cacheHelper = ctx.reader().getReaderCacheHelper();
    if (cacheHelper == null) {
      log.debug("Cannot cache IndexFingerprint as reader does not support caching. searcher:{} reader:{} readerHash:{} maxVersion:{}", searcher, ctx.reader(), ctx.reader().hashCode(), maxVersion);
      return IndexFingerprint.getFingerprint(searcher, ctx, maxVersion);
    }

    IndexFingerprint f = null;
    f = perSegmentFingerprintCache.get(cacheHelper.getKey());
    // fingerprint is either not cached or
    // if we want fingerprint only up to a version less than maxVersionEncountered in the segment, or
    // documents were deleted from segment for which fingerprint was cached
    //
    if (f == null || (f.getMaxInHash() > maxVersion) || (f.getNumDocs() != ctx.reader().numDocs())) {
      log.debug("IndexFingerprint cache miss for searcher:{} reader:{} readerHash:{} maxVersion:{}", searcher, ctx.reader(), ctx.reader().hashCode(), maxVersion);
      f = IndexFingerprint.getFingerprint(searcher, ctx, maxVersion);
      // cache fingerprint for the segment only if all the versions in the segment are included in the fingerprint
      if (f.getMaxVersionEncountered() == f.getMaxInHash()) {
        log.debug("Caching fingerprint for searcher:{} leafReaderContext:{} mavVersion:{}", searcher, ctx, maxVersion);
        perSegmentFingerprintCache.put(cacheHelper.getKey(), f);
      }

    } else {
      log.debug("IndexFingerprint cache hit for searcher:{} reader:{} readerHash:{} maxVersion:{}", searcher, ctx.reader(), ctx.reader().hashCode(), maxVersion);
    }
    log.debug("Cache Size: {}, Segments Size:{}", perSegmentFingerprintCache.size(), searcher.getTopReaderContext().leaves().size());
    return f;
  }

  /**
   * Returns the current registered searcher with its reference count incremented, or null if none are registered.
   */
  public RefCounted<SolrIndexSearcher> getRegisteredSearcher() {
    synchronized (searcherLock) {
      if (_searcher != null) {
        _searcher.incref();
      }
      return _searcher;
    }
  }

  /**
   * Return the newest normal {@link RefCounted}&lt;{@link SolrIndexSearcher}&gt; with
   * the reference count incremented.  It <b>must</b> be decremented when no longer needed.
   * If no searcher is currently open, then if openNew==true a new searcher will be opened,
   * or null is returned if openNew==false.
   */
  public RefCounted<SolrIndexSearcher> getNewestSearcher(boolean openNew) {
    synchronized (searcherLock) {
      if (!_searchers.isEmpty()) {
        RefCounted<SolrIndexSearcher> newest = _searchers.getLast();
        newest.incref();
        return newest;
      }
    }

    return openNew ? getRealtimeSearcher() : null;
  }

  /**
   * Gets the latest real-time searcher w/o forcing open a new searcher if one already exists.
   * The reference count will be incremented.
   */
  public RefCounted<SolrIndexSearcher> getRealtimeSearcher() {
    synchronized (searcherLock) {
      if (realtimeSearcher != null) {
        realtimeSearcher.incref();
        return realtimeSearcher;
      }
    }

    // use the searcher lock to prevent multiple people from trying to open at once
    openSearcherLock.lock();
    try {

      // try again
      synchronized (searcherLock) {
        if (realtimeSearcher != null) {
          realtimeSearcher.incref();
          return realtimeSearcher;
        }
      }

      // force a new searcher open
      return openNewSearcher(true, true);
    } finally {
      openSearcherLock.unlock();
    }
  }


  public RefCounted<SolrIndexSearcher> getSearcher(boolean forceNew, boolean returnSearcher, final Future[] waitSearcher) {
    return getSearcher(forceNew, returnSearcher, waitSearcher, false);
  }


  /**
   * Opens a new searcher and returns a RefCounted&lt;SolrIndexSearcher&gt; with its reference incremented.
   * <p>
   * "realtime" means that we need to open quickly for a realtime view of the index, hence don't do any
   * autowarming and add to the _realtimeSearchers queue rather than the _searchers queue (so it won't
   * be used for autowarming by a future normal searcher).  A "realtime" searcher will currently never
   * become "registered" (since it currently lacks caching).
   * <p>
   * realtimeSearcher is updated to the latest opened searcher, regardless of the value of "realtime".
   * <p>
   * This method acquires openSearcherLock - do not call with searchLock held!
   */
  public RefCounted<SolrIndexSearcher> openNewSearcher(boolean updateHandlerReopens, boolean realtime) {
    if (isClosed()) { // catch some errors quicker
      throw new SolrCoreState.CoreIsClosedException();
    }

    SolrIndexSearcher tmp;
    RefCounted<SolrIndexSearcher> newestSearcher = null;

    openSearcherLock.lock();
    try {
      String newIndexDir = getNewIndexDir();
      String indexDirFile = null;
      String newIndexDirFile = null;

      // if it's not a normal near-realtime update, check that paths haven't changed.
      if (!updateHandlerReopens) {
        indexDirFile = getDirectoryFactory().normalize(getIndexDir());
        newIndexDirFile = getDirectoryFactory().normalize(newIndexDir);
      }

      synchronized (searcherLock) {
        newestSearcher = realtimeSearcher;
        if (newestSearcher != null) {
          newestSearcher.incref();      // the matching decref is in the finally block
        }
      }

      if (newestSearcher != null && (updateHandlerReopens || indexDirFile.equals(newIndexDirFile))) {

        DirectoryReader newReader;
        DirectoryReader currentReader = newestSearcher.get().getRawReader();

        // SolrCore.verbose("start reopen from",previousSearcher,"writer=",writer);

        RefCounted<IndexWriter> writer = getSolrCoreState().getIndexWriter(null);

        try {
          if (writer != null) {
            // if in NRT mode, open from the writer
            newReader = DirectoryReader.openIfChanged(currentReader, writer.get(), true);
          } else {
            // verbose("start reopen without writer, reader=", currentReader);
            newReader = DirectoryReader.openIfChanged(currentReader);
            // verbose("reopen result", newReader);
          }
        } finally {
          if (writer != null) {
            writer.decref();
          }
        }

        if (newReader == null) { // the underlying index has not changed at all

          if (realtime) {
            // if this is a request for a realtime searcher, just return the same searcher
            newestSearcher.incref();
            return newestSearcher;

          } else if (newestSearcher.get().isCachingEnabled() && newestSearcher.get().getSchema() == getLatestSchema()) {
            // absolutely nothing has changed, can use the same searcher
            // but log a message about it to minimize confusion

            newestSearcher.incref();
            log.debug("SolrIndexSearcher has not changed - not re-opening: {}", newestSearcher.get().getName());
            return newestSearcher;

          } // ELSE: open a new searcher against the old reader...
          currentReader.incRef();
          newReader = currentReader;
        }

        // for now, turn off caches if this is for a realtime reader 
        // (caches take a little while to instantiate)
        final boolean useCaches = !realtime;
        final String newName = realtime ? "realtime" : "main";
        tmp = new SolrIndexSearcher(this, newIndexDir, getLatestSchema(), newName,
            newReader, true, useCaches, true, directoryFactory);

      } else {
        // newestSearcher == null at this point

        if (newReaderCreator != null) {
          // this is set in the constructor if there is a currently open index writer
          // so that we pick up any uncommitted changes and so we don't go backwards
          // in time on a core reload
          DirectoryReader newReader = newReaderCreator.call();
          tmp = new SolrIndexSearcher(this, newIndexDir, getLatestSchema(),
              (realtime ? "realtime" : "main"), newReader, true, !realtime, true, directoryFactory);
        } else {
          RefCounted<IndexWriter> writer = getSolrCoreState().getIndexWriter(this);
          DirectoryReader newReader = null;
          try {
            newReader = indexReaderFactory.newReader(writer.get(), this);
          } finally {
            writer.decref();
          }
          tmp = new SolrIndexSearcher(this, newIndexDir, getLatestSchema(),
              (realtime ? "realtime" : "main"), newReader, true, !realtime, true, directoryFactory);
        }
      }

      List<RefCounted<SolrIndexSearcher>> searcherList = realtime ? _realtimeSearchers : _searchers;
      RefCounted<SolrIndexSearcher> newSearcher = newHolder(tmp, searcherList);    // refcount now at 1

      // Increment reference again for "realtimeSearcher" variable.  It should be at 2 after.
      // When it's decremented by both the caller of this method, and by realtimeSearcher being replaced,
      // it will be closed.
      newSearcher.incref();

      synchronized (searcherLock) {
        // Check if the core is closed again inside the lock in case this method is racing with a close. If the core is
        // closed, clean up the new searcher and bail.
        if (isClosed()) {
          newSearcher.decref(); // once for caller since we're not returning it
          newSearcher.decref(); // once for ourselves since it won't be "replaced"
          throw new SolrException(ErrorCode.SERVER_ERROR, "openNewSearcher called on closed core");
        }

        if (realtimeSearcher != null) {
          realtimeSearcher.decref();
        }
        realtimeSearcher = newSearcher;
        searcherList.add(realtimeSearcher);
      }

      return newSearcher;

    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error opening new searcher", e);
    } finally {
      openSearcherLock.unlock();
      if (newestSearcher != null) {
        newestSearcher.decref();
      }
    }
  }

  /**
   * Get a {@link SolrIndexSearcher} or start the process of creating a new one.
   * <p>
   * The registered searcher is the default searcher used to service queries.
   * A searcher will normally be registered after all of the warming
   * and event handlers (newSearcher or firstSearcher events) have run.
   * In the case where there is no registered searcher, the newly created searcher will
   * be registered before running the event handlers (a slow searcher is better than no searcher).
   *
   * <p>
   * These searchers contain read-only IndexReaders. To access a non read-only IndexReader,
   * see newSearcher(String name, boolean readOnly).
   *
   * <p>
   * If <tt>forceNew==true</tt> then
   * A new searcher will be opened and registered regardless of whether there is already
   * a registered searcher or other searchers in the process of being created.
   * <p>
   * If <tt>forceNew==false</tt> then:<ul>
   * <li>If a searcher is already registered, that searcher will be returned</li>
   * <li>If no searcher is currently registered, but at least one is in the process of being created, then
   * this call will block until the first searcher is registered</li>
   * <li>If no searcher is currently registered, and no searchers in the process of being registered, a new
   * searcher will be created.</li>
   * </ul>
   * <p>
   * If <tt>returnSearcher==true</tt> then a {@link RefCounted}&lt;{@link SolrIndexSearcher}&gt; will be returned with
   * the reference count incremented.  It <b>must</b> be decremented when no longer needed.
   * <p>
   * If <tt>waitSearcher!=null</tt> and a new {@link SolrIndexSearcher} was created,
   * then it is filled in with a Future that will return after the searcher is registered.  The Future may be set to
   * <tt>null</tt> in which case the SolrIndexSearcher created has already been registered at the time
   * this method returned.
   * <p>
   *
   * @param forceNew             if true, force the open of a new index searcher regardless if there is already one open.
   * @param returnSearcher       if true, returns a {@link SolrIndexSearcher} holder with the refcount already incremented.
   * @param waitSearcher         if non-null, will be filled in with a {@link Future} that will return after the new searcher is registered.
   * @param updateHandlerReopens if true, the UpdateHandler will be used when reopening a {@link SolrIndexSearcher}.
   */
  public RefCounted<SolrIndexSearcher> getSearcher(boolean forceNew, boolean returnSearcher, final Future[] waitSearcher, boolean updateHandlerReopens) {
    // it may take some time to open an index.... we may need to make
    // sure that two threads aren't trying to open one at the same time
    // if it isn't necessary.

    synchronized (searcherLock) {
      for (; ; ) { // this loop is so w can retry in the event that we exceed maxWarmingSearchers
        // see if we can return the current searcher
        if (_searcher != null && !forceNew) {
          if (returnSearcher) {
            _searcher.incref();
            return _searcher;
          } else {
            return null;
          }
        }

        // check to see if we can wait for someone else's searcher to be set
        if (onDeckSearchers > 0 && !forceNew && _searcher == null) {
          try {
            searcherLock.wait();
          } catch (InterruptedException e) {
            log.info(SolrException.toStr(e));
          }
        }

        // check again: see if we can return right now
        if (_searcher != null && !forceNew) {
          if (returnSearcher) {
            _searcher.incref();
            return _searcher;
          } else {
            return null;
          }
        }

        // At this point, we know we need to open a new searcher...
        // first: increment count to signal other threads that we are
        //        opening a new searcher.
        onDeckSearchers++;
        newSearcherCounter.inc();
        if (onDeckSearchers < 1) {
          // should never happen... just a sanity check
          log.error("{}ERROR!!! onDeckSearchers is {}", logid, onDeckSearchers);
          onDeckSearchers = 1;  // reset
        } else if (onDeckSearchers > maxWarmingSearchers) {
          onDeckSearchers--;
          newSearcherMaxReachedCounter.inc();
          try {
            searcherLock.wait();
          } catch (InterruptedException e) {
            log.info(SolrException.toStr(e));
          }
          continue;  // go back to the top of the loop and retry
        } else if (onDeckSearchers > 1) {
          log.warn("{}PERFORMANCE WARNING: Overlapping onDeckSearchers={}", logid, onDeckSearchers);
        }

        break; // I can now exit the loop and proceed to open a searcher
      }
    }

    // a signal to decrement onDeckSearchers if something goes wrong.
    final boolean[] decrementOnDeckCount = new boolean[]{true};
    RefCounted<SolrIndexSearcher> currSearcherHolder = null;     // searcher we are autowarming from
    RefCounted<SolrIndexSearcher> searchHolder = null;
    boolean success = false;

    openSearcherLock.lock();
    Timer.Context timerContext = newSearcherTimer.time();
    try {
      searchHolder = openNewSearcher(updateHandlerReopens, false);
      // the searchHolder will be incremented once already (and it will eventually be assigned to _searcher when registered)
      // increment it again if we are going to return it to the caller.
      if (returnSearcher) {
        searchHolder.incref();
      }


      final RefCounted<SolrIndexSearcher> newSearchHolder = searchHolder;
      final SolrIndexSearcher newSearcher = newSearchHolder.get();


      boolean alreadyRegistered = false;
      synchronized (searcherLock) {
        if (_searcher == null) {
          // if there isn't a current searcher then we may
          // want to register this one before warming is complete instead of waiting.
          if (solrConfig.useColdSearcher) {
            registerSearcher(newSearchHolder);
            decrementOnDeckCount[0] = false;
            alreadyRegistered = true;
          }
        } else {
          // get a reference to the current searcher for purposes of autowarming.
          currSearcherHolder = _searcher;
          currSearcherHolder.incref();
        }
      }


      final SolrIndexSearcher currSearcher = currSearcherHolder == null ? null : currSearcherHolder.get();

      Future future = null;

      // if the underlying searcher has not changed, no warming is needed
      if (newSearcher != currSearcher) {

        // warm the new searcher based on the current searcher.
        // should this go before the other event handlers or after?
        if (currSearcher != null) {
          future = searcherExecutor.submit(() -> {
            Timer.Context warmupContext = newSearcherWarmupTimer.time();
            try {
              newSearcher.warm(currSearcher);
            } catch (Throwable e) {
              SolrException.log(log, e);
              if (e instanceof Error) {
                throw (Error) e;
              }
            } finally {
              warmupContext.close();
            }
            return null;
          });
        }

        if (currSearcher == null) {
          future = searcherExecutor.submit(() -> {
            try {
              for (SolrEventListener listener : firstSearcherListeners) {
                listener.newSearcher(newSearcher, null);
              }
            } catch (Throwable e) {
              SolrException.log(log, null, e);
              if (e instanceof Error) {
                throw (Error) e;
              }
            }
            return null;
          });
        }

        if (currSearcher != null) {
          future = searcherExecutor.submit(() -> {
            try {
              for (SolrEventListener listener : newSearcherListeners) {
                listener.newSearcher(newSearcher, currSearcher);
              }
            } catch (Throwable e) {
              SolrException.log(log, null, e);
              if (e instanceof Error) {
                throw (Error) e;
              }
            }
            return null;
          });
        }

      }


      // WARNING: this code assumes a single threaded executor (that all tasks
      // queued will finish first).
      final RefCounted<SolrIndexSearcher> currSearcherHolderF = currSearcherHolder;
      if (!alreadyRegistered) {
        future = searcherExecutor.submit(
            () -> {
              try {
                // registerSearcher will decrement onDeckSearchers and
                // do a notify, even if it fails.
                registerSearcher(newSearchHolder);
              } catch (Throwable e) {
                SolrException.log(log, e);
                if (e instanceof Error) {
                  throw (Error) e;
                }
              } finally {
                // we are all done with the old searcher we used
                // for warming...
                if (currSearcherHolderF != null) currSearcherHolderF.decref();
              }
              return null;
            }
        );
      }

      if (waitSearcher != null) {
        waitSearcher[0] = future;
      }

      success = true;

      // Return the searcher as the warming tasks run in parallel
      // callers may wait on the waitSearcher future returned.
      return returnSearcher ? newSearchHolder : null;

    } catch (Exception e) {
      if (e instanceof RuntimeException) throw (RuntimeException) e;
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } finally {

      timerContext.close();

      if (!success) {
        newSearcherOtherErrorsCounter.inc();
        ;
        synchronized (searcherLock) {
          onDeckSearchers--;

          if (onDeckSearchers < 0) {
            // sanity check... should never happen
            log.error("{}ERROR!!! onDeckSearchers after decrement={}", logid, onDeckSearchers);
            onDeckSearchers = 0; // try and recover
          }
          // if we failed, we need to wake up at least one waiter to continue the process
          searcherLock.notify();
        }

        if (currSearcherHolder != null) {
          currSearcherHolder.decref();
        }

        if (searchHolder != null) {
          searchHolder.decref();      // decrement 1 for _searcher (searchHolder will never become _searcher now)
          if (returnSearcher) {
            searchHolder.decref();    // decrement 1 because we won't be returning the searcher to the user
          }
        }
      }

      // we want to do this after we decrement onDeckSearchers so another thread
      // doesn't increment first and throw a false warning.
      openSearcherLock.unlock();

    }

  }


  private RefCounted<SolrIndexSearcher> newHolder(SolrIndexSearcher newSearcher, final List<RefCounted<SolrIndexSearcher>> searcherList) {
    RefCounted<SolrIndexSearcher> holder = new RefCounted<SolrIndexSearcher>(newSearcher) {
      @Override
      public void close() {
        try {
          synchronized (searcherLock) {
            // it's possible for someone to get a reference via the _searchers queue
            // and increment the refcount while RefCounted.close() is being called.
            // we check the refcount again to see if this has happened and abort the close.
            // This relies on the RefCounted class allowing close() to be called every
            // time the counter hits zero.
            if (refcount.get() > 0) return;
            searcherList.remove(this);
          }
          resource.close();
        } catch (Exception e) {
          // do not allow decref() operations to fail since they are typically called in finally blocks
          // and throwing another exception would be very unexpected.
          SolrException.log(log, "Error closing searcher:" + this, e);
        }
      }
    };
    holder.incref();  // set ref count to 1 to account for this._searcher
    return holder;
  }

  public boolean isReloaded() {
    return isReloaded;
  }

  // Take control of newSearcherHolder (which should have a reference count of at
  // least 1 already.  If the caller wishes to use the newSearcherHolder directly
  // after registering it, then they should increment the reference count *before*
  // calling this method.
  //
  // onDeckSearchers will also be decremented (it should have been incremented
  // as a result of opening a new searcher).
  private void registerSearcher(RefCounted<SolrIndexSearcher> newSearcherHolder) {
    synchronized (searcherLock) {
      try {
        if (_searcher == newSearcherHolder) {
          // trying to re-register the same searcher... this can now happen when a commit has been done but
          // there were no changes to the index.
          newSearcherHolder.decref();  // decref since the caller should have still incref'd (since they didn't know the searcher was the same)
          return;  // still execute the finally block to notify anyone waiting.
        }

        if (_searcher != null) {
          _searcher.decref();   // dec refcount for this._searcher
          _searcher = null;
        }

        _searcher = newSearcherHolder;
        SolrIndexSearcher newSearcher = newSearcherHolder.get();

        /***
         // a searcher may have been warming asynchronously while the core was being closed.
         // if this happens, just close the searcher.
         if (isClosed()) {
         // NOTE: this should not happen now - see close() for details.
         // *BUT* if we left it enabled, this could still happen before
         // close() stopped the executor - so disable this test for now.
         log.error("Ignoring searcher register on closed core:" + newSearcher);
         _searcher.decref();
         }
         ***/

        newSearcher.register(); // register subitems (caches)
        log.info("{}Registered new searcher {}", logid, newSearcher);

      } catch (Exception e) {
        // an exception in register() shouldn't be fatal.
        log(e);
      } finally {
        // wake up anyone waiting for a searcher
        // even in the face of errors.
        onDeckSearchers--;
        searcherLock.notifyAll();
        assert TestInjection.injectSearcherHooks(getCoreDescriptor() != null && getCoreDescriptor().getCloudDescriptor() != null ? getCoreDescriptor().getCloudDescriptor().getCollectionName() : null);
      }
    }
  }


  public void closeSearcher() {
    log.debug("{}Closing main searcher on request.", logid);
    synchronized (searcherLock) {
      if (realtimeSearcher != null) {
        realtimeSearcher.decref();
        realtimeSearcher = null;
      }
      if (_searcher != null) {
        _searcher.decref();   // dec refcount for this._searcher
        _searcher = null; // isClosed() does check this
      }
    }
  }

  public void execute(SolrRequestHandler handler, SolrQueryRequest req, SolrQueryResponse rsp) {
    if (handler == null) {
      String msg = "Null Request Handler '" +
          req.getParams().get(CommonParams.QT) + "'";

      log.warn("{}{}:{}", logid, msg, req);

      throw new SolrException(ErrorCode.BAD_REQUEST, msg);
    }

    preDecorateResponse(req, rsp);

    /*
     * Keeping this usage of isDebugEnabled because the extraction of the log data as a string might be slow. TODO:
     * Determine how likely it is that something is going to go wrong that will prevent the logging at INFO further
     * down, and if possible, prevent that situation. The handleRequest and postDecorateResponse methods do not indicate
     * that they throw any checked exceptions, so it would have to be an unchecked exception that causes any problems.
     */
    if (requestLog.isDebugEnabled() && rsp.getToLog().size() > 0) {
      // log request at debug in case something goes wrong and we aren't able to log later
      requestLog.debug(rsp.getToLogAsString(logid));
    }

    // TODO: this doesn't seem to be working correctly and causes problems with the example server and distrib (for example /spell)
    // if (req.getParams().getBool(ShardParams.IS_SHARD,false) && !(handler instanceof SearchHandler))
    //   throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,"isShard is only acceptable with search handlers");

    handler.handleRequest(req, rsp);
    postDecorateResponse(handler, req, rsp);

    if (rsp.getToLog().size() > 0) {
      if (requestLog.isInfoEnabled()) {
        requestLog.info(rsp.getToLogAsString(logid));
      }

      /* slowQueryThresholdMillis defaults to -1 in SolrConfig -- not enabled.*/
      if (log.isWarnEnabled() && slowQueryThresholdMillis >= 0) {
        final long qtime = (long) (req.getRequestTimer().getTime());
        if (qtime >= slowQueryThresholdMillis) {
          slowLog.warn("slow: {}", rsp.getToLogAsString(logid));
        }
      }
    }
  }

  public static void preDecorateResponse(SolrQueryRequest req, SolrQueryResponse rsp) {
    // setup response header
    final NamedList<Object> responseHeader = new SimpleOrderedMap<>();
    rsp.addResponseHeader(responseHeader);

    // toLog is a local ref to the same NamedList used by the response
    NamedList<Object> toLog = rsp.getToLog();

    // for back compat, we set these now just in case other code
    // are expecting them during handleRequest
    toLog.add("webapp", req.getContext().get("webapp"));
    toLog.add(PATH, req.getContext().get(PATH));

    final SolrParams params = req.getParams();
    final String lpList = params.get(CommonParams.LOG_PARAMS_LIST);
    if (lpList == null) {
      toLog.add("params", "{" + req.getParamString() + "}");
    } else if (lpList.length() > 0) {

      // Filter params by those in LOG_PARAMS_LIST so that we can then call toString
      HashSet<String> lpSet = new HashSet<>(Arrays.asList(lpList.split(",")));
      SolrParams filteredParams = new SolrParams() {
        private static final long serialVersionUID = -643991638344314066L;

        @Override
        public Iterator<String> getParameterNamesIterator() {
          return Iterators.filter(params.getParameterNamesIterator(), lpSet::contains);
        }

        @Override
        public String get(String param) { // assume param is in lpSet
          return params.get(param);
        } //assume in lpSet

        @Override
        public String[] getParams(String param) { // assume param is in lpSet
          return params.getParams(param);
        } // assume in lpSet
      };

      toLog.add("params", "{" + filteredParams + "}");
    }
  }

  /**
   * Put status, QTime, and possibly request handler and params, in the response header
   */
  public static void postDecorateResponse
  (SolrRequestHandler handler, SolrQueryRequest req, SolrQueryResponse rsp) {
    // TODO should check that responseHeader has not been replaced by handler
    NamedList<Object> responseHeader = rsp.getResponseHeader();
    final int qtime = (int) (req.getRequestTimer().getTime());
    int status = 0;
    Exception exception = rsp.getException();
    if (exception != null) {
      if (exception instanceof SolrException)
        status = ((SolrException) exception).code();
      else
        status = 500;
    }
    responseHeader.add("status", status);
    responseHeader.add("QTime", qtime);

    if (rsp.getToLog().size() > 0) {
      rsp.getToLog().add("status", status);
      rsp.getToLog().add("QTime", qtime);
    }

    SolrParams params = req.getParams();
    if (null != handler && params.getBool(CommonParams.HEADER_ECHO_HANDLER, false)) {
      responseHeader.add("handler", handler.getName());
    }

    // Values for echoParams... false/true/all or false/explicit/all ???
    String ep = params.get(CommonParams.HEADER_ECHO_PARAMS, null);
    if (ep != null) {
      EchoParamStyle echoParams = EchoParamStyle.get(ep);
      if (echoParams == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid value '" + ep + "' for " + CommonParams.HEADER_ECHO_PARAMS
            + " parameter, use '" + EchoParamStyle.EXPLICIT + "' or '" + EchoParamStyle.ALL + "'");
      }
      if (echoParams == EchoParamStyle.EXPLICIT) {
        responseHeader.add("params", req.getOriginalParams().toNamedList());
      } else if (echoParams == EchoParamStyle.ALL) {
        responseHeader.add("params", req.getParams().toNamedList());
      }
    }
  }

  final public static void log(Throwable e) {
    SolrException.log(log, null, e);
  }

  public PluginBag<QueryResponseWriter> getResponseWriters() {
    return responseWriters;
  }

  private final PluginBag<QueryResponseWriter> responseWriters = new PluginBag<>(QueryResponseWriter.class, this);
  public static final Map<String, QueryResponseWriter> DEFAULT_RESPONSE_WRITERS;

  static {
    HashMap<String, QueryResponseWriter> m = new HashMap<>(15, 1);
    m.put("xml", new XMLResponseWriter());
    m.put(CommonParams.JSON, new JSONResponseWriter());
    m.put("standard", m.get(CommonParams.JSON));
    m.put("geojson", new GeoJSONResponseWriter());
    m.put("graphml", new GraphMLResponseWriter());
    m.put("python", new PythonResponseWriter());
    m.put("php", new PHPResponseWriter());
    m.put("phps", new PHPSerializedResponseWriter());
    m.put("ruby", new RubyResponseWriter());
    m.put("raw", new RawResponseWriter());
    m.put(CommonParams.JAVABIN, new BinaryResponseWriter());
    m.put("csv", new CSVResponseWriter());
    m.put("schema.xml", new SchemaXmlResponseWriter());
    m.put("smile", new SmileResponseWriter());
    m.put(ReplicationHandler.FILE_STREAM, getFileStreamWriter());
    DEFAULT_RESPONSE_WRITERS = Collections.unmodifiableMap(m);
    try {
      m.put("xlsx",
          (QueryResponseWriter) Class.forName("org.apache.solr.handler.extraction.XLSXResponseWriter").getConstructor().newInstance());
    } catch (Exception e) {
      //don't worry; solrcell contrib not in class path
    }
  }

  private static BinaryResponseWriter getFileStreamWriter() {
    return new BinaryResponseWriter() {
      @Override
      public void write(OutputStream out, SolrQueryRequest req, SolrQueryResponse response) throws IOException {
        RawWriter rawWriter = (RawWriter) response.getValues().get(ReplicationHandler.FILE_STREAM);
        if (rawWriter != null) {
          rawWriter.write(out);
          if (rawWriter instanceof Closeable) ((Closeable) rawWriter).close();
        }

      }

      @Override
      public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
        RawWriter rawWriter = (RawWriter) response.getValues().get(ReplicationHandler.FILE_STREAM);
        if (rawWriter != null) {
          return rawWriter.getContentType();
        } else {
          return BinaryResponseParser.BINARY_CONTENT_TYPE;
        }
      }
    };
  }

  public MemClassLoader getMemClassLoader() {
    return memClassLoader;
  }

  public interface RawWriter {
    default String getContentType() {
      return BinaryResponseParser.BINARY_CONTENT_TYPE;
    }

    void write(OutputStream os) throws IOException;
  }

  /**
   * Configure the query response writers. There will always be a default writer; additional
   * writers may also be configured.
   */
  private void initWriters() {
    responseWriters.init(DEFAULT_RESPONSE_WRITERS, this);
    // configure the default response writer; this one should never be null
    if (responseWriters.getDefault() == null) responseWriters.setDefault("standard");
  }


  /**
   * Finds a writer by name, or returns the default writer if not found.
   */
  public final QueryResponseWriter getQueryResponseWriter(String writerName) {
    return responseWriters.get(writerName, true);
  }

  /**
   * Returns the appropriate writer for a request. If the request specifies a writer via the
   * 'wt' parameter, attempts to find that one; otherwise return the default writer.
   */
  public final QueryResponseWriter getQueryResponseWriter(SolrQueryRequest request) {
    return getQueryResponseWriter(request.getParams().get(CommonParams.WT));
  }


  private final PluginBag<QParserPlugin> qParserPlugins = new PluginBag<>(QParserPlugin.class, this);

  public QParserPlugin getQueryPlugin(String parserName) {
    return qParserPlugins.get(parserName);
  }

  private final PluginBag<ValueSourceParser> valueSourceParsers = new PluginBag<>(ValueSourceParser.class, this);

  private final PluginBag<TransformerFactory> transformerFactories = new PluginBag<>(TransformerFactory.class, this);

  <T> Map<String, T> createInstances(Map<String, Class<? extends T>> map) {
    Map<String, T> result = new LinkedHashMap<>(map.size(), 1);
    for (Map.Entry<String, Class<? extends T>> e : map.entrySet()) {
      try {
        Object o = getResourceLoader().newInstance(e.getValue().getName(), e.getValue());
        result.put(e.getKey(), (T) o);
      } catch (Exception exp) {
        //should never happen
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to instantiate class", exp);
      }
    }
    return result;
  }

  public TransformerFactory getTransformerFactory(String name) {
    return transformerFactories.get(name);
  }

  public void addTransformerFactory(String name, TransformerFactory factory) {
    transformerFactories.put(name, factory);
  }


  /**
   * @param registry     The map to which the instance should be added to. The key is the name attribute
   * @param type         the class or interface that the instance should extend or implement.
   * @param defClassName If PluginInfo does not have a classname, use this as the classname
   * @return The default instance . The one with (default=true)
   */
  private <T> T initPlugins(Map<String, T> registry, Class<T> type, String defClassName) {
    return initPlugins(solrConfig.getPluginInfos(type.getName()), registry, type, defClassName);
  }

  public <T> T initPlugins(List<PluginInfo> pluginInfos, Map<String, T> registry, Class<T> type, String defClassName) {
    T def = null;
    for (PluginInfo info : pluginInfos) {
      T o = createInitInstance(info, type, type.getSimpleName(), defClassName);
      registry.put(info.name, o);
      if (o instanceof SolrMetricProducer) {
        coreMetricManager.registerMetricProducer(type.getSimpleName() + "." + info.name, (SolrMetricProducer) o);
      }
      if (info.isDefault()) {
        def = o;
      }
    }
    return def;
  }

  public void initDefaultPlugin(Object plugin, Class type) {
    if (plugin instanceof SolrMetricProducer) {
      coreMetricManager.registerMetricProducer(type.getSimpleName() + ".default", (SolrMetricProducer) plugin);
    }
  }

  /**
   * For a given List of PluginInfo return the instances as a List
   *
   * @param defClassName The default classname if PluginInfo#className == null
   * @return The instances initialized
   */
  public <T> List<T> initPlugins(List<PluginInfo> pluginInfos, Class<T> type, String defClassName) {
    if (pluginInfos.isEmpty()) return Collections.emptyList();
    List<T> result = new ArrayList<>(pluginInfos.size());
    for (PluginInfo info : pluginInfos) result.add(createInitInstance(info, type, type.getSimpleName(), defClassName));
    return result;
  }

  /**
   * @param registry The map to which the instance should be added to. The key is the name attribute
   * @param type     The type of the Plugin. These should be standard ones registered by type.getName() in SolrConfig
   * @return The default if any
   */
  public <T> T initPlugins(Map<String, T> registry, Class<T> type) {
    return initPlugins(registry, type, null);
  }

  public ValueSourceParser getValueSourceParser(String parserName) {
    return valueSourceParsers.get(parserName);
  }

  /**
   * Creates and initializes a RestManager based on configuration args in solrconfig.xml.
   * RestManager provides basic storage support for managed resource data, such as to
   * persist stopwords to ZooKeeper if running in SolrCloud mode.
   */
  @SuppressWarnings("unchecked")
  protected RestManager initRestManager() throws SolrException {

    PluginInfo restManagerPluginInfo =
        getSolrConfig().getPluginInfo(RestManager.class.getName());

    NamedList<String> initArgs = null;
    RestManager mgr = null;
    if (restManagerPluginInfo != null) {
      if (restManagerPluginInfo.className != null) {
        mgr = resourceLoader.newInstance(restManagerPluginInfo.className, RestManager.class);
      }

      if (restManagerPluginInfo.initArgs != null) {
        initArgs = (NamedList<String>) restManagerPluginInfo.initArgs;
      }
    }

    if (mgr == null)
      mgr = new RestManager();

    if (initArgs == null)
      initArgs = new NamedList<>();

    String collection = getCoreDescriptor().getCollectionName();
    StorageIO storageIO =
        ManagedResourceStorage.newStorageIO(collection, resourceLoader, initArgs);
    mgr.init(resourceLoader, initArgs, storageIO);

    return mgr;
  }

  public CoreDescriptor getCoreDescriptor() {
    return coreContainer.getCoreDescriptor(name);
  }

  public IndexDeletionPolicyWrapper getDeletionPolicy() {
    return solrDelPolicy;
  }

  /**
   * @return A reference of {@linkplain SolrSnapshotMetaDataManager}
   * managing the persistent snapshots for this Solr core.
   */
  public SolrSnapshotMetaDataManager getSnapshotMetaDataManager() {
    return snapshotMgr;
  }

  public ReentrantLock getRuleExpiryLock() {
    return ruleExpiryLock;
  }

  /////////////////////////////////////////////////////////////////////
  // SolrInfoBean stuff: Statistics and Module Info
  /////////////////////////////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "SolrCore";
  }

  @Override
  public Category getCategory() {
    return Category.CORE;
  }

  public Codec getCodec() {
    return codec;
  }

  public void unloadOnClose(final CoreDescriptor desc, boolean deleteIndexDir, boolean deleteDataDir, boolean deleteInstanceDir) {
    if (deleteIndexDir) {
      try {
        directoryFactory.remove(getIndexDir());
      } catch (Exception e) {
        SolrException.log(log, "Failed to flag index dir for removal for core:" + name + " dir:" + getIndexDir());
      }
    }
    if (deleteDataDir) {
      try {
        directoryFactory.remove(getDataDir(), true);
      } catch (Exception e) {
        SolrException.log(log, "Failed to flag data dir for removal for core:" + name + " dir:" + getDataDir());
      }
    }
    if (deleteInstanceDir) {
      addCloseHook(new CloseHook() {
        @Override
        public void preClose(SolrCore core) {
          // empty block
        }

        @Override
        public void postClose(SolrCore core) {
          if (desc != null) {
            try {
              FileUtils.deleteDirectory(desc.getInstanceDir().toFile());
            } catch (IOException e) {
              SolrException.log(log, "Failed to delete instance dir for core:"
                  + core.getName() + " dir:" + desc.getInstanceDir());
            }
          }
        }
      });
    }
  }

  public static void deleteUnloadedCore(CoreDescriptor cd, boolean deleteDataDir, boolean deleteInstanceDir) {
    if (deleteDataDir) {
      File dataDir = new File(cd.getInstanceDir().resolve(cd.getDataDir()).toAbsolutePath().toString());
      try {
        FileUtils.deleteDirectory(dataDir);
      } catch (IOException e) {
        log.error("Failed to delete data dir for unloaded core: {} dir: {}", cd.getName(), dataDir.getAbsolutePath(), e);
      }
    }
    if (deleteInstanceDir) {
      try {
        FileUtils.deleteDirectory(cd.getInstanceDir().toFile());
      } catch (IOException e) {
        log.error("Failed to delete instance dir for unloaded core: {} dir: {}", cd.getName(), cd.getInstanceDir(), e);
      }
    }
  }


  /**
   * Register to notify for any file change in the conf directory.
   * If the file change results in a core reload , then the listener
   * is not fired
   */
  public void addConfListener(Runnable runnable) {
    confListeners.add(runnable);
  }

  /**
   * Remove a listener
   */
  public boolean removeConfListener(Runnable runnable) {
    return confListeners.remove(runnable);
  }

  /**
   * This registers one listener for the entire conf directory. In zookeeper
   * there is no event fired when children are modified. So , we expect everyone
   * to 'touch' the /conf directory by setting some data  so that events are triggered.
   */
  private void registerConfListener() {
    if (!(resourceLoader instanceof ZkSolrResourceLoader)) return;
    final ZkSolrResourceLoader zkSolrResourceLoader = (ZkSolrResourceLoader) resourceLoader;
    if (zkSolrResourceLoader != null)
      zkSolrResourceLoader.getZkController().registerConfListenerForCore(
          zkSolrResourceLoader.getConfigSetZkPath(),
          this,
          getConfListener(this, zkSolrResourceLoader));

  }


  public static Runnable getConfListener(SolrCore core, ZkSolrResourceLoader zkSolrResourceLoader) {
    final String coreName = core.getName();
    final CoreContainer cc = core.getCoreContainer();
    final String overlayPath = zkSolrResourceLoader.getConfigSetZkPath() + "/" + ConfigOverlay.RESOURCE_NAME;
    final String solrConfigPath = zkSolrResourceLoader.getConfigSetZkPath() + "/" + core.getSolrConfig().getName();
    String schemaRes = null;
    if (core.getLatestSchema().isMutable() && core.getLatestSchema() instanceof ManagedIndexSchema) {
      ManagedIndexSchema mis = (ManagedIndexSchema) core.getLatestSchema();
      schemaRes = mis.getResourceName();
    }
    final String managedSchmaResourcePath = schemaRes == null ? null : zkSolrResourceLoader.getConfigSetZkPath() + "/" + schemaRes;
    return () -> {
      log.info("config update listener called for core {}", coreName);
      SolrZkClient zkClient = cc.getZkController().getZkClient();
      int solrConfigversion, overlayVersion, managedSchemaVersion = 0;
      SolrConfig cfg = null;
      try (SolrCore solrCore = cc.solrCores.getCoreFromAnyList(coreName, true)) {
        if (solrCore == null || solrCore.isClosed() || solrCore.getCoreContainer().isShutDown()) return;
        cfg = solrCore.getSolrConfig();
        solrConfigversion = solrCore.getSolrConfig().getOverlay().getZnodeVersion();
        overlayVersion = solrCore.getSolrConfig().getZnodeVersion();
        if (managedSchmaResourcePath != null) {
          managedSchemaVersion = ((ManagedIndexSchema) solrCore.getLatestSchema()).getSchemaZkVersion();
        }

      }
      if (cfg != null) {
        cfg.refreshRequestParams();
      }
      if (checkStale(zkClient, overlayPath, solrConfigversion) ||
          checkStale(zkClient, solrConfigPath, overlayVersion) ||
          checkStale(zkClient, managedSchmaResourcePath, managedSchemaVersion)) {
        log.info("core reload {}", coreName);
        SolrConfigHandler configHandler = ((SolrConfigHandler) core.getRequestHandler("/config"));
        if (configHandler.getReloadLock().tryLock()) {

          try {
            cc.reload(coreName);
          } catch (SolrCoreState.CoreIsClosedException e) {
            /*no problem this core is already closed*/
          } finally {
            configHandler.getReloadLock().unlock();
          }

        } else {
          log.info("Another reload is in progress. Not doing anything.");
        }
        return;
      }
      //some files in conf directory may have  other than managedschema, overlay, params
      try (SolrCore solrCore = cc.solrCores.getCoreFromAnyList(coreName, true)) {
        if (solrCore == null || solrCore.isClosed() || cc.isShutDown()) return;
        for (Runnable listener : solrCore.confListeners) {
          try {
            listener.run();
          } catch (Exception e) {
            log.error("Error in listener ", e);
          }
        }
      }

    };
  }

  public void registerInfoBean(String name, SolrInfoBean solrInfoBean) {
    infoRegistry.put(name, solrInfoBean);

    if (solrInfoBean instanceof SolrMetricProducer) {
      SolrMetricProducer producer = (SolrMetricProducer) solrInfoBean;
      coreMetricManager.registerMetricProducer(name, producer);
    }
  }

  private static boolean checkStale(SolrZkClient zkClient, String zkPath, int currentVersion) {
    if (zkPath == null) return false;
    try {
      Stat stat = zkClient.exists(zkPath, null, true);
      if (stat == null) {
        if (currentVersion > -1) return true;
        return false;
      }
      if (stat.getVersion() > currentVersion) {
        log.debug("{} is stale will need an update from {} to {}", zkPath, currentVersion, stat.getVersion());
        return true;
      }
      return false;
    } catch (KeeperException.NoNodeException nne) {
      //no problem
    } catch (KeeperException e) {
      log.error("error refreshing solrconfig ", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return false;
  }

  public void cleanupOldIndexDirectories(boolean reload) {
    final DirectoryFactory myDirFactory = getDirectoryFactory();
    final String myDataDir = getDataDir();
    final String myIndexDir = getNewIndexDir(); // ensure the latest replicated index is protected 
    final String coreName = getName();
    if (myDirFactory != null && myDataDir != null && myIndexDir != null) {
      Thread cleanupThread = new Thread(() -> {
        log.debug("Looking for old index directories to cleanup for core {} in {}", coreName, myDataDir);
        try {
          myDirFactory.cleanupOldIndexDirectories(myDataDir, myIndexDir, reload);
        } catch (Exception exc) {
          log.error("Failed to cleanup old index directories for core {}", coreName, exc);
        }
      }, "OldIndexDirectoryCleanupThreadForCore-" + coreName);
      cleanupThread.setDaemon(true);
      cleanupThread.start();
    }
  }

  private static final Map implicitPluginsInfo = (Map) Utils.fromJSONResource("ImplicitPlugins.json");

  public List<PluginInfo> getImplicitHandlers() {
    List<PluginInfo> implicits = new ArrayList<>();
    Map requestHandlers = (Map) implicitPluginsInfo.get(SolrRequestHandler.TYPE);
    for (Object o : requestHandlers.entrySet()) {
      Map.Entry<String, Map> entry = (Map.Entry<String, Map>) o;
      Map info = Utils.getDeepCopy(entry.getValue(), 4);
      info.put(NAME, entry.getKey());
      implicits.add(new PluginInfo(SolrRequestHandler.TYPE, info));
    }
    return implicits;
  }

  /**
   * Convenience method to load a blob. This method minimizes the degree to which component and other code needs
   * to depend on the structure of solr's object graph and ensures that a proper close hook is registered. This method
   * should normally be called in {@link SolrCoreAware#inform(SolrCore)}, and should never be called during request
   * processing. The Decoder will only run on the first invocations, subsequent invocations will return the
   * cached object.
   *
   * @param key     A key in the format of name/version for a blob stored in the
   *                {@link CollectionAdminParams#SYSTEM_COLL} blob store via the Blob Store API
   * @param decoder a decoder with which to convert the blob into a Java Object representation (first time only)
   * @return a reference to the blob that has already cached the decoded version.
   */
  public BlobRepository.BlobContentRef loadDecodeAndCacheBlob(String key, BlobRepository.Decoder<Object> decoder) {
    // make sure component authors don't give us oddball keys with no version...
    if (!BlobRepository.BLOB_KEY_PATTERN_CHECKER.matcher(key).matches()) {
      throw new IllegalArgumentException("invalid key format, must end in /N where N is the version number");
    }
    // define the blob
    BlobRepository.BlobContentRef blobRef = coreContainer.getBlobRepository().getBlobIncRef(key, decoder);
    addCloseHook(new CloseHook() {
      @Override
      public void preClose(SolrCore core) {
      }

      @Override
      public void postClose(SolrCore core) {
        coreContainer.getBlobRepository().decrementBlobRefCount(blobRef);
      }
    });
    return blobRef;
  }

  /**
   * Run an arbitrary task in it's own thread. This is an expert option and is
   * a method you should use with great care. It would be bad to run something that never stopped
   * or run something that took a very long time. Typically this is intended for actions that take
   * a few seconds, and therefore would be bad to wait for within a request, but but would not pose
   * a significant hindrance to server shut down times. It is not intended for long running tasks
   * and if you are using a Runnable with a loop in it, you are almost certainly doing it wrong.
   * <p>
   * WARNING: Solr wil not be able to shut down gracefully until this task completes!
   * <p>
   * A significant upside of using this method vs creating your own ExecutorService is that your code
   * does not have to properly shutdown executors which typically is risky from a unit testing
   * perspective since the test framework will complain if you don't carefully ensure the executor
   * shuts down before the end of the test. Also the threads running this task are sure to have
   * a proper MDC for logging.
   *
   * @param r the task to run
   */
  public void runAsync(Runnable r) {
    coreAsyncTaskExecutor.submit(r);
  }
}
