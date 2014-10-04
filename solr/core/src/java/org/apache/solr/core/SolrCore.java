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

import org.apache.commons.io.FileUtils;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CommonParams.EchoParamStyle;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.SnapPuller;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.handler.admin.ShowFileRequestHandler;
import org.apache.solr.handler.component.DebugComponent;
import org.apache.solr.handler.component.ExpandComponent;
import org.apache.solr.handler.component.FacetComponent;
import org.apache.solr.handler.component.HighlightComponent;
import org.apache.solr.handler.component.MoreLikeThisComponent;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.StatsComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.BinaryResponseWriter;
import org.apache.solr.response.CSVResponseWriter;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.solr.response.PHPResponseWriter;
import org.apache.solr.response.PHPSerializedResponseWriter;
import org.apache.solr.response.PythonResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.RubyResponseWriter;
import org.apache.solr.response.SchemaXmlResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.SortingResponseWriter;
import org.apache.solr.response.XMLResponseWriter;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.rest.ManagedResourceStorage;
import org.apache.solr.rest.ManagedResourceStorage.StorageIO;
import org.apache.solr.rest.RestManager;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrFieldCacheMBean;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.update.DefaultSolrCoreState;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.update.SolrCoreState.IndexWriterCloser;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.VersionInfo;
import org.apache.solr.update.processor.DistributedUpdateProcessorFactory;
import org.apache.solr.update.processor.LogUpdateProcessorFactory;
import org.apache.solr.update.processor.RunUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.IOUtils;
import org.apache.solr.util.PropertiesInputStream;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public final class SolrCore implements SolrInfoMBean, Closeable {
  public static final String version="1.0";  

  // These should *only* be used for debugging or monitoring purposes
  public static final AtomicLong numOpens = new AtomicLong();
  public static final AtomicLong numCloses = new AtomicLong();
  public static Map<SolrCore,Exception> openHandles = Collections.synchronizedMap(new IdentityHashMap<SolrCore, Exception>());

  
  public static Logger log = LoggerFactory.getLogger(SolrCore.class);

  private String name;
  private String logid; // used to show what name is set
  private CoreDescriptor coreDescriptor;

  private boolean isReloaded = false;

  private final SolrConfig solrConfig;
  private final SolrResourceLoader resourceLoader;
  private volatile IndexSchema schema;
  private final String dataDir;
  private final String ulogDir;
  private final UpdateHandler updateHandler;
  private final SolrCoreState solrCoreState;
  
  private final long startTime;
  private final RequestHandlers reqHandlers;
  private final Map<String,SearchComponent> searchComponents;
  private final Map<String,UpdateRequestProcessorChain> updateProcessorChains;
  private final Map<String, SolrInfoMBean> infoRegistry;
  private IndexDeletionPolicyWrapper solrDelPolicy;
  private DirectoryFactory directoryFactory;
  private IndexReaderFactory indexReaderFactory;
  private final Codec codec;
  
  private final ReentrantLock ruleExpiryLock;
  
  public long getStartTime() { return startTime; }
  
  private RestManager restManager;
  
  public RestManager getRestManager() {
    return restManager;
  }
  
  static int boolean_query_max_clause_count = Integer.MIN_VALUE;
  // only change the BooleanQuery maxClauseCount once for ALL cores...
  void booleanQueryMaxClauseCount()  {
    synchronized(SolrCore.class) {
      if (boolean_query_max_clause_count == Integer.MIN_VALUE) {
        boolean_query_max_clause_count = solrConfig.booleanQueryMaxClauseCount;
        BooleanQuery.setMaxClauseCount(boolean_query_max_clause_count);
      } else if (boolean_query_max_clause_count != solrConfig.booleanQueryMaxClauseCount ) {
        log.debug("BooleanQuery.maxClauseCount= " +boolean_query_max_clause_count+ ", ignoring " +solrConfig.booleanQueryMaxClauseCount);
      }
    }
  }
    
  /**
   * The SolrResourceLoader used to load all resources for this core.
   * @since solr 1.3
   */
  public SolrResourceLoader getResourceLoader() {
    return resourceLoader;
  }

  /**
   * Gets the configuration resource name used by this core instance.
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
   * @since solr 1.3
   */
  public String getSchemaResource() {
    return getLatestSchema().getResourceName();
  }

  /** @return the latest snapshot of the schema used by this core instance. */
  public IndexSchema getLatestSchema() { 
    return schema;
  }
  
  /** Sets the latest schema snapshot to be used by this core instance. */
  public void setLatestSchema(IndexSchema replacementSchema) {
    schema = replacementSchema;
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
   * in dataDir that is returned Else dataDir/index is returned. Only called for creating new indexSearchers
   * and indexwriters. Use the getIndexDir() method to know the active index directory
   *
   * @return the indexdir as given in index.properties
   */
  public String getNewIndexDir() {
    String result = dataDir + "index/";
    Properties p = new Properties();
    Directory dir = null;
    try {
      dir = getDirectoryFactory().get(getDataDir(), DirContext.META_DATA, getSolrConfig().indexConfig.lockType);
      IndexInput input;
      try {
        input = dir.openInput(SnapPuller.INDEX_PROPERTIES, IOContext.DEFAULT);
      } catch (FileNotFoundException | NoSuchFileException e) {
        input = null;
      }

      if (input != null) {
        final InputStream is = new PropertiesInputStream(input);
        try {
          p.load(new InputStreamReader(is, StandardCharsets.UTF_8));
          
          String s = p.getProperty("index");
          if (s != null && s.trim().length() > 0) {
              result = dataDir + s;
          }
          
        } catch (Exception e) {
          log.error("Unable to load " + SnapPuller.INDEX_PROPERTIES, e);
        } finally {
          IOUtils.closeQuietly(is);
        }
      }
    } catch (IOException e) {
      SolrException.log(log, "", e);
    } finally {
      if (dir != null) {
        try {
          getDirectoryFactory().release(dir);
        } catch (IOException e) {
          SolrException.log(log, "", e);
        }
      }
    }
    if (!result.equals(lastNewIndexDir)) {
      log.info("New index directory detected: old="+lastNewIndexDir + " new=" + result);
    }
    lastNewIndexDir = result;
    return result;
  }
  private String lastNewIndexDir; // for debugging purposes only... access not synchronized, but that's ok

  
  public DirectoryFactory getDirectoryFactory() {
    return directoryFactory;
  }
  
  public IndexReaderFactory getIndexReaderFactory() {
    return indexReaderFactory;
  }
  
  @Override
  public String getName() {
    return name;
  }

  public void setName(String v) {
    this.name = v;
    this.logid = (v==null)?"":("["+v+"] ");
    this.coreDescriptor = new CoreDescriptor(v, this.coreDescriptor);
  }

  public String getLogId()
  {
    return this.logid;
  }

  /**
   * Returns a Map of name vs SolrInfoMBean objects. The returned map is an instance of
   * a ConcurrentHashMap and therefore no synchronization is needed for putting, removing
   * or iterating over it.
   *
   * @return the Info Registry map which contains SolrInfoMBean objects keyed by name
   * @since solr 1.3
   */
  public Map<String, SolrInfoMBean> getInfoRegistry() {
    return infoRegistry;
  }

   private void initDeletionPolicy() {
     PluginInfo info = solrConfig.getPluginInfo(IndexDeletionPolicy.class.getName());
     IndexDeletionPolicy delPolicy = null;
     if(info != null){
       delPolicy = createInstance(info.className,IndexDeletionPolicy.class,"Deletion Policy for SOLR");
       if (delPolicy instanceof NamedListInitializedPlugin) {
         ((NamedListInitializedPlugin) delPolicy).init(info.initArgs);
       }
     } else {
       delPolicy = new SolrDeletionPolicy();
     }     
     solrDelPolicy = new IndexDeletionPolicyWrapper(delPolicy);
   }

  private void initListeners() {
    final Class<SolrEventListener> clazz = SolrEventListener.class;
    final String label = "Event Listener";
    for (PluginInfo info : solrConfig.getPluginInfos(SolrEventListener.class.getName())) {
      String event = info.attributes.get("event");
      if("firstSearcher".equals(event) ){
        SolrEventListener obj = createInitInstance(info,clazz,label,null);
        firstSearcherListeners.add(obj);
        log.info(logid + "Added SolrEventListener for firstSearcher: " + obj);
      } else if("newSearcher".equals(event) ){
        SolrEventListener obj = createInitInstance(info,clazz,label,null);
        newSearcherListeners.add(obj);
        log.info(logid + "Added SolrEventListener for newSearcher: " + obj);
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
  public void registerFirstSearcherListener( SolrEventListener listener )
  {
    firstSearcherListeners.add( listener );
  }

  /**
   * NOTE: this function is not thread safe.  However, it is safe to call within the
   * <code>inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes.
   * Outside <code>inform</code>, this could potentially throw a ConcurrentModificationException
   * 
   * @see SolrCoreAware
   */
  public void registerNewSearcherListener( SolrEventListener listener )
  {
    newSearcherListeners.add( listener );
  }

  /**
   * NOTE: this function is not thread safe.  However, it is safe to call within the
   * <code>inform( SolrCore core )</code> function for <code>SolrCoreAware</code> classes.
   * Outside <code>inform</code>, this could potentially throw a ConcurrentModificationException
   * 
   * @see SolrCoreAware
   */
  public QueryResponseWriter registerResponseWriter( String name, QueryResponseWriter responseWriter ){
    return responseWriters.put(name, responseWriter);
  }

  public SolrCore reload(ConfigSet coreConfig, SolrCore prev) throws IOException,
      ParserConfigurationException, SAXException {
    
    solrCoreState.increfSolrCoreState();
    boolean indexDirChange = !getNewIndexDir().equals(getIndexDir());
    if (indexDirChange || !coreConfig.getSolrConfig().nrtMode) {
      // the directory is changing, don't pass on state
      prev = null;
    }
    
    SolrCore core = new SolrCore(getName(), getDataDir(), coreConfig.getSolrConfig(),
        coreConfig.getIndexSchema(), coreDescriptor, updateHandler, this.solrDelPolicy, prev);
    core.solrDelPolicy = this.solrDelPolicy;
    

    // we open a new indexwriter to pick up the latest config
    core.getUpdateHandler().getSolrCoreState().newIndexWriter(core, false);
    
    core.getSearcher(true, false, null, true);
    
    return core;
  }


  // gets a non-caching searcher
  public SolrIndexSearcher newSearcher(String name) throws IOException {
    return new SolrIndexSearcher(this, getNewIndexDir(), getLatestSchema(), getSolrConfig().indexConfig, 
                                 name, false, directoryFactory);
  }


   private void initDirectoryFactory() {
    DirectoryFactory dirFactory;
    PluginInfo info = solrConfig.getPluginInfo(DirectoryFactory.class.getName());
    if (info != null) {
      log.info(info.className);
      dirFactory = getResourceLoader().newInstance(info.className, DirectoryFactory.class);
      dirFactory.init(info.initArgs);
    } else {
      log.info("solr.NRTCachingDirectoryFactory");
      dirFactory = new NRTCachingDirectoryFactory();
    }
    // And set it
    directoryFactory = dirFactory;
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

  void initIndex(boolean reload) throws IOException {
 
      String indexDir = getNewIndexDir();
      boolean indexExists = getDirectoryFactory().exists(indexDir);
      boolean firstTime;
      synchronized (SolrCore.class) {
        firstTime = dirs.add(getDirectoryFactory().normalize(indexDir));
      }
      boolean removeLocks = solrConfig.unlockOnStartup;

      initIndexReaderFactory();

      if (indexExists && firstTime && !reload) {
        
        Directory dir = directoryFactory.get(indexDir, DirContext.DEFAULT,
            getSolrConfig().indexConfig.lockType);
        try {
          if (IndexWriter.isLocked(dir)) {
            if (removeLocks) {
              log.warn(
                  logid
                      + "WARNING: Solr index directory '{}' is locked.  Unlocking...",
                  indexDir);
              IndexWriter.unlock(dir);
            } else {
              log.error(logid
                  + "Solr index directory '{}' is locked.  Throwing exception",
                  indexDir);
              throw new LockObtainFailedException(
                  "Index locked for write for core " + name);
            }
            
          }
        } finally {
          directoryFactory.release(dir);
        }
      }

      // Create the index if it doesn't exist.
      if(!indexExists) {
        log.warn(logid+"Solr index directory '" + new File(indexDir) + "' doesn't exist."
                + " Creating new index...");

        SolrIndexWriter writer = SolrIndexWriter.create("SolrCore.initIndex", indexDir, getDirectoryFactory(), true, 
                                                        getLatestSchema(), solrConfig.indexConfig, solrDelPolicy, codec);
        writer.close();
      }

 
  }

  /** Creates an instance by trying a constructor that accepts a SolrCore before
   *  trying the default (no arg) constructor.
   *@param className the instance class to create
   *@param cast the class or interface that the instance should extend or implement
   *@param msg a message helping compose the exception error if any occurs.
   *@return the desired instance
   *@throws SolrException if the object could not be instantiated
   */
  private <T> T createInstance(String className, Class<T> cast, String msg) {
    Class<? extends T> clazz = null;
    if (msg == null) msg = "SolrCore Object";
    try {
        clazz = getResourceLoader().findClass(className, cast);
        //most of the classes do not have constructors which takes SolrCore argument. It is recommended to obtain SolrCore by implementing SolrCoreAware.
        // So invariably always it will cause a  NoSuchMethodException. So iterate though the list of available constructors
        Constructor<?>[] cons =  clazz.getConstructors();
        for (Constructor<?> con : cons) {
          Class<?>[] types = con.getParameterTypes();
          if(types.length == 1 && types[0] == SolrCore.class){
            return cast.cast(con.newInstance(this));
          }
        }
        return getResourceLoader().newInstance(className, cast);//use the empty constructor
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      // The JVM likes to wrap our helpful SolrExceptions in things like
      // "InvocationTargetException" that have no useful getMessage
      if (null != e.getCause() && e.getCause() instanceof SolrException) {
        SolrException inner = (SolrException) e.getCause();
        throw inner;
      }

      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Error Instantiating "+msg+", "+className+ " failed to instantiate " +cast.getName(), e);
    }
  }
  
  private UpdateHandler createReloadedUpdateHandler(String className, String msg, UpdateHandler updateHandler) {
    Class<? extends UpdateHandler> clazz = null;
    if (msg == null) msg = "SolrCore Object";
    try {
        clazz = getResourceLoader().findClass(className, UpdateHandler.class);
        //most of the classes do not have constructors which takes SolrCore argument. It is recommended to obtain SolrCore by implementing SolrCoreAware.
        // So invariably always it will cause a  NoSuchMethodException. So iterate though the list of available constructors
        Constructor<?>[] cons =  clazz.getConstructors();
        for (Constructor<?> con : cons) {
          Class<?>[] types = con.getParameterTypes();
          if(types.length == 2 && types[0] == SolrCore.class && types[1] == UpdateHandler.class){
            return UpdateHandler.class.cast(con.newInstance(this, updateHandler));
          } 
        }
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Error Instantiating "+msg+", "+className+ " could not find proper constructor for " + UpdateHandler.class.getName());
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      // The JVM likes to wrap our helpful SolrExceptions in things like
      // "InvocationTargetException" that have no useful getMessage
      if (null != e.getCause() && e.getCause() instanceof SolrException) {
        SolrException inner = (SolrException) e.getCause();
        throw inner;
      }

      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Error Instantiating "+msg+", "+className+ " failed to instantiate " + UpdateHandler.class.getName(), e);
    }
  }

  public <T extends Object> T createInitInstance(PluginInfo info,Class<T> cast, String msg, String defClassName){
    if(info == null) return null;
    T o = createInstance(info.className == null ? defClassName : info.className,cast, msg);
    if (o instanceof PluginInfoInitialized) {
      ((PluginInfoInitialized) o).init(info);
    } else if (o instanceof NamedListInitializedPlugin) {
      ((NamedListInitializedPlugin) o).init(info.initArgs);
    }
    if(o instanceof SearchComponent) {
      ((SearchComponent) o).setName(info.name);
    }
    return o;
  }

  public SolrEventListener createEventListener(String className) {
    return createInstance(className, SolrEventListener.class, "Event Listener");
  }

  public SolrRequestHandler createRequestHandler(String className) {
    return createInstance(className, SolrRequestHandler.class, "Request Handler");
  }

  private UpdateHandler createUpdateHandler(String className) {
    return createInstance(className, UpdateHandler.class, "Update Handler");
  }
  
  private UpdateHandler createUpdateHandler(String className, UpdateHandler updateHandler) {
    return createReloadedUpdateHandler(className, "Update Handler", updateHandler);
  }

  private QueryResponseWriter createQueryResponseWriter(String className) {
    return createInstance(className, QueryResponseWriter.class, "Query Response Writer");
  }
  
  /**
   * Creates a new core and register it in the list of cores.
   * If a core with the same name already exists, it will be stopped and replaced by this one.
   *
   * @param dataDir the index directory
   * @param config a solr config instance
   * @param schema a solr schema instance
   *
   * @since solr 1.3
   */
  public SolrCore(String name, String dataDir, SolrConfig config, IndexSchema schema, CoreDescriptor cd) {
    this(name, dataDir, config, schema, cd, null, null, null);
  }

  public SolrCore(CoreDescriptor cd, ConfigSet coreConfig) {
    this(cd.getName(), null, coreConfig.getSolrConfig(), coreConfig.getIndexSchema(), cd, null, null, null);
  }


  /**
   * Creates a new core that is to be loaded lazily. i.e. lazyLoad="true" in solr.xml
   * @since solr 4.1
   */
  public SolrCore(String name, CoreDescriptor cd) {
    coreDescriptor = cd;
    this.setName(name);
    this.schema = null;
    this.dataDir = null;
    this.ulogDir = null;
    this.solrConfig = null;
    this.startTime = System.currentTimeMillis();
    this.maxWarmingSearchers = 2;  // we don't have a config yet, just pick a number.
    this.resourceLoader = null;
    this.updateHandler = null;
    this.isReloaded = true;
    this.reqHandlers = null;
    this.searchComponents = null;
    this.updateProcessorChains = null;
    this.infoRegistry = null;
    this.codec = null;
    this.ruleExpiryLock = null;

    solrCoreState = null;
  }
  /**
   * Creates a new core and register it in the list of cores.
   * If a core with the same name already exists, it will be stopped and replaced by this one.
   *@param dataDir the index directory
   *@param config a solr config instance
   *@param schema a solr schema instance
   *
   *@since solr 1.3
   */
  public SolrCore(String name, String dataDir, SolrConfig config, IndexSchema schema, CoreDescriptor cd, UpdateHandler updateHandler, IndexDeletionPolicyWrapper delPolicy, SolrCore prev) {
    coreDescriptor = cd;
    this.setName( name );
    resourceLoader = config.getResourceLoader();
    this.solrConfig = config;
    
    if (updateHandler == null) {
      initDirectoryFactory();
    }

    if (dataDir == null) {
      if (cd.usingDefaultDataDir()) dataDir = config.getDataDir();
      if (dataDir == null) {
        try {
          dataDir = cd.getDataDir();
          if (!directoryFactory.isAbsolute(dataDir)) {
            dataDir = directoryFactory.getDataHome(cd);
          }
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, null, e);
        }
      }
    }
    dataDir = SolrResourceLoader.normalizeDir(dataDir);

    String updateLogDir = cd.getUlogDir();
    if (updateLogDir == null) {
      updateLogDir = dataDir;
      if (new File(updateLogDir).isAbsolute() == false) {
        updateLogDir = SolrResourceLoader.normalizeDir(cd.getInstanceDir()) + updateLogDir;
      }
    }
    ulogDir = updateLogDir;


    log.info(logid+"Opening new SolrCore at " + resourceLoader.getInstanceDir() + ", dataDir="+dataDir);

    if (null != cd && null != cd.getCloudDescriptor()) {
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
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                                "Schema will not work with SolrCloud mode: " +
                                e.getMessage(), e);
      }
    }

    //Initialize JMX
    if (config.jmxConfig.enabled) {
      infoRegistry = new JmxMonitoredMap<String, SolrInfoMBean>(name, String.valueOf(this.hashCode()), config.jmxConfig);
    } else  {
      log.info("JMX monitoring not detected for core: " + name);
      infoRegistry = new ConcurrentHashMap<>();
    }

    infoRegistry.put("fieldCache", new SolrFieldCacheMBean());

    if (schema==null) {
      schema = IndexSchemaFactory.buildIndexSchema(IndexSchema.DEFAULT_SCHEMA_FILE, config);
    }
    this.schema = schema;
    final SimilarityFactory similarityFactory = schema.getSimilarityFactory(); 
    if (similarityFactory instanceof SolrCoreAware) {
      // Similarity needs SolrCore before inform() is called on all registered SolrCoreAware listeners below
      ((SolrCoreAware)similarityFactory).inform(this);
    }

    this.dataDir = dataDir;
    this.startTime = System.currentTimeMillis();
    this.maxWarmingSearchers = config.maxWarmingSearchers;

    booleanQueryMaxClauseCount();
  
    final CountDownLatch latch = new CountDownLatch(1);

    try {
      
      initListeners();
      
      if (delPolicy == null) {
        initDeletionPolicy();
      } else {
        this.solrDelPolicy = delPolicy;
      }
      
      this.codec = initCodec(solrConfig, schema);
      
      if (updateHandler == null) {
        solrCoreState = new DefaultSolrCoreState(getDirectoryFactory());
      } else {
        solrCoreState = updateHandler.getSolrCoreState();
        directoryFactory = solrCoreState.getDirectoryFactory();
        this.isReloaded = true;
      }
      
      initIndex(prev != null);
      
      initWriters();
      initQParsers();
      initValueSourceParsers();
      initTransformerFactories();
      
      this.searchComponents = Collections
          .unmodifiableMap(loadSearchComponents());
      
      // Processors initialized before the handlers
      updateProcessorChains = loadUpdateProcessorChains();
      reqHandlers = new RequestHandlers(this);
      List<PluginInfo> implicitReqHandlerInfo = new ArrayList<>();
      UpdateRequestHandler.addImplicits(implicitReqHandlerInfo);
      reqHandlers.initHandlersFromConfig(solrConfig, implicitReqHandlerInfo);

      // Handle things that should eventually go away
      initDeprecatedSupport();
      
      // cause the executor to stall so firstSearcher events won't fire
      // until after inform() has been called for all components.
      // searchExecutor must be single-threaded for this to work
      searcherExecutor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          latch.await();
          return null;
        }
      });
      
      // use the (old) writer to open the first searcher
      RefCounted<IndexWriter> iwRef = null;
      if (prev != null) {
        iwRef = prev.getUpdateHandler().getSolrCoreState().getIndexWriter(null);
        if (iwRef != null) {
          final IndexWriter iw = iwRef.get();
          final SolrCore core = this;
          newReaderCreator = new Callable<DirectoryReader>() {
            // this is used during a core reload

            @Override
            public DirectoryReader call() throws Exception {
              if(getSolrConfig().nrtMode) {
                // if in NRT mode, need to open from the previous writer
                return indexReaderFactory.newReader(iw, core);
              } else {
                // if not NRT, need to create a new reader from the directory
                return indexReaderFactory.newReader(iw.getDirectory(), core);
              }
            }
          };
        }
      }
      
      String updateHandlerClass = solrConfig.getUpdateHandlerInfo().className;
      
      if (updateHandler == null) {
        this.updateHandler = createUpdateHandler(updateHandlerClass == null ? DirectUpdateHandler2.class
            .getName() : updateHandlerClass);
      } else {
        this.updateHandler = createUpdateHandler(
            updateHandlerClass == null ? DirectUpdateHandler2.class.getName()
                : updateHandlerClass, updateHandler);
      }
      infoRegistry.put("updateHandler", this.updateHandler);

      try {
        getSearcher(false, false, null, true);
      } finally {
        newReaderCreator = null;
        if (iwRef != null) iwRef.decref();
      }
      
      // Initialize the RestManager
      restManager = initRestManager();
            
      // Finally tell anyone who wants to know
      resourceLoader.inform(resourceLoader);
      resourceLoader.inform(this); // last call before the latch is released.
    } catch (Throwable e) {
      latch.countDown();//release the latch, otherwise we block trying to do the close.  This should be fine, since counting down on a latch of 0 is still fine
      //close down the searcher and any other resources, if it exists, as this is not recoverable
      if (e instanceof OutOfMemoryError) {
        throw (OutOfMemoryError)e;
      }
      
      try {
       this.close();
      } catch (Throwable t) {
        if (t instanceof OutOfMemoryError) {
          throw (OutOfMemoryError)t;
        }
        log.error("Error while closing", t);
      }
      
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
                              e.getMessage(), e);
    } finally {
      // allow firstSearcher events to fire and make sure it is released
      latch.countDown();
    }

    infoRegistry.put("core", this);
    
    // register any SolrInfoMBeans SolrResourceLoader initialized
    //
    // this must happen after the latch is released, because a JMX server impl may
    // choose to block on registering until properties can be fetched from an MBean,
    // and a SolrCoreAware MBean may have properties that depend on getting a Searcher
    // from the core.
    resourceLoader.inform(infoRegistry);
    
    CoreContainer cc = cd.getCoreContainer();

    if (cc != null && cc.isZooKeeperAware()) {
      SolrRequestHandler realtimeGetHandler = reqHandlers.get("/get");
      if (realtimeGetHandler == null) {
        log.warn("WARNING: RealTimeGetHandler is not registered at /get. " +
            "SolrCloud will always use full index replication instead of the more efficient PeerSync method.");
      }

      // ZK pre-Register would have already happened so we read slice properties now
      ClusterState clusterState = cc.getZkController().getClusterState();
      Slice slice = clusterState.getSlice(cd.getCloudDescriptor().getCollectionName(),
          cd.getCloudDescriptor().getShardId());
      if (Slice.CONSTRUCTION.equals(slice.getState())) {
        // set update log to buffer before publishing the core
        getUpdateHandler().getUpdateLog().bufferUpdates();
      }
    }
    // For debugging   
//    numOpens.incrementAndGet();
//    openHandles.put(this, new RuntimeException("unclosed core - name:" + getName() + " refs: " + refCount.get()));

    ruleExpiryLock = new ReentrantLock();
  }
    
  private Codec initCodec(SolrConfig solrConfig, final IndexSchema schema) {
    final PluginInfo info = solrConfig.getPluginInfo(CodecFactory.class.getName());
    final CodecFactory factory;
    if (info != null) {
      factory = schema.getResourceLoader().newInstance(info.className, CodecFactory.class);
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
      ((SolrCoreAware)factory).inform(this);
    } else {
      for (FieldType ft : schema.getFieldTypes().values()) {
        if (null != ft.getPostingsFormat()) {
          String msg = "FieldType '" + ft.getTypeName() + "' is configured with a postings format, but the codec does not support it: " + factory.getClass();
          log.error(msg);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
        }
        if (null != ft.getDocValuesFormat()) {
          String msg = "FieldType '" + ft.getTypeName() + "' is configured with a docValues format, but the codec does not support it: " + factory.getClass();
          log.error(msg);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
        }
      }
    }
    return factory.getCodec();
  }

  /**
   * Load the request processors
   */
   private Map<String,UpdateRequestProcessorChain> loadUpdateProcessorChains() {
    Map<String, UpdateRequestProcessorChain> map = new HashMap<>();
    UpdateRequestProcessorChain def = initPlugins(map,UpdateRequestProcessorChain.class, UpdateRequestProcessorChain.class.getName());
    if(def == null){
      def = map.get(null);
    } 
    if (def == null) {
      log.info("no updateRequestProcessorChain defined as default, creating implicit default");
      // construct the default chain
      UpdateRequestProcessorFactory[] factories = new UpdateRequestProcessorFactory[]{
              new LogUpdateProcessorFactory(),
              new DistributedUpdateProcessorFactory(),
              new RunUpdateProcessorFactory()
      };
      def = new UpdateRequestProcessorChain(factories, this);
    }
    map.put(null, def);
    map.put("", def);
    return map;
  }
   
  public SolrCoreState getSolrCoreState() {
    return solrCoreState;
  }  

  /**
   * @return an update processor registered to the given name.  Throw an exception if this chain is undefined
   */    
  public UpdateRequestProcessorChain getUpdateProcessingChain( final String name )
  {
    UpdateRequestProcessorChain chain = updateProcessorChains.get( name );
    if( chain == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "unknown UpdateRequestProcessorChain: "+name );
    }
    return chain;
  }
  
  // this core current usage count
  private final AtomicInteger refCount = new AtomicInteger(1);

  /** expert: increments the core reference count */
  public void open() {
    refCount.incrementAndGet();
  }
  
  /**
   * Close all resources allocated by the core if it is no longer in use...
   * <ul>
   *   <li>searcher</li>
   *   <li>updateHandler</li>
   *   <li>all CloseHooks will be notified</li>
   *   <li>All MBeans will be unregistered from MBeanServer if JMX was enabled
   *       </li>
   * </ul>
   * <p>   
   * <p>
   * The behavior of this method is determined by the result of decrementing
   * the core's reference count (A core is created with a reference count of 1)...
   * </p>
   * <ul>
   *   <li>If reference count is > 0, the usage count is decreased by 1 and no
   *       resources are released.
   *   </li>
   *   <li>If reference count is == 0, the resources are released.
   *   <li>If reference count is &lt; 0, and error is logged and no further action
   *       is taken.
   *   </li>
   * </ul>
   * @see #isClosed() 
   */
  public void close() {
    int count = refCount.decrementAndGet();
    if (count > 0) return; // close is called often, and only actually closes if nothing is using it.
    if (count < 0) {
      log.error("Too many close [count:{}] on {}. Please report this exception to solr-user@lucene.apache.org", count, this );
      assert false : "Too many closes on SolrCore";
      return;
    }
    log.info(logid+" CLOSING SolrCore " + this);


    if( closeHooks != null ) {
       for( CloseHook hook : closeHooks ) {
         try {
           hook.preClose( this );
         } catch (Throwable e) {
           SolrException.log(log, e);       
           if (e instanceof Error) {
             throw (Error) e;
           }
         }
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

    try {
      if (null != updateHandler) {
        updateHandler.close();
      }
    } catch (Throwable e) {
      SolrException.log(log,e);
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
      SolrException.log(log,e);
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

    
    if( closeHooks != null ) {
       for( CloseHook hook : closeHooks ) {
         try {
           hook.postClose( this );
         } catch (Throwable e) {
           SolrException.log(log, e);
           if (e instanceof Error) {
             throw (Error) e;
           }
         }
      }
    }
    
    // For debugging 
//    numCloses.incrementAndGet();
//    openHandles.remove(this);
  }

  /** Current core usage count. */
  public int getOpenCount() {
    return refCount.get();
  }
  
  /** Whether this core is closed. */
  public boolean isClosed() {
      return refCount.get() <= 0;
  }
  
  @Override
  protected void finalize() throws Throwable {
    try {
      if (getOpenCount() != 0) {
        log.error("REFCOUNT ERROR: unreferenced " + this + " (" + getName()
            + ") has a reference count of " + getOpenCount());
      }
    } finally {
      super.finalize();
    }
  }

  private Collection<CloseHook> closeHooks = null;

   /**
    * Add a close callback hook
    */
   public void addCloseHook( CloseHook hook )
   {
     if( closeHooks == null ) {
       closeHooks = new ArrayList<>();
     }
     closeHooks.add( hook );
   }

  /** @lucene.internal
   *  Debugging aid only.  No non-test code should be released with uncommented verbose() calls.  */
  public static boolean VERBOSE = Boolean.parseBoolean(System.getProperty("tests.verbose","false"));
  public static void verbose(Object... args) {
    if (!VERBOSE) return;
    StringBuilder sb = new StringBuilder("VERBOSE:");
//    sb.append(Thread.currentThread().getName());
//    sb.append(':');
    for (Object o : args) {
      sb.append(' ');
      sb.append(o==null ? "(null)" : o.toString());
    }
    // System.out.println(sb.toString());
    log.info(sb.toString());
  }


  ////////////////////////////////////////////////////////////////////////////////
  // Request Handler
  ////////////////////////////////////////////////////////////////////////////////

  /**
   * Get the request handler registered to a given name.  
   * 
   * This function is thread safe.
   */
  public SolrRequestHandler getRequestHandler(String handlerName) {
    return RequestHandlerBase.getRequestHandler(RequestHandlers.normalize(handlerName), reqHandlers.getRequestHandlers());
  }

  /**
   * Returns an unmodifiable Map containing the registered handlers of the specified type.
   */
  public <T extends SolrRequestHandler> Map<String,T> getRequestHandlers(Class<T> clazz) {
    return reqHandlers.getAll(clazz);
  }
  
  /**
   * Returns an unmodifiable Map containing the registered handlers
   */
  public Map<String,SolrRequestHandler> getRequestHandlers() {
    return reqHandlers.getRequestHandlers();
  }


  /**
   * Registers a handler at the specified location.  If one exists there, it will be replaced.
   * To remove a handler, register <code>null</code> at its path
   * 
   * Once registered the handler can be accessed through:
   * <pre>
   *   http://${host}:${port}/${context}/${handlerName}
   * or:  
   *   http://${host}:${port}/${context}/select?qt=${handlerName}
   * </pre>  
   * 
   * Handlers <em>must</em> be initialized before getting registered.  Registered
   * handlers can immediately accept requests.
   * 
   * This call is thread safe.
   *  
   * @return the previous <code>SolrRequestHandler</code> registered to this name <code>null</code> if none.
   */
  public SolrRequestHandler registerRequestHandler(String handlerName, SolrRequestHandler handler) {
    return reqHandlers.register(handlerName,handler);
  }
  
  /**
   * Register the default search components
   */
  private Map<String, SearchComponent> loadSearchComponents()
  {
    Map<String, SearchComponent> components = new HashMap<>();
    initPlugins(components,SearchComponent.class);
    for (Map.Entry<String, SearchComponent> e : components.entrySet()) {
      SearchComponent c = e.getValue();
      if (c instanceof HighlightComponent) {
        HighlightComponent hl = (HighlightComponent) c;
        if(!HighlightComponent.COMPONENT_NAME.equals(e.getKey())){
          components.put(HighlightComponent.COMPONENT_NAME,hl);
        }
        break;
      }
    }
    addIfNotPresent(components,HighlightComponent.COMPONENT_NAME,HighlightComponent.class);
    addIfNotPresent(components,QueryComponent.COMPONENT_NAME,QueryComponent.class);
    addIfNotPresent(components,FacetComponent.COMPONENT_NAME,FacetComponent.class);
    addIfNotPresent(components,MoreLikeThisComponent.COMPONENT_NAME,MoreLikeThisComponent.class);
    addIfNotPresent(components,StatsComponent.COMPONENT_NAME,StatsComponent.class);
    addIfNotPresent(components,DebugComponent.COMPONENT_NAME,DebugComponent.class);
    addIfNotPresent(components,RealTimeGetComponent.COMPONENT_NAME,RealTimeGetComponent.class);
    addIfNotPresent(components,ExpandComponent.COMPONENT_NAME,ExpandComponent.class);

    return components;
  }
  private <T> void addIfNotPresent(Map<String ,T> registry, String name, Class<? extends  T> c){
    if(!registry.containsKey(name)){
      T searchComp = resourceLoader.newInstance(c.getName(), c);
      if (searchComp instanceof NamedListInitializedPlugin){
        ((NamedListInitializedPlugin)searchComp).init( new NamedList<String>() );
      }
      if(searchComp instanceof SearchComponent) {
        ((SearchComponent)searchComp).setName(name);
      }
      registry.put(name, searchComp);
      if (searchComp instanceof SolrInfoMBean){
        infoRegistry.put(((SolrInfoMBean)searchComp).getName(), (SolrInfoMBean)searchComp);
      }
    }
  }
  
  /**
   * @return a Search Component registered to a given name.  Throw an exception if the component is undefined
   */
  public SearchComponent getSearchComponent( String name )
  {
    SearchComponent component = searchComponents.get( name );
    if( component == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "Unknown Search Component: "+name );
    }
    return component;
  }

  /**
   * Accessor for all the Search Components
   * @return An unmodifiable Map of Search Components
   */
  public Map<String, SearchComponent> getSearchComponents() {
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

  final ExecutorService searcherExecutor = Executors.newSingleThreadExecutor(
      new DefaultSolrThreadFactory("searcherExecutor"));
  private int onDeckSearchers;  // number of searchers preparing
  // Lock ordering: one can acquire the openSearcherLock and then the searcherLock, but not vice-versa.
  private Object searcherLock = new Object();  // the sync object for the searcher
  private ReentrantLock openSearcherLock = new ReentrantLock(true);     // used to serialize opens/reopens for absolute ordering
  private final int maxWarmingSearchers;  // max number of on-deck searchers allowed

  private RefCounted<SolrIndexSearcher> realtimeSearcher;
  private Callable<DirectoryReader> newReaderCreator;

  /**
  * Return a registered {@link RefCounted}&lt;{@link SolrIndexSearcher}&gt; with
  * the reference count incremented.  It <b>must</b> be decremented when no longer needed.
  * This method should not be called from SolrCoreAware.inform() since it can result
  * in a deadlock if useColdSearcher==false.
  * If handling a normal request, the searcher should be obtained from
   * {@link org.apache.solr.request.SolrQueryRequest#getSearcher()} instead.
  */
  public RefCounted<SolrIndexSearcher> getSearcher() {
    return getSearcher(false,true,null);
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

  /** Gets the latest real-time searcher w/o forcing open a new searcher if one already exists.
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


  /** Opens a new searcher and returns a RefCounted&lt;SolrIndexSearcher&gt; with it's reference incremented.
   *
   * "realtime" means that we need to open quickly for a realtime view of the index, hence don't do any
   * autowarming and add to the _realtimeSearchers queue rather than the _searchers queue (so it won't
   * be used for autowarming by a future normal searcher).  A "realtime" searcher will currently never
   * become "registered" (since it currently lacks caching).
   *
   * realtimeSearcher is updated to the latest opened searcher, regardless of the value of "realtime".
   *
   * This method acquires openSearcherLock - do not call with searckLock held!
   */
  public RefCounted<SolrIndexSearcher>  openNewSearcher(boolean updateHandlerReopens, boolean realtime) {
    if (isClosed()) { // catch some errors quicker
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "openNewSearcher called on closed core");
    }

    SolrIndexSearcher tmp;
    RefCounted<SolrIndexSearcher> newestSearcher = null;
    boolean nrt = solrConfig.nrtMode && updateHandlerReopens;

    openSearcherLock.lock();
    try {
      String newIndexDir = getNewIndexDir();
      String indexDirFile = null;
      String newIndexDirFile = null;

      // if it's not a normal near-realtime update, check that paths haven't changed.
      if (!nrt) {
        indexDirFile = getDirectoryFactory().normalize(getIndexDir());
        newIndexDirFile = getDirectoryFactory().normalize(newIndexDir);
      }

      synchronized (searcherLock) {
        newestSearcher = realtimeSearcher;
        if (newestSearcher != null) {
          newestSearcher.incref();      // the matching decref is in the finally block
        }
      }

      if (newestSearcher != null && (nrt || indexDirFile.equals(newIndexDirFile))) {

        DirectoryReader newReader;
        DirectoryReader currentReader = newestSearcher.get().getRawReader();

        // SolrCore.verbose("start reopen from",previousSearcher,"writer=",writer);
        
        RefCounted<IndexWriter> writer = getUpdateHandler().getSolrCoreState()
            .getIndexWriter(null);
        try {
          if (writer != null && solrConfig.nrtMode) {
            // if in NRT mode, open from the writer
            newReader = DirectoryReader.openIfChanged(currentReader, writer.get(), true);
          } else {
            // verbose("start reopen without writer, reader=", currentReader);
            // if not in NRT mode, just re-open the reader
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
            log.info("SolrIndexSearcher has not changed - not re-opening: " + newestSearcher.get().getName());
            return newestSearcher;

          } // ELSE: open a new searcher against the old reader...
          currentReader.incRef();
          newReader = currentReader;
        }

        // for now, turn off caches if this is for a realtime reader 
        // (caches take a little while to instantiate)
        final boolean useCaches = !realtime;
        final String newName = realtime ? "realtime" : "main";
        tmp = new SolrIndexSearcher(this, newIndexDir, getLatestSchema(), 
                                    getSolrConfig().indexConfig, newName,
                                    newReader, true, useCaches, true, directoryFactory);

      } else {
        // newestSearcher == null at this point

        if (newReaderCreator != null) {
          // this is set in the constructor if there is a currently open index writer
          // so that we pick up any uncommitted changes and so we don't go backwards
          // in time on a core reload
          DirectoryReader newReader = newReaderCreator.call();
          tmp = new SolrIndexSearcher(this, newIndexDir, getLatestSchema(), getSolrConfig().indexConfig, 
              (realtime ? "realtime":"main"), newReader, true, !realtime, true, directoryFactory);
        } else if (solrConfig.nrtMode) {
          RefCounted<IndexWriter> writer = getUpdateHandler().getSolrCoreState().getIndexWriter(this);
          DirectoryReader newReader = null;
          try {
            newReader = indexReaderFactory.newReader(writer.get(), this);
          } finally {
            writer.decref();
          }
          tmp = new SolrIndexSearcher(this, newIndexDir, getLatestSchema(), getSolrConfig().indexConfig, 
              (realtime ? "realtime":"main"), newReader, true, !realtime, true, directoryFactory);
        } else {
         // normal open that happens at startup
        // verbose("non-reopen START:");
        tmp = new SolrIndexSearcher(this, newIndexDir, getLatestSchema(), getSolrConfig().indexConfig,
                                    "main", true, directoryFactory);
        // verbose("non-reopen DONE: searcher=",tmp);
        }
      }

      List<RefCounted<SolrIndexSearcher>> searcherList = realtime ? _realtimeSearchers : _searchers;
      RefCounted<SolrIndexSearcher> newSearcher = newHolder(tmp, searcherList);    // refcount now at 1

      // Increment reference again for "realtimeSearcher" variable.  It should be at 2 after.
      // When it's decremented by both the caller of this method, and by realtimeSearcher being replaced,
      // it will be closed.
      newSearcher.incref();

      synchronized (searcherLock) {
        if (realtimeSearcher != null) {
          realtimeSearcher.decref();
        }
        realtimeSearcher = newSearcher;
        searcherList.add(realtimeSearcher);
      }

      return newSearcher;

    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error opening new searcher", e);
    }
    finally {
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
   *  A new searcher will be opened and registered regardless of whether there is already
   *    a registered searcher or other searchers in the process of being created.
   * <p>
   * If <tt>forceNew==false</tt> then:<ul>
   *   <li>If a searcher is already registered, that searcher will be returned</li>
   *   <li>If no searcher is currently registered, but at least one is in the process of being created, then
   * this call will block until the first searcher is registered</li>
   *   <li>If no searcher is currently registered, and no searchers in the process of being registered, a new
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
      // see if we can return the current searcher
      if (_searcher!=null && !forceNew) {
        if (returnSearcher) {
          _searcher.incref();
          return _searcher;
        } else {
          return null;
        }
      }

      // check to see if we can wait for someone else's searcher to be set
      if (onDeckSearchers>0 && !forceNew && _searcher==null) {
        try {
          searcherLock.wait();
        } catch (InterruptedException e) {
          log.info(SolrException.toStr(e));
        }
      }

      // check again: see if we can return right now
      if (_searcher!=null && !forceNew) {
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
      if (onDeckSearchers < 1) {
        // should never happen... just a sanity check
        log.error(logid+"ERROR!!! onDeckSearchers is " + onDeckSearchers);
        onDeckSearchers=1;  // reset
      } else if (onDeckSearchers > maxWarmingSearchers) {
        onDeckSearchers--;
        String msg="Error opening new searcher. exceeded limit of maxWarmingSearchers="+maxWarmingSearchers + ", try again later.";
        log.warn(logid+""+ msg);
        // HTTP 503==service unavailable, or 409==Conflict
        throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,msg);
      } else if (onDeckSearchers > 1) {
        log.warn(logid+"PERFORMANCE WARNING: Overlapping onDeckSearchers=" + onDeckSearchers);
      }
    }

    // a signal to decrement onDeckSearchers if something goes wrong.
    final boolean[] decrementOnDeckCount=new boolean[]{true};
    RefCounted<SolrIndexSearcher> currSearcherHolder = null;     // searcher we are autowarming from
    RefCounted<SolrIndexSearcher> searchHolder = null;
    boolean success = false;

    openSearcherLock.lock();
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
            decrementOnDeckCount[0]=false;
            alreadyRegistered=true;
          }
        } else {
          // get a reference to the current searcher for purposes of autowarming.
          currSearcherHolder=_searcher;
          currSearcherHolder.incref();
        }
      }


      final SolrIndexSearcher currSearcher = currSearcherHolder==null ? null : currSearcherHolder.get();

      Future future=null;

      // if the underlying seracher has not changed, no warming is needed
      if (newSearcher != currSearcher) {
        
        // warm the new searcher based on the current searcher.
        // should this go before the other event handlers or after?
        if (currSearcher != null) {
          future = searcherExecutor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
              try {
                newSearcher.warm(currSearcher);
              } catch (Throwable e) {
                SolrException.log(log, e);
                if (e instanceof Error) {
                  throw (Error) e;
                }
              }
              return null;
            }
          });
        }
        
        if (currSearcher == null && firstSearcherListeners.size() > 0) {
          future = searcherExecutor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
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
            }
          });
        }
        
        if (currSearcher != null && newSearcherListeners.size() > 0) {
          future = searcherExecutor.submit(new Callable() {
            @Override
            public Object call() throws Exception {
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
            }
          });
        }
        
      }


      // WARNING: this code assumes a single threaded executor (that all tasks
      // queued will finish first).
      final RefCounted<SolrIndexSearcher> currSearcherHolderF = currSearcherHolder;
      if (!alreadyRegistered) {
        future = searcherExecutor.submit(
            new Callable() {
              @Override
              public Object call() throws Exception {
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
                  if (currSearcherHolderF!=null) currSearcherHolderF.decref();
                }
                return null;
              }
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
      if (e instanceof SolrException) throw (SolrException)e;
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {

      if (!success) {
        synchronized (searcherLock) {
          onDeckSearchers--;

          if (onDeckSearchers < 0) {
            // sanity check... should never happen
            log.error(logid+"ERROR!!! onDeckSearchers after decrement=" + onDeckSearchers);
            onDeckSearchers=0; // try and recover
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
          synchronized(searcherLock) {
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
          _searcher=null;
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
        log.info(logid+"Registered new searcher " + newSearcher);

      } catch (Exception e) {
        // an exception in register() shouldn't be fatal.
        log(e);
      } finally {
        // wake up anyone waiting for a searcher
        // even in the face of errors.
        onDeckSearchers--;
        searcherLock.notifyAll();
      }
    }
  }



  public void closeSearcher() {
    log.info(logid+"Closing main searcher on request.");
    synchronized (searcherLock) {
      if (realtimeSearcher != null) {
        realtimeSearcher.decref();
        realtimeSearcher = null;
      }
      if (_searcher != null) {
        _searcher.decref();   // dec refcount for this._searcher
        _searcher = null; // isClosed() does check this
        infoRegistry.remove("currentSearcher");
      }
    }
  }

  public void execute(SolrRequestHandler handler, SolrQueryRequest req, SolrQueryResponse rsp) {
    if (handler==null) {
      String msg = "Null Request Handler '" +
        req.getParams().get(CommonParams.QT) + "'";
      
      if (log.isWarnEnabled()) log.warn(logid + msg + ":" + req);
      
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
    }

    preDecorateResponse(req, rsp);

    // TODO: this doesn't seem to be working correctly and causes problems with the example server and distrib (for example /spell)
    // if (req.getParams().getBool(ShardParams.IS_SHARD,false) && !(handler instanceof SearchHandler))
    //   throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,"isShard is only acceptable with search handlers");


    handler.handleRequest(req,rsp);
    postDecorateResponse(handler, req, rsp);

    if (log.isInfoEnabled() && rsp.getToLog().size() > 0) {
      log.info(rsp.getToLogAsString(logid));
    }
  }

  public static void preDecorateResponse(SolrQueryRequest req, SolrQueryResponse rsp) {
    // setup response header
    final NamedList<Object> responseHeader = new SimpleOrderedMap<>();
    rsp.add("responseHeader", responseHeader);

    // toLog is a local ref to the same NamedList used by the response
    NamedList<Object> toLog = rsp.getToLog();

    // for back compat, we set these now just in case other code
    // are expecting them during handleRequest
    toLog.add("webapp", req.getContext().get("webapp"));
    toLog.add("path", req.getContext().get("path"));

    final SolrParams params = req.getParams();
    final String lpList = params.get(CommonParams.LOG_PARAMS_LIST);
    if (lpList == null) {
      toLog.add("params", "{" + req.getParamString() + "}");
    } else if (lpList.length() > 0) {
      toLog.add("params", "{" + params.toFilteredSolrParams(Arrays.asList(lpList.split(","))).toString() + "}");
    }
  }

  /** Put status, QTime, and possibly request handler and params, in the response header */
  public static void postDecorateResponse
      (SolrRequestHandler handler, SolrQueryRequest req, SolrQueryResponse rsp) {
    // TODO should check that responseHeader has not been replaced by handler
    NamedList<Object> responseHeader = rsp.getResponseHeader();
    final int qtime=(int)(rsp.getEndTime() - req.getStartTime());
    int status = 0;
    Exception exception = rsp.getException();
    if( exception != null ){
      if( exception instanceof SolrException )
        status = ((SolrException)exception).code();
      else
        status = 500;
    }
    responseHeader.add("status",status);
    responseHeader.add("QTime",qtime);

    if (rsp.getToLog().size() > 0) {
      rsp.getToLog().add("status",status);
      rsp.getToLog().add("QTime",qtime);
    }

    SolrParams params = req.getParams();
    if( null != handler && params.getBool(CommonParams.HEADER_ECHO_HANDLER, false) ) {
      responseHeader.add("handler", handler.getName() );
    }

    // Values for echoParams... false/true/all or false/explicit/all ???
    String ep = params.get( CommonParams.HEADER_ECHO_PARAMS, null );
    if( ep != null ) {
      EchoParamStyle echoParams = EchoParamStyle.get( ep );
      if( echoParams == null ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Invalid value '" + ep + "' for " + CommonParams.HEADER_ECHO_PARAMS 
            + " parameter, use '" + EchoParamStyle.EXPLICIT + "' or '" + EchoParamStyle.ALL + "'" );
      }
      if( echoParams == EchoParamStyle.EXPLICIT ) {
        responseHeader.add("params", req.getOriginalParams().toNamedList());
      } else if( echoParams == EchoParamStyle.ALL ) {
        responseHeader.add("params", req.getParams().toNamedList());
      }
    }
  }

  final public static void log(Throwable e) {
    SolrException.log(log,null,e);
  }

  
  
  private QueryResponseWriter defaultResponseWriter;
  private final Map<String, QueryResponseWriter> responseWriters = new HashMap<>();
  public static final Map<String ,QueryResponseWriter> DEFAULT_RESPONSE_WRITERS ;
  static{
    HashMap<String, QueryResponseWriter> m= new HashMap<>();
    m.put("xml", new XMLResponseWriter());
    m.put("standard", m.get("xml"));
    m.put("json", new JSONResponseWriter());
    m.put("python", new PythonResponseWriter());
    m.put("php", new PHPResponseWriter());
    m.put("phps", new PHPSerializedResponseWriter());
    m.put("ruby", new RubyResponseWriter());
    m.put("raw", new RawResponseWriter());
    m.put("javabin", new BinaryResponseWriter());
    m.put("csv", new CSVResponseWriter());
    m.put("xsort", new SortingResponseWriter());
    m.put("schema.xml", new SchemaXmlResponseWriter());
    DEFAULT_RESPONSE_WRITERS = Collections.unmodifiableMap(m);
  }
  
  /** Configure the query response writers. There will always be a default writer; additional
   * writers may also be configured. */
  private void initWriters() {
    // use link map so we iterate in the same order
    Map<PluginInfo,QueryResponseWriter> writers = new LinkedHashMap<>();
    for (PluginInfo info : solrConfig.getPluginInfos(QueryResponseWriter.class.getName())) {
      try {
        QueryResponseWriter writer;
        String startup = info.attributes.get("startup") ;
        if( startup != null ) {
          if( "lazy".equals(startup) ) {
            log.info("adding lazy queryResponseWriter: " + info.className);
            writer = new LazyQueryResponseWriterWrapper(this, info.className, info.initArgs );
          } else {
            throw new Exception( "Unknown startup value: '"+startup+"' for: "+info.className );
          }
        } else {
          writer = createQueryResponseWriter(info.className);
        }
        writers.put(info,writer);
        QueryResponseWriter old = registerResponseWriter(info.name, writer);
        if(old != null) {
          log.warn("Multiple queryResponseWriter registered to the same name: " + info.name + " ignoring: " + old.getClass().getName());
        }
        if(info.isDefault()){
          if(defaultResponseWriter != null)
            log.warn("Multiple default queryResponseWriter registered, using: " + info.name);
          defaultResponseWriter = writer;
        }
        log.info("created "+info.name+": " + info.className);
      } catch (Exception ex) {
          SolrException e = new SolrException
            (SolrException.ErrorCode.SERVER_ERROR, "QueryResponseWriter init failure", ex);
          SolrException.log(log,null,e);
          throw e;
      }
    }

    // we've now registered all handlers, time to init them in the same order
    for (Map.Entry<PluginInfo,QueryResponseWriter> entry : writers.entrySet()) {
      PluginInfo info = entry.getKey();
      QueryResponseWriter writer = entry.getValue();
      responseWriters.put(info.name, writer);
      if (writer instanceof PluginInfoInitialized) {
        ((PluginInfoInitialized) writer).init(info);
      } else{
        writer.init(info.initArgs);
      }
    }

    NamedList emptyList = new NamedList();
    for (Map.Entry<String, QueryResponseWriter> entry : DEFAULT_RESPONSE_WRITERS.entrySet()) {
      if(responseWriters.get(entry.getKey()) == null) {
        responseWriters.put(entry.getKey(), entry.getValue());
        // call init so any logic in the default writers gets invoked
        entry.getValue().init(emptyList);
      }
    }
    
    // configure the default response writer; this one should never be null
    if (defaultResponseWriter == null) {
      defaultResponseWriter = responseWriters.get("standard");
    }

  }
  
  /** Finds a writer by name, or returns the default writer if not found. */
  public final QueryResponseWriter getQueryResponseWriter(String writerName) {
    if (writerName != null) {
        QueryResponseWriter writer = responseWriters.get(writerName);
        if (writer != null) {
            return writer;
        }
    }
    return defaultResponseWriter;
  }

  /** Returns the appropriate writer for a request. If the request specifies a writer via the
   * 'wt' parameter, attempts to find that one; otherwise return the default writer.
   */
  public final QueryResponseWriter getQueryResponseWriter(SolrQueryRequest request) {
    return getQueryResponseWriter(request.getParams().get(CommonParams.WT)); 
  }

  private final Map<String, QParserPlugin> qParserPlugins = new HashMap<>();

  /** Configure the query parsers. */
  private void initQParsers() {
    initPlugins(qParserPlugins,QParserPlugin.class);
    // default parsers
    for (int i=0; i<QParserPlugin.standardPlugins.length; i+=2) {
     try {
       String name = (String)QParserPlugin.standardPlugins[i];
       if (null == qParserPlugins.get(name)) {
         Class<QParserPlugin> clazz = (Class<QParserPlugin>)QParserPlugin.standardPlugins[i+1];
         QParserPlugin plugin = clazz.newInstance();
         qParserPlugins.put(name, plugin);
         plugin.init(null);
         infoRegistry.put(name, plugin);
       }
     } catch (Exception e) {
       throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
     }
    }
  }

  public QParserPlugin getQueryPlugin(String parserName) {
    QParserPlugin plugin = qParserPlugins.get(parserName);
    if (plugin != null) return plugin;
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown query parser '"+parserName+"'");
  }
  
  private final HashMap<String, ValueSourceParser> valueSourceParsers = new HashMap<>();
  
  /** Configure the ValueSource (function) plugins */
  private void initValueSourceParsers() {
    initPlugins(valueSourceParsers,ValueSourceParser.class);
    // default value source parsers
    for (Map.Entry<String, ValueSourceParser> entry : ValueSourceParser.standardValueSourceParsers.entrySet()) {
      try {
        String name = entry.getKey();
        if (null == valueSourceParsers.get(name)) {
          ValueSourceParser valueSourceParser = entry.getValue();
          valueSourceParsers.put(name, valueSourceParser);
          valueSourceParser.init(null);
        }
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }
  

  private final HashMap<String, TransformerFactory> transformerFactories = new HashMap<>();
  
  /** Configure the TransformerFactory plugins */
  private void initTransformerFactories() {
    // Load any transformer factories
    initPlugins(transformerFactories,TransformerFactory.class);
    
    // Tell each transformer what its name is
    for( Map.Entry<String, TransformerFactory> entry : TransformerFactory.defaultFactories.entrySet() ) {
      try {
        String name = entry.getKey();
        if (null == valueSourceParsers.get(name)) {
          TransformerFactory f = entry.getValue();
          transformerFactories.put(name, f);
          // f.init(null); default ones don't need init
        }
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }
  
  public TransformerFactory getTransformerFactory(String name) {
    return transformerFactories.get(name);
  }

  public void addTransformerFactory(String name, TransformerFactory factory){
    transformerFactories.put(name, factory);
  }
  

  /**
   * @param registry The map to which the instance should be added to. The key is the name attribute
   * @param type the class or interface that the instance should extend or implement.
   * @param defClassName If PluginInfo does not have a classname, use this as the classname
   * @return The default instance . The one with (default=true)
   */
  public <T> T initPlugins(Map<String ,T> registry, Class<T> type, String defClassName){
    return initPlugins(solrConfig.getPluginInfos(type.getName()), registry, type, defClassName);
  }

  public <T> T initPlugins(List<PluginInfo> pluginInfos, Map<String, T> registry, Class<T> type, String defClassName) {
    T def = null;
    for (PluginInfo info : pluginInfos) {
      T o = createInitInstance(info,type, type.getSimpleName(), defClassName);
      registry.put(info.name, o);
      if(info.isDefault()){
        def = o;
      }
    }
    return def;
  }

  /**For a given List of PluginInfo return the instances as a List
   * @param defClassName The default classname if PluginInfo#className == null
   * @return The instances initialized
   */
  public <T> List<T> initPlugins(List<PluginInfo> pluginInfos, Class<T> type, String defClassName) {
    if(pluginInfos.isEmpty()) return Collections.emptyList();
    List<T> result = new ArrayList<>();
    for (PluginInfo info : pluginInfos) result.add(createInitInstance(info,type, type.getSimpleName(), defClassName));
    return result;
  }

  /**
   *
   * @param registry The map to which the instance should be added to. The key is the name attribute
   * @param type The type of the Plugin. These should be standard ones registered by type.getName() in SolrConfig
   * @return     The default if any
   */
  public <T> T initPlugins(Map<String, T> registry, Class<T> type) {
    return initPlugins(registry, type, null);
  }

  public ValueSourceParser getValueSourceParser(String parserName) {
    return valueSourceParsers.get(parserName);
  }
  
  /**
   * Manage anything that should be taken care of in case configs change
   */
  private void initDeprecatedSupport()
  {
    // TODO -- this should be removed in deprecation release...
    String gettable = solrConfig.get("admin/gettableFiles", null );
    if( gettable != null ) {
      log.warn( 
          "solrconfig.xml uses deprecated <admin/gettableFiles>, Please "+
          "update your config to use the ShowFileRequestHandler." );
      if( getRequestHandler( "/admin/file" ) == null ) {
        NamedList<String> invariants = new NamedList<>();
        
        // Hide everything...
        Set<String> hide = new HashSet<>();

        for (String file : solrConfig.getResourceLoader().listConfigDir()) {
          hide.add(file.toUpperCase(Locale.ROOT));
        }    
        
        // except the "gettable" list
        StringTokenizer st = new StringTokenizer( gettable );
        while( st.hasMoreTokens() ) {
          hide.remove( st.nextToken().toUpperCase(Locale.ROOT) );
        }
        for( String s : hide ) {
          invariants.add( ShowFileRequestHandler.HIDDEN, s );
        }
        
        NamedList<Object> args = new NamedList<>();
        args.add( "invariants", invariants );
        ShowFileRequestHandler handler = new ShowFileRequestHandler();
        handler.init( args );
        reqHandlers.register("/admin/file", handler);

        log.warn( "adding ShowFileRequestHandler with hidden files: "+hide );
      }
    }

    String facetSort = solrConfig.get("//bool[@name='facet.sort']", null);
    if (facetSort != null) {
      log.warn( 
          "solrconfig.xml uses deprecated <bool name='facet.sort'>. Please "+
          "update your config to use <string name='facet.sort'>.");
    }
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
        initArgs = (NamedList<String>)restManagerPluginInfo.initArgs;        
      }
    }
    
    if (mgr == null) 
      mgr = new RestManager();
    
    if (initArgs == null)
      initArgs = new NamedList<>();
                                
    String collection = coreDescriptor.getCollectionName();
    StorageIO storageIO = 
        ManagedResourceStorage.newStorageIO(collection, resourceLoader, initArgs);    
    mgr.init(resourceLoader, initArgs, storageIO);
    
    return mgr;
  }  
  
  public CoreDescriptor getCoreDescriptor() {
    return coreDescriptor;
  }

  public IndexDeletionPolicyWrapper getDeletionPolicy(){
    return solrDelPolicy;
  }

  public ReentrantLock getRuleExpiryLock() {
    return ruleExpiryLock;
  }

  /////////////////////////////////////////////////////////////////////
  // SolrInfoMBean stuff: Statistics and Module Info
  /////////////////////////////////////////////////////////////////////

  @Override
  public String getVersion() {
    return SolrCore.version;
  }

  @Override
  public String getDescription() {
    return "SolrCore";
  }

  @Override
  public Category getCategory() {
    return Category.CORE;
  }

  @Override
  public String getSource() {
    return null;
  }

  @Override
  public URL[] getDocs() {
    return null;
  }

  @Override
  public NamedList getStatistics() {
    NamedList<Object> lst = new SimpleOrderedMap<>();
    lst.add("coreName", name==null ? "(null)" : name);
    lst.add("startTime", new Date(startTime));
    lst.add("refCount", getOpenCount());
    lst.add("instanceDir", resourceLoader.getInstanceDir());
    lst.add("indexDir", getIndexDir());

    CoreDescriptor cd = getCoreDescriptor();
    if (cd != null) {
      if (null != cd && cd.getCoreContainer() != null) {
        lst.add("aliases", getCoreDescriptor().getCoreContainer().getCoreNames(this));
      }
      CloudDescriptor cloudDesc = cd.getCloudDescriptor();
      if (cloudDesc != null) {
        String collection = cloudDesc.getCollectionName();
        if (collection == null) {
          collection = "_notset_";
        }
        lst.add("collection", collection);
        String shard = cloudDesc.getShardId();
        if (shard == null) {
          shard = "_auto_";
        }
        lst.add("shard", shard);
      }
    }
    
    return lst;
  }
  
  public Codec getCodec() {
    return codec;
  }

  public void unloadOnClose(boolean deleteIndexDir, boolean deleteDataDir, boolean deleteInstanceDir) {
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
        }

        @Override
        public void postClose(SolrCore core) {
          CoreDescriptor cd = core.getCoreDescriptor();
          if (cd != null) {
            File instanceDir = new File(cd.getInstanceDir());
            try {
              FileUtils.deleteDirectory(instanceDir);
            } catch (IOException e) {
              SolrException.log(log, "Failed to delete instance dir for core:"
                  + core.getName() + " dir:" + instanceDir.getAbsolutePath());
            }
          }
        }
      });
    }
  }

  public static void deleteUnloadedCore(CoreDescriptor cd, boolean deleteDataDir, boolean deleteInstanceDir) {
    if (deleteDataDir) {
      File dataDir = new File(cd.getDataDir());
      try {
        FileUtils.deleteDirectory(dataDir);
      } catch (IOException e) {
        SolrException.log(log, "Failed to delete data dir for unloaded core:" + cd.getName()
            + " dir:" + dataDir.getAbsolutePath());
      }
    }
    if (deleteInstanceDir) {
      File instanceDir = new File(cd.getInstanceDir());
      try {
        FileUtils.deleteDirectory(instanceDir);
      } catch (IOException e) {
        SolrException.log(log, "Failed to delete instance dir for unloaded core:" + cd.getName()
            + " dir:" + instanceDir.getAbsolutePath());
      }
    }
  }

  public final class LazyQueryResponseWriterWrapper implements QueryResponseWriter {
    private SolrCore _core;
    private String _className;
    private NamedList _args;
    private QueryResponseWriter _writer;

    public LazyQueryResponseWriterWrapper(SolrCore core, String className, NamedList args) {
      _core = core;
      _className = className;
      _args = args;
      _writer = null;
    }

    public synchronized QueryResponseWriter getWrappedWriter()
    {
      if( _writer == null ) {
        try {
          QueryResponseWriter writer = createQueryResponseWriter(_className);
          writer.init( _args );
          _writer = writer;
        }
        catch( Exception ex ) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "lazy loading error", ex );
        }
      }
      return _writer;
    }


    @Override
    public void init(NamedList args) {
      // do nothing
    }

    @Override
    public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
      getWrappedWriter().write(writer, request, response);
    }

    @Override
    public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
      return getWrappedWriter().getContentType(request, response);
    }
  }
}



