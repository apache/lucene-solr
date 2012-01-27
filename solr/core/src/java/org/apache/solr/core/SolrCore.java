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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.store.Directory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CommonParams.EchoParamStyle;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.admin.ShowFileRequestHandler;
import org.apache.solr.handler.component.*;
import org.apache.solr.request.*;
import org.apache.solr.response.*;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrFieldCacheMBean;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.processor.*;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.commons.io.IOUtils;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URL;
import java.lang.reflect.Constructor;
import java.util.concurrent.locks.ReentrantLock;


/**
 *
 */
public final class SolrCore implements SolrInfoMBean {
  public static final String version="1.0";  

  public static Logger log = LoggerFactory.getLogger(SolrCore.class);

  private String name;
  private String logid; // used to show what name is set
  private final CoreDescriptor coreDescriptor;

  private boolean isReloaded = false;

  private final SolrConfig solrConfig;
  private final SolrResourceLoader resourceLoader;
  private final IndexSchema schema;
  private final String dataDir;
  private final UpdateHandler updateHandler;
  private final long startTime;
  private final RequestHandlers reqHandlers;
  private final Map<String,SearchComponent> searchComponents;
  private final Map<String,UpdateRequestProcessorChain> updateProcessorChains;
  private final Map<String, SolrInfoMBean> infoRegistry;
  private IndexDeletionPolicyWrapper solrDelPolicy;
  private DirectoryFactory directoryFactory;
  private IndexReaderFactory indexReaderFactory;
  private final Codec codec;

  public long getStartTime() { return startTime; }

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
    return schema.getResourceName();
  }

  /**
   * Gets the schema object used by this core instance.
   */
  public IndexSchema getSchema() { 
    return schema;
  }
  
  public String getDataDir() {
    return dataDir;
  }

  public String getIndexDir() {
    synchronized (searcherLock) {
      if (_searcher == null)
        return dataDir + "index/";
      SolrIndexSearcher searcher = _searcher.get();
      return searcher.getIndexDir() == null ? dataDir + "index/" : searcher.getIndexDir();
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
    File propsFile = new File(dataDir + "index.properties");
    if (propsFile.exists()) {
      Properties p = new Properties();
      InputStream is = null;
      try {
        is = new FileInputStream(propsFile);
        p.load(is);
      } catch (IOException e) {
        /*no op*/
      } finally {
        IOUtils.closeQuietly(is);
      }
      String s = p.getProperty("index");
      if (s != null && s.trim().length() > 0) {
        File tmp = new File(dataDir + s);
        if (tmp.exists() && tmp.isDirectory())
          result = dataDir + s;
      }
    }
    return result;
  }
  
  public DirectoryFactory getDirectoryFactory() {
    return directoryFactory;
  }
  
  public IndexReaderFactory getIndexReaderFactory() {
    return indexReaderFactory;
  }
  
  public String getName() {
    return name;
  }

  public void setName(String v) {
    this.name = v;
    this.logid = (v==null)?"":("["+v+"] ");
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

  final List<SolrEventListener> firstSearcherListeners = new ArrayList<SolrEventListener>();
  final List<SolrEventListener> newSearcherListeners = new ArrayList<SolrEventListener>();

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
  
  public SolrCore reload(SolrResourceLoader resourceLoader) throws IOException,
      ParserConfigurationException, SAXException {
    // TODO - what if indexwriter settings have changed
    
    SolrConfig config = new SolrConfig(resourceLoader,
        getSolrConfig().getName(), null);
    
    IndexSchema schema = new IndexSchema(config,
        getSchema().getResourceName(), null);
    
    updateHandler.incref();
    SolrCore core = new SolrCore(getName(), null, config,
        schema, coreDescriptor, updateHandler);
    return core;
  }


  // gets a non-caching searcher
  public SolrIndexSearcher newSearcher(String name) throws IOException {
    return new SolrIndexSearcher(this, getNewIndexDir(), schema, getSolrConfig().mainIndexConfig, name, false, directoryFactory);
  }


   private void initDirectoryFactory() {
    DirectoryFactory dirFactory;
    PluginInfo info = solrConfig.getPluginInfo(DirectoryFactory.class.getName());
    if (info != null) {
      dirFactory = (DirectoryFactory) getResourceLoader().newInstance(info.className);
      dirFactory.init(info.initArgs);
    } else {
      dirFactory = new StandardDirectoryFactory();
    }
    // And set it
    directoryFactory = dirFactory;
  }

  private void initIndexReaderFactory() {
    IndexReaderFactory indexReaderFactory;
    PluginInfo info = solrConfig.getPluginInfo(IndexReaderFactory.class.getName());
    if (info != null) {
      indexReaderFactory = (IndexReaderFactory) resourceLoader.newInstance(info.className);
      indexReaderFactory.init(info.initArgs);
    } else {
      indexReaderFactory = new StandardIndexReaderFactory();
    } 
    this.indexReaderFactory = indexReaderFactory;
  }
  
  // protect via synchronized(SolrCore.class)
  private static Set<String> dirs = new HashSet<String>();

  void initIndex() {
    try {
      String indexDir = getNewIndexDir();
      boolean indexExists = getDirectoryFactory().exists(indexDir);
      boolean firstTime;
      synchronized (SolrCore.class) {
        firstTime = dirs.add(new File(indexDir).getCanonicalPath());
      }
      boolean removeLocks = solrConfig.unlockOnStartup;

      initIndexReaderFactory();

      if (indexExists && firstTime && removeLocks) {
        // to remove locks, the directory must already exist... so we create it
        // if it didn't exist already...
        Directory dir = directoryFactory.get(indexDir, getSolrConfig().mainIndexConfig.lockType);
        if (dir != null)  {
          if (IndexWriter.isLocked(dir)) {
            log.warn(logid+"WARNING: Solr index directory '" + indexDir+ "' is locked.  Unlocking...");
            IndexWriter.unlock(dir);
          }
          directoryFactory.release(dir);
        }
      }

      // Create the index if it doesn't exist.
      if(!indexExists) {
        log.warn(logid+"Solr index directory '" + new File(indexDir) + "' doesn't exist."
                + " Creating new index...");

        SolrIndexWriter writer = new SolrIndexWriter("SolrCore.initIndex", indexDir, getDirectoryFactory(), true, schema, solrConfig.mainIndexConfig, solrDelPolicy, codec, false);
        writer.close();
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
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
  private <T extends Object> T createInstance(String className, Class<T> cast, String msg) {
    Class clazz = null;
    if (msg == null) msg = "SolrCore Object";
    try {
        clazz = getResourceLoader().findClass(className);
        if (cast != null && !cast.isAssignableFrom(clazz)) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Error Instantiating "+msg+", "+className+ " is not a " +cast.getName());
        }
      //most of the classes do not have constructors which takes SolrCore argument. It is recommended to obtain SolrCore by implementing SolrCoreAware.
      // So invariably always it will cause a  NoSuchMethodException. So iterate though the list of available constructors
        Constructor[] cons =  clazz.getConstructors();
        for (Constructor con : cons) {
          Class[] types = con.getParameterTypes();
          if(types.length == 1 && types[0] == SolrCore.class){
            return (T)con.newInstance(this);
          }
        }
        return (T) getResourceLoader().newInstance(className);//use the empty constructor      
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Error Instantiating "+msg+", "+className+ " failed to instantiate " +cast.getName(), e);
    }
  }
  
  private <T extends Object> T createReloadedUpdateHandler(String className, Class<UpdateHandler> class1, String msg, UpdateHandler updateHandler) {
    Class clazz = null;
    if (msg == null) msg = "SolrCore Object";
    try {
        clazz = getResourceLoader().findClass(className);
        if (class1 != null && !class1.isAssignableFrom(clazz)) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Error Instantiating "+msg+", "+className+ " is not a " +class1.getName());
        }
      //most of the classes do not have constructors which takes SolrCore argument. It is recommended to obtain SolrCore by implementing SolrCoreAware.
      // So invariably always it will cause a  NoSuchMethodException. So iterate though the list of available constructors
        Constructor justSolrCoreCon = null;
        Constructor[] cons =  clazz.getConstructors();
        for (Constructor con : cons) {
          Class[] types = con.getParameterTypes();
          if(types.length == 2 && types[0] == SolrCore.class && types[1] == UpdateHandler.class){
            return (T)con.newInstance(this, updateHandler);
          } 
        }
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Error Instantiating "+msg+", "+className+ " could not find proper constructor for " +class1.getName());
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Error Instantiating "+msg+", "+className+ " failed to instantiate " +class1.getName(), e);
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
    return createReloadedUpdateHandler(className, UpdateHandler.class, "Update Handler", updateHandler);
  }

  private QueryResponseWriter createQueryResponseWriter(String className) {
    return createInstance(className, QueryResponseWriter.class, "Query Response Writer");
  }
  
  /**
   * Creates a new core and register it in the list of cores.
   * If a core with the same name already exists, it will be stopped and replaced by this one.
   *
   * @param name
   * @param dataDir the index directory
   * @param config a solr config instance
   * @param schema a solr schema instance
   *
   * @since solr 1.3
   */
  public SolrCore(String name, String dataDir, SolrConfig config, IndexSchema schema, CoreDescriptor cd) {
    this(name, dataDir, config, schema, cd, null);
  }
  
  /**
   * Creates a new core and register it in the list of cores.
   * If a core with the same name already exists, it will be stopped and replaced by this one.
   *@param dataDir the index directory
   *@param config a solr config instance
   *@param schema a solr schema instance
   *@param updateHandler
   *
   *@since solr 1.3
   */
  public SolrCore(String name, String dataDir, SolrConfig config, IndexSchema schema, CoreDescriptor cd, UpdateHandler updateHandler) {
    coreDescriptor = cd;
    this.setName( name );
    resourceLoader = config.getResourceLoader();
    if (dataDir == null){
      if(cd.usingDefaultDataDir()) dataDir = config.getDataDir();
      if(dataDir == null) dataDir = cd.getDataDir();
    }

    dataDir = SolrResourceLoader.normalizeDir(dataDir);

    log.info(logid+"Opening new SolrCore at " + resourceLoader.getInstanceDir() + ", dataDir="+dataDir);

    if (schema==null) {
      schema = new IndexSchema(config, IndexSchema.DEFAULT_SCHEMA_FILE, null);
    }

    //Initialize JMX
    if (config.jmxConfig.enabled) {
      infoRegistry = new JmxMonitoredMap<String, SolrInfoMBean>(name, String.valueOf(this.hashCode()), config.jmxConfig);
    } else  {
      log.info("JMX monitoring not detected for core: " + name);
      infoRegistry = new ConcurrentHashMap<String, SolrInfoMBean>();
    }

    infoRegistry.put("fieldCache", new SolrFieldCacheMBean());

    this.schema = schema;
    this.dataDir = dataDir;
    this.solrConfig = config;
    this.startTime = System.currentTimeMillis();
    this.maxWarmingSearchers = config.maxWarmingSearchers;

    booleanQueryMaxClauseCount();
  
    initListeners();

    initDeletionPolicy();

    this.codec= initCodec(solrConfig, schema);
    
    if (updateHandler == null) {
      initDirectoryFactory();
    } else {
      directoryFactory = updateHandler.getSolrCoreState().getDirectoryFactory();
      this.isReloaded = true;
    }
    
    initIndex();

    initWriters();
    initQParsers();
    initValueSourceParsers();
    initTransformerFactories();

    this.searchComponents = Collections.unmodifiableMap(loadSearchComponents());

    // Processors initialized before the handlers
    updateProcessorChains = loadUpdateProcessorChains();
    reqHandlers = new RequestHandlers(this);
    reqHandlers.initHandlersFromConfig( solrConfig );


    // Handle things that should eventually go away
    initDeprecatedSupport();

    final CountDownLatch latch = new CountDownLatch(1);

    try {
      // cause the executor to stall so firstSearcher events won't fire
      // until after inform() has been called for all components.
      // searchExecutor must be single-threaded for this to work
      searcherExecutor.submit(new Callable() {
        public Object call() throws Exception {
          latch.await();
          return null;
        }
      });

      // Open the searcher *before* the update handler so we don't end up opening
      // one in the middle.
      // With lockless commits in Lucene now, this probably shouldn't be an issue anymore
      getSearcher(false,false,null);

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

      // Finally tell anyone who wants to know
      resourceLoader.inform( resourceLoader );
      resourceLoader.inform( this );  // last call before the latch is released.
    } catch (Throwable e) {
      latch.countDown();//release the latch, otherwise we block trying to do the close.  This should be fine, since counting down on a latch of 0 is still fine
      //close down the searcher and any other resources, if it exists, as this is not recoverable
      close();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, null, e);
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
  }

  private Codec initCodec(SolrConfig solrConfig, final IndexSchema schema) {
    final PluginInfo info = solrConfig.getPluginInfo(CodecFactory.class.getName());
    final CodecFactory factory;
    if (info != null) {
      factory = (CodecFactory) schema.getResourceLoader().newInstance(info.className);
      factory.init(info.initArgs);
    } else {
      factory = new DefaultCodecFactory();
    }
    return factory.create(schema);
  }

  /**
   * Load the request processors
   */
   private Map<String,UpdateRequestProcessorChain> loadUpdateProcessorChains() {
    Map<String, UpdateRequestProcessorChain> map = new HashMap<String, UpdateRequestProcessorChain>();
    UpdateRequestProcessorChain def = initPlugins(map,UpdateRequestProcessorChain.class, UpdateRequestProcessorChain.class.getName());
    if(def == null){
      def = map.get(null);
    } 
    if (def == null) {
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

  final void open() {
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
   * the core's reference count (A core is created with a refrence count of 1)...
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
      return;
    }
    log.info(logid+" CLOSING SolrCore " + this);


    if( closeHooks != null ) {
       for( CloseHook hook : closeHooks ) {
         try {
           hook.preClose( this );
         } catch (Throwable e) {
           SolrException.log(log, e);           
         }
      }
    }


    try {
      infoRegistry.clear();
    } catch (Exception e) {
      SolrException.log(log, e);
    }

    try {
      searcherExecutor.shutdown();
      if (!searcherExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        log.error("Timeout waiting for searchExecutor to terminate");
      }
    } catch (Exception e) {
      SolrException.log(log,e);
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
    } catch (Exception e) {
      SolrException.log(log,e);
    }

    try {
      updateHandler.close();
    } catch (Throwable e) {
      SolrException.log(log,e);
    }
    
    if( closeHooks != null ) {
       for( CloseHook hook : closeHooks ) {
         try {
           hook.postClose( this );
         } catch (Throwable e) {
           SolrException.log(log, e);
         }
      }
    }
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
       closeHooks = new ArrayList<CloseHook>();
     }
     closeHooks.add( hook );
   }

  /** @lucene.internal
   *  Debugging aid only.  No non-test code should be released with uncommented verbose() calls.  */
  public static boolean VERBOSE = Boolean.parseBoolean(System.getProperty("tests.verbose","false"));
  public static void verbose(Object... args) {
    if (!VERBOSE) return;
    StringBuilder sb = new StringBuilder("VERBOSE:");
    sb.append(Thread.currentThread().getName());
    sb.append(':');
    for (Object o : args) {
      sb.append(' ');
      sb.append(o==null ? "(null)" : o.toString());
    }
    System.out.println(sb.toString());
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
    return reqHandlers.get(handlerName);
  }

  /**
   * Returns an unmodifieable Map containing the registered handlers of the specified type.
   */
  public Map<String,SolrRequestHandler> getRequestHandlers(Class clazz) {
    return reqHandlers.getAll(clazz);
  }
  
  /**
   * Returns an unmodifieable Map containing the registered handlers
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
   * Handlers <em>must</em> be initalized before getting registered.  Registered
   * handlers can immediatly accept requests.
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
    Map<String, SearchComponent> components = new HashMap<String, SearchComponent>();
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
    return components;
  }
  private <T> void addIfNotPresent(Map<String ,T> registry, String name, Class<? extends  T> c){
    if(!registry.containsKey(name)){
      T searchComp = (T) resourceLoader.newInstance(c.getName());
      if (searchComp instanceof NamedListInitializedPlugin){
        ((NamedListInitializedPlugin)searchComp).init( new NamedList() );
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
  private final LinkedList<RefCounted<SolrIndexSearcher>> _searchers = new LinkedList<RefCounted<SolrIndexSearcher>>();
  private final LinkedList<RefCounted<SolrIndexSearcher>> _realtimeSearchers = new LinkedList<RefCounted<SolrIndexSearcher>>();

  final ExecutorService searcherExecutor = Executors.newSingleThreadExecutor();
  private int onDeckSearchers;  // number of searchers preparing
  // Lock ordering: one can acquire the openSearcherLock and then the searcherLock, but not vice-versa.
  private Object searcherLock = new Object();  // the sync object for the searcher
  private ReentrantLock openSearcherLock = new ReentrantLock(true);     // used to serialize opens/reopens for absolute ordering
  private final int maxWarmingSearchers;  // max number of on-deck searchers allowed

  private RefCounted<SolrIndexSearcher> realtimeSearcher;


  /**
  * Return a registered {@link RefCounted}&lt;{@link SolrIndexSearcher}&gt; with
  * the reference count incremented.  It <b>must</b> be decremented when no longer needed.
  * This method should not be called from SolrCoreAware.inform() since it can result
  * in a deadlock if useColdSearcher==false.
  * If handling a normal request, the searcher should be obtained from
   * {@link org.apache.solr.request.SolrQueryRequest#getSearcher()} instead.
  */
  public RefCounted<SolrIndexSearcher> getSearcher() {
    try {
      return getSearcher(false,true,null);
    } catch (IOException e) {
      SolrException.log(log,null,e);
      return null;
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


  public RefCounted<SolrIndexSearcher> getSearcher(boolean forceNew, boolean returnSearcher, final Future[] waitSearcher) throws IOException {
    return getSearcher(forceNew, returnSearcher, waitSearcher, false);
  }


  /** Opens a new searcher and returns a RefCounted<SolrIndexSearcher> with it's reference incremented.
   *
   * "realtime" means that we need to open quickly for a realtime view of the index, hence don't do any
   * autowarming and add to the _realtimeSearchers queue rather than the _searchers queue (so it won't
   * be used for autowarming by a future normal searcher).  A "realtime" searcher will currently never
   * become "registered" (since it currently lacks caching).
   *
   * realtimeSearcher is updated to the latest opened searcher, regardless of the value of "realtime".
   *
   * This method aquires openSearcherLock - do not call with searckLock held!
   */
  public RefCounted<SolrIndexSearcher> openNewSearcher(boolean updateHandlerReopens, boolean realtime) {
    SolrIndexSearcher tmp;
    RefCounted<SolrIndexSearcher> newestSearcher = null;
    boolean nrt = solrConfig.reopenReaders && updateHandlerReopens;

    openSearcherLock.lock();
    try {
      String newIndexDir = getNewIndexDir();
      File indexDirFile = null;
      File newIndexDirFile = null;

      // if it's not a normal near-realtime update, check that paths haven't changed.
      if (!nrt) {
        indexDirFile = new File(getIndexDir()).getCanonicalFile();
        newIndexDirFile = new File(newIndexDir).getCanonicalFile();
      }

      synchronized (searcherLock) {
        newestSearcher = realtimeSearcher;
        if (newestSearcher != null) {
          newestSearcher.incref();      // the matching decref is in the finally block
        }
      }

      if (newestSearcher != null && solrConfig.reopenReaders
          && (nrt || indexDirFile.equals(newIndexDirFile))) {

        IndexReader newReader;
        IndexReader currentReader = newestSearcher.get().getIndexReader();

        if (updateHandlerReopens) {
          // SolrCore.verbose("start reopen from",previousSearcher,"writer=",writer);
          IndexWriter writer = getUpdateHandler().getSolrCoreState().getIndexWriter(this);
          newReader = IndexReader.openIfChanged(currentReader, writer, true);

        } else {
          // verbose("start reopen without writer, reader=", currentReader);
          newReader = IndexReader.openIfChanged(currentReader);
          // verbose("reopen result", newReader);
        }

        if (newReader == null) {
          // if this is a request for a realtime searcher, just return the same searcher if there haven't been any changes.
          if (realtime) {
            newestSearcher.incref();
            return newestSearcher;
          }

          currentReader.incRef();
          newReader = currentReader;
        }

       // for now, turn off caches if this is for a realtime reader (caches take a little while to instantiate)
        tmp = new SolrIndexSearcher(this, schema, (realtime ? "realtime":"main"), newReader, true, !realtime, true, directoryFactory);

      } else {
        // verbose("non-reopen START:");
        tmp = new SolrIndexSearcher(this, newIndexDir, schema, getSolrConfig().mainIndexConfig, "main", true, directoryFactory);
        // verbose("non-reopen DONE: searcher=",tmp);
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
   * @throws IOException
   */
  public RefCounted<SolrIndexSearcher> getSearcher(boolean forceNew, boolean returnSearcher, final Future[] waitSearcher, boolean updateHandlerReopens) throws IOException {
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
        log.info(logid+"PERFORMANCE WARNING: Overlapping onDeckSearchers=" + onDeckSearchers);
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

      // warm the new searcher based on the current searcher.
      // should this go before the other event handlers or after?
      if (currSearcher != null) {
        future = searcherExecutor.submit(
            new Callable() {
              public Object call() throws Exception {
                try {
                  newSearcher.warm(currSearcher);
                } catch (Throwable e) {
                  SolrException.log(log,e);
                }
                return null;
              }
            }
        );
      }

      if (currSearcher==null && firstSearcherListeners.size() > 0) {
        future = searcherExecutor.submit(
            new Callable() {
              public Object call() throws Exception {
                try {
                  for (SolrEventListener listener : firstSearcherListeners) {
                    listener.newSearcher(newSearcher,null);
                  }
                } catch (Throwable e) {
                  SolrException.log(log,null,e);
                }
                return null;
              }
            }
        );
      }

      if (currSearcher!=null && newSearcherListeners.size() > 0) {
        future = searcherExecutor.submit(
            new Callable() {
              public Object call() throws Exception {
                try {
                  for (SolrEventListener listener : newSearcherListeners) {
                    listener.newSearcher(newSearcher, currSearcher);
                  }
                } catch (Throwable e) {
                  SolrException.log(log,null,e);
                }
                return null;
              }
            }
        );
      }

      // WARNING: this code assumes a single threaded executor (that all tasks
      // queued will finish first).
      final RefCounted<SolrIndexSearcher> currSearcherHolderF = currSearcherHolder;
      if (!alreadyRegistered) {
        future = searcherExecutor.submit(
            new Callable() {
              public Object call() throws Exception {
                try {
                  // registerSearcher will decrement onDeckSearchers and
                  // do a notify, even if it fails.
                  registerSearcher(newSearchHolder);
                } catch (Throwable e) {
                  SolrException.log(log, e);
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
        } catch (Throwable e) {
          // do not allow decref() operations to fail since they are typically called in finally blocks
          // and throwing another exception would be very unexpected.
          SolrException.log(log, "Error closing searcher:", e);
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
  private void registerSearcher(RefCounted<SolrIndexSearcher> newSearcherHolder) throws IOException {
    synchronized (searcherLock) {
      try {
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

      } catch (Throwable e) {
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
    // setup response header and handle request
    final NamedList<Object> responseHeader = new SimpleOrderedMap<Object>();
    rsp.add("responseHeader", responseHeader);

    // toLog is a local ref to the same NamedList used by the request
    NamedList<Object> toLog = rsp.getToLog();
    // for back compat, we set these now just in case other code
    // are expecting them during handleRequest
    toLog.add("webapp", req.getContext().get("webapp"));
    toLog.add("path", req.getContext().get("path"));
    toLog.add("params", "{" + req.getParamString() + "}");
    
    handler.handleRequest(req,rsp);
    setResponseHeaderValues(handler,req,rsp);

    if (log.isInfoEnabled()) {
      StringBuilder sb = new StringBuilder(logid);
      for (int i=0; i<toLog.size(); i++) {
        String name = toLog.getName(i);
        Object val = toLog.getVal(i);
        sb.append(name).append("=").append(val).append(" ");
      }
      log.info(sb.toString());
    }

  }


  
  public static void setResponseHeaderValues(SolrRequestHandler handler, SolrQueryRequest req, SolrQueryResponse rsp) {
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
    rsp.getToLog().add("status",status);
    rsp.getToLog().add("QTime",qtime);
    
    SolrParams params = req.getParams();
    if( params.getBool(CommonParams.HEADER_ECHO_HANDLER, false) ) {
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
  private final Map<String, QueryResponseWriter> responseWriters = new HashMap<String, QueryResponseWriter>();
  public static final Map<String ,QueryResponseWriter> DEFAULT_RESPONSE_WRITERS ;
  static{
    HashMap<String, QueryResponseWriter> m= new HashMap<String, QueryResponseWriter>();
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
    DEFAULT_RESPONSE_WRITERS = Collections.unmodifiableMap(m);
  }
  
  /** Configure the query response writers. There will always be a default writer; additional
   * writers may also be configured. */
  private void initWriters() {
    // use link map so we iterate in the same order
    Map<PluginInfo,QueryResponseWriter> writers = new LinkedHashMap<PluginInfo,QueryResponseWriter>();
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
          defaultResponseWriter = writer;
          if(defaultResponseWriter != null)
            log.warn("Multiple default queryResponseWriter registered ignoring: " + old.getClass().getName());
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

    for (Map.Entry<String, QueryResponseWriter> entry : DEFAULT_RESPONSE_WRITERS.entrySet()) {
      if(responseWriters.get(entry.getKey()) == null) responseWriters.put(entry.getKey(), entry.getValue());
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

  private final Map<String, QParserPlugin> qParserPlugins = new HashMap<String, QParserPlugin>();

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
       }
     } catch (Exception e) {
       throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
     }
    }
  }

  public QParserPlugin getQueryPlugin(String parserName) {
    QParserPlugin plugin = qParserPlugins.get(parserName);
    if (plugin != null) return plugin;
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown query type '"+parserName+"'");
  }
  
  private final HashMap<String, ValueSourceParser> valueSourceParsers = new HashMap<String, ValueSourceParser>();
  
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
  

  private final HashMap<String, TransformerFactory> transformerFactories = new HashMap<String, TransformerFactory>();
  
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
    List<T> result = new ArrayList<T>();
    for (PluginInfo info : pluginInfos) result.add(createInitInstance(info,type, type.getSimpleName(), defClassName));
    return result;
  }

  /**
   *
   * @param registry The map to which the instance should be added to. The key is the name attribute
   * @param type The type of the Plugin. These should be standard ones registerd by type.getName() in SolrConfig
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
        NamedList<String> invariants = new NamedList<String>();
        
        // Hide everything...
        Set<String> hide = new HashSet<String>();

        for (String file : solrConfig.getResourceLoader().listConfigDir()) {
          hide.add(file.toUpperCase(Locale.ENGLISH));
        }    
        
        // except the "gettable" list
        StringTokenizer st = new StringTokenizer( gettable );
        while( st.hasMoreTokens() ) {
          hide.remove( st.nextToken().toUpperCase(Locale.ENGLISH) );
        }
        for( String s : hide ) {
          invariants.add( ShowFileRequestHandler.HIDDEN, s );
        }
        
        NamedList<Object> args = new NamedList<Object>();
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

  public CoreDescriptor getCoreDescriptor() {
    return coreDescriptor;
  }

  public IndexDeletionPolicyWrapper getDeletionPolicy(){
    return solrDelPolicy;
  }

  /////////////////////////////////////////////////////////////////////
  // SolrInfoMBean stuff: Statistics and Module Info
  /////////////////////////////////////////////////////////////////////

  public String getVersion() {
    return SolrCore.version;
  }

  public String getDescription() {
    return "SolrCore";
  }

  public Category getCategory() {
    return Category.CORE;
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public URL[] getDocs() {
    return null;
  }

  public NamedList getStatistics() {
    NamedList<Object> lst = new SimpleOrderedMap<Object>();
    lst.add("coreName", name==null ? "(null)" : name);
    lst.add("startTime", new Date(startTime));
    lst.add("refCount", getOpenCount());

    if (null != getCoreDescriptor() && null != getCoreDescriptor().getCoreContainer()) {
      lst.add("aliases", getCoreDescriptor().getCoreContainer().getCoreNames(this));
    }
    return lst;
  }
  
  public Codec getCodec() {
    return codec;
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



