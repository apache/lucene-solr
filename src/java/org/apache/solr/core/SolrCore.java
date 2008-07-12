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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CommonParams.EchoParamStyle;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.*;
import org.apache.solr.highlight.DefaultSolrHighlighter;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.request.*;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.update.DirectUpdateHandler;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.processor.ChainedUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.AbstractPluginLoader;
import org.apache.solr.util.plugin.NamedListPluginLoader;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;


/**
 * @version $Id$
 */
public final class SolrCore {
  public static final String version="1.0";  

  public static Logger log = Logger.getLogger(SolrCore.class.getName());

  private String name;
  private String logid; // used to show what name is set
  
  private final SolrConfig solrConfig;
  private final IndexSchema schema;
  private final String dataDir;
  private final UpdateHandler updateHandler;
  private final long startTime;
  private final RequestHandlers reqHandlers;
  private final SolrHighlighter highlighter;
  private final Map<String,SearchComponent> searchComponents;
  private final Map<String,UpdateRequestProcessorFactory> updateProcessors;
  private final Map<String,SolrInfoMBean> infoRegistry = new java.util.HashMap<String,SolrInfoMBean>();
  
  public long getStartTime() { return startTime; }

  @Deprecated
  private static SolrCore instance;

  static int boolean_query_max_clause_count = Integer.MIN_VALUE;
  // only change the BooleanQuery maxClauseCount once for ALL cores...
  void booleanQueryMaxClauseCount()  {
    synchronized(SolrCore.class) {
      if (boolean_query_max_clause_count == Integer.MIN_VALUE) {
        boolean_query_max_clause_count = solrConfig.booleanQueryMaxClauseCount;
        BooleanQuery.setMaxClauseCount(boolean_query_max_clause_count);
      } else if (boolean_query_max_clause_count != solrConfig.booleanQueryMaxClauseCount ) {
        log.fine("BooleanQuery.maxClauseCount= " +boolean_query_max_clause_count+ ", ignoring " +solrConfig.booleanQueryMaxClauseCount);
      }
    }
  }

  
  /**
   * The SolrResourceLoader used to load all resources for this core.
   * @since solr 1.3
   */
  public SolrResourceLoader getResourceLoader() {
    return solrConfig.getResourceLoader();
  }

  /**
   * Gets the configuration resource name used by this core instance.
   * @since solr 1.3
   */
  public String getConfigResource() {
    return solrConfig.getResourceName();
  }
  
  /**
   * Gets the configuration resource name used by this core instance.
   * @see #getConfigResource()
   */
  @Deprecated
  public String getConfigFile() {
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
   * Gets the schema resource name used by this core instance.
   * @see #getSchemaResource() 
   */
  @Deprecated
  public String getSchemaFile() {
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
    return dataDir + "index/";
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
   * @since solr 1.3
   */
  public Map<String,SolrInfoMBean> getInfoRegistry() {
    return infoRegistry;
  }
  
  
  public List<SolrEventListener> parseListener(String path) {
    List<SolrEventListener> lst = new ArrayList<SolrEventListener>();
    log.info( logid+"Searching for listeners: " +path);
    NodeList nodes = (NodeList)solrConfig.evaluate(path, XPathConstants.NODESET);
    if (nodes!=null) {
      for (int i=0; i<nodes.getLength(); i++) {
        Node node = nodes.item(i);
        String className = DOMUtil.getAttr(node,"class");
        SolrEventListener listener = createEventListener(className);
        listener.init(DOMUtil.childNodesToNamedList(node));
        lst.add(listener);
        log.info( logid+"Added SolrEventListener: " + listener);
      }
    }
    return lst;
  }

  List<SolrEventListener> firstSearcherListeners;
  List<SolrEventListener> newSearcherListeners;
  private void parseListeners() {
    firstSearcherListeners = parseListener("//listener[@event=\"firstSearcher\"]");
    newSearcherListeners = parseListener("//listener[@event=\"newSearcher\"]");
  }
  
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
  public void registerResponseWriter( String name, QueryResponseWriter responseWriter ){
    responseWriters.put(name, responseWriter);
  }


  // gets a non-caching searcher
  public SolrIndexSearcher newSearcher(String name) throws IOException {
    return new SolrIndexSearcher(this, schema, name,getIndexDir(),false);
  }


  void initIndex() {
    try {
      File dirFile = new File(getIndexDir());
      boolean indexExists = dirFile.canRead();

      boolean removeLocks = solrConfig.getBool("mainIndex/unlockOnStartup", false);
      if (indexExists && removeLocks) {
        // to remove locks, the directory must already exist... so we create it
        // if it didn't exist already...
        Directory dir = SolrIndexWriter.getDirectory(getIndexDir(), solrConfig.mainIndexConfig);
        if (dir != null && IndexWriter.isLocked(dir)) {
          log.warning(logid+"WARNING: Solr index directory '" + getIndexDir() + "' is locked.  Unlocking...");
          IndexWriter.unlock(dir);
        }
      }

      // Create the index if it doesn't exist.
      if(!indexExists) {
        log.warning(logid+"Solr index directory '" + dirFile + "' doesn't exist."
                + " Creating new index...");

        SolrIndexWriter writer = new SolrIndexWriter("SolrCore.initIndex",getIndexDir(), true, schema, solrConfig.mainIndexConfig);
        writer.close();
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Creates an instance by trying a constructor that accepts a SolrCore before
   *  trying the default (no arg) constructor.
   *@param className the instance class to create
   *@cast the class or interface that the instance should extend or implement
   *@param msg a message helping compose the exception error if any occurs.
   *@return the desired instance
   *@throws SolrException if the object could not be instantiated
   */
  private <T extends Object> T createInstance(String className, Class<T> cast, String msg) {
    Class clazz = null;
    if (msg == null) msg = "SolrCore Object";
    try {
      try {
        clazz = solrConfig.getResourceLoader().findClass(className);
        if (cast != null && !cast.isAssignableFrom(clazz))
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Error Instantiating "+msg+", "+className+ " is not a " +cast.getName());
        
        java.lang.reflect.Constructor cons = clazz.getConstructor(new Class[]{SolrCore.class});
        return (T) cons.newInstance(new Object[]{this});
      } catch(NoSuchMethodException xnomethod) {
        return (T) clazz.newInstance();
      }
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"Error Instantiating "+msg+", "+className+ " failed to instantiate " +cast.getName(), e);
    }
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
  
  private SolrHighlighter createHighlighter(String className) {
	return createInstance(className, SolrHighlighter.class, "Highlighter");
  }
  
  /** 
   * @return the last core initialized.  If you are using multiple cores, 
   * this is not a function to use.
   */
  @Deprecated
  public static SolrCore getSolrCore() {
    synchronized( SolrCore.class ) {
      if( instance == null ) {
        try {
          // sets 'instance' to the latest solr core
          instance = new SolrCore( null, null, new SolrConfig(), null);
        } catch(Exception xany) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
              "error creating core", xany );
        }
      }
    }
    return instance;
  }
  
  public SolrCore(String dataDir, IndexSchema schema) throws ParserConfigurationException, IOException, SAXException {
    this( null, dataDir, new SolrConfig(), schema );
  }
  
  /**
   * Creates a new core and register it in the list of cores.
   * If a core with the same name already exists, it will be stopped and replaced by this one.
   *@param dataDir the index directory
   *@param config a solr config instance
   *@param schema a solr schema instance
   */
  public SolrCore(String name, String dataDir, SolrConfig config, IndexSchema schema) {
    synchronized (SolrCore.class) {
      // this is for backward compatibility (and also the reason
      // the sync block is needed)
      instance = this;   // set singleton
      this.setName( name );
      SolrResourceLoader loader = config.getResourceLoader();
      if (dataDir == null)
        dataDir = config.get("dataDir",loader.getInstanceDir()+"data/");

      dataDir = SolrResourceLoader.normalizeDir(dataDir);

      log.info(logid+"Opening new SolrCore at " + loader.getInstanceDir() + ", dataDir="+dataDir);

      if (schema==null) {
        schema = new IndexSchema(config, IndexSchema.DEFAULT_SCHEMA_FILE, null);
      }

      this.schema = schema;
      this.dataDir = dataDir;
      this.solrConfig = config;
      this.startTime = System.currentTimeMillis();
      this.maxWarmingSearchers = config.getInt("query/maxWarmingSearchers",Integer.MAX_VALUE);

      booleanQueryMaxClauseCount();
  
      parseListeners();
  
      initIndex();
      
      initWriters();
      initQParsers();
      initValueSourceParsers();
      
      this.searchComponents = loadSearchComponents( config );

      // Processors initialized before the handlers
      updateProcessors = loadUpdateProcessors();
      reqHandlers = new RequestHandlers(this);
      reqHandlers.initHandlersFromConfig( solrConfig );
  
      highlighter = createHighlighter(
    		  solrConfig.get("highlighting/@class", DefaultSolrHighlighter.class.getName())
      );
      highlighter.initalize( solrConfig );

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
  
        updateHandler = createUpdateHandler(
          solrConfig.get("updateHandler/@class", DirectUpdateHandler.class.getName())
        );


        // Finally tell anyone who wants to know
        loader.inform( loader );
        loader.inform( this );

      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } finally {
        // allow firstSearcher events to fire
        latch.countDown();
      }
    } // end synchronized
  }
  


  /**
   * Load the request processors configured in solrconfig.xml
   */
  private Map<String, UpdateRequestProcessorFactory> loadUpdateProcessors() {
    final Map<String,UpdateRequestProcessorFactory> map = new HashMap<String, UpdateRequestProcessorFactory>();
    
    // If this is a more general use-case, this could be a regular type
    final SolrCore thiscore = this;
    AbstractPluginLoader<UpdateRequestProcessorFactory> loader 
      = new AbstractPluginLoader<UpdateRequestProcessorFactory>( "updateRequestProcessor" ) {

      @Override
      protected void init(UpdateRequestProcessorFactory plugin, Node node) throws Exception {
        plugin.init( thiscore, node );
      }

      @Override
      protected UpdateRequestProcessorFactory register(String name, UpdateRequestProcessorFactory plugin) throws Exception {
        return map.put( name, plugin );
      }
    };

    NodeList nodes = (NodeList)solrConfig.evaluate("updateRequestProcessor/factory", XPathConstants.NODESET);
    UpdateRequestProcessorFactory def = loader.load( solrConfig.getResourceLoader(), nodes ); 
    if( def == null ) {
      def = new ChainedUpdateProcessorFactory(); // the default
      def.init( thiscore, null );
    }
    map.put( null, def );
    map.put( "", def );
    return map;
  }
  
  /**
   * @return an update processor registered to the given name.  Throw an exception if this factory is undefined
   */
  public UpdateRequestProcessorFactory getUpdateProcessorFactory( String name )
  {
    UpdateRequestProcessorFactory factory = updateProcessors.get( name );
    if( factory == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "unknown UpdateProcessorFactory: "+name );
    }
    return factory;
  }

  /**
   * Close all resources allocated by the core.
   *  1. searcher
   *  2. updateHandler
   *  3. all CloseHooks will be notified
   */
  public void close() {
    log.info(logid+" CLOSING SolrCore!");
    try {
      closeSearcher();
    } catch (Exception e) {
      SolrException.log(log,e);
    }
    try {
      searcherExecutor.shutdown();
    } catch (Exception e) {
      SolrException.log(log,e);
    }
    try {
      updateHandler.close();
    } catch (Exception e) {
      SolrException.log(log,e);
    }
    if( closeHooks != null ) {
       for( CloseHook hook : closeHooks ) {
         hook.close( this );
       }
     }
  }

  public boolean isClosed() {
    return _searcher == null;
  }
  
  @Override
  protected void finalize() { close(); }

  private List<CloseHook> closeHooks = null;

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

  /**
   * Returns a Request object based on the admin/pingQuery section
   * of the Solr config file.
   * 
   * @use {@link org.apache.solr.handler.PingRequestHandler} instead
   */
  @Deprecated
  public SolrQueryRequest getPingQueryRequest() {
    return solrConfig.getPingQueryRequest(this);
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
   * Returns an unmodifieable Map containing the registered handlers
   */
  public Map<String,SolrRequestHandler> getRequestHandlers() {
    return reqHandlers.getRequestHandlers();
  }

  /**
   * Get the SolrHighlighter
   */
  public SolrHighlighter getHighlighter() {
    return highlighter;
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
  private static Map<String, SearchComponent> loadSearchComponents( SolrConfig config )
  {
    Map<String, SearchComponent> components = new HashMap<String, SearchComponent>();
  
    String xpath = "searchComponent";
    NamedListPluginLoader<SearchComponent> loader = new NamedListPluginLoader<SearchComponent>( xpath, components );
    loader.load( config.getResourceLoader(), (NodeList)config.evaluate( xpath, XPathConstants.NODESET ) );
  
    final Map<String,Class<? extends SearchComponent>> standardcomponents 
        = new HashMap<String, Class<? extends SearchComponent>>();
    standardcomponents.put( QueryComponent.COMPONENT_NAME,        QueryComponent.class        );
    standardcomponents.put( FacetComponent.COMPONENT_NAME,        FacetComponent.class        );
    standardcomponents.put( MoreLikeThisComponent.COMPONENT_NAME, MoreLikeThisComponent.class );
    standardcomponents.put( HighlightComponent.COMPONENT_NAME,    HighlightComponent.class    );
    standardcomponents.put( DebugComponent.COMPONENT_NAME,        DebugComponent.class        );
    for( Map.Entry<String, Class<? extends SearchComponent>> entry : standardcomponents.entrySet() ) {
      if( components.get( entry.getKey() ) == null ) {
        try {
          SearchComponent comp = entry.getValue().newInstance();
          comp.init( null ); // default components initialized with nothing
          components.put( entry.getKey(), comp );
        }
        catch (Exception e) {
          SolrConfig.severeErrors.add( e );
          SolrException.logOnce(log,null,e);
        }
      }
    }
    return components;
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
    return Collections.unmodifiableMap(searchComponents);
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
  // get it (and it will increment the ref count at the same time)
  private RefCounted<SolrIndexSearcher> _searcher;

  final ExecutorService searcherExecutor = Executors.newSingleThreadExecutor();
  private int onDeckSearchers;  // number of searchers preparing
  private Object searcherLock = new Object();  // the sync object for the searcher
  private final int maxWarmingSearchers;  // max number of on-deck searchers allowed


  public RefCounted<SolrIndexSearcher> getSearcher() {
    try {
      return getSearcher(false,true,null);
    } catch (IOException e) {
      SolrException.log(log,null,e);
      return null;
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
   * @param forceNew           if true, force the open of a new index searcher regardless if there is already one open.
   * @param returnSearcher     if true, returns a {@link SolrIndexSearcher} holder with the refcount already incremented.
   * @param waitSearcher       if non-null, will be filled in with a {@link Future} that will return after the new searcher is registered.
   * @throws IOException
   */
  public RefCounted<SolrIndexSearcher> getSearcher(boolean forceNew, boolean returnSearcher, final Future[] waitSearcher) throws IOException {
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
        log.severe(logid+"ERROR!!! onDeckSearchers is " + onDeckSearchers);
        onDeckSearchers=1;  // reset
      } else if (onDeckSearchers > maxWarmingSearchers) {
        onDeckSearchers--;
        String msg="Error opening new searcher. exceeded limit of maxWarmingSearchers="+maxWarmingSearchers + ", try again later.";
        log.warning(logid+""+ msg);
        // HTTP 503==service unavailable, or 409==Conflict
        throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,msg,true);
      } else if (onDeckSearchers > 1) {
        log.info(logid+"PERFORMANCE WARNING: Overlapping onDeckSearchers=" + onDeckSearchers);
      }
    }

    // open the index synchronously
    // if this fails, we need to decrement onDeckSearchers again.
    SolrIndexSearcher tmp;
    try {
      tmp = new SolrIndexSearcher(this, schema, "main", getIndexDir(), true);
    } catch (Throwable th) {
      synchronized(searcherLock) {
        onDeckSearchers--;
        // notify another waiter to continue... it may succeed
        // and wake any others.
        searcherLock.notify();
      }
      // need to close the searcher here??? we shouldn't have to.
      throw new RuntimeException(th);
    }

    final SolrIndexSearcher newSearcher=tmp;

    RefCounted<SolrIndexSearcher> currSearcherHolder=null;
    final RefCounted<SolrIndexSearcher> newSearchHolder=newHolder(newSearcher);
    if (returnSearcher) newSearchHolder.incref();

    // a signal to decrement onDeckSearchers if something goes wrong.
    final boolean[] decrementOnDeckCount=new boolean[1];
    decrementOnDeckCount[0]=true;

    try {

      boolean alreadyRegistered = false;
      synchronized (searcherLock) {
        if (_searcher == null) {
          // if there isn't a current searcher then we may
          // want to register this one before warming is complete instead of waiting.
          if (solrConfig.getBool("query/useColdSearcher",false)) {
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

      //
      // Note! if we registered the new searcher (but didn't increment it's
      // reference count because returnSearcher==false, it's possible for
      // someone else to register another searcher, and thus cause newSearcher
      // to close while we are warming.
      //
      // Should we protect against that by incrementing the reference count?
      // Maybe we should just let it fail?   After all, if returnSearcher==false
      // and newSearcher has been de-registered, what's the point of continuing?
      //

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
                      SolrException.logOnce(log,null,e);
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
                      SolrException.logOnce(log,null,e);
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
                        listener.newSearcher(newSearcher,null);
                      }
                    } catch (Throwable e) {
                      SolrException.logOnce(log,null,e);
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
                      // signal that we no longer need to decrement
                      // the count *before* registering the searcher since
                      // registerSearcher will decrement even if it errors.
                      decrementOnDeckCount[0]=false;
                      registerSearcher(newSearchHolder);
                    } catch (Throwable e) {
                      SolrException.logOnce(log,null,e);
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

      // Return the searcher as the warming tasks run in parallel
      // callers may wait on the waitSearcher future returned.
      return returnSearcher ? newSearchHolder : null;

    } catch (Exception e) {
      SolrException.logOnce(log,null,e);
      if (currSearcherHolder != null) currSearcherHolder.decref();

      synchronized (searcherLock) {
        if (decrementOnDeckCount[0]) {
          onDeckSearchers--;
        }
        if (onDeckSearchers < 0) {
          // sanity check... should never happen
          log.severe(logid+"ERROR!!! onDeckSearchers after decrement=" + onDeckSearchers);
          onDeckSearchers=0; // try and recover
        }
        // if we failed, we need to wake up at least one waiter to continue the process
        searcherLock.notify();
      }

      // since the indexreader was already opened, assume we can continue on
      // even though we got an exception.
      return returnSearcher ? newSearchHolder : null;
    }

  }


  private RefCounted<SolrIndexSearcher> newHolder(SolrIndexSearcher newSearcher) {
    RefCounted<SolrIndexSearcher> holder = new RefCounted<SolrIndexSearcher>(newSearcher) {
      public void close() {
        try {
          resource.close();
        } catch (IOException e) {
          log.severe("Error closing searcher:" + SolrException.toStr(e));
        }
      }
    };
    holder.incref();  // set ref count to 1 to account for this._searcher
    return holder;
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

        newSearcher.register(); // register subitems (caches)
        log.info(logid+"Registered new searcher " + newSearcher);

      } catch (Throwable e) {
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
      if (_searcher != null) {
        _searcher.decref();   // dec refcount for this._searcher
        _searcher=null; // isClosed() does check this
        infoRegistry.remove("currentSearcher");
      }
    }
  }


  public void execute(SolrRequestHandler handler, SolrQueryRequest req, SolrQueryResponse rsp) {
    if (handler==null) {
      log.warning(logid+"Null Request Handler '" + req.getQueryType() +"' :" + req);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,"Null Request Handler '" + req.getQueryType() + "'", true);
    }
    // setup response header and handle request
    final NamedList<Object> responseHeader = new SimpleOrderedMap<Object>();
    rsp.add("responseHeader", responseHeader);
    NamedList toLog = rsp.getToLog();
    //toLog.add("core", getName());
    toLog.add("webapp", req.getContext().get("webapp"));
    toLog.add("path", req.getContext().get("path"));
    toLog.add("params", "{" + req.getParamString() + "}");
    handler.handleRequest(req,rsp);
    setResponseHeaderValues(handler,req,rsp);
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<toLog.size(); i++) {
     	String name = toLog.getName(i);
     	Object val = toLog.getVal(i);
     	sb.append(name).append("=").append(val).append(" ");
    }
    log.info(logid +  sb.toString());
    /*log.info(logid+"" + req.getContext().get("path") + " "
            + req.getParamString()+ " 0 "+
       (int)(rsp.getEndTime() - req.getStartTime()));*/
  }

  @Deprecated
  public void execute(SolrQueryRequest req, SolrQueryResponse rsp) {
    SolrRequestHandler handler = getRequestHandler(req.getQueryType());
    if (handler==null) {
      log.warning(logid+"Unknown Request Handler '" + req.getQueryType() +"' :" + req);
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,"Unknown Request Handler '" + req.getQueryType() + "'", true);
    }
    execute(handler, req, rsp);
  }
  
  protected void setResponseHeaderValues(SolrRequestHandler handler, SolrQueryRequest req, SolrQueryResponse rsp) {
    // TODO should check that responseHeader has not been replaced by handler
	NamedList responseHeader = rsp.getResponseHeader();
    final int qtime=(int)(rsp.getEndTime() - req.getStartTime());
    responseHeader.add("status",rsp.getException()==null ? 0 : 500);
    responseHeader.add("QTime",qtime);
    rsp.getToLog().add("status",rsp.getException()==null ? 0 : 500);
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
    SolrException.logOnce(log,null,e);
  }

  
  
  private QueryResponseWriter defaultResponseWriter;
  private final Map<String, QueryResponseWriter> responseWriters = new HashMap<String, QueryResponseWriter>();
  
  /** Configure the query response writers. There will always be a default writer; additional 
   * writers may also be configured. */
  private void initWriters() {
    String xpath = "queryResponseWriter";
    NodeList nodes = (NodeList) solrConfig.evaluate(xpath, XPathConstants.NODESET);
    
    NamedListPluginLoader<QueryResponseWriter> loader = 
      new NamedListPluginLoader<QueryResponseWriter>( "[solrconfig.xml] "+xpath, responseWriters );
    
    defaultResponseWriter = loader.load( solrConfig.getResourceLoader(), nodes );
    
    // configure the default response writer; this one should never be null
    if (defaultResponseWriter == null) {
      defaultResponseWriter = responseWriters.get("standard");
      if( defaultResponseWriter == null ) {
        defaultResponseWriter = new XMLResponseWriter();
      }
    }

    // make JSON response writers available by default
    if (responseWriters.get("json")==null) {
      responseWriters.put("json", new JSONResponseWriter());
    }
    if (responseWriters.get("python")==null) {
      responseWriters.put("python", new PythonResponseWriter());
    }
    if (responseWriters.get("ruby")==null) {
      responseWriters.put("ruby", new RubyResponseWriter());
    }
    if (responseWriters.get("raw")==null) {
      responseWriters.put("raw", new RawResponseWriter());
    }
    if (responseWriters.get("javabin") == null) {
      responseWriters.put("javabin", new BinaryResponseWriter());
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
    return getQueryResponseWriter(request.getParam("wt")); 
  }

  private final Map<String, QParserPlugin> qParserPlugins = new HashMap<String, QParserPlugin>();

  /** Configure the query parsers. */
  private void initQParsers() {
    String xpath = "queryParser";
    NodeList nodes = (NodeList) solrConfig.evaluate(xpath, XPathConstants.NODESET);

    NamedListPluginLoader<QParserPlugin> loader =
      new NamedListPluginLoader<QParserPlugin>( "[solrconfig.xml] "+xpath, qParserPlugins);

    loader.load( solrConfig.getResourceLoader(), nodes );

    // default parsers
    for (int i=0; i<QParserPlugin.standardPlugins.length; i+=2) {
     try {
       String name = (String)QParserPlugin.standardPlugins[i];
       Class<QParserPlugin> clazz = (Class<QParserPlugin>)QParserPlugin.standardPlugins[i+1];
       QParserPlugin plugin = clazz.newInstance();
       qParserPlugins.put(name, plugin);
       plugin.init(null);
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
    String xpath = "valueSourceParser";
    NodeList nodes = (NodeList) solrConfig.evaluate(xpath, XPathConstants.NODESET);

    NamedListPluginLoader<ValueSourceParser> loader =
      new NamedListPluginLoader<ValueSourceParser>( "[solrconfig.xml] "+xpath, valueSourceParsers);

    loader.load( solrConfig.getResourceLoader(), nodes );

    // default value source parsers
    for (Map.Entry<String, ValueSourceParser> entry : ValueSourceParser.standardValueSourceParsers.entrySet()) {
      try {
        String name = entry.getKey();
        ValueSourceParser valueSourceParser = entry.getValue();
        valueSourceParsers.put(name, valueSourceParser);
        valueSourceParser.init(null);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }
  
  public ValueSourceParser getValueSourceParser(String parserName) {
    return valueSourceParsers.get(parserName);
  }
  
}



