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


import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.Version;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.rest.RestManager;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.search.CacheConfig;
import org.apache.solr.search.FastLRUCache;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.servlet.SolrRequestParsers;
import org.apache.solr.spelling.QueryConverter;
import org.apache.solr.update.SolrIndexConfig;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.RegexFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.solr.core.SolrConfig.PluginOpts.MULTI_OK;
import static org.apache.solr.core.SolrConfig.PluginOpts.NOOP;
import static org.apache.solr.core.SolrConfig.PluginOpts.REQUIRE_CLASS;
import static org.apache.solr.core.SolrConfig.PluginOpts.REQUIRE_NAME;


/**
 * Provides a static reference to a Config object modeling the main
 * configuration data for a a Solr instance -- typically found in
 * "solrconfig.xml".
 */
public class SolrConfig extends Config {

  public static final Logger log = LoggerFactory.getLogger(SolrConfig.class);
  
  public static final String DEFAULT_CONF_FILE = "solrconfig.xml";

  static enum PluginOpts {
    MULTI_OK, 
    REQUIRE_NAME,
    REQUIRE_CLASS,
    // EnumSet.of and/or EnumSet.copyOf(Collection) are anoying
    // because of type determination
    NOOP
    }

  private int multipartUploadLimitKB;

  private int formUploadLimitKB;

  private boolean enableRemoteStreams;

  private boolean handleSelect;

  private boolean addHttpRequestToContext;

  private final SolrRequestParsers solrRequestParsers;
  
  /** Creates a default instance from the solrconfig.xml. */
  public SolrConfig()
  throws ParserConfigurationException, IOException, SAXException {
    this( (SolrResourceLoader) null, DEFAULT_CONF_FILE, null );
  }
  
  /** Creates a configuration instance from a configuration name.
   * A default resource loader will be created (@see SolrResourceLoader)
   *@param name the configuration name used by the loader
   */
  public SolrConfig(String name)
  throws ParserConfigurationException, IOException, SAXException {
    this( (SolrResourceLoader) null, name, null);
  }

  /** Creates a configuration instance from a configuration name and stream.
   * A default resource loader will be created (@see SolrResourceLoader).
   * If the stream is null, the resource loader will open the configuration stream.
   * If the stream is not null, no attempt to load the resource will occur (the name is not used).
   *@param name the configuration name
   *@param is the configuration stream
   */
  public SolrConfig(String name, InputSource is)
  throws ParserConfigurationException, IOException, SAXException {
    this( (SolrResourceLoader) null, name, is );
  }
  
  /** Creates a configuration instance from an instance directory, configuration name and stream.
   *@param instanceDir the directory used to create the resource loader
   *@param name the configuration name used by the loader if the stream is null
   *@param is the configuration stream 
   */
  public SolrConfig(String instanceDir, String name, InputSource is)
  throws ParserConfigurationException, IOException, SAXException {
    this(new SolrResourceLoader(instanceDir), name, is);
  }

  public static SolrConfig readFromResourceLoader(SolrResourceLoader loader, String name) {
    try {
      return new SolrConfig(loader, name, null);
    }
    catch (Exception e) {
      String resource;
      if (loader instanceof ZkSolrResourceLoader) {
        resource = name;
      } else {
        resource = loader.getConfigDir() + name;
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error loading solr config from " + resource, e);
    }
  }
  
   /** Creates a configuration instance from a resource loader, a configuration name and a stream.
   * If the stream is null, the resource loader will open the configuration stream.
   * If the stream is not null, no attempt to load the resource will occur (the name is not used).
   *@param loader the resource loader
   *@param name the configuration name
   *@param is the configuration stream
   */
  public SolrConfig(SolrResourceLoader loader, String name, InputSource is)
  throws ParserConfigurationException, IOException, SAXException {
    super(loader, name, is, "/config/");
    initLibs();
    luceneMatchVersion = getLuceneVersion("luceneMatchVersion");
    String indexConfigPrefix;

    // Old indexDefaults and mainIndex sections are deprecated and fails fast for luceneMatchVersion=>LUCENE_4_0_0.
    // For older solrconfig.xml's we allow the old sections, but never mixed with the new <indexConfig>
    boolean hasDeprecatedIndexConfig = (getNode("indexDefaults", false) != null) || (getNode("mainIndex", false) != null);
    boolean hasNewIndexConfig = getNode("indexConfig", false) != null;
    if(hasDeprecatedIndexConfig){
      if(luceneMatchVersion.onOrAfter(Version.LUCENE_4_0_0_ALPHA)) {
        throw new SolrException(ErrorCode.FORBIDDEN, "<indexDefaults> and <mainIndex> configuration sections are discontinued. Use <indexConfig> instead.");
      } else {
        // Still allow the old sections for older LuceneMatchVersion's
        if(hasNewIndexConfig) {
          throw new SolrException(ErrorCode.FORBIDDEN, "Cannot specify both <indexDefaults>, <mainIndex> and <indexConfig> at the same time. Please use <indexConfig> only.");
        }
        log.warn("<indexDefaults> and <mainIndex> configuration sections are deprecated and will fail for luceneMatchVersion=LUCENE_4_0_0 and later. Please use <indexConfig> instead.");
        defaultIndexConfig = new SolrIndexConfig(this, "indexDefaults", null);
        mainIndexConfig = new SolrIndexConfig(this, "mainIndex", defaultIndexConfig);
        indexConfigPrefix = "mainIndex";
      }
    } else {
      defaultIndexConfig = mainIndexConfig = null;
      indexConfigPrefix = "indexConfig";
    }
    nrtMode = getBool(indexConfigPrefix+"/nrtMode", true);
    // Parse indexConfig section, using mainIndex as backup in case old config is used
    indexConfig = new SolrIndexConfig(this, "indexConfig", mainIndexConfig);

    booleanQueryMaxClauseCount = getInt("query/maxBooleanClauses", BooleanQuery.getMaxClauseCount());
    log.info("Using Lucene MatchVersion: " + luceneMatchVersion);

    // Warn about deprecated / discontinued parameters
    // boolToFilterOptimizer has had no effect since 3.1
    if(get("query/boolTofilterOptimizer", null) != null)
      log.warn("solrconfig.xml: <boolTofilterOptimizer> is currently not implemented and has no effect.");
    if(get("query/HashDocSet", null) != null)
      log.warn("solrconfig.xml: <HashDocSet> is deprecated and no longer recommended used.");

// TODO: Old code - in case somebody wants to re-enable. Also see SolrIndexSearcher#search()
//    filtOptEnabled = getBool("query/boolTofilterOptimizer/@enabled", false);
//    filtOptCacheSize = getInt("query/boolTofilterOptimizer/@cacheSize",32);
//    filtOptThreshold = getFloat("query/boolTofilterOptimizer/@threshold",.05f);

    useFilterForSortedQuery = getBool("query/useFilterForSortedQuery", false);
    queryResultWindowSize = Math.max(1, getInt("query/queryResultWindowSize", 1));
    queryResultMaxDocsCached = getInt("query/queryResultMaxDocsCached", Integer.MAX_VALUE);
    enableLazyFieldLoading = getBool("query/enableLazyFieldLoading", false);


    filterCacheConfig = CacheConfig.getConfig(this, "query/filterCache");
    queryResultCacheConfig = CacheConfig.getConfig(this, "query/queryResultCache");
    documentCacheConfig = CacheConfig.getConfig(this, "query/documentCache");
    CacheConfig conf = CacheConfig.getConfig(this, "query/fieldValueCache");
    if (conf == null) {
      Map<String,String> args = new HashMap<>();
      args.put("name","fieldValueCache");
      args.put("size","10000");
      args.put("initialSize","10");
      args.put("showItems","-1");
      conf = new CacheConfig(FastLRUCache.class, args, null);
    }
    fieldValueCacheConfig = conf;
    unlockOnStartup = getBool(indexConfigPrefix+"/unlockOnStartup", false);
    useColdSearcher = getBool("query/useColdSearcher",false);
    dataDir = get("dataDir", null);
    if (dataDir != null && dataDir.length()==0) dataDir=null;

    userCacheConfigs = CacheConfig.getMultipleConfigs(this, "query/cache");

    org.apache.solr.search.SolrIndexSearcher.initRegenerators(this);

    hashSetInverseLoadFactor = 1.0f / getFloat("//HashDocSet/@loadFactor",0.75f);
    hashDocSetMaxSize= getInt("//HashDocSet/@maxSize",3000);

    httpCachingConfig = new HttpCachingConfig(this);

    Node jmx = getNode("jmx", false);
    if (jmx != null) {
      jmxConfig = new JmxConfiguration(true,
                                       get("jmx/@agentId", null),
                                       get("jmx/@serviceUrl", null),
                                       get("jmx/@rootName", null));

    } else {
      jmxConfig = new JmxConfiguration(false, null, null, null);
    }
     maxWarmingSearchers = getInt("query/maxWarmingSearchers",Integer.MAX_VALUE);

     loadPluginInfo(SolrRequestHandler.class,"requestHandler",
                    REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK);
     loadPluginInfo(QParserPlugin.class,"queryParser",
                    REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK);
     loadPluginInfo(QueryResponseWriter.class,"queryResponseWriter",
                    REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK);
     loadPluginInfo(ValueSourceParser.class,"valueSourceParser",
                    REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK);
     loadPluginInfo(TransformerFactory.class,"transformer",
                    REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK);
     loadPluginInfo(SearchComponent.class,"searchComponent",
                    REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK);

     // TODO: WTF is up with queryConverter???
     // it aparently *only* works as a singleton? - SOLR-4304
     // and even then -- only if there is a single SpellCheckComponent
     // because of queryConverter.setIndexAnalyzer
     loadPluginInfo(QueryConverter.class,"queryConverter",
                    REQUIRE_NAME, REQUIRE_CLASS);

     // this is hackish, since it picks up all SolrEventListeners,
     // regardless of when/how/why they are used (or even if they are
     // declared outside of the appropriate context) but there's no nice
     // way around that in the PluginInfo framework
     loadPluginInfo(SolrEventListener.class, "//listener",
                    REQUIRE_CLASS, MULTI_OK);

     loadPluginInfo(DirectoryFactory.class,"directoryFactory",
                    REQUIRE_CLASS);
     loadPluginInfo(IndexDeletionPolicy.class,indexConfigPrefix+"/deletionPolicy",
                    REQUIRE_CLASS);
     loadPluginInfo(CodecFactory.class,"codecFactory",
                    REQUIRE_CLASS);
     loadPluginInfo(IndexReaderFactory.class,"indexReaderFactory",
                    REQUIRE_CLASS);
     loadPluginInfo(UpdateRequestProcessorChain.class,"updateRequestProcessorChain",
                    MULTI_OK);
     loadPluginInfo(UpdateLog.class,"updateHandler/updateLog");
     loadPluginInfo(IndexSchemaFactory.class,"schemaFactory",
                    REQUIRE_CLASS);
     loadPluginInfo(RestManager.class, "restManager");
     updateHandlerInfo = loadUpdatehandlerInfo();
     
     multipartUploadLimitKB = getInt( 
         "requestDispatcher/requestParsers/@multipartUploadLimitInKB", 2048 );
     
     formUploadLimitKB = getInt( 
         "requestDispatcher/requestParsers/@formdataUploadLimitInKB", 2048 );
     
     enableRemoteStreams = getBool( 
         "requestDispatcher/requestParsers/@enableRemoteStreaming", false ); 
 
     // Let this filter take care of /select?xxx format
     handleSelect = getBool( 
         "requestDispatcher/@handleSelect", true ); 
     
     addHttpRequestToContext = getBool( 
         "requestDispatcher/requestParsers/@addHttpRequestToContext", false );

    loadPluginInfo(InitParams.class, InitParams.TYPE, MULTI_OK);
    List<PluginInfo> argsInfos =  pluginStore.get(InitParams.class.getName()) ;
    if(argsInfos!=null){
      Map<String,InitParams> argsMap = new HashMap<>();
      for (PluginInfo p : argsInfos) {
        InitParams args = new InitParams(p);
        argsMap.put(args.name == null ? String.valueOf(args.hashCode()) : args.name, args);
      }
      this.initParams = Collections.unmodifiableMap(argsMap);

    }

    solrRequestParsers = new SolrRequestParsers(this);
    Config.log.info("Loaded SolrConfig: " + name);
  }
  private Map<String,InitParams> initParams = Collections.emptyMap();
  public Map<String, InitParams> getInitParams() {
    return initParams;
  }
  protected UpdateHandlerInfo loadUpdatehandlerInfo() {
    return new UpdateHandlerInfo(get("updateHandler/@class",null),
            getInt("updateHandler/autoCommit/maxDocs",-1),
            getInt("updateHandler/autoCommit/maxTime",-1),
            getBool("updateHandler/indexWriter/closeWaitsForMerges",true),
            getBool("updateHandler/autoCommit/openSearcher",true),
            getInt("updateHandler/commitIntervalLowerBound",-1),
            getInt("updateHandler/autoSoftCommit/maxDocs",-1),
            getInt("updateHandler/autoSoftCommit/maxTime",-1),
            getBool("updateHandler/commitWithin/softCommit",true));
  }

  private void loadPluginInfo(Class clazz, String tag, PluginOpts... opts) {
    EnumSet<PluginOpts> options = EnumSet.<PluginOpts>of(NOOP, opts);
    boolean requireName = options.contains(REQUIRE_NAME);
    boolean requireClass = options.contains(REQUIRE_CLASS);

    List<PluginInfo> result = readPluginInfos(tag, requireName, requireClass);

    if (1 < result.size() && ! options.contains(MULTI_OK)) {
        throw new SolrException
          (SolrException.ErrorCode.SERVER_ERROR,
           "Found " + result.size() + " configuration sections when at most "
           + "1 is allowed matching expression: " + tag);
    }
    if(!result.isEmpty()) pluginStore.put(clazz.getName(),result);
  }

  public List<PluginInfo> readPluginInfos(String tag, boolean requireName, boolean requireClass) {
    ArrayList<PluginInfo> result = new ArrayList<>();
    NodeList nodes = (NodeList) evaluate(tag, XPathConstants.NODESET);
    for (int i=0; i<nodes.getLength(); i++) {
      PluginInfo pluginInfo = new PluginInfo(nodes.item(i), "[solrconfig.xml] " + tag, requireName, requireClass);
      if(pluginInfo.isEnabled()) result.add(pluginInfo);
    }
    return result;
  }
  
  public SolrRequestParsers getRequestParsers() {
    return solrRequestParsers;
  }

  /* The set of materialized parameters: */
  public final int booleanQueryMaxClauseCount;
// SolrIndexSearcher - nutch optimizer -- Disabled since 3.1
//  public final boolean filtOptEnabled;
//  public final int filtOptCacheSize;
//  public final float filtOptThreshold;
  // SolrIndexSearcher - caches configurations
  public final CacheConfig filterCacheConfig ;
  public final CacheConfig queryResultCacheConfig;
  public final CacheConfig documentCacheConfig;
  public final CacheConfig fieldValueCacheConfig;
  public final CacheConfig[] userCacheConfigs;
  // SolrIndexSearcher - more...
  public final boolean useFilterForSortedQuery;
  public final int queryResultWindowSize;
  public final int queryResultMaxDocsCached;
  public final boolean enableLazyFieldLoading;
  public final boolean nrtMode;
  // DocSet
  public final float hashSetInverseLoadFactor;
  public final int hashDocSetMaxSize;
  // default & main index configurations, deprecated as of 3.6
  @Deprecated
  public final SolrIndexConfig defaultIndexConfig;
  @Deprecated
  public final SolrIndexConfig mainIndexConfig;
  // IndexConfig settings
  public final SolrIndexConfig indexConfig;

  protected UpdateHandlerInfo updateHandlerInfo ;

  private Map<String, List<PluginInfo>> pluginStore = new LinkedHashMap<>();

  public final int maxWarmingSearchers;
  public final boolean unlockOnStartup;
  public final boolean useColdSearcher;
  public final Version luceneMatchVersion;
  protected String dataDir;
  
  //JMX configuration
  public final JmxConfiguration jmxConfig;
  
  private final HttpCachingConfig httpCachingConfig;
  public HttpCachingConfig getHttpCachingConfig() {
    return httpCachingConfig;
  }

  public static class JmxConfiguration {
    public boolean enabled = false;
    public String agentId;
    public String serviceUrl;
    public String rootName;

    public JmxConfiguration(boolean enabled, 
                            String agentId, 
                            String serviceUrl,
                            String rootName) {
      this.enabled = enabled;
      this.agentId = agentId;
      this.serviceUrl = serviceUrl;
      this.rootName = rootName;

      if (agentId != null && serviceUrl != null) {
        throw new SolrException
          (SolrException.ErrorCode.SERVER_ERROR,
           "Incorrect JMX Configuration in solrconfig.xml, "+
           "both agentId and serviceUrl cannot be specified at the same time");
      }
      
    }
  }

  public static class HttpCachingConfig {

    /** config xpath prefix for getting HTTP Caching options */
    private final static String CACHE_PRE
      = "requestDispatcher/httpCaching/";
    
    /** For extracting Expires "ttl" from <cacheControl> config */
    private final static Pattern MAX_AGE
      = Pattern.compile("\\bmax-age=(\\d+)");
    
    public static enum LastModFrom {
      OPENTIME, DIRLASTMOD, BOGUS;

      /** Input must not be null */
      public static LastModFrom parse(final String s) {
        try {
          return valueOf(s.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
          log.warn( "Unrecognized value for lastModFrom: " + s, e);
          return BOGUS;
        }
      }
    }
    
    private final boolean never304;
    private final String etagSeed;
    private final String cacheControlHeader;
    private final Long maxAge;
    private final LastModFrom lastModFrom;
    
    private HttpCachingConfig(SolrConfig conf) {

      never304 = conf.getBool(CACHE_PRE+"@never304", false);
      
      etagSeed = conf.get(CACHE_PRE+"@etagSeed", "Solr");
      

      lastModFrom = LastModFrom.parse(conf.get(CACHE_PRE+"@lastModFrom",
                                               "openTime"));
      
      cacheControlHeader = conf.get(CACHE_PRE+"cacheControl",null);

      Long tmp = null; // maxAge
      if (null != cacheControlHeader) {
        try { 
          final Matcher ttlMatcher = MAX_AGE.matcher(cacheControlHeader);
          final String ttlStr = ttlMatcher.find() ? ttlMatcher.group(1) : null;
          tmp = (null != ttlStr && !"".equals(ttlStr))
            ? Long.valueOf(ttlStr)
            : null;
        } catch (Exception e) {
          log.warn( "Ignoring exception while attempting to " +
                    "extract max-age from cacheControl config: " +
                    cacheControlHeader, e);
        }
      }
      maxAge = tmp;

    }
    
    public boolean isNever304() { return never304; }
    public String getEtagSeed() { return etagSeed; }
    /** null if no Cache-Control header */
    public String getCacheControlHeader() { return cacheControlHeader; }
    /** null if no max age limitation */
    public Long getMaxAge() { return maxAge; }
    public LastModFrom getLastModFrom() { return lastModFrom; }
  }

  public static class UpdateHandlerInfo{
    public final String className;
    public final int autoCommmitMaxDocs,autoCommmitMaxTime,commitIntervalLowerBound,
        autoSoftCommmitMaxDocs,autoSoftCommmitMaxTime;
    public final boolean indexWriterCloseWaitsForMerges;
    public final boolean openSearcher;  // is opening a new searcher part of hard autocommit?
    public final boolean commitWithinSoftCommit;

    /**
     * @param autoCommmitMaxDocs set -1 as default
     * @param autoCommmitMaxTime set -1 as default
     * @param commitIntervalLowerBound set -1 as default
     */
    public UpdateHandlerInfo(String className, int autoCommmitMaxDocs, int autoCommmitMaxTime, boolean indexWriterCloseWaitsForMerges, boolean openSearcher, int commitIntervalLowerBound,
        int autoSoftCommmitMaxDocs, int autoSoftCommmitMaxTime, boolean commitWithinSoftCommit) {
      this.className = className;
      this.autoCommmitMaxDocs = autoCommmitMaxDocs;
      this.autoCommmitMaxTime = autoCommmitMaxTime;
      this.indexWriterCloseWaitsForMerges = indexWriterCloseWaitsForMerges;
      this.openSearcher = openSearcher;
      this.commitIntervalLowerBound = commitIntervalLowerBound;
      
      this.autoSoftCommmitMaxDocs = autoSoftCommmitMaxDocs;
      this.autoSoftCommmitMaxTime = autoSoftCommmitMaxTime;
      
      this.commitWithinSoftCommit = commitWithinSoftCommit;
    } 
  }

//  public Map<String, List<PluginInfo>> getUpdateProcessorChainInfo() { return updateProcessorChainInfo; }

  public UpdateHandlerInfo getUpdateHandlerInfo() { return updateHandlerInfo; }

  public String getDataDir() { return dataDir; }

  /**SolrConfig keeps a repository of plugins by the type. The known interfaces are the types.
   * @param type The key is FQN of the plugin class there are a few  known types : SolrFormatter, SolrFragmenter
   * SolrRequestHandler,QParserPlugin, QueryResponseWriter,ValueSourceParser,
   * SearchComponent, QueryConverter, SolrEventListener, DirectoryFactory,
   * IndexDeletionPolicy, IndexReaderFactory, {@link TransformerFactory}
   */
  public List<PluginInfo> getPluginInfos(String  type){
    List<PluginInfo> result = pluginStore.get(type);
    return result == null ? Collections.<PluginInfo>emptyList(): result; 
  }
  public PluginInfo getPluginInfo(String  type){
    List<PluginInfo> result = pluginStore.get(type);
    if (result == null || result.isEmpty()) {
      return null;
    }
    if (1 == result.size()) {
      return result.get(0);
    }

    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                            "Multiple plugins configured for type: " + type);
  }
  
  private void initLibs() {
    NodeList nodes = (NodeList) evaluate("lib", XPathConstants.NODESET);
    if (nodes == null || nodes.getLength() == 0) return;
    
    log.info("Adding specified lib dirs to ClassLoader");
    SolrResourceLoader loader = getResourceLoader();
    
    try {
      for (int i = 0; i < nodes.getLength(); i++) {
        Node node = nodes.item(i);
        
        String baseDir = DOMUtil.getAttr(node, "dir");
        String path = DOMUtil.getAttr(node, "path");
        if (null != baseDir) {
          // :TODO: add support for a simpler 'glob' mutually exclusive of regex
          String regex = DOMUtil.getAttr(node, "regex");
          FileFilter filter = (null == regex) ? null : new RegexFileFilter(regex);
          loader.addToClassLoader(baseDir, filter, false);
        } else if (null != path) {
          final File file = FileUtils.resolvePath(new File(loader.getInstanceDir()), path);
          loader.addToClassLoader(file.getParent(), new FileFilter() {
            @Override
            public boolean accept(File pathname) {
              return pathname.equals(file);
            }
          }, false);
        } else {
          throw new RuntimeException(
              "lib: missing mandatory attributes: 'dir' or 'path'");
        }
      }
    } finally {
      loader.reloadLuceneSPI();
    }
  }
  
  public int getMultipartUploadLimitKB() {
    return multipartUploadLimitKB;
  }

  public int getFormUploadLimitKB() {
    return formUploadLimitKB;
  }

  public boolean isHandleSelect() {
    return handleSelect;
  }

  public boolean isAddHttpRequestToContext() {
    return addHttpRequestToContext;
  }

  public boolean isEnableRemoteStreams() {
    return enableRemoteStreams;
  }


}
