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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.RegexFileFilter;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.PingRequestHandler;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.transform.TransformerFactory;

import org.apache.solr.search.CacheConfig;
import org.apache.solr.search.FastLRUCache;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.update.SolrIndexConfig;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.spelling.QueryConverter;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.util.Version;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.FileFilter;
import java.io.IOException;


/**
 * Provides a static reference to a Config object modeling the main
 * configuration data for a a Solr instance -- typically found in
 * "solrconfig.xml".
 *
 * @version $Id$
 */
public class SolrConfig extends Config {

  public static final Logger log = LoggerFactory.getLogger(SolrConfig.class);
  
  public static final String DEFAULT_CONF_FILE = "solrconfig.xml";


  /**
   * Singleton keeping track of configuration errors
   *
   * @deprecated All exceptions encountered during config parsing are now thrown by the respective constructors, preventing initialization.
   */
  @Deprecated
  public static final Collection<Throwable> severeErrors = new HashSet<Throwable>();

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
  
   /** Creates a configuration instance from a resource loader, a configuration name and a stream.
   * If the stream is null, the resource loader will open the configuration stream.
   * If the stream is not null, no attempt to load the resource will occur (the name is not used).
   *@param loader the resource loader
   *@param name the configuration name
   *@param is the configuration stream
   */
  SolrConfig(SolrResourceLoader loader, String name, InputSource is)
  throws ParserConfigurationException, IOException, SAXException {
    super(loader, name, is, "/config/");
    initLibs();
    luceneMatchVersion = getLuceneVersion("luceneMatchVersion");
    defaultIndexConfig = new SolrIndexConfig(this, null, null);
    mainIndexConfig = new SolrIndexConfig(this, "mainIndex", defaultIndexConfig);
    reopenReaders = getBool("mainIndex/reopenReaders", true);
    
    booleanQueryMaxClauseCount = getInt("query/maxBooleanClauses", BooleanQuery.getMaxClauseCount());
    log.info("Using Lucene MatchVersion: " + luceneMatchVersion);

    filtOptEnabled = getBool("query/boolTofilterOptimizer/@enabled", false);
    filtOptCacheSize = getInt("query/boolTofilterOptimizer/@cacheSize",32);
    filtOptThreshold = getFloat("query/boolTofilterOptimizer/@threshold",.05f);
    
    useFilterForSortedQuery = getBool("query/useFilterForSortedQuery", false);
    queryResultWindowSize = Math.max(1, getInt("query/queryResultWindowSize", 1));
    queryResultMaxDocsCached = getInt("query/queryResultMaxDocsCached", Integer.MAX_VALUE);
    enableLazyFieldLoading = getBool("query/enableLazyFieldLoading", false);

    
    filterCacheConfig = CacheConfig.getConfig(this, "query/filterCache");
    queryResultCacheConfig = CacheConfig.getConfig(this, "query/queryResultCache");
    documentCacheConfig = CacheConfig.getConfig(this, "query/documentCache");
    CacheConfig conf = CacheConfig.getConfig(this, "query/fieldValueCache");
    if (conf == null) {
      Map<String,String> args = new HashMap<String,String>();
      args.put("name","fieldValueCache");
      args.put("size","10000");
      args.put("initialSize","10");
      args.put("showItems","-1");
      conf = new CacheConfig(FastLRUCache.class, args, null);
    }
    fieldValueCacheConfig = conf;
    unlockOnStartup = getBool("mainIndex/unlockOnStartup", false);
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

     loadPluginInfo(SolrRequestHandler.class,"requestHandler",true, true);
     loadPluginInfo(QParserPlugin.class,"queryParser",true, true);
     loadPluginInfo(QueryResponseWriter.class,"queryResponseWriter",true, true);
     loadPluginInfo(ValueSourceParser.class,"valueSourceParser",true, true);
     loadPluginInfo(TransformerFactory.class,"transformer",true, true);
     loadPluginInfo(SearchComponent.class,"searchComponent",true, true);
     loadPluginInfo(QueryConverter.class,"queryConverter",true, true);

     // this is hackish, since it picks up all SolrEventListeners,
     // regardless of when/how/why thye are used (or even if they are 
     // declared outside of the appropriate context) but there's no nice 
     // way arround that in the PluginInfo framework
     loadPluginInfo(SolrEventListener.class, "//listener",false, true);

     loadPluginInfo(DirectoryFactory.class,"directoryFactory",false, true);
     loadPluginInfo(IndexDeletionPolicy.class,"mainIndex/deletionPolicy",false, true);
     loadPluginInfo(IndexReaderFactory.class,"indexReaderFactory",false, true);
     loadPluginInfo(UpdateRequestProcessorChain.class,"updateRequestProcessorChain",false, false);

     updateHandlerInfo = loadUpdatehandlerInfo();

    Config.log.info("Loaded SolrConfig: " + name);
  }

  protected UpdateHandlerInfo loadUpdatehandlerInfo() {
    return new UpdateHandlerInfo(get("updateHandler/@class",null),
            getInt("updateHandler/autoCommit/maxDocs",-1),
            getInt("updateHandler/autoCommit/maxTime",-1),
            getInt("updateHandler/commitIntervalLowerBound",-1));
  }

  private void loadPluginInfo(Class clazz, String tag, boolean requireName, boolean requireClass) {
    List<PluginInfo> result = readPluginInfos(tag, requireName, requireClass);
    if(!result.isEmpty()) pluginStore.put(clazz.getName(),result);
  }

  public List<PluginInfo> readPluginInfos(String tag, boolean requireName, boolean requireClass) {
    ArrayList<PluginInfo> result = new ArrayList<PluginInfo>();
    NodeList nodes = (NodeList) evaluate(tag, XPathConstants.NODESET);
    for (int i=0; i<nodes.getLength(); i++) {
      PluginInfo pluginInfo = new PluginInfo(nodes.item(i), "[solrconfig.xml] " + tag, requireName, requireClass);
      if(pluginInfo.isEnabled()) result.add(pluginInfo);
    }
    return result;
  }

  /* The set of materialized parameters: */
  public final int booleanQueryMaxClauseCount;
  // SolrIndexSearcher - nutch optimizer
  public final boolean filtOptEnabled;
  public final int filtOptCacheSize;
  public final float filtOptThreshold;
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
  public final boolean reopenReaders;
  // DocSet
  public final float hashSetInverseLoadFactor;
  public final int hashDocSetMaxSize;
  // default & main index configurations
  public final SolrIndexConfig defaultIndexConfig;
  public final SolrIndexConfig mainIndexConfig;

  protected UpdateHandlerInfo updateHandlerInfo ;

  private Map<String, List<PluginInfo>> pluginStore = new LinkedHashMap<String, List<PluginInfo>>();

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
          return valueOf(s.toUpperCase(Locale.ENGLISH));
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
    public final int autoCommmitMaxDocs,autoCommmitMaxTime,commitIntervalLowerBound;

    /**
     * @param className
     * @param autoCommmitMaxDocs set -1 as default
     * @param autoCommmitMaxTime set -1 as default
     * @param commitIntervalLowerBound set -1 as default
     */
    public UpdateHandlerInfo(String className, int autoCommmitMaxDocs, int autoCommmitMaxTime, int commitIntervalLowerBound) {
      this.className = className;
      this.autoCommmitMaxDocs = autoCommmitMaxDocs;
      this.autoCommmitMaxTime = autoCommmitMaxTime;
      this.commitIntervalLowerBound = commitIntervalLowerBound;
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
    return result == null ?
            (List<PluginInfo>) Collections.EMPTY_LIST:
            result; 
  }
  public PluginInfo getPluginInfo(String  type){
    List<PluginInfo> result = pluginStore.get(type);
    return result == null || result.isEmpty() ? null: result.get(0);
  }
  
  private void initLibs() {
    
    NodeList nodes = (NodeList) evaluate("lib", XPathConstants.NODESET);
    if (nodes==null || nodes.getLength()==0)
      return;
    
    log.info("Adding specified lib dirs to ClassLoader");
    
     for (int i=0; i<nodes.getLength(); i++) {
       Node node = nodes.item(i);

       String baseDir = DOMUtil.getAttr(node, "dir");
       String path = DOMUtil.getAttr(node, "path");
       if (null != baseDir) {
         // :TODO: add support for a simpler 'glob' mutually eclusive of regex
         String regex = DOMUtil.getAttr(node, "regex");
         FileFilter filter = (null == regex) ? null : new RegexFileFilter(regex);
         getResourceLoader().addToClassLoader(baseDir, filter);
       } else if (null != path) {
         getResourceLoader().addToClassLoader(path);
       } else {
         throw new RuntimeException
           ("lib: missing mandatory attributes: 'dir' or 'path'");
       }
     }
  }
}
