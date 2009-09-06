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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.PingRequestHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;

import org.apache.solr.search.CacheConfig;
import org.apache.solr.search.FastLRUCache;
import org.apache.solr.update.SolrIndexConfig;
import org.apache.lucene.search.BooleanQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.IOException;
import java.io.InputStream;


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
   * Compatibility feature for single-core (pre-solr{215,350} patch); should go away at solr-2.0
   * @deprecated Use {@link SolrCore#getSolrConfig()} instead.
   */
  @Deprecated
  public static SolrConfig config = null; 

  /**
   * Singleton keeping track of configuration errors
   */
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
  public SolrConfig(String name, InputStream is)
  throws ParserConfigurationException, IOException, SAXException {
    this( (SolrResourceLoader) null, name, is );
  }
  
  /** Creates a configuration instance from an instance directory, configuration name and stream.
   *@param instanceDir the directory used to create the resource loader
   *@param name the configuration name used by the loader if the stream is null
   *@param is the configuration stream 
   */
  public SolrConfig(String instanceDir, String name, InputStream is)
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
  SolrConfig(SolrResourceLoader loader, String name, InputStream is)
  throws ParserConfigurationException, IOException, SAXException {
    super(loader, name, is, "/config/");
    defaultIndexConfig = new SolrIndexConfig(this, null, null);
    mainIndexConfig = new SolrIndexConfig(this, "mainIndex", defaultIndexConfig);
    reopenReaders = getBool("mainIndex/reopenReaders", true);
    
    booleanQueryMaxClauseCount = getInt("query/maxBooleanClauses", BooleanQuery.getMaxClauseCount());
    filtOptEnabled = getBool("query/boolTofilterOptimizer/@enabled", false);
    filtOptCacheSize = getInt("query/boolTofilterOptimizer/@cacheSize",32);
    filtOptThreshold = getFloat("query/boolTofilterOptimizer/@threshold",.05f);
    
    useFilterForSortedQuery = getBool("query/useFilterForSortedQuery", false);
    queryResultWindowSize = getInt("query/queryResultWindowSize", 1);
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

    userCacheConfigs = CacheConfig.getMultipleConfigs(this, "query/cache");

    org.apache.solr.search.SolrIndexSearcher.initRegenerators(this);

    hashSetInverseLoadFactor = 1.0f / getFloat("//HashDocSet/@loadFactor",0.75f);
    hashDocSetMaxSize= getInt("//HashDocSet/@maxSize",3000);
    
    pingQueryParams = readPingQueryParams(this);

    httpCachingConfig = new HttpCachingConfig(this);
    
    Node jmx = (Node) getNode("jmx", false);
    if (jmx != null) {
      jmxConfig = new JmxConfiguration(true, get("jmx/@agentId", null), get(
          "jmx/@serviceUrl", null));
    } else {
      jmxConfig = new JmxConfiguration(false, null, null);
    }
     maxWarmingSearchers = getInt("query/maxWarmingSearchers",Integer.MAX_VALUE);

     loadPluginInfo();
     updateProcessorChainInfo = loadUpdateProcessorInfo();
     updateHandlerInfo = loadUpdatehandlerInfo();
     loadHighLightingPlugins();

    Config.log.info("Loaded SolrConfig: " + name);
    
    // TODO -- at solr 2.0. this should go away
    config = this;
  }

  public String getHighLghtingClass() {
    return highLghtingClass;
  }

  protected void loadHighLightingPlugins() {
    highLghtingClass =  get("highlighting/@class",null);
    highlightingFragmenterInfo = loadPluginInfo("highlighting/fragmenter",true,true);
    highlightingFormatterInfo = loadPluginInfo("highlighting/formatter",true, true);
  }

  protected UpdateHandlerInfo loadUpdatehandlerInfo() {
    return new UpdateHandlerInfo(get("updateHandler/@class",null),
            getInt("updateHandler/autoCommit/maxDocs",-1),
            getInt("updateHandler/autoCommit/maxTime",-1),
            getInt("updateHandler/commitIntervalLowerBound",-1));
  }

  protected void loadPluginInfo() {
    reqHandlerInfo = loadPluginInfo("requestHandler",true, true);
    respWriterInfo = loadPluginInfo("queryResponseWriter",true, true);
    valueSourceParserInfo = loadPluginInfo("valueSourceParser",true, true);
    queryParserInfo = loadPluginInfo("queryParser",true, true);
    searchComponentInfo = loadPluginInfo("searchComponent",true, true);
    queryConverterInfo = loadPluginInfo("queryConverter",true, true);
    directoryfactoryInfo = loadSinglePlugin("directoryFactory");
    deletionPolicyInfo = loadSinglePlugin("mainIndex/deletionPolicy");
    indexReaderFactoryInfo = loadSinglePlugin("indexReaderFactory");
    firstSearcherListenerInfo = loadPluginInfo("//listener[@event='firstSearcher']",false, true);
    newSearcherListenerInfo = loadPluginInfo("//listener[@event='newSearcher']",false, true);
  }

  protected Map<String, List<PluginInfo>> loadUpdateProcessorInfo() {
    HashMap<String, List<PluginInfo>> chains = new HashMap<String, List<PluginInfo>>();
    NodeList nodes = (NodeList) evaluate("updateRequestProcessorChain", XPathConstants.NODESET);
    if (nodes != null) {
      boolean requireName = nodes.getLength() > 1;
      for (int i = 0; i < nodes.getLength(); i++) {
        Node node = nodes.item(i);
        String name       = DOMUtil.getAttr(node,"name", requireName ? "[solrconfig.xml] updateRequestProcessorChain":null);
        boolean isDefault = "true".equals( DOMUtil.getAttr(node,"default", null ) );
        XPath xpath = getXPath();
        try {
          NodeList nl = (NodeList) xpath.evaluate("processor",node, XPathConstants.NODESET);
          if((nl.getLength() <1)) {
            throw new RuntimeException( "updateRequestProcessorChain require at least one processor");
          }
          ArrayList<PluginInfo> result = new ArrayList<PluginInfo>();
          for (int j=0; j<nl.getLength(); j++) {
            PluginInfo pluginInfo = new PluginInfo(nl.item(j), "[solrconfig.xml] processor", false, true);
            if(pluginInfo.isEnabled()) result.add(pluginInfo);
          }
          chains.put(name,result);
          if(isDefault || nodes.getLength() == 1) chains.put(null,result);
        } catch (XPathExpressionException e) {
          throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,"Error reading processors",e,false);
        }
      }
    }

    return chains.isEmpty() ?
            Collections.<String, List<PluginInfo>>emptyMap():
            Collections.unmodifiableMap(chains);
  }
  private PluginInfo loadSinglePlugin(String tag) {
    List<PluginInfo> l = loadPluginInfo(tag, false, true);
    return l.isEmpty() ? null : l.get(0);
  }

  private List<PluginInfo> loadPluginInfo(String tag, boolean requireName, boolean requireClass) {
    ArrayList<PluginInfo> result = new ArrayList<PluginInfo>();
    NodeList nodes = (NodeList) evaluate(tag, XPathConstants.NODESET);
     for (int i=0; i<nodes.getLength(); i++) {
       PluginInfo pluginInfo = new PluginInfo(nodes.item(i), "[solrconfig.xml] " + tag, requireName, requireClass);
       if(pluginInfo.isEnabled()) result.add(pluginInfo);
     }
    return result.isEmpty() ?
            Collections.<PluginInfo>emptyList() :
            Collections.unmodifiableList(result) ;
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

  protected List<PluginInfo> reqHandlerInfo;
  protected List<PluginInfo> queryParserInfo;
  protected List<PluginInfo> respWriterInfo;
  protected List<PluginInfo> valueSourceParserInfo;
  protected List<PluginInfo> searchComponentInfo;
  protected List<PluginInfo> highlightingFragmenterInfo;
  protected List<PluginInfo> highlightingFormatterInfo;
  protected List<PluginInfo> firstSearcherListenerInfo;
  protected PluginInfo deletionPolicyInfo;
  protected PluginInfo indexReaderFactoryInfo;
  protected List<PluginInfo> newSearcherListenerInfo;
  protected List<PluginInfo> queryConverterInfo;

  protected PluginInfo directoryfactoryInfo;
  protected Map<String ,List<PluginInfo>> updateProcessorChainInfo ;
  protected UpdateHandlerInfo updateHandlerInfo ;
  protected String highLghtingClass;

  public final int maxWarmingSearchers;
  public final boolean unlockOnStartup;
  public final boolean useColdSearcher;
  protected String dataDir;
  
  //JMX configuration
  public final JmxConfiguration jmxConfig;
  
  private final HttpCachingConfig httpCachingConfig;
  public HttpCachingConfig getHttpCachingConfig() {
    return httpCachingConfig;
  }
  
  /**
   * ping query request parameters
   * @deprecated Use {@link PingRequestHandler} instead.
   */
  @Deprecated
  private final NamedList pingQueryParams;

  static private NamedList readPingQueryParams(SolrConfig config) {  
    String urlSnippet = config.get("admin/pingQuery", "").trim();
    
    StringTokenizer qtokens = new StringTokenizer(urlSnippet,"&");
    String tok;
    NamedList params = new NamedList();
    while (qtokens.hasMoreTokens()) {
      tok = qtokens.nextToken();
      String[] split = tok.split("=", 2);
      params.add(split[0], split[1]);
    }
    if (0 < params.size()) {
      log.warn("The <pingQuery> syntax is deprecated, " +
               "please use PingRequestHandler instead");
    }
    return params;
  }
  
  /**
   * Returns a Request object based on the admin/pingQuery section
   * of the Solr config file.
   * 
   * @deprecated use {@link PingRequestHandler} instead 
   */
  @Deprecated
  public SolrQueryRequest getPingQueryRequest(SolrCore core) {
    if(pingQueryParams.size() == 0) {
      throw new IllegalStateException
        ("<pingQuery> not configured (consider registering " +
         "PingRequestHandler with the name '/admin/ping' instead)");
    }
    return new LocalSolrQueryRequest(core, pingQueryParams);
  }

  public static class JmxConfiguration {
    public boolean enabled = false;

    public String agentId;

    public String serviceUrl;

    public JmxConfiguration(boolean enabled, String agentId, String serviceUrl) {
      this.enabled = enabled;
      this.agentId = agentId;
      this.serviceUrl = serviceUrl;
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
          return valueOf(s.toUpperCase());
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


  public List<PluginInfo> getReqHandlerInfo() { return reqHandlerInfo; }

  public List<PluginInfo> getQueryParserInfo() { return queryParserInfo; }

  public List<PluginInfo> getRespWriterInfo() { return respWriterInfo; }

  public List<PluginInfo> getValueSourceParserInfo() { return valueSourceParserInfo; }

  public List<PluginInfo> getSearchComponentInfo() { return searchComponentInfo; }

  public List<PluginInfo> getFirstSearcherListenerInfo() { return firstSearcherListenerInfo; }

  public List<PluginInfo> getNewSearcherListenerInfo() { return newSearcherListenerInfo; }

  public PluginInfo getDirectoryFactoryInfo() { return directoryfactoryInfo; }

  public PluginInfo getDeletionPolicyInfo() { return deletionPolicyInfo; }

  public Map<String, List<PluginInfo>> getUpdateProcessorChainInfo() { return updateProcessorChainInfo; }

  public List<PluginInfo> getQueryConverterInfo() { return queryConverterInfo; }

  public UpdateHandlerInfo getUpdateHandlerInfo() { return updateHandlerInfo; }

  public PluginInfo getIndexReaderFactoryInfo() { return indexReaderFactoryInfo; }

  public List<PluginInfo> getHighlightingFormatterInfo() { return highlightingFormatterInfo; }

  public List<PluginInfo> getHighlightingFragmenterInfo() { return highlightingFragmenterInfo; }

  public String getDataDir() { return dataDir; }
}
