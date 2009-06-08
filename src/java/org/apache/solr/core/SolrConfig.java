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
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;

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

    Config.log.info("Loaded SolrConfig: " + name);
    
    // TODO -- at solr 2.0. this should go away
    config = this;
  }

  protected void loadPluginInfo() {
    List<String> reqFields = Arrays.asList("name","class");
    reqHandlerInfo = loadPluginInfo("requestHandler",reqFields);
    respWriterInfo = loadPluginInfo("queryResponseWriter",reqFields);
    valueSourceParserInfo = loadPluginInfo("valueSourceParser",reqFields);
    queryParserInfo = loadPluginInfo("queryParser",reqFields);
    searchComponentInfo = loadPluginInfo("searchComponent",reqFields);
    List<PluginInfo> plugins =  loadPluginInfo("directoryFactory",reqFields);
    directoryfactoryInfo = plugins.isEmpty() ? null:plugins.get(0);
    reqFields = Arrays.asList("class");
    plugins = loadPluginInfo("mainIndex/deletionPolicy",reqFields);
    deletionPolicyInfo = plugins.isEmpty() ? null : plugins.get(0);
    firstSearcherListenerInfo = loadPluginInfo("//listener[@event='firstSearcher']",reqFields);
    newSearcherListenerInfo = loadPluginInfo("//listener[@event='newSearcher']",reqFields);
  }

  private List<PluginInfo> loadPluginInfo(String tag, List<String> reqFields) {
    ArrayList<PluginInfo> result = new ArrayList<PluginInfo>();
    NodeList nodes = (NodeList) evaluate(tag, XPathConstants.NODESET);
     for (int i=0; i<nodes.getLength(); i++) {
       result.add(new PluginInfo(nodes.item(i) ,"[solrconfig.xml] "+tag,reqFields));
     }
    return Collections.unmodifiableList(result) ;
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
  protected List<PluginInfo> firstSearcherListenerInfo;
  protected PluginInfo deletionPolicyInfo;
  protected List<PluginInfo> newSearcherListenerInfo;
  protected PluginInfo directoryfactoryInfo;

  public final int maxWarmingSearchers;
  public final boolean unlockOnStartup;
  public final boolean useColdSearcher;
  
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

  public static class PluginInfo {
    final String startup, name, className;
    final boolean isDefault;    
    final NamedList initArgs;

    public PluginInfo(String startup, String name, String className,
                      boolean isdefault, NamedList initArgs) {
      this.startup = startup;
      this.name = name;
      this.className = className;
      this.isDefault = isdefault;
      this.initArgs = initArgs;
    }


    public PluginInfo(Node node, String err, List<String> reqFields) {
      startup = getVal( node, "startup",reqFields,err);
      name = getVal(node, "name", reqFields,err);
      className = getVal(node, "class",reqFields,err);
      isDefault = Boolean.parseBoolean(getVal(node,"default",reqFields,err));
      initArgs = DOMUtil.childNodesToNamedList(node);
  }

   private String getVal(Node node, String name, List<String> required, String err) {
    return DOMUtil.getAttr(node, name, required.contains(name) ? err : null);
   }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("{");
      if(name != null) sb.append("name = "+name +",");
      if(className != null) sb.append("class = "+className +",");
      if(isDefault) sb.append("default = "+isDefault +",");
      if(initArgs.size() >0) sb.append("args = "+initArgs);
      sb.append("}");
      return sb.toString();    
    }
  }

  public List<PluginInfo> getReqHandlerInfo() { return reqHandlerInfo; }

  public List<PluginInfo> getQueryParserInfo() { return queryParserInfo; }

  public List<PluginInfo> getRespWriterInfo() { return respWriterInfo; }

  public List<PluginInfo> getValueSourceParserInfo() { return valueSourceParserInfo; }

  public List<PluginInfo> getSearchComponentInfo() { return searchComponentInfo; }

  public List<PluginInfo> getFirstSearcherListenerInfo() { return firstSearcherListenerInfo; }

  public List<PluginInfo> getNewSearcherListenerInfo() { return newSearcherListenerInfo; }

  public PluginInfo getDirectoryfactoryInfo() { return directoryfactoryInfo; }

  public PluginInfo getDeletionPolicyInfo() { return deletionPolicyInfo; }
}
