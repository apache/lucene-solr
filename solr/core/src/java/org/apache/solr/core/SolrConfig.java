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


import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.common.util.Utils.makeMap;
import static org.apache.solr.core.ConfigOverlay.ZNODEVER;
import static org.apache.solr.core.SolrConfig.PluginOpts.LAZY;
import static org.apache.solr.core.SolrConfig.PluginOpts.MULTI_OK;
import static org.apache.solr.core.SolrConfig.PluginOpts.NOOP;
import static org.apache.solr.core.SolrConfig.PluginOpts.REQUIRE_CLASS;
import static org.apache.solr.core.SolrConfig.PluginOpts.REQUIRE_NAME;
import static org.apache.solr.core.SolrConfig.PluginOpts.REQUIRE_NAME_IN_OVERLAY;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;

import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.rest.RestManager;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.search.CacheConfig;
import org.apache.solr.search.FastLRUCache;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.search.stats.StatsCache;
import org.apache.solr.servlet.SolrRequestParsers;
import org.apache.solr.spelling.QueryConverter;
import org.apache.solr.update.SolrIndexConfig;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.DOMUtil;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.common.collect.ImmutableList;


/**
 * Provides a static reference to a Config object modeling the main
 * configuration data for a a Solr instance -- typically found in
 * "solrconfig.xml".
 */
public class SolrConfig extends Config implements MapSerializable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String DEFAULT_CONF_FILE = "solrconfig.xml";
  private RequestParams requestParams;

  public enum PluginOpts {
    MULTI_OK,
    REQUIRE_NAME,
    REQUIRE_NAME_IN_OVERLAY,
    REQUIRE_CLASS,
    LAZY,
    // EnumSet.of and/or EnumSet.copyOf(Collection) are annoying
    // because of type determination
    NOOP
  }

  private int multipartUploadLimitKB;

  private int formUploadLimitKB;

  private boolean enableRemoteStreams;

  private boolean handleSelect;

  private boolean addHttpRequestToContext;

  private final SolrRequestParsers solrRequestParsers;

  /**
   * Creates a default instance from the solrconfig.xml.
   */
  public SolrConfig()
      throws ParserConfigurationException, IOException, SAXException {
    this((SolrResourceLoader) null, DEFAULT_CONF_FILE, null);
  }

  /**
   * Creates a configuration instance from a configuration name.
   * A default resource loader will be created (@see SolrResourceLoader)
   *
   * @param name the configuration name used by the loader
   */
  public SolrConfig(String name)
      throws ParserConfigurationException, IOException, SAXException {
    this((SolrResourceLoader) null, name, null);
  }

  /**
   * Creates a configuration instance from a configuration name and stream.
   * A default resource loader will be created (@see SolrResourceLoader).
   * If the stream is null, the resource loader will open the configuration stream.
   * If the stream is not null, no attempt to load the resource will occur (the name is not used).
   *
   * @param name the configuration name
   * @param is   the configuration stream
   */
  public SolrConfig(String name, InputSource is)
      throws ParserConfigurationException, IOException, SAXException {
    this((SolrResourceLoader) null, name, is);
  }

  /**
   * Creates a configuration instance from an instance directory, configuration name and stream.
   *
   * @param instanceDir the directory used to create the resource loader
   * @param name        the configuration name used by the loader if the stream is null
   * @param is          the configuration stream
   */
  public SolrConfig(Path instanceDir, String name, InputSource is)
      throws ParserConfigurationException, IOException, SAXException {
    this(new SolrResourceLoader(instanceDir), name, is);
  }

  public static SolrConfig readFromResourceLoader(SolrResourceLoader loader, String name) {
    try {
      return new SolrConfig(loader, name, null);
    } catch (Exception e) {
      String resource;
      if (loader instanceof ZkSolrResourceLoader) {
        resource = name;
      } else {
        resource = Paths.get(loader.getConfigDir()).resolve(name).toString();
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error loading solr config from " + resource, e);
    }
  }

  /**
   * Creates a configuration instance from a resource loader, a configuration name and a stream.
   * If the stream is null, the resource loader will open the configuration stream.
   * If the stream is not null, no attempt to load the resource will occur (the name is not used).
   *
   * @param loader the resource loader
   * @param name   the configuration name
   * @param is     the configuration stream
   */
  public SolrConfig(SolrResourceLoader loader, String name, InputSource is)
      throws ParserConfigurationException, IOException, SAXException {
    super(loader, name, is, "/config/");
    getOverlay();//just in case it is not initialized
    getRequestParams();
    initLibs();
    luceneMatchVersion = getLuceneVersion("luceneMatchVersion");
    String indexConfigPrefix;

    // Old indexDefaults and mainIndex sections are deprecated and fails fast for luceneMatchVersion=>LUCENE_4_0_0.
    // For older solrconfig.xml's we allow the old sections, but never mixed with the new <indexConfig>
    boolean hasDeprecatedIndexConfig = (getNode("indexDefaults", false) != null) || (getNode("mainIndex", false) != null);
    if (hasDeprecatedIndexConfig) {
      throw new SolrException(ErrorCode.FORBIDDEN, "<indexDefaults> and <mainIndex> configuration sections are discontinued. Use <indexConfig> instead.");
    } else {
      indexConfigPrefix = "indexConfig";
    }
    assertWarnOrFail("The <nrtMode> config has been discontinued and NRT mode is always used by Solr." +
            " This config will be removed in future versions.", getNode(indexConfigPrefix + "/nrtMode", false) == null,
        true
    );
    assertWarnOrFail("Solr no longer supports forceful unlocking via the 'unlockOnStartup' option.  "+
                     "This is no longer necessary for the default lockType except in situations where "+
                     "it would be dangerous and should not be done.  For other lockTypes and/or "+
                     "directoryFactory options it may also be dangerous and users must resolve "+
                     "problematic locks manually.",
                     null == getNode(indexConfigPrefix + "/unlockOnStartup", false),
                     true // 'fail' in trunk
                     );
                     
    // Parse indexConfig section, using mainIndex as backup in case old config is used
    indexConfig = new SolrIndexConfig(this, "indexConfig", null);

    booleanQueryMaxClauseCount = getInt("query/maxBooleanClauses", BooleanQuery.getMaxClauseCount());
    log.info("Using Lucene MatchVersion: {}", luceneMatchVersion);

    // Warn about deprecated / discontinued parameters
    // boolToFilterOptimizer has had no effect since 3.1
    if (get("query/boolTofilterOptimizer", null) != null)
      log.warn("solrconfig.xml: <boolTofilterOptimizer> is currently not implemented and has no effect.");
    if (get("query/HashDocSet", null) != null)
      log.warn("solrconfig.xml: <HashDocSet> is deprecated and no longer recommended used.");

// TODO: Old code - in case somebody wants to re-enable. Also see SolrIndexSearcher#search()
//    filtOptEnabled = getBool("query/boolTofilterOptimizer/@enabled", false);
//    filtOptCacheSize = getInt("query/boolTofilterOptimizer/@cacheSize",32);
//    filtOptThreshold = getFloat("query/boolTofilterOptimizer/@threshold",.05f);

    useFilterForSortedQuery = getBool("query/useFilterForSortedQuery", false);
    queryResultWindowSize = Math.max(1, getInt("query/queryResultWindowSize", 1));
    queryResultMaxDocsCached = getInt("query/queryResultMaxDocsCached", Integer.MAX_VALUE);
    enableLazyFieldLoading = getBool("query/enableLazyFieldLoading", false);
    
    useRangeVersionsForPeerSync = getBool("peerSync/useRangeVersions", true);

    filterCacheConfig = CacheConfig.getConfig(this, "query/filterCache");
    queryResultCacheConfig = CacheConfig.getConfig(this, "query/queryResultCache");
    documentCacheConfig = CacheConfig.getConfig(this, "query/documentCache");
    CacheConfig conf = CacheConfig.getConfig(this, "query/fieldValueCache");
    if (conf == null) {
      Map<String, String> args = new HashMap<>();
      args.put(NAME, "fieldValueCache");
      args.put("size", "10000");
      args.put("initialSize", "10");
      args.put("showItems", "-1");
      conf = new CacheConfig(FastLRUCache.class, args, null);
    }
    fieldValueCacheConfig = conf;
    useColdSearcher = getBool("query/useColdSearcher", false);
    dataDir = get("dataDir", null);
    if (dataDir != null && dataDir.length() == 0) dataDir = null;


    org.apache.solr.search.SolrIndexSearcher.initRegenerators(this);

    hashSetInverseLoadFactor = 1.0f / getFloat("//HashDocSet/@loadFactor", 0.75f);
    hashDocSetMaxSize = getInt("//HashDocSet/@maxSize", 3000);

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
    maxWarmingSearchers = getInt("query/maxWarmingSearchers", 1);
    slowQueryThresholdMillis = getInt("query/slowQueryThresholdMillis", -1);
    for (SolrPluginInfo plugin : plugins) loadPluginInfo(plugin);

    Map<String, CacheConfig> userCacheConfigs = CacheConfig.getMultipleConfigs(this, "query/cache");
    List<PluginInfo> caches = getPluginInfos(SolrCache.class.getName());
    if (!caches.isEmpty()) {
      for (PluginInfo c : caches) {
        userCacheConfigs.put(c.name, CacheConfig.getConfig(this, "cache", c.attributes, null));
      }
    }
    this.userCacheConfigs = Collections.unmodifiableMap(userCacheConfigs);

    updateHandlerInfo = loadUpdatehandlerInfo();

    multipartUploadLimitKB = getInt(
        "requestDispatcher/requestParsers/@multipartUploadLimitInKB", 2048);

    formUploadLimitKB = getInt(
        "requestDispatcher/requestParsers/@formdataUploadLimitInKB", 2048);

    enableRemoteStreams = getBool(
        "requestDispatcher/requestParsers/@enableRemoteStreaming", false);

    // Let this filter take care of /select?xxx format
    handleSelect = getBool(
        "requestDispatcher/@handleSelect", true);

    addHttpRequestToContext = getBool(
        "requestDispatcher/requestParsers/@addHttpRequestToContext", false);

    List<PluginInfo> argsInfos = getPluginInfos(InitParams.class.getName());
    if (argsInfos != null) {
      Map<String, InitParams> argsMap = new HashMap<>();
      for (PluginInfo p : argsInfos) {
        InitParams args = new InitParams(p);
        argsMap.put(args.name == null ? String.valueOf(args.hashCode()) : args.name, args);
      }
      this.initParams = Collections.unmodifiableMap(argsMap);

    }

    solrRequestParsers = new SolrRequestParsers(this);
    log.debug("Loaded SolrConfig: {}", name);
  }

  public static final List<SolrPluginInfo> plugins = ImmutableList.<SolrPluginInfo>builder()
      .add(new SolrPluginInfo(SolrRequestHandler.class, SolrRequestHandler.TYPE, REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK, LAZY))
      .add(new SolrPluginInfo(QParserPlugin.class, "queryParser", REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK))
      .add(new SolrPluginInfo(Expressible.class, "expressible", REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK))
      .add(new SolrPluginInfo(QueryResponseWriter.class, "queryResponseWriter", REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK, LAZY))
      .add(new SolrPluginInfo(ValueSourceParser.class, "valueSourceParser", REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK))
      .add(new SolrPluginInfo(TransformerFactory.class, "transformer", REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK))
      .add(new SolrPluginInfo(SearchComponent.class, "searchComponent", REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK))
      .add(new SolrPluginInfo(UpdateRequestProcessorFactory.class, "updateProcessor", REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK))
      .add(new SolrPluginInfo(SolrCache.class, "cache", REQUIRE_NAME, REQUIRE_CLASS, MULTI_OK))
          // TODO: WTF is up with queryConverter???
          // it apparently *only* works as a singleton? - SOLR-4304
          // and even then -- only if there is a single SpellCheckComponent
          // because of queryConverter.setIndexAnalyzer
      .add(new SolrPluginInfo(QueryConverter.class, "queryConverter", REQUIRE_NAME, REQUIRE_CLASS))
      .add(new SolrPluginInfo(PluginBag.RuntimeLib.class, "runtimeLib", REQUIRE_NAME, MULTI_OK))
          // this is hackish, since it picks up all SolrEventListeners,
          // regardless of when/how/why they are used (or even if they are
          // declared outside of the appropriate context) but there's no nice
          // way around that in the PluginInfo framework
      .add(new SolrPluginInfo(InitParams.class, InitParams.TYPE, MULTI_OK, REQUIRE_NAME_IN_OVERLAY))
      .add(new SolrPluginInfo(SolrEventListener.class, "//listener", REQUIRE_CLASS, MULTI_OK, REQUIRE_NAME_IN_OVERLAY))

      .add(new SolrPluginInfo(DirectoryFactory.class, "directoryFactory", REQUIRE_CLASS))
      .add(new SolrPluginInfo(IndexDeletionPolicy.class, "indexConfig/deletionPolicy", REQUIRE_CLASS))
      .add(new SolrPluginInfo(CodecFactory.class, "codecFactory", REQUIRE_CLASS))
      .add(new SolrPluginInfo(IndexReaderFactory.class, "indexReaderFactory", REQUIRE_CLASS))
      .add(new SolrPluginInfo(UpdateRequestProcessorChain.class, "updateRequestProcessorChain", MULTI_OK))
      .add(new SolrPluginInfo(UpdateLog.class, "updateHandler/updateLog"))
      .add(new SolrPluginInfo(IndexSchemaFactory.class, "schemaFactory", REQUIRE_CLASS))
      .add(new SolrPluginInfo(RestManager.class, "restManager"))
      .add(new SolrPluginInfo(StatsCache.class, "statsCache", REQUIRE_CLASS))
      .build();
  public static final Map<String, SolrPluginInfo> classVsSolrPluginInfo;

  static {
    Map<String, SolrPluginInfo> map = new HashMap<>();
    for (SolrPluginInfo plugin : plugins) map.put(plugin.clazz.getName(), plugin);
    classVsSolrPluginInfo = Collections.unmodifiableMap(map);
  }

  public static class SolrPluginInfo {

    public final Class clazz;
    public final String tag;
    public final Set<PluginOpts> options;


    private SolrPluginInfo(Class clz, String tag, PluginOpts... opts) {
      this.clazz = clz;
      this.tag = tag;
      this.options = opts == null ? Collections.EMPTY_SET : EnumSet.of(NOOP, opts);
    }

    public String getCleanTag() {
      return tag.replaceAll("/", "");
    }

    public String getTagCleanLower() {
      return getCleanTag().toLowerCase(Locale.ROOT);

    }
  }

  public static ConfigOverlay getConfigOverlay(SolrResourceLoader loader) {
    InputStream in = null;
    InputStreamReader isr = null;
    try {
      try {
        in = loader.openResource(ConfigOverlay.RESOURCE_NAME);
      } catch (IOException e) {
        // TODO: we should be explicitly looking for file not found exceptions
        // and logging if it's not the expected IOException
        // hopefully no problem, assume no overlay.json file
        return new ConfigOverlay(Collections.EMPTY_MAP, -1);
      }
      
      int version = 0; // will be always 0 for file based resourceLoader
      if (in instanceof ZkSolrResourceLoader.ZkByteArrayInputStream) {
        version = ((ZkSolrResourceLoader.ZkByteArrayInputStream) in).getStat().getVersion();
        log.debug("Config overlay loaded. version : {} ", version);
      }
      isr = new InputStreamReader(in, StandardCharsets.UTF_8);
      Map m = (Map) ObjectBuilder.getVal(new JSONParser(isr));
      return new ConfigOverlay(m, version);
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error reading config overlay", e);
    } finally {
      IOUtils.closeQuietly(isr);
      IOUtils.closeQuietly(in);
    }
  }

  private Map<String, InitParams> initParams = Collections.emptyMap();

  public Map<String, InitParams> getInitParams() {
    return initParams;
  }

  protected UpdateHandlerInfo loadUpdatehandlerInfo() {
    return new UpdateHandlerInfo(get("updateHandler/@class", null),
        getInt("updateHandler/autoCommit/maxDocs", -1),
        getInt("updateHandler/autoCommit/maxTime", -1),
        getBool("updateHandler/indexWriter/closeWaitsForMerges", true),
        getBool("updateHandler/autoCommit/openSearcher", true),
        getInt("updateHandler/autoSoftCommit/maxDocs", -1),
        getInt("updateHandler/autoSoftCommit/maxTime", -1),
        getBool("updateHandler/commitWithin/softCommit", true));
  }

  private void loadPluginInfo(SolrPluginInfo pluginInfo) {
    boolean requireName = pluginInfo.options.contains(REQUIRE_NAME);
    boolean requireClass = pluginInfo.options.contains(REQUIRE_CLASS);

    List<PluginInfo> result = readPluginInfos(pluginInfo.tag, requireName, requireClass);

    if (1 < result.size() && !pluginInfo.options.contains(MULTI_OK)) {
      throw new SolrException
          (SolrException.ErrorCode.SERVER_ERROR,
              "Found " + result.size() + " configuration sections when at most "
                  + "1 is allowed matching expression: " + pluginInfo.getCleanTag());
    }
    if (!result.isEmpty()) pluginStore.put(pluginInfo.clazz.getName(), result);
  }

  public List<PluginInfo> readPluginInfos(String tag, boolean requireName, boolean requireClass) {
    ArrayList<PluginInfo> result = new ArrayList<>();
    NodeList nodes = (NodeList) evaluate(tag, XPathConstants.NODESET);
    for (int i = 0; i < nodes.getLength(); i++) {
      PluginInfo pluginInfo = new PluginInfo(nodes.item(i), "[solrconfig.xml] " + tag, requireName, requireClass);
      if (pluginInfo.isEnabled()) result.add(pluginInfo);
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
  public final CacheConfig filterCacheConfig;
  public final CacheConfig queryResultCacheConfig;
  public final CacheConfig documentCacheConfig;
  public final CacheConfig fieldValueCacheConfig;
  public final Map<String, CacheConfig> userCacheConfigs;
  // SolrIndexSearcher - more...
  public final boolean useFilterForSortedQuery;
  public final int queryResultWindowSize;
  public final int queryResultMaxDocsCached;
  public final boolean enableLazyFieldLoading;
  
  public final boolean useRangeVersionsForPeerSync;
  
  // DocSet
  public final float hashSetInverseLoadFactor;
  public final int hashDocSetMaxSize;
  // IndexConfig settings
  public final SolrIndexConfig indexConfig;

  protected UpdateHandlerInfo updateHandlerInfo;

  private Map<String, List<PluginInfo>> pluginStore = new LinkedHashMap<>();

  public final int maxWarmingSearchers;
  public final boolean useColdSearcher;
  public final Version luceneMatchVersion;
  protected String dataDir;
  public final int slowQueryThresholdMillis;  // threshold above which a query is considered slow

  //JMX configuration
  public final JmxConfiguration jmxConfig;

  private final HttpCachingConfig httpCachingConfig;

  public HttpCachingConfig getHttpCachingConfig() {
    return httpCachingConfig;
  }

  public static class JmxConfiguration implements MapSerializable {
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
                "Incorrect JMX Configuration in solrconfig.xml, " +
                    "both agentId and serviceUrl cannot be specified at the same time");
      }

    }

    @Override
    public Map<String, Object> toMap(Map<String, Object> map) {
      map.put("agentId", agentId);
      map.put("serviceUrl", serviceUrl);
      map.put("rootName", rootName);
      return map;
    }
  }

  public static class HttpCachingConfig implements MapSerializable {

    /**
     * config xpath prefix for getting HTTP Caching options
     */
    private final static String CACHE_PRE
        = "requestDispatcher/httpCaching/";

    /**
     * For extracting Expires "ttl" from <cacheControl> config
     */
    private final static Pattern MAX_AGE
        = Pattern.compile("\\bmax-age=(\\d+)");

    @Override
    public Map<String, Object> toMap(Map<String, Object> map) {
      return makeMap("never304", never304,
          "etagSeed", etagSeed,
          "lastModFrom", lastModFrom.name().toLowerCase(Locale.ROOT),
          "cacheControl", cacheControlHeader);
    }

    public static enum LastModFrom {
      OPENTIME, DIRLASTMOD, BOGUS;

      /**
       * Input must not be null
       */
      public static LastModFrom parse(final String s) {
        try {
          return valueOf(s.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
          log.warn("Unrecognized value for lastModFrom: " + s, e);
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

      never304 = conf.getBool(CACHE_PRE + "@never304", false);

      etagSeed = conf.get(CACHE_PRE + "@etagSeed", "Solr");


      lastModFrom = LastModFrom.parse(conf.get(CACHE_PRE + "@lastModFrom",
          "openTime"));

      cacheControlHeader = conf.get(CACHE_PRE + "cacheControl", null);

      Long tmp = null; // maxAge
      if (null != cacheControlHeader) {
        try {
          final Matcher ttlMatcher = MAX_AGE.matcher(cacheControlHeader);
          final String ttlStr = ttlMatcher.find() ? ttlMatcher.group(1) : null;
          tmp = (null != ttlStr && !"".equals(ttlStr))
              ? Long.valueOf(ttlStr)
              : null;
        } catch (Exception e) {
          log.warn("Ignoring exception while attempting to " +
              "extract max-age from cacheControl config: " +
              cacheControlHeader, e);
        }
      }
      maxAge = tmp;

    }

    public boolean isNever304() {
      return never304;
    }

    public String getEtagSeed() {
      return etagSeed;
    }

    /**
     * null if no Cache-Control header
     */
    public String getCacheControlHeader() {
      return cacheControlHeader;
    }

    /**
     * null if no max age limitation
     */
    public Long getMaxAge() {
      return maxAge;
    }

    public LastModFrom getLastModFrom() {
      return lastModFrom;
    }
  }

  public static class UpdateHandlerInfo implements MapSerializable {
    public final String className;
    public final int autoCommmitMaxDocs, autoCommmitMaxTime,
        autoSoftCommmitMaxDocs, autoSoftCommmitMaxTime;
    public final boolean indexWriterCloseWaitsForMerges;
    public final boolean openSearcher;  // is opening a new searcher part of hard autocommit?
    public final boolean commitWithinSoftCommit;

    /**
     * @param autoCommmitMaxDocs       set -1 as default
     * @param autoCommmitMaxTime       set -1 as default
     */
    public UpdateHandlerInfo(String className, int autoCommmitMaxDocs, int autoCommmitMaxTime, boolean indexWriterCloseWaitsForMerges, boolean openSearcher,
                             int autoSoftCommmitMaxDocs, int autoSoftCommmitMaxTime, boolean commitWithinSoftCommit) {
      this.className = className;
      this.autoCommmitMaxDocs = autoCommmitMaxDocs;
      this.autoCommmitMaxTime = autoCommmitMaxTime;
      this.indexWriterCloseWaitsForMerges = indexWriterCloseWaitsForMerges;
      this.openSearcher = openSearcher;

      this.autoSoftCommmitMaxDocs = autoSoftCommmitMaxDocs;
      this.autoSoftCommmitMaxTime = autoSoftCommmitMaxTime;

      this.commitWithinSoftCommit = commitWithinSoftCommit;
    }


    @Override
    public Map<String, Object> toMap(Map<String, Object> map) {
      LinkedHashMap result = new LinkedHashMap();
      result.put("indexWriter", makeMap("closeWaitsForMerges", indexWriterCloseWaitsForMerges));
      result.put("commitWithin", makeMap("softCommit", commitWithinSoftCommit));
      result.put("autoCommit", makeMap(
          "maxDocs", autoCommmitMaxDocs,
          "maxTime", autoCommmitMaxTime,
          "openSearcher", openSearcher
      ));
      result.put("autoSoftCommit",
          makeMap("maxDocs", autoSoftCommmitMaxDocs,
              "maxTime", autoSoftCommmitMaxTime));
      return result;
    }
  }

//  public Map<String, List<PluginInfo>> getUpdateProcessorChainInfo() { return updateProcessorChainInfo; }

  public UpdateHandlerInfo getUpdateHandlerInfo() {
    return updateHandlerInfo;
  }

  public String getDataDir() {
    return dataDir;
  }

  /**
   * SolrConfig keeps a repository of plugins by the type. The known interfaces are the types.
   *
   * @param type The key is FQN of the plugin class there are a few  known types : SolrFormatter, SolrFragmenter
   *             SolrRequestHandler,QParserPlugin, QueryResponseWriter,ValueSourceParser,
   *             SearchComponent, QueryConverter, SolrEventListener, DirectoryFactory,
   *             IndexDeletionPolicy, IndexReaderFactory, {@link TransformerFactory}
   */
  public List<PluginInfo> getPluginInfos(String type) {
    List<PluginInfo> result = pluginStore.get(type);
    SolrPluginInfo info = classVsSolrPluginInfo.get(type);
    if (info != null &&
        (info.options.contains(REQUIRE_NAME) || info.options.contains(REQUIRE_NAME_IN_OVERLAY))) {
      Map<String, Map> infos = overlay.getNamedPlugins(info.getCleanTag());
      if (!infos.isEmpty()) {
        LinkedHashMap<String, PluginInfo> map = new LinkedHashMap<>();
        if (result != null) for (PluginInfo pluginInfo : result) {
          //just create a UUID for the time being so that map key is not null
          String name = pluginInfo.name == null ?
              UUID.randomUUID().toString().toLowerCase(Locale.ROOT) :
              pluginInfo.name;
          map.put(name, pluginInfo);
        }
        for (Map.Entry<String, Map> e : infos.entrySet()) {
          map.put(e.getKey(), new PluginInfo(info.getCleanTag(), e.getValue()));
        }
        result = new ArrayList<>(map.values());
      }
    }
    return result == null ? Collections.<PluginInfo>emptyList() : result;
  }

  public PluginInfo getPluginInfo(String type) {
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

    log.debug("Adding specified lib dirs to ClassLoader");
    SolrResourceLoader loader = getResourceLoader();
    List<URL> urls = new ArrayList<>();

    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      String baseDir = DOMUtil.getAttr(node, "dir");
      String path = DOMUtil.getAttr(node, PATH);
      if (null != baseDir) {
        // :TODO: add support for a simpler 'glob' mutually exclusive of regex
        Path dir = loader.getInstancePath().resolve(baseDir);
        String regex = DOMUtil.getAttr(node, "regex");
        try {
          if (regex == null)
            urls.addAll(SolrResourceLoader.getURLs(dir));
          else
            urls.addAll(SolrResourceLoader.getFilteredURLs(dir, regex));
        } catch (IOException e) {
          log.warn("Couldn't add files from {} filtered by {} to classpath: {}", dir, regex, e.getMessage());
        }
      } else if (null != path) {
        final Path dir = loader.getInstancePath().resolve(path);
        try {
          urls.add(dir.toUri().toURL());
        } catch (MalformedURLException e) {
          log.warn("Couldn't add file {} to classpath: {}", dir, e.getMessage());
        }
      } else {
        throw new RuntimeException("lib: missing mandatory attributes: 'dir' or 'path'");
      }
    }

    if (urls.size() > 0) {
      loader.addToClassLoader(urls);
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

  @Override
  public int getInt(String path) {
    return getInt(path, 0);
  }

  @Override
  public int getInt(String path, int def) {
    Object val = overlay.getXPathProperty(path);
    if (val != null) return Integer.parseInt(val.toString());
    return super.getInt(path, def);
  }

  @Override
  public boolean getBool(String path, boolean def) {
    Object val = overlay.getXPathProperty(path);
    if (val != null) return Boolean.parseBoolean(val.toString());
    return super.getBool(path, def);
  }

  @Override
  public String get(String path) {
    Object val = overlay.getXPathProperty(path, true);
    return val != null ? val.toString() : super.get(path);
  }

  @Override
  public String get(String path, String def) {
    Object val = overlay.getXPathProperty(path, true);
    return val != null ? val.toString() : super.get(path, def);

  }

  @Override
  public Map<String, Object> toMap(Map<String, Object> result) {
    if (getZnodeVersion() > -1) result.put(ZNODEVER, getZnodeVersion());
    result.put("luceneMatchVersion", luceneMatchVersion);
    result.put("updateHandler", getUpdateHandlerInfo());
    Map m = new LinkedHashMap();
    result.put("query", m);
    m.put("useFilterForSortedQuery", useFilterForSortedQuery);
    m.put("queryResultWindowSize", queryResultWindowSize);
    m.put("queryResultMaxDocsCached", queryResultMaxDocsCached);
    m.put("enableLazyFieldLoading", enableLazyFieldLoading);
    m.put("maxBooleanClauses", booleanQueryMaxClauseCount);
    if (jmxConfig != null) result.put("jmx", jmxConfig);
    for (SolrPluginInfo plugin : plugins) {
      List<PluginInfo> infos = getPluginInfos(plugin.clazz.getName());
      if (infos == null || infos.isEmpty()) continue;
      String tag = plugin.getCleanTag();
      tag = tag.replace("/", "");
      if (plugin.options.contains(PluginOpts.REQUIRE_NAME)) {
        LinkedHashMap items = new LinkedHashMap();
        for (PluginInfo info : infos) items.put(info.name, info);
        for (Map.Entry e : overlay.getNamedPlugins(plugin.tag).entrySet()) items.put(e.getKey(), e.getValue());
        result.put(tag, items);
      } else {
        if (plugin.options.contains(MULTI_OK)) {
          ArrayList<MapSerializable> l = new ArrayList<>();
          for (PluginInfo info : infos) l.add(info);
          result.put(tag, l);
        } else {
          result.put(tag, infos.get(0));
        }

      }

    }


    addCacheConfig(m, filterCacheConfig, queryResultCacheConfig, documentCacheConfig, fieldValueCacheConfig);
    if (jmxConfig != null) result.put("jmx", jmxConfig);
    m = new LinkedHashMap();
    result.put("requestDispatcher", m);
    m.put("handleSelect", handleSelect);
    if (httpCachingConfig != null) m.put("httpCaching", httpCachingConfig);
    m.put("requestParsers", makeMap("multipartUploadLimitKB", multipartUploadLimitKB,
        "formUploadLimitKB", formUploadLimitKB,
        "addHttpRequestToContext", addHttpRequestToContext));
    if (indexConfig != null) result.put("indexConfig", indexConfig);

    m = new LinkedHashMap();
    result.put("peerSync", m);
    m.put("useRangeVersions", useRangeVersionsForPeerSync);

    //TODO there is more to add

    return result;
  }

  private void addCacheConfig(Map queryMap, CacheConfig... cache) {
    if (cache == null) return;
    for (CacheConfig config : cache) if (config != null) queryMap.put(config.getNodeName(), config);

  }

  @Override
  protected Properties getSubstituteProperties() {
    Map<String, Object> p = getOverlay().getUserProps();
    if (p == null || p.isEmpty()) return super.getSubstituteProperties();
    Properties result = new Properties(super.getSubstituteProperties());
    result.putAll(p);
    return result;
  }

  private ConfigOverlay overlay;

  public ConfigOverlay getOverlay() {
    if (overlay == null) {
      overlay = getConfigOverlay(getResourceLoader());
    }
    return overlay;
  }

  public RequestParams getRequestParams() {
    if (requestParams == null) {
      return refreshRequestParams();
    }
    return requestParams;
  }


  public RequestParams refreshRequestParams() {
    requestParams = RequestParams.getFreshRequestParams(getResourceLoader(), requestParams);
    log.debug("current version of requestparams : {}", requestParams.getZnodeVersion());
    return requestParams;
  }

}
