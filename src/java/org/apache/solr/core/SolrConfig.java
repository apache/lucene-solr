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
import org.apache.solr.handler.PingRequestHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;

import org.apache.solr.search.CacheConfig;
import org.apache.solr.update.SolrIndexConfig;
import org.apache.lucene.search.BooleanQuery;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import java.util.Map;
import java.util.Collection;
import java.util.HashSet;
import java.util.StringTokenizer;
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

  public static final String DEFAULT_CONF_FILE = "solrconfig.xml";

  /**
   * Singleton containing all configuration.
   * Compatibility feature for single-core (pre-solr215 patch) code.
   * Most usage should be converted by:
   * - using the configuration directly when used in Abstract{Tokeinizer,TokenFilter}Factory.init().
   * - getting the configuration through the owning core if accessible (SolrCore.getSolrConfig()).
   * - getting the core by name then its configuration as above
   */
  @Deprecated
  public static SolrConfig config = null; 

  /** An interface to denote objects that need a SolrConfig to be initialized.
   *  These are mainly TokenFilterFactory and TokenizerFactory subclasses.
   */
  public interface Initializable {
    /** <code>init</code> will be called just once, immediately after creation.
     */
    void init(SolrConfig solrConfig, Map<String,String> args);
  }

  public final String configFile;

  /**
   * Singleton keeping track of configuration errors
   */
  public static final Collection<Throwable> severeErrors = new HashSet<Throwable>();

  /** Creates a default instance from the solrconfig.xml. */
  public SolrConfig()
  throws ParserConfigurationException, IOException, SAXException {
    this( null, DEFAULT_CONF_FILE, null );
  }
  /** Creates a configuration instance from a file. */
  public SolrConfig(String file)
  throws ParserConfigurationException, IOException, SAXException {
     this( null, file, null);
  }

  @Deprecated
  public SolrConfig(String file, InputStream is)
  throws ParserConfigurationException, IOException, SAXException 
  {
    this( null, file, is );
  }
  
  /** Creates a configuration instance from an input stream. */
  public SolrConfig(String instanceDir, String file, InputStream is)
  throws ParserConfigurationException, IOException, SAXException 
  {
    super(instanceDir, file, is, "/config/");
    this.configFile = file;
    defaultIndexConfig = new SolrIndexConfig(this, null, null);
    mainIndexConfig = new SolrIndexConfig(this, "mainIndex", defaultIndexConfig);
    
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
    userCacheConfigs = CacheConfig.getMultipleConfigs(this, "query/cache");
    org.apache.solr.search.SolrIndexSearcher.initRegenerators(this);

    hashSetInverseLoadFactor = 1.0f / getFloat("//HashDocSet/@loadFactor",0.75f);
    hashDocSetMaxSize= getInt("//HashDocSet/@maxSize",-1);
    
    pingQueryParams = readPingQueryParams(this);
    Config.log.info("Loaded SolrConfig: " + file);
    
    // TODO -- at solr 2.0. this should go away
    config = this;
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
  public final CacheConfig[] userCacheConfigs;
  // SolrIndexSearcher - more...
  public final boolean useFilterForSortedQuery;
  public final int queryResultWindowSize;
  public final int queryResultMaxDocsCached;
  public final boolean enableLazyFieldLoading;
  // DocSet
  public final float hashSetInverseLoadFactor;
  public final int hashDocSetMaxSize;
  // default & main index configurations
  public final SolrIndexConfig defaultIndexConfig;
  public final SolrIndexConfig mainIndexConfig;
  
  // ping query request parameters
  @Deprecated
  private final NamedList pingQueryParams;

  static private NamedList readPingQueryParams(SolrConfig config) {  
    // TODO: check for nested tags and parse as a named list instead
    String urlSnippet = config.get("admin/pingQuery", "").trim();
    
    StringTokenizer qtokens = new StringTokenizer(urlSnippet,"&");
    String tok;
    NamedList params = new NamedList();
    while (qtokens.hasMoreTokens()) {
      tok = qtokens.nextToken();
      String[] split = tok.split("=", 2);
      params.add(split[0], split[1]);
    }
    return params;
  }
  
  /**
   * Returns a Request object based on the admin/pingQuery section
   * of the Solr config file.
   * 
   * @use {@link PingRequestHandler} instead 
   */
  @Deprecated
  public SolrQueryRequest getPingQueryRequest(SolrCore core) {
    return new LocalSolrQueryRequest(core, pingQueryParams);
  }
}
