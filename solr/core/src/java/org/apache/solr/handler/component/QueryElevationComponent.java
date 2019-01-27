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
package org.apache.solr.handler.component;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SimpleFieldComparator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.QueryElevationParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.XmlConfigFile;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.ElevatedMarkerFactory;
import org.apache.solr.response.transform.ExcludedMarkerFactory;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.grouping.GroupingSpecification;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.VersionedFile;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * A component to elevate some documents to the top of the result set.
 *
 * @since solr 1.3
 */
@SuppressWarnings("WeakerAccess")
public class QueryElevationComponent extends SearchComponent implements SolrCoreAware {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // Constants used in solrconfig.xml
  @VisibleForTesting
  static final String FIELD_TYPE = "queryFieldType";
  @VisibleForTesting
  static final String CONFIG_FILE = "config-file";
  private static final String EXCLUDE = "exclude";

  /** @see #getBoostDocs(SolrIndexSearcher, Set, Map) */
  private static final String BOOSTED_DOCIDS = "BOOSTED_DOCIDS";

  /** Key to {@link SolrQueryRequest#getContext()} for a {@code Set<BytesRef>} of included IDs in configured
   * order (so-called priority). */
  public static final String BOOSTED = "BOOSTED";
  /** Key to {@link SolrQueryRequest#getContext()} for a {@code Set<BytesRef>} of excluded IDs. */
  public static final String EXCLUDED = "EXCLUDED";

  private static final boolean DEFAULT_FORCE_ELEVATION = false;
  private static final boolean DEFAULT_USE_CONFIGURED_ELEVATED_ORDER = true;
  private static final boolean DEFAULT_SUBSET_MATCH = false;
  private static final String DEFAULT_EXCLUDE_MARKER_FIELD_NAME = "excluded";
  private static final String DEFAULT_EDITORIAL_MARKER_FIELD_NAME = "elevated";

  protected SolrParams initArgs;
  protected Analyzer queryAnalyzer;
  protected SchemaField uniqueKeyField;
  /** @see QueryElevationParams#FORCE_ELEVATION */
  protected boolean forceElevation;
  /** @see QueryElevationParams#USE_CONFIGURED_ELEVATED_ORDER */
  protected boolean useConfiguredElevatedOrder;

  protected boolean initialized;

  /**
   * For each IndexReader, keep an ElevationProvider when the configuration is loaded from the data directory.
   * The key is null if loaded from the config directory, and is never re-loaded.
   */
  private final Map<IndexReader, ElevationProvider> elevationProviderCache = new WeakHashMap<>();

  @Override
  public void init(NamedList args) {
    this.initArgs = args.toSolrParams();
  }

  @Override
  public void inform(SolrCore core) {
    initialized = false;
    try {
      parseFieldType(core);
      setUniqueKeyField(core);
      parseExcludedMarkerFieldName(core);
      parseEditorialMarkerFieldName(core);
      parseForceElevation();
      parseUseConfiguredOrderForElevations();
      loadElevationConfiguration(core);
      initialized = true;
    } catch (InitializationException e) {
      assert !initialized;
      handleInitializationException(e, e.exceptionCause);
    } catch (Exception e) {
      assert !initialized;
      handleInitializationException(e, InitializationExceptionCause.OTHER);
    }
  }

  private void parseFieldType(SolrCore core) throws InitializationException {
    String a = initArgs.get(FIELD_TYPE);
    if (a != null) {
      FieldType ft = core.getLatestSchema().getFieldTypes().get(a);
      if (ft == null) {
        throw new InitializationException("Parameter " + FIELD_TYPE + " defines an unknown field type \"" + a + "\"", InitializationExceptionCause.UNKNOWN_FIELD_TYPE);
      }
      queryAnalyzer = ft.getQueryAnalyzer();
    }
  }

  private void setUniqueKeyField(SolrCore core) throws InitializationException {
    uniqueKeyField = core.getLatestSchema().getUniqueKeyField();
    if (uniqueKeyField == null) {
      throw new InitializationException("This component requires the schema to have a uniqueKeyField", InitializationExceptionCause.MISSING_UNIQUE_KEY_FIELD);
    }
  }

  private void parseExcludedMarkerFieldName(SolrCore core) {
    String markerName = initArgs.get(QueryElevationParams.EXCLUDE_MARKER_FIELD_NAME, DEFAULT_EXCLUDE_MARKER_FIELD_NAME);
    core.addTransformerFactory(markerName, new ExcludedMarkerFactory());
  }

  private void parseEditorialMarkerFieldName(SolrCore core) {
    String markerName = initArgs.get(QueryElevationParams.EDITORIAL_MARKER_FIELD_NAME, DEFAULT_EDITORIAL_MARKER_FIELD_NAME);
    core.addTransformerFactory(markerName, new ElevatedMarkerFactory());
  }

  private void parseForceElevation() {
    forceElevation = initArgs.getBool(QueryElevationParams.FORCE_ELEVATION, DEFAULT_FORCE_ELEVATION);
  }

  private void parseUseConfiguredOrderForElevations() {
    useConfiguredElevatedOrder = initArgs.getBool(QueryElevationParams.USE_CONFIGURED_ELEVATED_ORDER, DEFAULT_USE_CONFIGURED_ELEVATED_ORDER);
  }

  /**
   * (Re)Loads elevation configuration.
   *
   * @param core The core holding this component.
   * @return The number of elevation rules parsed.
   */
  @SuppressWarnings("WeakerAccess")
  protected int loadElevationConfiguration(SolrCore core) throws Exception {
    synchronized (elevationProviderCache) {
      elevationProviderCache.clear();
      String configFileName = initArgs.get(CONFIG_FILE);
      if (configFileName == null) {
        // Throw an exception which is handled by handleInitializationException().
        // If not overridden handleInitializationException() simply skips this exception.
        throw new InitializationException("Missing component parameter " + CONFIG_FILE + " - it has to define the path to the elevation configuration file", InitializationExceptionCause.NO_CONFIG_FILE_DEFINED);
      }
      boolean configFileExists = false;
      ElevationProvider elevationProvider = NO_OP_ELEVATION_PROVIDER;

      // check if using ZooKeeper
      ZkController zkController = core.getCoreContainer().getZkController();
      if (zkController != null) {
        // TODO : shouldn't have to keep reading the config name when it has been read before
        configFileExists = zkController.configFileExists(zkController.getZkStateReader().readConfigName(core.getCoreDescriptor().getCloudDescriptor().getCollectionName()), configFileName);
      } else {
        File fC = new File(core.getResourceLoader().getConfigDir(), configFileName);
        File fD = new File(core.getDataDir(), configFileName);
        if (fC.exists() == fD.exists()) {
          InitializationException e = new InitializationException("Missing config file \"" + configFileName + "\" - either " + fC.getAbsolutePath() + " or " + fD.getAbsolutePath() + " must exist, but not both", InitializationExceptionCause.MISSING_CONFIG_FILE);
          elevationProvider = handleConfigLoadingException(e, true);
          elevationProviderCache.put(null, elevationProvider);
        } else if (fC.exists()) {
          if (fC.length() == 0) {
            InitializationException e = new InitializationException("Empty config file \"" + configFileName + "\" - " + fC.getAbsolutePath(), InitializationExceptionCause.EMPTY_CONFIG_FILE);
            elevationProvider = handleConfigLoadingException(e, true);
          } else {
            configFileExists = true;
            log.info("Loading QueryElevation from: " + fC.getAbsolutePath());
            XmlConfigFile cfg = new XmlConfigFile(core.getResourceLoader(), configFileName);
            elevationProvider = loadElevationProvider(cfg);
          }
          elevationProviderCache.put(null, elevationProvider);
        }
      }
      //in other words, we think this is in the data dir, not the conf dir
      if (!configFileExists) {
        // preload the first data
        RefCounted<SolrIndexSearcher> searchHolder = null;
        try {
          searchHolder = core.getNewestSearcher(false);
          if (searchHolder == null) {
            elevationProvider = NO_OP_ELEVATION_PROVIDER;
          } else {
            IndexReader reader = searchHolder.get().getIndexReader();
            elevationProvider = getElevationProvider(reader, core);
          }
        } finally {
          if (searchHolder != null) searchHolder.decref();
        }
      }
      return elevationProvider.size();
    }
  }

  /**
   * Handles the exception that occurred while initializing this component.
   * If this method does not throw an exception, this component silently fails to initialize
   * and is muted with field {@link #initialized} which becomes {@code false}.
   */
  protected void handleInitializationException(Exception exception, InitializationExceptionCause cause) {
    if (cause != InitializationExceptionCause.NO_CONFIG_FILE_DEFINED) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error initializing " + QueryElevationComponent.class.getSimpleName(), exception);
    }
  }

  /**
   * Handles an exception that occurred while loading the configuration resource.
   *
   * @param e                   The exception caught.
   * @param resourceAccessIssue <code>true</code> if the exception has been thrown
   *                            because the resource could not be accessed (missing or cannot be read)
   *                            or the config file is empty; <code>false</code> if the resource has
   *                            been found and accessed but the error occurred while loading the resource
   *                            (invalid format, incomplete or corrupted).
   * @return The {@link ElevationProvider} to use if the exception is absorbed. If {@code null}
   *         is returned, the {@link #NO_OP_ELEVATION_PROVIDER} is used but not cached in
   *         the {@link ElevationProvider} cache.
   * @throws E If the exception is not absorbed.
   */
  protected <E extends Exception> ElevationProvider handleConfigLoadingException(E e, boolean resourceAccessIssue) throws E {
    throw e;
  }

  /**
   * Gets the {@link ElevationProvider} from the data dir or from the cache.
   *
   * @return The cached or loaded {@link ElevationProvider}.
   * @throws java.io.IOException                  If the configuration resource cannot be found, or if an I/O error occurs while analyzing the triggering queries.
   * @throws org.xml.sax.SAXException                 If the configuration resource is not a valid XML content.
   * @throws javax.xml.parsers.ParserConfigurationException If the configuration resource is not a valid XML configuration.
   * @throws RuntimeException             If the configuration resource is not an XML content of the expected format
   *                                      (either {@link RuntimeException} or {@link org.apache.solr.common.SolrException}).
   */
  @VisibleForTesting
  ElevationProvider getElevationProvider(IndexReader reader, SolrCore core) throws Exception {
    synchronized (elevationProviderCache) {
      ElevationProvider elevationProvider;
      elevationProvider = elevationProviderCache.get(null);
      if (elevationProvider != null) return elevationProvider;

      elevationProvider = elevationProviderCache.get(reader);
      if (elevationProvider == null) {
        Exception loadingException = null;
        boolean resourceAccessIssue = false;
        try {
          elevationProvider = loadElevationProvider(core);
        } catch (IOException e) {
          loadingException = e;
          resourceAccessIssue = true;
        } catch (Exception e) {
          loadingException = e;
        }
        boolean shouldCache = true;
        if (loadingException != null) {
          elevationProvider = handleConfigLoadingException(loadingException, resourceAccessIssue);
          if (elevationProvider == null) {
            elevationProvider = NO_OP_ELEVATION_PROVIDER;
            shouldCache = false;
          }
        }
        if (shouldCache) {
          elevationProviderCache.put(reader, elevationProvider);
        }
      }
      assert elevationProvider != null;
      return elevationProvider;
    }
  }

  /**
   * Loads the {@link ElevationProvider} from the data dir.
   *
   * @return The loaded {@link ElevationProvider}.
   * @throws java.io.IOException                  If the configuration resource cannot be found, or if an I/O error occurs while analyzing the triggering queries.
   * @throws org.xml.sax.SAXException                 If the configuration resource is not a valid XML content.
   * @throws javax.xml.parsers.ParserConfigurationException If the configuration resource is not a valid XML configuration.
   * @throws RuntimeException             If the configuration resource is not an XML content of the expected format
   *                                      (either {@link RuntimeException} or {@link org.apache.solr.common.SolrException}).
   */
  private ElevationProvider loadElevationProvider(SolrCore core) throws IOException, SAXException, ParserConfigurationException {
    String configFileName = initArgs.get(CONFIG_FILE);
    if (configFileName == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "QueryElevationComponent must specify argument: " + CONFIG_FILE);
    }
    log.info("Loading QueryElevation from data dir: " + configFileName);

    XmlConfigFile cfg;
    ZkController zkController = core.getCoreContainer().getZkController();
    if (zkController != null) {
      cfg = new XmlConfigFile(core.getResourceLoader(), configFileName, null, null);
    } else {
      InputStream is = VersionedFile.getLatestFile(core.getDataDir(), configFileName);
      cfg = new XmlConfigFile(core.getResourceLoader(), configFileName, new InputSource(is), null);
    }
    ElevationProvider elevationProvider = loadElevationProvider(cfg);
    assert elevationProvider != null;
    return elevationProvider;
  }

  /**
   * Loads the {@link ElevationProvider}.
   *
   * @throws java.io.IOException      If an I/O error occurs while analyzing the triggering queries.
   * @throws RuntimeException If the config does not provide an XML content of the expected format
   *                          (either {@link RuntimeException} or {@link org.apache.solr.common.SolrException}).
   */
  @SuppressWarnings("WeakerAccess")
  protected ElevationProvider loadElevationProvider(XmlConfigFile config) throws IOException {
    Map<ElevatingQuery, ElevationBuilder> elevationBuilderMap = new LinkedHashMap<>();
    XPath xpath = XPathFactory.newInstance().newXPath();
    NodeList nodes = (NodeList) config.evaluate("elevate/query", XPathConstants.NODESET);
    for (int i = 0; i < nodes.getLength(); i++) {
      Node node = nodes.item(i);
      String queryString = DOMUtil.getAttr(node, "text", "missing query 'text'");
      String matchString = DOMUtil.getAttr(node, "match");
      ElevatingQuery elevatingQuery = new ElevatingQuery(queryString, parseMatchPolicy(matchString));

      NodeList children;
      try {
        children = (NodeList) xpath.evaluate("doc", node, XPathConstants.NODESET);
      } catch (XPathExpressionException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "query requires '<doc .../>' child");
      }

      if (children.getLength() == 0) { // weird
        continue;
      }
      ElevationBuilder elevationBuilder = new ElevationBuilder();
      for (int j = 0; j < children.getLength(); j++) {
        Node child = children.item(j);
        String id = DOMUtil.getAttr(child, "id", "missing 'id'");
        String e = DOMUtil.getAttr(child, EXCLUDE, null);
        if (e != null) {
          if (Boolean.valueOf(e)) {
            elevationBuilder.addExcludedIds(Collections.singleton(id));
            continue;
          }
        }
        elevationBuilder.addElevatedIds(Collections.singletonList(id));
      }

      // It is allowed to define multiple times different elevations for the same query. In this case the elevations
      // are merged in the ElevationBuilder (they will be triggered at the same time).
      ElevationBuilder previousElevationBuilder = elevationBuilderMap.get(elevatingQuery);
      if (previousElevationBuilder == null) {
        elevationBuilderMap.put(elevatingQuery, elevationBuilder);
      } else {
        previousElevationBuilder.merge(elevationBuilder);
      }
    }
    return createElevationProvider(queryAnalyzer, elevationBuilderMap);
  }

  private boolean parseMatchPolicy(String matchString) {
    if (matchString == null) {
      return DEFAULT_SUBSET_MATCH;
    } else if (matchString.equalsIgnoreCase("exact")) {
      return false;
    } else if (matchString.equalsIgnoreCase("subset")) {
      return true;
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "invalid value \"" + matchString + "\" for query match attribute");
    }
  }

  //---------------------------------------------------------------------------------
  // SearchComponent
  //---------------------------------------------------------------------------------

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (!initialized || !rb.req.getParams().getBool(QueryElevationParams.ENABLE, true)) {
      return;
    }

    Elevation elevation = getElevation(rb);
    if (elevation != null) {
      setQuery(rb, elevation);
      setSort(rb, elevation);
    }

    if (rb.isDebug() && rb.isDebugQuery()) {
      addDebugInfo(rb, elevation);
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    // Do nothing -- the real work is modifying the input query
  }

  protected Elevation getElevation(ResponseBuilder rb) {
    SolrParams localParams = rb.getQparser().getLocalParams();
    String queryString = localParams == null ? rb.getQueryString() : localParams.get(QueryParsing.V);
    if (queryString == null || rb.getQuery() == null) {
      return null;
    }

    SolrParams params = rb.req.getParams();
    String paramElevatedIds = params.get(QueryElevationParams.IDS);
    String paramExcludedIds = params.get(QueryElevationParams.EXCLUDE);
    try {
      if (paramElevatedIds != null || paramExcludedIds != null) {
        List<String> elevatedIds = paramElevatedIds != null ? StrUtils.splitSmart(paramElevatedIds,",", true) : Collections.emptyList();
        List<String> excludedIds = paramExcludedIds != null ? StrUtils.splitSmart(paramExcludedIds, ",", true) : Collections.emptyList();
        return new ElevationBuilder().addElevatedIds(elevatedIds).addExcludedIds(excludedIds).build();
      } else {
        IndexReader reader = rb.req.getSearcher().getIndexReader();
        return getElevationProvider(reader, rb.req.getCore()).getElevationForQuery(queryString);
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error loading elevation", e);
    }
  }

  private void setQuery(ResponseBuilder rb, Elevation elevation) {
    rb.req.getContext().put(BOOSTED, elevation.elevatedIds);

    // Change the query to insert forced documents
    SolrParams params = rb.req.getParams();
    if (params.getBool(QueryElevationParams.EXCLUSIVE, false)) {
      // We only want these elevated results
      rb.setQuery(new BoostQuery(elevation.includeQuery, 0f));
    } else {
      BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
      queryBuilder.add(rb.getQuery(), BooleanClause.Occur.SHOULD);
      queryBuilder.add(new BoostQuery(elevation.includeQuery, 0f), BooleanClause.Occur.SHOULD);
      if (elevation.excludeQueries != null) {
        if (params.getBool(QueryElevationParams.MARK_EXCLUDES, false)) {
          // We are only going to mark items as excluded, not actually exclude them.
          // This works with the EditorialMarkerFactory.
          rb.req.getContext().put(EXCLUDED, elevation.excludedIds);
        } else {
          for (TermQuery tq : elevation.excludeQueries) {
            queryBuilder.add(tq, BooleanClause.Occur.MUST_NOT);
          }
        }
      }
      rb.setQuery(queryBuilder.build());
    }
  }

  private void setSort(ResponseBuilder rb, Elevation elevation) throws IOException {
    if (elevation.elevatedIds.isEmpty()) {
      return;
    }
    boolean forceElevation = rb.req.getParams().getBool(QueryElevationParams.FORCE_ELEVATION, this.forceElevation);
    boolean useConfigured = rb.req.getParams().getBool(QueryElevationParams.USE_CONFIGURED_ELEVATED_ORDER, this.useConfiguredElevatedOrder);
    final IntIntHashMap elevatedWithPriority = getBoostDocs(rb.req.getSearcher(), elevation.elevatedIds, rb.req.getContext());
    ElevationComparatorSource comparator = new ElevationComparatorSource(elevatedWithPriority, useConfigured);
    setSortSpec(rb, forceElevation, comparator);
    setGroupingSpec(rb, forceElevation, comparator);
  }

  private void setSortSpec(ResponseBuilder rb, boolean forceElevation, ElevationComparatorSource comparator) {
    // if the sort is 'score desc' use a custom sorting method to
    // insert documents in their proper place
    SortSpec sortSpec = rb.getSortSpec();
    if (sortSpec.getSort() == null) {
      sortSpec.setSortAndFields(
              new Sort(
                      new SortField("_elevate_", comparator, true),
                      new SortField(null, SortField.Type.SCORE, false)),
              Arrays.asList(new SchemaField[2]));
    } else {
      // Check if the sort is based on score
      SortSpec modSortSpec = this.modifySortSpec(sortSpec, forceElevation, comparator);
      if (null != modSortSpec) {
        rb.setSortSpec(modSortSpec);
      }
    }
  }

  private void setGroupingSpec(ResponseBuilder rb, boolean forceElevation, ElevationComparatorSource comparator) {
    // alter the sorting in the grouping specification if there is one
    GroupingSpecification groupingSpec = rb.getGroupingSpec();
    if(groupingSpec != null) {
      SortSpec groupSortSpec = groupingSpec.getGroupSortSpec();
      SortSpec modGroupSortSpec = this.modifySortSpec(groupSortSpec, forceElevation, comparator);
      if (modGroupSortSpec != null) {
        groupingSpec.setGroupSortSpec(modGroupSortSpec);
      }
      SortSpec withinGroupSortSpec = groupingSpec.getWithinGroupSortSpec();
      SortSpec modWithinGroupSortSpec = this.modifySortSpec(withinGroupSortSpec, forceElevation, comparator);
      if (modWithinGroupSortSpec != null) {
        groupingSpec.setWithinGroupSortSpec(modWithinGroupSortSpec);
      }
    }
  }

  private SortSpec modifySortSpec(SortSpec current, boolean forceElevation, ElevationComparatorSource comparator) {
    boolean modify = false;
    SortField[] currentSorts = current.getSort().getSort();
    List<SchemaField> currentFields = current.getSchemaFields();

    ArrayList<SortField> sorts = new ArrayList<>(currentSorts.length + 1);
    List<SchemaField> fields = new ArrayList<>(currentFields.size() + 1);

    // Perhaps force it to always sort by score
    if (forceElevation && currentSorts[0].getType() != SortField.Type.SCORE) {
      sorts.add(new SortField("_elevate_", comparator, true));
      fields.add(null);
      modify = true;
    }
    for (int i = 0; i < currentSorts.length; i++) {
      SortField sf = currentSorts[i];
      if (sf.getType() == SortField.Type.SCORE) {
        sorts.add(new SortField("_elevate_", comparator, !sf.getReverse()));
        fields.add(null);
        modify = true;
      }
      sorts.add(sf);
      fields.add(currentFields.get(i));
    }
    return modify ?
            new SortSpec(new Sort(sorts.toArray(new SortField[sorts.size()])),
                    fields,
                    current.getCount(),
                    current.getOffset())
            : null;
  }

  private void addDebugInfo(ResponseBuilder rb, Elevation elevation) {
    List<String> match = null;
    if (elevation != null) {
      // Extract the elevated terms into a list
      match = new ArrayList<>(elevation.includeQuery.clauses().size());
      for (BooleanClause clause : elevation.includeQuery.clauses()) {
        TermQuery tq = (TermQuery) clause.getQuery();
        match.add(tq.getTerm().text());
      }
    }
    SimpleOrderedMap<Object> dbg = new SimpleOrderedMap<>();
    dbg.add("q", rb.getQueryString());
    dbg.add("match", match);
    rb.addDebugInfo("queryBoosting", dbg);
  }

  //---------------------------------------------------------------------------------
  // Boosted docs helper
  //---------------------------------------------------------------------------------

  /**
   * Resolves a set of boosted docs by uniqueKey to a map of docIds mapped to a priority value &gt; 0.
   * @param indexSearcher the SolrIndexSearcher; required
   * @param boosted are the set of uniqueKey values to be boosted in priority order.  If null; returns null.
   * @param context the {@link SolrQueryRequest#getContext()} or null if none.  We'll cache our results here.
   */
  //TODO consider simplifying to remove "boosted" arg which can be looked up in context via BOOSTED key?
  public static IntIntHashMap getBoostDocs(SolrIndexSearcher indexSearcher, Set<BytesRef> boosted, Map context) throws IOException {

    IntIntHashMap boostDocs = null;

    if (boosted != null) {

      //First see if it's already in the request context. Could have been put there by another caller.
      if (context != null) {
        boostDocs = (IntIntHashMap) context.get(BOOSTED_DOCIDS);
        if (boostDocs != null) {
          return boostDocs;
        }
      }

      //Not in the context yet so load it.
      boostDocs = new IntIntHashMap(boosted.size()); // docId to boost
      int priority = boosted.size() + 1; // the corresponding priority for each boosted key (starts at this; decrements down)
      for (BytesRef uniqueKey : boosted) {
        priority--; // therefore first == bosted.size(); last will be 1
        long segAndId = indexSearcher.lookupId(uniqueKey); // higher 32 bits == segment ID, low 32 bits == doc ID
        if (segAndId == -1) { // not found
          continue;
        }
        int seg = (int) (segAndId >> 32);
        int localDocId = (int) segAndId;
        final IndexReaderContext indexReaderContext = indexSearcher.getTopReaderContext().children().get(seg);
        int docId = indexReaderContext.docBaseInParent + localDocId;
        boostDocs.put(docId, priority);
      }
      assert priority == 1; // the last priority (lowest)
    }

    if (context != null) {
      //noinspection unchecked
      context.put(BOOSTED_DOCIDS, boostDocs);
    }

    return boostDocs;
  }

  //---------------------------------------------------------------------------------
  // SolrInfoBean
  //---------------------------------------------------------------------------------

  @Override
  public String getDescription() {
    return "Query Boosting -- boost particular documents for a given query";
  }

  //---------------------------------------------------------------------------------
  // Overrides
  //---------------------------------------------------------------------------------

  /**
   * Creates the {@link ElevationProvider} to set during configuration loading. The same instance will be used later
   * when elevating results for queries.
   *
   * @param queryAnalyzer to analyze and tokenize the query.
   * @param elevationBuilderMap map of all {@link ElevatingQuery} and their corresponding {@link ElevationBuilder}.
   * @return The created {@link ElevationProvider}.
   */
  protected ElevationProvider createElevationProvider(Analyzer queryAnalyzer, Map<ElevatingQuery, ElevationBuilder> elevationBuilderMap) {
    return new MapElevationProvider(elevationBuilderMap);
  }

  //---------------------------------------------------------------------------------
  // Query analysis and tokenization
  //---------------------------------------------------------------------------------

  /**
   * Analyzes the provided query string and returns a space concatenation of the analyzed tokens.
   */
  public String analyzeQuery(String query) {
    //split query terms with analyzer then join
    StringBuilder norm = new StringBuilder();
    try (TokenStream tokens = queryAnalyzer.tokenStream("", query)) {
      tokens.reset();
      CharTermAttribute termAtt = tokens.addAttribute(CharTermAttribute.class);
      while (tokens.incrementToken()) {
        norm.append(termAtt);
      }
      tokens.end();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    return norm.toString();
  }

  //---------------------------------------------------------------------------------
  // Testing
  //---------------------------------------------------------------------------------

  /**
   * Helpful for testing without loading config.xml.
   *
   * @param reader      The {@link org.apache.lucene.index.IndexReader}.
   * @param queryString The query for which to elevate some documents. If the query has already been defined an
   *                    elevation, this method overwrites it.
   * @param subsetMatch <code>true</code> for query subset match; <code>false</code> for query exact match.
   * @param elevatedIds The readable ids of the documents to set as top results for the provided query.
   * @param excludedIds The readable ids of the document to exclude from results for the provided query.
   * @throws java.io.IOException If there is a low-level I/O error.
   */
  @VisibleForTesting
  void setTopQueryResults(IndexReader reader, String queryString, boolean subsetMatch,
                          String[] elevatedIds, String[] excludedIds) throws IOException {
    clearElevationProviderCache();
    ElevatingQuery elevatingQuery = new ElevatingQuery(queryString, subsetMatch);
    ElevationBuilder elevationBuilder = new ElevationBuilder();
    elevationBuilder.addElevatedIds(elevatedIds == null ? Collections.emptyList() : Arrays.asList(elevatedIds));
    elevationBuilder.addExcludedIds(excludedIds == null ? Collections.emptyList() : Arrays.asList(excludedIds));
    Map<ElevatingQuery, ElevationBuilder> elevationBuilderMap = ImmutableMap.of(elevatingQuery, elevationBuilder);
    synchronized (elevationProviderCache) {
      elevationProviderCache.computeIfAbsent(reader, k -> createElevationProvider(queryAnalyzer, elevationBuilderMap));
    }
  }

  @VisibleForTesting
  void clearElevationProviderCache() {
    synchronized (elevationProviderCache) {
        elevationProviderCache.clear();
    }
  }

  //---------------------------------------------------------------------------------
  // Exception
  //---------------------------------------------------------------------------------

  private static class InitializationException extends Exception {

    private final InitializationExceptionCause exceptionCause;

    InitializationException(String message, InitializationExceptionCause exceptionCause) {
      super(message);
      this.exceptionCause = exceptionCause;
    }
  }

  protected enum InitializationExceptionCause {
    /**
     * The component parameter {@link #FIELD_TYPE} defines an unknown field type.
     */
    UNKNOWN_FIELD_TYPE,
    /**
     * This component requires the schema to have a uniqueKeyField, which it does not have.
     */
    MISSING_UNIQUE_KEY_FIELD,
    /**
     * Missing component parameter {@link #CONFIG_FILE} - it has to define the path to the elevation configuration file (e.g. elevate.xml).
     */
    NO_CONFIG_FILE_DEFINED,
    /**
     * The elevation configuration file (e.g. elevate.xml) cannot be found, or is defined in both conf/ and data/ directories.
     */
    MISSING_CONFIG_FILE,
    /**
     * The elevation configuration file (e.g. elevate.xml) is empty.
     */
    EMPTY_CONFIG_FILE,
    /**
     * Unclassified exception cause.
     */
    OTHER,
  }

  //---------------------------------------------------------------------------------
  // Elevation classes
  //---------------------------------------------------------------------------------

  /**
   * Provides the elevations defined for queries.
   */
  protected interface ElevationProvider {
    /**
     * Gets the elevation associated to the provided query.
     * <p>
     * By contract and by design, only one elevation may be associated
     * to a given query (this can be safely verified by an assertion).
     *
     * @param queryString The query string (not {@link #analyzeQuery(String) analyzed} yet,
     *              this {@link ElevationProvider} is in charge of analyzing it).
     * @return The elevation associated with the query; or <code>null</code> if none.
     */
    Elevation getElevationForQuery(String queryString);

    /**
     * Gets the number of query elevations in this {@link ElevationProvider}.
     */
    @VisibleForTesting
    int size();
  }

  /**
   * {@link ElevationProvider} that returns no elevation.
   */
  @SuppressWarnings("WeakerAccess")
  protected static final ElevationProvider NO_OP_ELEVATION_PROVIDER = new ElevationProvider() {
    @Override
    public Elevation getElevationForQuery(String queryString) {
      return null;
    }

    @Override
    public int size() {
      return 0;
    }
  };

  /**
   * Simple query exact match {@link ElevationProvider}.
   * <p>
   * It does not support subset matching (see {@link #parseMatchPolicy(String)}).
   * <p>
   * Immutable.
   */
  @SuppressWarnings("WeakerAccess")
  protected class MapElevationProvider implements ElevationProvider {

    private final Map<String, Elevation> elevationMap;

    public MapElevationProvider(Map<ElevatingQuery, ElevationBuilder> elevationBuilderMap) {
      elevationMap = buildElevationMap(elevationBuilderMap);
    }

    private Map<String, Elevation> buildElevationMap(Map<ElevatingQuery, ElevationBuilder> elevationBuilderMap) {
      Map<String, Elevation> elevationMap = Maps.newHashMapWithExpectedSize(elevationBuilderMap.size());
      for (Map.Entry<ElevatingQuery, ElevationBuilder> entry : elevationBuilderMap.entrySet()) {
        ElevatingQuery elevatingQuery = entry.getKey();
        if (elevatingQuery.subsetMatch) {
          throw new UnsupportedOperationException("Subset matching is not supported by " + getClass().getName());
        }
        String analyzedQuery = analyzeQuery(elevatingQuery.queryString);
        Elevation elevation = entry.getValue().build();
        Elevation duplicateElevation = elevationMap.put(analyzedQuery, elevation);
        if (duplicateElevation != null) {
          throw new IllegalArgumentException("Duplicate elevation for query.  Analyzed: \"" + analyzedQuery + "\"" +
              " Original: \"" + elevatingQuery.queryString + "\"");
        }
      }
      return Collections.unmodifiableMap(elevationMap);
    }

    @Override
    public Elevation getElevationForQuery(String queryString) {
      String analyzedQuery = analyzeQuery(queryString);
      return elevationMap.get(analyzedQuery);
    }

    @Override
    public int size() {
      return elevationMap.size();
    }
  }

  /**
   * Query triggering elevation.
   */
  @SuppressWarnings("WeakerAccess")
  protected static class ElevatingQuery {

    public final String queryString;
    public final boolean subsetMatch;

    /**
     * @param queryString The query to elevate documents for (not the analyzed form).
     * @param subsetMatch Whether to match a subset of query terms.
     */
    protected ElevatingQuery(String queryString, boolean subsetMatch) throws IOException {
      this.queryString = queryString;
      this.subsetMatch = subsetMatch;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ElevatingQuery)) {
        return false;
      }
      ElevatingQuery eq = (ElevatingQuery) o;
      return queryString.equals(eq.queryString) && subsetMatch == eq.subsetMatch;
    }

    @Override
    public int hashCode() {
      return queryString.hashCode() + (subsetMatch ? 1 : 0);
    }
  }

  /**
   * Builds an {@link Elevation}. This class is used to start defining query elevations, but allowing the merge of
   * multiple elevations for the same query.
   */
  @SuppressWarnings("WeakerAccess")
  public class ElevationBuilder {

    /**
     * The ids of the elevated documents that should appear on top of search results; can be <code>null</code>.
     * The order is retained.
     */
    private LinkedHashSet<BytesRef> elevatedIds;
    /**
     * The ids of the excluded documents that should not appear in search results; can be <code>null</code>.
     */
    private Set<BytesRef> excludedIds;

    // for temporary/transient use when adding an elevated or excluded ID
    private final BytesRefBuilder scratch = new BytesRefBuilder();

    public ElevationBuilder addElevatedIds(List<String> ids) {
      if (elevatedIds == null) {
        elevatedIds = new LinkedHashSet<>(Math.max(10, ids.size()));
      }
      for (String id : ids) {
        elevatedIds.add(toBytesRef(id));
      }
      return this;
    }

    public ElevationBuilder addExcludedIds(Collection<String> ids) {
      if (excludedIds == null) {
        excludedIds = new LinkedHashSet<>(Math.max(10, ids.size()));
      }
      for (String id : ids) {
        excludedIds.add(toBytesRef(id));
      }
      return this;
    }

    public BytesRef toBytesRef(String id) {
      uniqueKeyField.getType().readableToIndexed(id, scratch);
      return scratch.toBytesRef();
    }

    public ElevationBuilder merge(ElevationBuilder elevationBuilder) {
      if (elevatedIds == null) {
        elevatedIds = elevationBuilder.elevatedIds;
      } else if (elevationBuilder.elevatedIds != null) {
        elevatedIds.addAll(elevationBuilder.elevatedIds);
      }
      if (excludedIds == null) {
        excludedIds = elevationBuilder.excludedIds;
      } else if (elevationBuilder.excludedIds != null) {
        excludedIds.addAll(elevationBuilder.excludedIds);
      }
      return this;
    }

    public Elevation build() {
      return new Elevation(elevatedIds, excludedIds, uniqueKeyField.getName());
    }

  }

  /**
   * Elevation of some documents in search results, with potential exclusion of others.
   */
  protected static class Elevation {

    private static final BooleanQuery EMPTY_QUERY = new BooleanQuery.Builder().build();

    public final Set<BytesRef> elevatedIds; // in configured order; not null
    public final BooleanQuery includeQuery; // not null
    public final Set<BytesRef> excludedIds; // not null
    //just keep the term query, b/c we will not always explicitly exclude the item based on markExcludes query time param
    public final TermQuery[] excludeQueries; //may be null

    /**
     * Constructs an elevation.
     * @param elevatedIds           The ids of the elevated documents that should appear on top of search results; can be <code>null</code>.
     *                              In configured order.
     * @param excludedIds           The ids of the excluded documents that should not appear in search results; can be <code>null</code>.
     * @param queryFieldName        The field name to use to create query terms.
     */
    public Elevation(Set<BytesRef> elevatedIds, Set<BytesRef> excludedIds,
                      String queryFieldName) {
      if (elevatedIds == null || elevatedIds.isEmpty()) {
        includeQuery = EMPTY_QUERY;
        this.elevatedIds = Collections.emptySet();
      } else {
        this.elevatedIds = new LinkedHashSet<>(elevatedIds);
        BooleanQuery.Builder includeQueryBuilder = new BooleanQuery.Builder();
        for (BytesRef elevatedId : elevatedIds) {
          includeQueryBuilder.add(new TermQuery(new Term(queryFieldName, elevatedId)), BooleanClause.Occur.SHOULD);
        }
        includeQuery = includeQueryBuilder.build();
      }

      if (excludedIds == null || excludedIds.isEmpty()) {
        this.excludedIds = Collections.emptySet();
        excludeQueries = null;
      } else {
        this.excludedIds = ImmutableSet.copyOf(excludedIds);
        List<TermQuery> excludeQueriesBuilder = new ArrayList<>(excludedIds.size());
        for (BytesRef excludedId : excludedIds) {
          excludeQueriesBuilder.add(new TermQuery(new Term(queryFieldName, excludedId)));
        }
        excludeQueries = excludeQueriesBuilder.toArray(new TermQuery[excludeQueriesBuilder.size()]);
      }
    }

    @Override
    public String toString() {
      return "{elevatedIds=" + Collections2.transform(elevatedIds, BytesRef::utf8ToString) +
          ", excludedIds=" + Collections2.transform(excludedIds, BytesRef::utf8ToString) + "}";
    }
  }

  /** Elevates certain docs to the top. */
  private class ElevationComparatorSource extends FieldComparatorSource {

    private final IntIntHashMap elevatedWithPriority;
    private final boolean useConfiguredElevatedOrder;
    private final int[] sortedElevatedDocIds;

    private ElevationComparatorSource(IntIntHashMap elevatedWithPriority, boolean useConfiguredElevatedOrder) {
      this.elevatedWithPriority = elevatedWithPriority;
      this.useConfiguredElevatedOrder = useConfiguredElevatedOrder;

      // copy elevatedWithPriority keys (doc IDs) into sortedElevatedDocIds, sorted
      sortedElevatedDocIds = new int[elevatedWithPriority.size()];
      final Iterator<IntIntCursor> iterator = elevatedWithPriority.iterator();
      for (int i = 0; i < sortedElevatedDocIds.length; i++) {
        IntIntCursor next = iterator.next();
        sortedElevatedDocIds[i] = next.key;
      }
      assert iterator.hasNext() == false;
      Arrays.sort(sortedElevatedDocIds);
    }

    @Override
    public FieldComparator<Integer> newComparator(String fieldName, final int numHits, int sortPos, boolean reversed) {
      return new SimpleFieldComparator<Integer>() {
        final int[] values = new int[numHits];
        int bottomVal;
        int topVal;

        int docBase;
        boolean hasElevatedDocsThisSegment;

        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
          docBase = context.docBase;
          // ascertain if hasElevatedDocsThisSegment
          final int idx = Arrays.binarySearch(sortedElevatedDocIds, docBase);
          if (idx < 0) {
            //first doc in segment isn't elevated (typical).  Maybe another is?
            int nextIdx = -idx - 1;
            if (nextIdx < sortedElevatedDocIds.length) {
              int nextElevatedDocId = sortedElevatedDocIds[nextIdx];
              if (nextElevatedDocId > docBase + context.reader().maxDoc()) {
                hasElevatedDocsThisSegment = false;
                return;
              }
            }
          }
          hasElevatedDocsThisSegment = true;
        }

        @Override
        public int compare(int slot1, int slot2) {
          return values[slot1] - values[slot2];  // values will be small enough that there is no overflow concern
        }

        @Override
        public void setBottom(int slot) {
          bottomVal = values[slot];
        }

        @Override
        public void setTopValue(Integer value) {
          topVal = value;
        }

        private int docVal(int doc) {
          if (!hasElevatedDocsThisSegment) {
            assert elevatedWithPriority.containsKey(docBase + doc) == false;
            return -1;
          } else if (useConfiguredElevatedOrder) {
            return elevatedWithPriority.getOrDefault(docBase + doc, -1);
          } else {
            return elevatedWithPriority.containsKey(docBase + doc) ? 1 : -1;
          }
        }

        @Override
        public int compareBottom(int doc) {
          return bottomVal - docVal(doc);
        }

        @Override
        public void copy(int slot, int doc) {
          values[slot] = docVal(doc);
        }

        @Override
        public Integer value(int slot) {
          return values[slot];
        }

        @Override
        public int compareTop(int doc) {
          final int docValue = docVal(doc);
          return topVal - docValue;  // values will be small enough that there is no overflow concern
        }
      };
    }
  }
}