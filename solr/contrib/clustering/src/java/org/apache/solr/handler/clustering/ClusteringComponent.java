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
package org.apache.solr.handler.clustering;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.clustering.carrot2.CarrotClusteringEngine;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a plugin for performing cluster analysis. This can either be applied to 
 * search results (e.g., via <a href="http://project.carrot2.org">Carrot<sup>2</sup></a>) or for
 * clustering documents (e.g., via <a href="http://mahout.apache.org/">Mahout</a>).
 * <p>
 * See Solr example for configuration examples.</p>
 * 
 * @lucene.experimental
 */
public class ClusteringComponent extends SearchComponent implements SolrCoreAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Base name for all component parameters. This name is also used to
   * register this component with SearchHandler.
   */
  public static final String COMPONENT_NAME = "clustering";

  /**
   * Declaration-order list of search clustering engines.
   */
  private final LinkedHashMap<String, SearchClusteringEngine> searchClusteringEngines = new LinkedHashMap<>();

  /**
   * Declaration order list of document clustering engines.
   */
  private final LinkedHashMap<String, DocumentClusteringEngine> documentClusteringEngines = new LinkedHashMap<>();

  /**
   * An unmodifiable view of {@link #searchClusteringEngines}.
   */
  private final Map<String, SearchClusteringEngine> searchClusteringEnginesView = Collections.unmodifiableMap(searchClusteringEngines);

  /**
   * Initialization parameters temporarily saved here, the component
   * is initialized in {@link #inform(SolrCore)} because we need to know
   * the core's {@link SolrResourceLoader}.
   * 
   * @see #init(NamedList)
   */
  private NamedList<Object> initParams;

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void init(NamedList args) {
    this.initParams = args;
    super.init(args);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void inform(SolrCore core) {
    if (initParams != null) {
      log.info("Initializing Clustering Engines");

      // Our target list of engines, split into search-results and document clustering.
      SolrResourceLoader loader = core.getResourceLoader();
  
      for (Map.Entry<String,Object> entry : initParams) {
        if ("engine".equals(entry.getKey())) {
          NamedList<Object> engineInitParams = (NamedList<Object>) entry.getValue();
          Boolean optional = engineInitParams.getBooleanArg("optional");
          optional = (optional == null ? Boolean.FALSE : optional);

          String engineClassName = StringUtils.defaultIfBlank( 
              (String) engineInitParams.get("classname"),
              CarrotClusteringEngine.class.getName()); 
  
          // Instantiate the clustering engine and split to appropriate map. 
          final ClusteringEngine engine = loader.newInstance(engineClassName, ClusteringEngine.class);
          final String name = StringUtils.defaultIfBlank(engine.init(engineInitParams, core), "");

          if (!engine.isAvailable()) {
            if (optional) {
              log.info("Optional clustering engine not available: " + name);
            } else {
              throw new SolrException(ErrorCode.SERVER_ERROR, 
                  "A required clustering engine failed to initialize, check the logs: " + name);
            }
          }
          
          final ClusteringEngine previousEntry;
          if (engine instanceof SearchClusteringEngine) {
            previousEntry = searchClusteringEngines.put(name, (SearchClusteringEngine) engine);
          } else if (engine instanceof DocumentClusteringEngine) {
            previousEntry = documentClusteringEngines.put(name, (DocumentClusteringEngine) engine);
          } else {
            log.warn("Unknown type of a clustering engine for class: " + engineClassName);
            continue;
          }
          if (previousEntry != null) {
            log.warn("Duplicate clustering engine component named '" + name + "'.");
          }
        }
      }

      // Set up the default engine key for both types of engines.
      setupDefaultEngine("search results clustering", searchClusteringEngines);
      setupDefaultEngine("document clustering", documentClusteringEngines);

      log.info("Finished Initializing Clustering Engines");
    }
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) {
      return;
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) {
      return;
    }

    final String name = getClusteringEngineName(rb);
    boolean useResults = params.getBool(ClusteringParams.USE_SEARCH_RESULTS, false);
    if (useResults == true) {
      SearchClusteringEngine engine = searchClusteringEngines.get(name);
      if (engine != null) {
        checkAvailable(name, engine);
        DocListAndSet results = rb.getResults();
        Map<SolrDocument,Integer> docIds = new HashMap<>(results.docList.size());
        SolrDocumentList solrDocList = SolrPluginUtils.docListToSolrDocumentList(
            results.docList, rb.req.getSearcher(), engine.getFieldsToLoad(rb.req), docIds);
        Object clusters = engine.cluster(rb.getQuery(), solrDocList, docIds, rb.req);
        rb.rsp.add("clusters", clusters);
      } else {
        log.warn("No engine named: " + name);
      }
    }

    boolean useCollection = params.getBool(ClusteringParams.USE_COLLECTION, false);
    if (useCollection == true) {
      DocumentClusteringEngine engine = documentClusteringEngines.get(name);
      if (engine != null) {
        checkAvailable(name, engine);
        boolean useDocSet = params.getBool(ClusteringParams.USE_DOC_SET, false);
        NamedList<?> nl = null;

        // TODO: This likely needs to be made into a background task that runs in an executor
        if (useDocSet == true) {
          nl = engine.cluster(rb.getResults().docSet, params);
        } else {
          nl = engine.cluster(params);
        }
        rb.rsp.add("clusters", nl);
      } else {
        log.warn("No engine named: " + name);
      }
    }
  }

  private void checkAvailable(String name, ClusteringEngine engine) {
    if (!engine.isAvailable()) {
      throw new SolrException(ErrorCode.SERVER_ERROR, 
          "Clustering engine declared, but not available, check the logs: " + name);
    }
  }

  private String getClusteringEngineName(ResponseBuilder rb){
    return rb.req.getParams().get(ClusteringParams.ENGINE_NAME, ClusteringEngine.DEFAULT_ENGINE_NAME);
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false) || !params.getBool(ClusteringParams.USE_SEARCH_RESULTS, false)) {
      return;
    }
    sreq.params.remove(COMPONENT_NAME);
    if( ( sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS ) != 0 ){
      String fl = sreq.params.get(CommonParams.FL,"*");
      // if fl=* then we don't need to check.
      if (fl.indexOf('*') >= 0) { 
        return;
      }

      String name = getClusteringEngineName(rb);
      SearchClusteringEngine engine = searchClusteringEngines.get(name);
      if (engine != null) {
        checkAvailable(name, engine);
        Set<String> fields = engine.getFieldsToLoad(rb.req);
        if (fields == null || fields.size() == 0) { 
          return;
        }
  
        StringBuilder sb = new StringBuilder();
        String[] flparams = fl.split( "[,\\s]+" );
        Set<String> flParamSet = new HashSet<>(flparams.length);
        for (String flparam : flparams) {
          // no need trim() because of split() by \s+
          flParamSet.add(flparam);
        }
        for (String aFieldToLoad : fields) {
          if (!flParamSet.contains(aFieldToLoad )) {
            sb.append(',').append(aFieldToLoad);
          }
        }
        if (sb.length() > 0) {
          sreq.params.set(CommonParams.FL, fl + sb.toString());
        }
      } else {
        log.warn("No engine named: " + name);
      }
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false) || 
        !params.getBool(ClusteringParams.USE_SEARCH_RESULTS, false)) {
      return;
    }

    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      String name = getClusteringEngineName(rb);
      SearchClusteringEngine engine = searchClusteringEngines.get(name);
      if (engine != null) {
        checkAvailable(name, engine);
        SolrDocumentList solrDocList = (SolrDocumentList) rb.rsp.getResponse();
        // TODO: Currently, docIds is set to null in distributed environment.
        // This causes CarrotParams.PRODUCE_SUMMARY doesn't work.
        // To work CarrotParams.PRODUCE_SUMMARY under distributed mode, we can choose either one of:
        // (a) In each shard, ClusteringComponent produces summary and finishStage()
        //     merges these summaries.
        // (b) Adding doHighlighting(SolrDocumentList, ...) method to SolrHighlighter and
        //     making SolrHighlighter uses "external text" rather than stored values to produce snippets.
        Map<SolrDocument,Integer> docIds = null;
        Object clusters = engine.cluster(rb.getQuery(), solrDocList, docIds, rb.req);
        rb.rsp.add("clusters", clusters);
      } else {
        log.warn("No engine named: " + name);
      }
    }
  }

  /**
   * @return Expose for tests.
   */
  Map<String, SearchClusteringEngine> getSearchClusteringEngines() {
    return searchClusteringEnginesView;
  }

  @Override
  public String getDescription() {
    return "A Clustering component";
  }

  /**
   * Setup the default clustering engine.
   * @see "https://issues.apache.org/jira/browse/SOLR-5219"
   */
  private static <T extends ClusteringEngine> void setupDefaultEngine(String type, LinkedHashMap<String,T> map) {
    // If there's already a default algorithm, leave it as is.
    String engineName = ClusteringEngine.DEFAULT_ENGINE_NAME;
    T defaultEngine = map.get(engineName);

    if (defaultEngine == null ||
        !defaultEngine.isAvailable()) {
      // If there's no default algorithm, and there are any algorithms available, 
      // the first definition becomes the default algorithm.
      for (Map.Entry<String, T> e : map.entrySet()) {
        if (e.getValue().isAvailable()) {
          engineName = e.getKey();
          defaultEngine = e.getValue();
          map.put(ClusteringEngine.DEFAULT_ENGINE_NAME, defaultEngine);
          break;
        }
      }
    }

    if (defaultEngine != null) {
      log.info("Default engine for " + type + ": " + engineName + " [" + defaultEngine.getClass().getSimpleName() + "]");
    } else {
      log.warn("No default engine for " + type + ".");
    }
  }
}
