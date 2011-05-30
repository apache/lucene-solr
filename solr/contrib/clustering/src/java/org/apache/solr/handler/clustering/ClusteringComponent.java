package org.apache.solr.handler.clustering;
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

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
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
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Provide a plugin for clustering results.  Can either be for search results (i.e. via Carrot2) or for
 * clustering documents (i.e. via Mahout)
 * <p/>
 * This engine is experimental.  Output from this engine is subject to change in future releases.
 *
 * <pre class="prettyprint" >
 * &lt;searchComponent class="org.apache.solr.handler.clustering.ClusteringComponent" name="clustering"&gt;
 *   &lt;lst name="engine"&gt;
 *     &lt;str name="name"&gt;default&lt;/str&gt;
 *     &lt;str name="carrot.algorithm"&gt;org.carrot2.clustering.lingo.LingoClusteringAlgorithm&lt;/str&gt;
 *   &lt;/lst&gt;
 * &lt;/searchComponent&gt;</pre>
 */
public class ClusteringComponent extends SearchComponent implements SolrCoreAware {
  private transient static Logger log = LoggerFactory.getLogger(ClusteringComponent.class);

  private Map<String, SearchClusteringEngine> searchClusteringEngines = new HashMap<String, SearchClusteringEngine>();
  private Map<String, DocumentClusteringEngine> documentClusteringEngines = new HashMap<String, DocumentClusteringEngine>();
  /**
   * Base name for all spell checker query parameters. This name is also used to
   * register this component with SearchHandler.
   */
  public static final String COMPONENT_NAME = "clustering";
  private NamedList initParams;

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
    String name = getClusteringEngineName(rb);
    boolean useResults = params.getBool(ClusteringParams.USE_SEARCH_RESULTS, false);
    if (useResults == true) {
      SearchClusteringEngine engine = getSearchClusteringEngine(rb);
      if (engine != null) {
        DocListAndSet results = rb.getResults();
        Map<SolrDocument,Integer> docIds = new HashMap<SolrDocument, Integer>(results.docList.size());
        SolrDocumentList solrDocList = engine.getSolrDocumentList(results.docList, rb.req, docIds);
        Object clusters = engine.cluster(rb.getQuery(), solrDocList, docIds, rb.req);
        rb.rsp.add("clusters", clusters);
      } else {
        log.warn("No engine for: " + name);
      }
    }
    boolean useCollection = params.getBool(ClusteringParams.USE_COLLECTION, false);
    if (useCollection == true) {
      DocumentClusteringEngine engine = documentClusteringEngines.get(name);
      if (engine != null) {
        boolean useDocSet = params.getBool(ClusteringParams.USE_DOC_SET, false);
        NamedList nl = null;

        //TODO: This likely needs to be made into a background task that runs in an executor
        if (useDocSet == true) {
          nl = engine.cluster(rb.getResults().docSet, params);
        } else {
          nl = engine.cluster(params);
        }
        rb.rsp.add("clusters", nl);
      } else {
        log.warn("No engine for " + name);
      }
    }
  }
  
  private SearchClusteringEngine getSearchClusteringEngine(ResponseBuilder rb){
    return searchClusteringEngines.get(getClusteringEngineName(rb));
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
      // if fl=* then we don't need check
      if( fl.indexOf( '*' ) >= 0 ) return;
      Set<String> fields = getSearchClusteringEngine(rb).getFieldsToLoad(rb.req);
      if( fields == null || fields.size() == 0 ) return;
      StringBuilder sb = new StringBuilder();
      String[] flparams = fl.split( "[,\\s]+" );
      Set<String> flParamSet = new HashSet<String>(flparams.length);
      for( String flparam : flparams ){
        // no need trim() because of split() by \s+
        flParamSet.add(flparam);
      }
      for( String aFieldToLoad : fields ){
        if( !flParamSet.contains( aFieldToLoad ) ){
          sb.append( ',' ).append( aFieldToLoad );
        }
      }
      if( sb.length() > 0 ){
        sreq.params.set( CommonParams.FL, fl + sb.toString() );
      }
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false) || !params.getBool(ClusteringParams.USE_SEARCH_RESULTS, false)) {
      return;
    }
    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      SearchClusteringEngine engine = getSearchClusteringEngine(rb);
      if (engine != null) {
        SolrDocumentList solrDocList = (SolrDocumentList)rb.rsp.getValues().get("response");
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
        String name = getClusteringEngineName(rb);
        log.warn("No engine for: " + name);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(NamedList args) {
    super.init(args);
    this.initParams = args;
  }

  public void inform(SolrCore core) {
    if (initParams != null) {
      log.info("Initializing Clustering Engines");
      boolean searchHasDefault = false;
      boolean documentHasDefault = false;
      for (int i = 0; i < initParams.size(); i++) {
        if (initParams.getName(i).equals("engine")) {
          NamedList engineNL = (NamedList) initParams.getVal(i);
          String className = (String) engineNL.get("classname");
          if (className == null) {
            className = CarrotClusteringEngine.class.getName();
          }
          SolrResourceLoader loader = core.getResourceLoader();
          ClusteringEngine clusterer = (ClusteringEngine) loader.newInstance(className);
          if (clusterer != null) {
            String name = clusterer.init(engineNL, core);
            if (name != null) {
              boolean isDefault = name.equals(ClusteringEngine.DEFAULT_ENGINE_NAME);
              if (clusterer instanceof SearchClusteringEngine) {
                if (isDefault == true && searchHasDefault == false) {
                  searchHasDefault = true;
                } else if (isDefault == true && searchHasDefault == true) {
                  throw new RuntimeException("More than one engine is missing name: " + engineNL);
                }
                searchClusteringEngines.put(name, (SearchClusteringEngine) clusterer);
              } else if (clusterer instanceof DocumentClusteringEngine) {
                if (isDefault == true && documentHasDefault == false) {
                  searchHasDefault = true;
                } else if (isDefault == true && documentHasDefault == true) {
                  throw new RuntimeException("More than one engine is missing name: " + engineNL);
                }
                documentClusteringEngines.put(name, (DocumentClusteringEngine) clusterer);
              }
            } else {
              if (clusterer instanceof SearchClusteringEngine && searchHasDefault == false) {
                searchClusteringEngines.put(ClusteringEngine.DEFAULT_ENGINE_NAME, (SearchClusteringEngine) clusterer);
                searchHasDefault = true;
              } else if (clusterer instanceof DocumentClusteringEngine && documentHasDefault == false) {
                documentClusteringEngines.put(ClusteringEngine.DEFAULT_ENGINE_NAME, (DocumentClusteringEngine) clusterer);
                documentHasDefault = true;
              } else {
                throw new RuntimeException("More than one engine is missing name: " + engineNL);
              }
            }
          }
        }
      }
      log.info("Finished Initializing Clustering Engines");
    }
  }

  /*
  * @return Unmodifiable Map of the engines, key is the name from the config, value is the engine
  * */
  public Map<String, SearchClusteringEngine> getSearchClusteringEngines() {
    return Collections.unmodifiableMap(searchClusteringEngines);
  }

  // ///////////////////////////////////////////
  // / SolrInfoMBean
  // //////////////////////////////////////////

  @Override
  public String getDescription() {
    return "A Clustering component";
  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

}
