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
package org.apache.solr.ltr.search;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.LTRRescorer;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.LTRThreadModule;
import org.apache.solr.ltr.SolrQueryRequestContextUtils;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.store.rest.ManagedFeatureStore;
import org.apache.solr.ltr.store.rest.ManagedModelStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.search.AbstractReRankQuery;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.RankQuery;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrPluginUtils;

/**
 * Plug into solr a rerank model.
 *
 * Learning to Rank Query Parser Syntax: rq={!ltr model=6029760550880411648 reRankDocs=300
 * efi.myCompanyQueryIntent=0.98}
 *
 */
public class LTRQParserPlugin extends QParserPlugin implements ResourceLoaderAware, ManagedResourceObserver {
  public static final String NAME = "ltr";
  private static Query defaultQuery = new MatchAllDocsQuery();

  // params for setting custom external info that features can use, like query
  // intent
  static final String EXTERNAL_FEATURE_INFO = "efi.";

  private ManagedFeatureStore fr = null;
  private ManagedModelStore mr = null;

  private LTRThreadModule threadManager = null;

  /** query parser plugin: the name of the attribute for setting the model **/
  public static final String MODEL = "model";

  /** query parser plugin: default number of documents to rerank **/
  public static final int DEFAULT_RERANK_DOCS = 200;

  /**
   * query parser plugin:the param that will select how the number of document
   * to rerank
   **/
  public static final String RERANK_DOCS = "reRankDocs";

  @SuppressWarnings("unchecked")
  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
    super.init(args);
    threadManager = LTRThreadModule.getInstance(args);
    SolrPluginUtils.invokeSetters(this, args);
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams,
      SolrParams params, SolrQueryRequest req) {
    return new LTRQParser(qstr, localParams, params, req);
  }

  /**
   * Given a set of local SolrParams, extract all of the efi.key=value params into a map
   * @param localParams Local request parameters that might conatin efi params
   * @return Map of efi params, where the key is the name of the efi param, and the
   *  value is the value of the efi param
   */
  public static Map<String,String[]> extractEFIParams(SolrParams localParams) {
    final Map<String,String[]> externalFeatureInfo = new HashMap<>();
    for (final Iterator<String> it = localParams.getParameterNamesIterator(); it
        .hasNext();) {
      final String name = it.next();
      if (name.startsWith(EXTERNAL_FEATURE_INFO)) {
        externalFeatureInfo.put(
            name.substring(EXTERNAL_FEATURE_INFO.length()),
            new String[] {localParams.get(name)});
      }
    }
    return externalFeatureInfo;
  }


  @Override
  public void inform(ResourceLoader loader) throws IOException {
    final SolrResourceLoader solrResourceLoader = (SolrResourceLoader) loader;
    ManagedFeatureStore.registerManagedFeatureStore(solrResourceLoader, this);
    ManagedModelStore.registerManagedModelStore(solrResourceLoader, this);
  }

  @Override
  public void onManagedResourceInitialized(NamedList<?> args, ManagedResource res) throws SolrException {
    if (res instanceof ManagedFeatureStore) {
      fr = (ManagedFeatureStore)res;
    }
    if (res instanceof ManagedModelStore){
      mr = (ManagedModelStore)res;
    }
    if (mr != null && fr != null){
      mr.setManagedFeatureStore(fr);
      // now we can safely load the models
      mr.loadStoredModels();

    }
  }

  public class LTRQParser extends QParser {

    public LTRQParser(String qstr, SolrParams localParams, SolrParams params,
        SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }

    @Override
    public Query parse() throws SyntaxError {
      // ReRanking Model
      final String modelName = localParams.get(LTRQParserPlugin.MODEL);
      if ((modelName == null) || modelName.isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Must provide model in the request");
      }

      final LTRScoringModel ltrScoringModel = mr.getModel(modelName);
      if (ltrScoringModel == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "cannot find " + LTRQParserPlugin.MODEL + " " + modelName);
      }

      final String modelFeatureStoreName = ltrScoringModel.getFeatureStoreName();
      final boolean extractFeatures = SolrQueryRequestContextUtils.isExtractingFeatures(req);
      final String fvStoreName = SolrQueryRequestContextUtils.getFvStoreName(req);
      // Check if features are requested and if the model feature store and feature-transform feature store are the same
      final boolean featuresRequestedFromSameStore = (modelFeatureStoreName.equals(fvStoreName) || fvStoreName == null) ? extractFeatures:false;
      if (threadManager != null) {
        threadManager.setExecutor(req.getCore().getCoreContainer().getUpdateShardHandler().getUpdateExecutor());
      }
      final LTRScoringQuery scoringQuery = new LTRScoringQuery(ltrScoringModel,
          extractEFIParams(localParams),
          featuresRequestedFromSameStore, threadManager);

      // Enable the feature vector caching if we are extracting features, and the features
      // we requested are the same ones we are reranking with
      if (featuresRequestedFromSameStore) {
        scoringQuery.setFeatureLogger( SolrQueryRequestContextUtils.getFeatureLogger(req) );
      }
      SolrQueryRequestContextUtils.setScoringQuery(req, scoringQuery);

      int reRankDocs = localParams.getInt(RERANK_DOCS, DEFAULT_RERANK_DOCS);
      if (reRankDocs <= 0) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Must rerank at least 1 document");
      }

      // External features
      scoringQuery.setRequest(req);

      return new LTRQuery(scoringQuery, reRankDocs);
    }
  }

  /**
   * A learning to rank Query, will incapsulate a learning to rank model, and delegate to it the rescoring
   * of the documents.
   **/
  public class LTRQuery extends AbstractReRankQuery {
    private final LTRScoringQuery scoringQuery;

    public LTRQuery(LTRScoringQuery scoringQuery, int reRankDocs) {
      super(defaultQuery, reRankDocs, new LTRRescorer(scoringQuery));
      this.scoringQuery = scoringQuery;
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + (mainQuery.hashCode() + scoringQuery.hashCode() + reRankDocs);
    }

    @Override
    public boolean equals(Object o) {
      return sameClassAs(o) &&  equalsTo(getClass().cast(o));
    }

    private boolean equalsTo(LTRQuery other) {
      return (mainQuery.equals(other.mainQuery)
          && scoringQuery.equals(other.scoringQuery) && (reRankDocs == other.reRankDocs));
    }

    @Override
    public RankQuery wrap(Query _mainQuery) {
      super.wrap(_mainQuery);
      scoringQuery.setOriginalQuery(_mainQuery);
      return this;
    }

    @Override
    public String toString(String field) {
      return "{!ltr mainQuery='" + mainQuery.toString() + "' scoringQuery='"
          + scoringQuery.toString() + "' reRankDocs=" + reRankDocs + "}";
    }

    @Override
    protected Query rewrite(Query rewrittenMainQuery) throws IOException {
      return new LTRQuery(scoringQuery, reRankDocs).wrap(rewrittenMainQuery);
    }
  }

}
