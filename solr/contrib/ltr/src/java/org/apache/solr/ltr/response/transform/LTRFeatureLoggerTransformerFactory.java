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
package org.apache.solr.ltr.response.transform;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.ltr.FeatureLogger;
import org.apache.solr.ltr.LTRRescorer;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.LTRThreadModule;
import org.apache.solr.ltr.SolrQueryRequestContextUtils;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.model.LTRScoringModel;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.ltr.search.LTRQParserPlugin;
import org.apache.solr.ltr.store.FeatureStore;
import org.apache.solr.ltr.store.rest.ManagedFeatureStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.SolrPluginUtils;

/**
 * This transformer will take care to generate and append in the response the
 * features declared in the feature store of the current model. The class is
 * useful if you are not interested in the reranking (e.g., bootstrapping a
 * machine learning framework).
 */
public class LTRFeatureLoggerTransformerFactory extends TransformerFactory {

  // used inside fl to specify the output format (csv/json) of the extracted features
  private static final String FV_RESPONSE_WRITER = "fvwt";

  // used inside fl to specify the format (dense|sparse) of the extracted features
  private static final String FV_FORMAT = "format";

  // used inside fl to specify the feature store to use for the feature extraction
  private static final String FV_STORE = "store";

  private static String DEFAULT_LOGGING_MODEL_NAME = "logging-model";

  private String loggingModelName = DEFAULT_LOGGING_MODEL_NAME;
  private String defaultFvStore;
  private String defaultFvwt;
  private String defaultFvFormat;

  private LTRThreadModule threadManager = null;

  public void setLoggingModelName(String loggingModelName) {
    this.loggingModelName = loggingModelName;
  }

  public void setStore(String defaultFvStore) {
    this.defaultFvStore = defaultFvStore;
  }

  public void setFvwt(String defaultFvwt) {
    this.defaultFvwt = defaultFvwt;
  }

  public void setFormat(String defaultFvFormat) {
    this.defaultFvFormat = defaultFvFormat;
  }

  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
    super.init(args);
    threadManager = LTRThreadModule.getInstance(args);
    SolrPluginUtils.invokeSetters(this, args);
  }

  @Override
  public DocTransformer create(String name, SolrParams params,
      SolrQueryRequest req) {

    // Hint to enable feature vector cache since we are requesting features
    SolrQueryRequestContextUtils.setIsExtractingFeatures(req);

    // Communicate which feature store we are requesting features for
    SolrQueryRequestContextUtils.setFvStoreName(req, params.get(FV_STORE, defaultFvStore));

    // Create and supply the feature logger to be used
    SolrQueryRequestContextUtils.setFeatureLogger(req,
        FeatureLogger.createFeatureLogger(
            params.get(FV_RESPONSE_WRITER, defaultFvwt),
            params.get(FV_FORMAT, defaultFvFormat)));

    return new FeatureTransformer(name, params, req);
  }

  class FeatureTransformer extends DocTransformer {

    final private String name;
    final private SolrParams params;
    final private SolrQueryRequest req;

    private List<LeafReaderContext> leafContexts;
    private SolrIndexSearcher searcher;
    private LTRScoringQuery scoringQuery;
    private LTRScoringQuery.ModelWeight modelWeight;
    private FeatureLogger<?> featureLogger;
    private boolean docsWereNotReranked;

    /**
     * @param name
     *          Name of the field to be added in a document representing the
     *          feature vectors
     */
    public FeatureTransformer(String name, SolrParams params,
        SolrQueryRequest req) {
      this.name = name;
      this.params = params;
      this.req = req;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void setContext(ResultContext context) {
      super.setContext(context);
      if (context == null) {
        return;
      }
      if (context.getRequest() == null) {
        return;
      }

      searcher = context.getSearcher();
      if (searcher == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "searcher is null");
      }
      leafContexts = searcher.getTopReaderContext().leaves();

      // Setup LTRScoringQuery
      scoringQuery = SolrQueryRequestContextUtils.getScoringQuery(req);
      docsWereNotReranked = (scoringQuery == null);
      String featureStoreName = SolrQueryRequestContextUtils.getFvStoreName(req);
      if (docsWereNotReranked || (featureStoreName != null && (!featureStoreName.equals(scoringQuery.getScoringModel().getFeatureStoreName())))) {
        // if store is set in the transformer we should overwrite the logger

        final ManagedFeatureStore fr = ManagedFeatureStore.getManagedFeatureStore(req.getCore());

        final FeatureStore store = fr.getFeatureStore(featureStoreName);
        featureStoreName = store.getName(); // if featureStoreName was null before this gets actual name

        try {
          final LoggingModel lm = new LoggingModel(loggingModelName,
              featureStoreName, store.getFeatures());

          scoringQuery = new LTRScoringQuery(lm,
              LTRQParserPlugin.extractEFIParams(params),
              true,
              threadManager); // request feature weights to be created for all features

          // Local transformer efi if provided
          scoringQuery.setOriginalQuery(context.getQuery());

        }catch (final Exception e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "retrieving the feature store "+featureStoreName, e);
        }
      }

      if (scoringQuery.getFeatureLogger() == null){
        scoringQuery.setFeatureLogger( SolrQueryRequestContextUtils.getFeatureLogger(req) );
      }
      scoringQuery.setRequest(req);

      featureLogger = scoringQuery.getFeatureLogger();

      try {
        modelWeight = scoringQuery.createWeight(searcher, true, 1f);
      } catch (final IOException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.getMessage(), e);
      }
      if (modelWeight == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "error logging the features, model weight is null");
      }
    }

    @Override
    public void transform(SolrDocument doc, int docid, float score)
        throws IOException {
      Object fv = featureLogger.getFeatureVector(docid, scoringQuery, searcher);
      if (fv == null) { // FV for this document was not in the cache
        fv = featureLogger.makeFeatureVector(
            LTRRescorer.extractFeaturesInfo(
                modelWeight,
                docid,
                (docsWereNotReranked ? new Float(score) : null),
                leafContexts));
      }

      doc.addField(name, fv);
    }

  }

  private static class LoggingModel extends LTRScoringModel {

    public LoggingModel(String name, String featureStoreName, List<Feature> allFeatures){
      this(name, Collections.emptyList(), Collections.emptyList(),
          featureStoreName, allFeatures, Collections.emptyMap());
    }

    protected LoggingModel(String name, List<Feature> features,
        List<Normalizer> norms, String featureStoreName,
        List<Feature> allFeatures, Map<String,Object> params) {
      super(name, features, norms, featureStoreName, allFeatures, params);
    }

    @Override
    public float score(float[] modelFeatureValuesNormalized) {
      return 0;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc, float finalScore, List<Explanation> featureExplanations) {
      return Explanation.match(finalScore, toString()
          + " logging model, used only for logging the features");
    }

  }

}
