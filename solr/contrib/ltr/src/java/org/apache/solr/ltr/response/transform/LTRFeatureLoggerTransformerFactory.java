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
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.ltr.CSVFeatureLogger;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This transformer will take care to generate and append in the response the
 * features declared in the feature store of the current reranking model,
 * or a specified feature store.  Ex. <code>fl=id,[features store=myStore efi.user_text="ibm"]</code>
 * 
 * <h3>Parameters</h3>
 * <code>store</code> - The feature store to extract features from. If not provided it
 * will default to the features used by your reranking model.<br>
 * <code>efi.*</code> - External feature information variables required by the features
 * you are extracting.<br>
 * <code>format</code> - The format you want the features to be returned in.  Supports (dense|sparse). Defaults to dense.<br>
*/

public class LTRFeatureLoggerTransformerFactory extends TransformerFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // used inside fl to specify the format (dense|sparse) of the extracted features
  private static final String FV_FORMAT = "format";

  // used inside fl to specify the feature store to use for the feature extraction
  private static final String FV_STORE = "store";

  private static String DEFAULT_LOGGING_MODEL_NAME = "logging-model";

  private String fvCacheName;
  private String loggingModelName = DEFAULT_LOGGING_MODEL_NAME;
  private String defaultStore;
  private FeatureLogger.FeatureFormat defaultFormat = FeatureLogger.FeatureFormat.DENSE;
  private char csvKeyValueDelimiter = CSVFeatureLogger.DEFAULT_KEY_VALUE_SEPARATOR;
  private char csvFeatureSeparator = CSVFeatureLogger.DEFAULT_FEATURE_SEPARATOR;

  private LTRThreadModule threadManager = null;

  public void setFvCacheName(String fvCacheName) {
    this.fvCacheName = fvCacheName;
  }

  public void setLoggingModelName(String loggingModelName) {
    this.loggingModelName = loggingModelName;
  }

  public void setDefaultStore(String defaultStore) {
    this.defaultStore = defaultStore;
  }

  public void setDefaultFormat(String defaultFormat) {
    this.defaultFormat = FeatureLogger.FeatureFormat.valueOf(defaultFormat.toUpperCase(Locale.ROOT));
  }

  public void setCsvKeyValueDelimiter(String csvKeyValueDelimiter) {
    if (csvKeyValueDelimiter.length() != 1) {
      throw new IllegalArgumentException("csvKeyValueDelimiter must be exactly 1 character");
    }
    this.csvKeyValueDelimiter = csvKeyValueDelimiter.charAt(0);
  }

  public void setCsvFeatureSeparator(String csvFeatureSeparator) {
    if (csvFeatureSeparator.length() != 1) {
      throw new IllegalArgumentException("csvFeatureSeparator must be exactly 1 character");
    }
    this.csvFeatureSeparator = csvFeatureSeparator.charAt(0);
  }

  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
    super.init(args);
    threadManager = LTRThreadModule.getInstance(args);
    SolrPluginUtils.invokeSetters(this, args);
  }

  @Override
  public DocTransformer create(String name, SolrParams localparams,
      SolrQueryRequest req) {

    // Hint to enable feature vector cache since we are requesting features
    SolrQueryRequestContextUtils.setIsExtractingFeatures(req);

    // Communicate which feature store we are requesting features for
    SolrQueryRequestContextUtils.setFvStoreName(req, localparams.get(FV_STORE, defaultStore));

    // Create and supply the feature logger to be used
    SolrQueryRequestContextUtils.setFeatureLogger(req,
        createFeatureLogger(
            localparams.get(FV_FORMAT)));

    return new FeatureTransformer(name, localparams, req);
  }

  /**
   * returns a FeatureLogger that logs the features
   * 'featureFormat' param: 'dense' will write features in dense format,
   * 'sparse' will write the features in sparse format, null or empty will
   * default to 'sparse'
   *
   *
   * @return a feature logger for the format specified.
   */
  private FeatureLogger createFeatureLogger(String formatStr) {
    final FeatureLogger.FeatureFormat format;
    if (formatStr != null) {
      format = FeatureLogger.FeatureFormat.valueOf(formatStr.toUpperCase(Locale.ROOT));
    } else {
      format = this.defaultFormat;
    }
    if (fvCacheName == null) {
      throw new IllegalArgumentException("a fvCacheName must be configured");
    }
    return new CSVFeatureLogger(fvCacheName, format, csvKeyValueDelimiter, csvFeatureSeparator);
  }

  class FeatureTransformer extends DocTransformer {

    final private String name;
    final private SolrParams localparams;
    final private SolrQueryRequest req;

    private List<LeafReaderContext> leafContexts;
    private SolrIndexSearcher searcher;
    private LTRScoringQuery scoringQuery;
    private LTRScoringQuery.ModelWeight modelWeight;
    private FeatureLogger featureLogger;
    private boolean docsWereNotReranked;

    /**
     * @param name
     *          Name of the field to be added in a document representing the
     *          feature vectors
     */
    public FeatureTransformer(String name, SolrParams localparams,
        SolrQueryRequest req) {
      this.name = name;
      this.localparams = localparams;
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
              LTRQParserPlugin.extractEFIParams(localparams),
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
