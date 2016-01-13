package org.apache.solr.ltr.ranking;

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

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.ltr.feature.ModelMetadata;
import org.apache.solr.ltr.log.FeatureLogger;
import org.apache.solr.ltr.ranking.LTRComponent.LTRParams;
import org.apache.solr.ltr.rest.ManagedModelStore;
import org.apache.solr.ltr.util.ModelException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SyntaxError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plug into solr a rerank model.
 *
 * Learning to Rank Query Parser Syntax: rq={!ltr model=6029760550880411648
 * reRankDocs=300 efi.myCompanyQueryIntent=0.98}
 *
 */
public class LTRQParserPlugin extends QParserPlugin {
  public static final String NAME = "ltr";

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {}

  @Override
  public QParser createParser(String qstr, SolrParams localParams,
      SolrParams params, SolrQueryRequest req) {
    return new LTRQParser(qstr, localParams, params, req);
  }

  public class LTRQParser extends QParser {
    // param for setting the model
    public static final String MODEL = "model";

    // param for setting how many documents the should be reranked
    public static final String RERANK_DOCS = "reRankDocs";

    // params for setting custom external info that features can use, like query
    // intent
    // TODO: Can we just pass the entire request all the way down to all
    // models/features?
    public static final String EXTERNAL_FEATURE_INFO = "efi.";

    ManagedModelStore mr = null;

    public LTRQParser(String qstr, SolrParams localParams, SolrParams params,
        SolrQueryRequest req) {
      super(qstr, localParams, params, req);

      mr = (ManagedModelStore) req.getCore().getRestManager()
          .getManagedResource(LTRParams.MSTORE_END_POINT);
    }

    @Override
    public Query parse() throws SyntaxError {
      // ReRanking Model
      String modelName = localParams.get(MODEL);
      if (modelName == null || modelName.isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Must provide model in the request");
      }

      ModelQuery reRankModel = null;
      try {
        ModelMetadata meta = mr.getModel(modelName);
        reRankModel = new ModelQuery(meta);
      } catch (ModelException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }

      // String[] fl = req.getParams().getParams(CommonParams.FL); Contains the
      // [transformer]
      // ReRank doc count
      // Allow reranking more docs than shown, since the nth doc might be the
      // best one after reranking,
      // but not showing more than is reranked.
      int reRankDocs = localParams.getInt(RERANK_DOCS, 200);
      int start = params.getInt(CommonParams.START, 0);
      int rows = params.getInt(CommonParams.ROWS, 10);
      // Feature Vectors
      // FIXME: Exception if feature vectors requested without specifying what
      // features to return??
      // For training a new model offline you need feature vectors, but dont yet
      // have a model. Should provide the FeatureStore name as an arg to the
      // feature vector
      // transformer and remove the duplicate fv=true arg
      boolean returnFeatureVectors = params.getBool(LTRParams.FV, false);

      if (returnFeatureVectors) {

        FeatureLogger<?> solrLogger = FeatureLogger.getFeatureLogger(params
            .get(LTRParams.FV_RESPONSE_WRITER));
        reRankModel.setFeatureLogger(solrLogger);
        req.getContext().put(LTRComponent.LOGGER_NAME, solrLogger);
        req.getContext().put(MODEL, reRankModel);
      }

      if (start + rows > reRankDocs) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Requesting more documents than being reranked.");
      }
      reRankDocs = Math.max(start + rows, reRankDocs);

      // External features
      Map<String,String> externalFeatureInfo = new HashMap<>();
      for (Iterator<String> it = localParams.getParameterNamesIterator(); it
          .hasNext();) {
        final String name = it.next();
        if (name.startsWith(EXTERNAL_FEATURE_INFO)) {
          externalFeatureInfo.put(
              name.substring(EXTERNAL_FEATURE_INFO.length()),
              localParams.get(name));
        }
      }
      reRankModel.setExternalFeatureInfo(externalFeatureInfo);

      logger.info("Reranking {} docs using model {}", reRankDocs, reRankModel
          .getMetadata().getName());
      reRankModel.setRequest(req);

      return new LTRQuery(reRankModel, reRankDocs);
    }
  }
}
