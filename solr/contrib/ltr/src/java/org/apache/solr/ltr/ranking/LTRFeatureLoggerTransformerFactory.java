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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.ltr.log.FeatureLogger;
import org.apache.solr.ltr.ranking.LTRQParserPlugin.LTRQParser;
import org.apache.solr.ltr.ranking.ModelQuery.ModelWeight;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * This transformer will take care to generate and append in the response the
 * features declared in the feature store of the current model. The class is
 * useful if you are not interested in the reranking (e.g., bootstrapping a
 * machine learning framework).
 */
public class LTRFeatureLoggerTransformerFactory extends TransformerFactory {

  SolrQueryRequest req;

  @Override
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
    super.init(args);
  }

  @Override
  public DocTransformer create(String name, SolrParams params,
      SolrQueryRequest req) {
    this.req = req;
    return new FeatureTransformer(name);
  }

  class FeatureTransformer extends DocTransformer {

    String name;
    List<LeafReaderContext> leafContexts;
    SolrIndexSearcher searcher;
    ModelQuery reRankModel;
    ModelWeight modelWeight;
    FeatureLogger<?> featurelLogger;

    /**
     * @param name
     *          Name of the field to be added in a document representing the
     *          feature vectors
     */
    public FeatureTransformer(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void setContext(ResultContext context) {
      super.setContext(context);
      if (context == null) return;
      if (context.getRequest() == null) return;
      reRankModel = (ModelQuery) req.getContext()
          .get(LTRQParser.MODEL);
      if (reRankModel == null) throw new SolrException(
          org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST,
          "model is null");
      reRankModel.setRequest(context.getRequest());
      featurelLogger = reRankModel.getFeatureLogger();
      searcher = context.getSearcher();
      if (searcher == null) throw new SolrException(
          org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST,
          "searcher is null");
      leafContexts = searcher.getTopReaderContext().leaves();
      Weight w;
      try {
        w = reRankModel.createWeight(searcher, true);
      } catch (IOException e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e.getMessage(), e);
      }
      if (w == null || !(w instanceof ModelWeight)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "error logging the features, model weight is null");
      }
      modelWeight = (ModelWeight) w;

    }

    @Override
    public void transform(SolrDocument doc, int docid, float score)
        throws IOException {
      Object fv = featurelLogger.getFeatureVector(docid, reRankModel, searcher);
      if (fv == null) { // FV for this document was not in the cache
        int n = ReaderUtil.subIndex(docid, leafContexts);
        final LeafReaderContext atomicContext = leafContexts.get(n);
        int deBasedDoc = docid - atomicContext.docBase;
        Scorer r = modelWeight.scorer(atomicContext);
        if ((r == null || r.iterator().advance(deBasedDoc) != docid)
            && fv == null) {
          doc.addField(name, featurelLogger.makeFeatureVector(new String[0],
              new float[0], new boolean[0]));
        } else {
          float finalScore = r.score();
          String[] names = modelWeight.allFeatureNames;
          float[] values = modelWeight.allFeatureValues;
          boolean[] valuesUsed = modelWeight.allFeaturesUsed;
          doc.addField(name,
              featurelLogger.makeFeatureVector(names, values, valuesUsed));
        }
      } else {
        doc.addField(name, fv);
      }

    }

  }

}