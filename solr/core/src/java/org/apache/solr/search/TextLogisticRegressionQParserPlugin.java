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

package org.apache.solr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.solr.client.solrj.io.ClassificationEvaluation;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;

/**
 *   Returns an AnalyticsQuery implementation that performs
 *   one Gradient Descent iteration of a result set to train a
 *   logistic regression model
 *
 *   The TextLogitStream provides the parallel iterative framework for this class.
 **/

public class TextLogisticRegressionQParserPlugin extends QParserPlugin {
  public static final String NAME = "tlogit";

  @Override
  public void init(NamedList args) {
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new TextLogisticRegressionQParser(qstr, localParams, params, req);
  }

  private static class TextLogisticRegressionQParser extends QParser{

    TextLogisticRegressionQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }

    public Query parse() {

      String fs = params.get("feature");
      String[] terms = params.get("terms").split(",");
      String ws = params.get("weights");
      String dfsStr = params.get("idfs");
      int iteration = params.getInt("iteration", 0);
      String outcome = params.get("outcome");
      int positiveLabel = params.getInt("positiveLabel", 1);
      double threshold = params.getDouble("threshold", 0.5);
      double alpha = params.getDouble("alpha", 0.01);

      double[] idfs = new double[terms.length];
      String[] idfsArr = dfsStr.split(",");
      for (int i = 0; i < idfsArr.length; i++) {
        idfs[i] = Double.parseDouble(idfsArr[i]);
      }

      double[] weights = new double[terms.length+1];

      if(ws != null) {
        String[] wa = ws.split(",");
        for (int i = 0; i < wa.length; i++) {
          weights[i] = Double.parseDouble(wa[i]);
        }
      } else {
        for(int i=0; i<weights.length; i++) {
          weights[i]= 1.0d;
        }
      }

      TrainingParams input = new TrainingParams(fs, terms, idfs, outcome, weights, iteration, alpha, positiveLabel, threshold);

      return new TextLogisticRegressionQuery(input);
    }
  }

  private static class TextLogisticRegressionQuery extends AnalyticsQuery {
    private TrainingParams trainingParams;

    public TextLogisticRegressionQuery(TrainingParams trainingParams) {
      this.trainingParams = trainingParams;
    }

    public DelegatingCollector getAnalyticsCollector(ResponseBuilder rbsp, IndexSearcher indexSearcher) {
      return new TextLogisticRegressionCollector(rbsp, indexSearcher, trainingParams);
    }
  }

  private static class TextLogisticRegressionCollector extends DelegatingCollector {
    private TrainingParams trainingParams;
    private LeafReader leafReader;

    private double[] workingDeltas;
    private ClassificationEvaluation classificationEvaluation;
    private double[] weights;

    private ResponseBuilder rbsp;
    private NumericDocValues leafOutcomeValue;
    private double totalError;
    private SparseFixedBitSet positiveDocsSet;
    private SparseFixedBitSet docsSet;
    private IndexSearcher searcher;

    TextLogisticRegressionCollector(ResponseBuilder rbsp, IndexSearcher searcher,
                                    TrainingParams trainingParams) {
      this.trainingParams = trainingParams;
      this.workingDeltas = new double[trainingParams.weights.length];
      this.weights = Arrays.copyOf(trainingParams.weights, trainingParams.weights.length);
      this.rbsp = rbsp;
      this.classificationEvaluation = new ClassificationEvaluation();
      this.searcher = searcher;
      positiveDocsSet = new SparseFixedBitSet(searcher.getIndexReader().numDocs());
      docsSet = new SparseFixedBitSet(searcher.getIndexReader().numDocs());
    }

    public void doSetNextReader(LeafReaderContext context) throws IOException {
      super.doSetNextReader(context);
      leafReader = context.reader();
      leafOutcomeValue = leafReader.getNumericDocValues(trainingParams.outcome);
    }

    public void collect(int doc) throws IOException{
      int valuesDocID = leafOutcomeValue.docID();
      if (valuesDocID < doc) {
        valuesDocID = leafOutcomeValue.advance(doc);
      }
      int outcome;
      if (valuesDocID == doc) {
        outcome = (int) leafOutcomeValue.longValue();
      } else {
        outcome = 0;
      }

      outcome = trainingParams.positiveLabel == outcome? 1 : 0;
      if (outcome == 1) {
        positiveDocsSet.set(context.docBase + doc);
      }
      docsSet.set(context.docBase+doc);

    }

    public void finish() throws IOException {

      Map<Integer, double[]> docVectors = new HashMap<>();
      Terms terms = MultiFields.getFields(searcher.getIndexReader()).terms(trainingParams.feature);
      TermsEnum termsEnum = terms.iterator();
      PostingsEnum postingsEnum = null;
      int termIndex = 0;
      for (String termStr : trainingParams.terms) {
        BytesRef term = new BytesRef(termStr);
        if (termsEnum.seekExact(term)) {
          postingsEnum = termsEnum.postings(postingsEnum);
          while (postingsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            int docId = postingsEnum.docID();
            if (docsSet.get(docId)) {
              double[] vector = docVectors.get(docId);
              if (vector == null) {
                vector = new double[trainingParams.terms.length+1];
                vector[0] = 1.0;
                docVectors.put(docId, vector);
              }
              vector[termIndex + 1] = trainingParams.idfs[termIndex] * (1.0 + Math.log(postingsEnum.freq()));
            }
          }
        }
        termIndex++;
      }

      for (Map.Entry<Integer, double[]> entry : docVectors.entrySet()) {
        double[] vector = entry.getValue();
        int outcome = 0;
        if (positiveDocsSet.get(entry.getKey())) {
          outcome = 1;
        }
        double sig = sigmoid(sum(multiply(vector, weights)));
        double error = sig - outcome;
        double lastSig = sigmoid(sum(multiply(vector, trainingParams.weights)));
        totalError += Math.abs(lastSig - outcome);
        classificationEvaluation.count(outcome,  lastSig >= trainingParams.threshold ? 1 : 0);

        workingDeltas = multiply(error * trainingParams.alpha, vector);

        for(int i = 0; i< workingDeltas.length; i++) {
          weights[i] -= workingDeltas[i];
        }
      }

      NamedList analytics = new NamedList();
      rbsp.rsp.add("logit", analytics);

      List<Double> outWeights = new ArrayList<>();
      for(Double d : weights) {
        outWeights.add(d);
      }

      analytics.add("weights", outWeights);
      analytics.add("error", totalError);
      analytics.add("evaluation", classificationEvaluation.toMap());
      analytics.add("feature", trainingParams.feature);
      analytics.add("positiveLabel", trainingParams.positiveLabel);
      if(this.delegate instanceof DelegatingCollector) {
        ((DelegatingCollector)this.delegate).finish();
      }
    }

    private double sigmoid(double in) {
      double d = 1.0 / (1+Math.exp(-in));
      return d;
    }

    private double[] multiply(double[] vals, double[] weights) {
      for(int i = 0; i < vals.length; ++i) {
        workingDeltas[i] = vals[i] * weights[i];
      }

      return workingDeltas;
    }

    private double[] multiply(double d, double[] vals) {
      for(int i = 0; i<vals.length; ++i) {
        workingDeltas[i] = vals[i] * d;
      }

      return workingDeltas;
    }

    private double sum(double[] vals) {
      double d = 0.0d;
      for(double val : vals) {
        d += val;
      }

      return d;
    }

  }

  private static class TrainingParams {
    public final String feature;
    public final String[] terms;
    public final double[] idfs;
    public final String outcome;
    public final double[] weights;
    public final int interation;
    public final int positiveLabel;
    public final double threshold;
    public final double alpha;

    public TrainingParams(String feature, String[] terms, double[] idfs, String outcome, double[] weights, int interation, double alpha, int positiveLabel, double threshold) {
      this.feature = feature;
      this.terms = terms;
      this.idfs = idfs;
      this.outcome = outcome;
      this.weights = weights;
      this.alpha = alpha;
      this.interation = interation;
      this.positiveLabel = positiveLabel;
      this.threshold = threshold;
    }
  }
}
