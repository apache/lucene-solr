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

package org.apache.solr.update.processor;

import java.util.Locale;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.LuceneQParser;
import org.apache.solr.search.SyntaxError;

import static org.apache.solr.update.processor.ClassificationUpdateProcessorFactory.Algorithm.KNN;

/**
 * This class implements an UpdateProcessorFactory for the Classification Update Processor.
 * It takes in input a series of parameter that will be necessary to instantiate and use the Classifier
 */
public class ClassificationUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  // Update Processor Config params
  private static final String INPUT_FIELDS_PARAM = "inputFields";
  private static final String TRAINING_CLASS_FIELD_PARAM = "classField";
  private static final String PREDICTED_CLASS_FIELD_PARAM = "predictedClassField";
  private static final String MAX_CLASSES_TO_ASSIGN_PARAM = "predictedClass.maxCount";
  private static final String ALGORITHM_PARAM = "algorithm";
  private static final String KNN_MIN_TF_PARAM = "knn.minTf";
  private static final String KNN_MIN_DF_PARAM = "knn.minDf";
  private static final String KNN_K_PARAM = "knn.k";
  private static final String KNN_FILTER_QUERY = "knn.filterQuery";

  public enum Algorithm {KNN, BAYES}

  //Update Processor Defaults
  private static final int DEFAULT_MAX_CLASSES_TO_ASSIGN = 1;
  private static final int DEFAULT_MIN_TF = 1;
  private static final int DEFAULT_MIN_DF = 1;
  private static final int DEFAULT_K = 10;
  private static final Algorithm DEFAULT_ALGORITHM = KNN;

  private SolrParams params;
  private ClassificationUpdateProcessorParams classificationParams;

  @Override
  public void init(final NamedList args) {
    if (args != null) {
      params = SolrParams.toSolrParams(args);
      classificationParams = new ClassificationUpdateProcessorParams();

      String fieldNames = params.get(INPUT_FIELDS_PARAM);// must be a comma separated list of fields
      checkNotNull(INPUT_FIELDS_PARAM, fieldNames);
      classificationParams.setInputFieldNames(fieldNames.split("\\,"));

      String trainingClassField = (params.get(TRAINING_CLASS_FIELD_PARAM));
      checkNotNull(TRAINING_CLASS_FIELD_PARAM, trainingClassField);
      classificationParams.setTrainingClassField(trainingClassField);

      String predictedClassField = (params.get(PREDICTED_CLASS_FIELD_PARAM));
      if (predictedClassField == null || predictedClassField.isEmpty()) {
        predictedClassField = trainingClassField;
      }
      classificationParams.setPredictedClassField(predictedClassField);

      classificationParams.setMaxPredictedClasses(getIntParam(params, MAX_CLASSES_TO_ASSIGN_PARAM, DEFAULT_MAX_CLASSES_TO_ASSIGN));

      String algorithmString = params.get(ALGORITHM_PARAM);
      Algorithm classificationAlgorithm;
      try {
        if (algorithmString == null || Algorithm.valueOf(algorithmString.toUpperCase(Locale.ROOT)) == null) {
          classificationAlgorithm = DEFAULT_ALGORITHM;
        } else {
          classificationAlgorithm = Algorithm.valueOf(algorithmString.toUpperCase(Locale.ROOT));
        }
      } catch (IllegalArgumentException e) {
        throw new SolrException
            (SolrException.ErrorCode.SERVER_ERROR,
                "Classification UpdateProcessor Algorithm: '" + algorithmString + "' not supported");
      }
      classificationParams.setAlgorithm(classificationAlgorithm);

      classificationParams.setMinTf(getIntParam(params, KNN_MIN_TF_PARAM, DEFAULT_MIN_TF));
      classificationParams.setMinDf(getIntParam(params, KNN_MIN_DF_PARAM, DEFAULT_MIN_DF));
      classificationParams.setK(getIntParam(params, KNN_K_PARAM, DEFAULT_K));
    }
  }

  /*
   * Returns an Int parsed param or a default if the param is null
   *
   * @param params       Solr params in input
   * @param name         the param name
   * @param defaultValue the param default
   * @return the Int value for the param
   */
  private int getIntParam(SolrParams params, String name, int defaultValue) {
    String paramString = params.get(name);
    int paramInt;
    if (paramString != null && !paramString.isEmpty()) {
      paramInt = Integer.parseInt(paramString);
    } else {
      paramInt = defaultValue;
    }
    return paramInt;
  }

  private void checkNotNull(String paramName, Object param) {
    if (param == null) {
      throw new SolrException
          (SolrException.ErrorCode.SERVER_ERROR,
              "Classification UpdateProcessor '" + paramName + "' can not be null");
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    String trainingFilterQueryString = (params.get(KNN_FILTER_QUERY));
    try {
      if (trainingFilterQueryString != null && !trainingFilterQueryString.isEmpty()) {
        Query trainingFilterQuery = this.parseFilterQuery(trainingFilterQueryString, params, req);
        classificationParams.setTrainingFilterQuery(trainingFilterQuery);
      }
    } catch (SyntaxError | RuntimeException syntaxError) {
      throw new SolrException
          (SolrException.ErrorCode.SERVER_ERROR,
              "Classification UpdateProcessor Training Filter Query: '" + trainingFilterQueryString + "' is not supported", syntaxError);
    }

    IndexSchema schema = req.getSchema();
    IndexReader indexReader = req.getSearcher().getIndexReader();

    return new ClassificationUpdateProcessor(classificationParams, next, indexReader, schema);
  }

  private Query parseFilterQuery(String trainingFilterQueryString, SolrParams params, SolrQueryRequest req) throws SyntaxError {
    LuceneQParser parser = new LuceneQParser(trainingFilterQueryString, null, params, req);
    return parser.parse();
  }

  public ClassificationUpdateProcessorParams getClassificationParams() {
    return classificationParams;
  }

  public void setClassificationParams(ClassificationUpdateProcessorParams classificationParams) {
    this.classificationParams = classificationParams;
  }
}
