package org.apache.solr.update.processor;

import org.apache.lucene.index.LeafReader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;

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

/**
 * This class implements an UpdateProcessorFactory for the Classification Update Processor.
 * It takes in input a series of parameter that will be necessary to instantiate and use the Classifier
 */
public class ClassificationUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  // Update Processor Config params
  private static final String INPUT_FIELDS_PARAM = "inputFields";
  private static final String CLASS_FIELD_PARAM = "classField";
  private static final String ALGORITHM_PARAM = "algorithm";
  private static final String KNN_MIN_TF_PARAM = "knn.minTf";
  private static final String KNN_MIN_DF_PARAM = "knn.minDf";
  private static final String KNN_K_PARAM = "knn.k";

  //Update Processor Defaults
  private static final int DEFAULT_MIN_TF = 1;
  private static final int DEFAULT_MIN_DF = 1;
  private static final int DEFAULT_K = 10;
  private static final String DEFAULT_ALGORITHM = "knn";

  private String[] inputFieldNames; // the array of fields to be sent to the Classifier

  private String classFieldName; // the field containing the class for the Document

  private String algorithm; // the Classification Algorithm to use - currently 'knn' or 'bayes'

  private int minTf; // knn specific - the minimum Term Frequency for considering a term

  private int minDf; // knn specific - the minimum Document Frequency for considering a term

  private int k; // knn specific - thw window of top results to evaluate, when assgning the class

  @Override
  public void init(final NamedList args) {
    if (args != null) {
      SolrParams params = SolrParams.toSolrParams(args);

      String fieldNames = params.get(INPUT_FIELDS_PARAM);// must be a comma separated list of fields
      checkNotNull(INPUT_FIELDS_PARAM, fieldNames);
      inputFieldNames = fieldNames.split("\\,");

      classFieldName = params.get(CLASS_FIELD_PARAM);
      checkNotNull(CLASS_FIELD_PARAM, classFieldName);

      algorithm = params.get(ALGORITHM_PARAM);
      if (algorithm == null)
        algorithm = DEFAULT_ALGORITHM;

      minTf = getIntParam(params, KNN_MIN_TF_PARAM, DEFAULT_MIN_TF);
      minDf = getIntParam(params, KNN_MIN_DF_PARAM, DEFAULT_MIN_DF);
      k = getIntParam(params, KNN_K_PARAM, DEFAULT_K);
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
    IndexSchema schema = req.getSchema();
    LeafReader leafReader = req.getSearcher().getLeafReader();
    return new ClassificationUpdateProcessor(inputFieldNames, classFieldName, minDf, minTf, k, algorithm, next, leafReader, schema);
  }

  /**
   * get field names used as classifier's inputs
   *
   * @return the input field names
   */
  public String[] getInputFieldNames() {
    return inputFieldNames;
  }

  /**
   * set field names used as classifier's inputs
   *
   * @param inputFieldNames the input field names
   */
  public void setInputFieldNames(String[] inputFieldNames) {
    this.inputFieldNames = inputFieldNames;
  }

  /**
   * get field names used as classifier's output
   *
   * @return the output field name
   */
  public String getClassFieldName() {
    return classFieldName;
  }

  /**
   * set field names used as classifier's output
   *
   * @param classFieldName the output field name
   */
  public void setClassFieldName(String classFieldName) {
    this.classFieldName = classFieldName;
  }

  /**
   * get the name of the classifier algorithm used
   *
   * @return the classifier algorithm used
   */
  public String getAlgorithm() {
    return algorithm;
  }

  /**
   * set the name of the classifier algorithm used
   *
   * @param algorithm the classifier algorithm used
   */
  public void setAlgorithm(String algorithm) {
    this.algorithm = algorithm;
  }

  /**
   * get the min term frequency value to be used in case algorithm is {@code "knn"}
   *
   * @return the min term frequency
   */
  public int getMinTf() {
    return minTf;
  }

  /**
   * set the min term frequency value to be used in case algorithm is {@code "knn"}
   *
   * @param minTf the min term frequency
   */
  public void setMinTf(int minTf) {
    this.minTf = minTf;
  }

  /**
   * get the min document frequency value to be used in case algorithm is {@code "knn"}
   *
   * @return the min document frequency
   */
  public int getMinDf() {
    return minDf;
  }

  /**
   * set the min document frequency value to be used in case algorithm is {@code "knn"}
   *
   * @param minDf the min document frequency
   */
  public void setMinDf(int minDf) {
    this.minDf = minDf;
  }

  /**
   * get the the no. of nearest neighbor to analyze, to be used in case algorithm is {@code "knn"}
   *
   * @return the no. of neighbors to analyze
   */
  public int getK() {
    return k;
  }

  /**
   * set the the no. of nearest neighbor to analyze, to be used in case algorithm is {@code "knn"}
   *
   * @param k the no. of neighbors to analyze
   */
  public void setK(int k) {
    this.k = k;
  }
}
