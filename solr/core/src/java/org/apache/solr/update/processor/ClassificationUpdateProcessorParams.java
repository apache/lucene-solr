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

import org.apache.lucene.search.Query;

public class ClassificationUpdateProcessorParams {

  private String[] inputFieldNames; // the array of fields to be sent to the Classifier

  private Query trainingFilterQuery; // a filter query to reduce the training set to a subset

  private String trainingClassField; // the field containing the class for the Document

  private String predictedClassField; // the field that will contain the predicted class

  private int maxPredictedClasses; // the max number of classes to assign

  private ClassificationUpdateProcessorFactory.Algorithm algorithm; // the Classification Algorithm to use - currently 'knn' or 'bayes'

  private int minTf; // knn specific - the minimum Term Frequency for considering a term

  private int minDf; // knn specific - the minimum Document Frequency for considering a term

  private int k; // knn specific - thw window of top results to evaluate, when assigning the class

  public String[] getInputFieldNames() {
    return inputFieldNames;
  }

  public void setInputFieldNames(String[] inputFieldNames) {
    this.inputFieldNames = inputFieldNames;
  }

  public Query getTrainingFilterQuery() {
    return trainingFilterQuery;
  }

  public void setTrainingFilterQuery(Query trainingFilterQuery) {
    this.trainingFilterQuery = trainingFilterQuery;
  }

  public String getTrainingClassField() {
    return trainingClassField;
  }

  public void setTrainingClassField(String trainingClassField) {
    this.trainingClassField = trainingClassField;
  }

  public String getPredictedClassField() {
    return predictedClassField;
  }

  public void setPredictedClassField(String predictedClassField) {
    this.predictedClassField = predictedClassField;
  }

  public int getMaxPredictedClasses() {
    return maxPredictedClasses;
  }

  public void setMaxPredictedClasses(int maxPredictedClasses) {
    this.maxPredictedClasses = maxPredictedClasses;
  }

  public ClassificationUpdateProcessorFactory.Algorithm getAlgorithm() {
    return algorithm;
  }

  public void setAlgorithm(ClassificationUpdateProcessorFactory.Algorithm algorithm) {
    this.algorithm = algorithm;
  }

  public int getMinTf() {
    return minTf;
  }

  public void setMinTf(int minTf) {
    this.minTf = minTf;
  }

  public int getMinDf() {
    return minDf;
  }

  public void setMinDf(int minDf) {
    this.minDf = minDf;
  }

  public int getK() {
    return k;
  }

  public void setK(int k) {
    this.k = k;
  }
}
