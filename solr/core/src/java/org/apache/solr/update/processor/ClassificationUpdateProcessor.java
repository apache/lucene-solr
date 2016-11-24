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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.classification.ClassificationResult;
import org.apache.lucene.classification.document.DocumentClassifier;
import org.apache.lucene.classification.document.KNearestNeighborDocumentClassifier;
import org.apache.lucene.classification.document.SimpleNaiveBayesDocumentClassifier;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.ClassificationUpdateProcessorFactory.Algorithm;

/**
 * This Class is a Request Update Processor to classify the document in input and add a field
 * containing the class to the Document.
 * It uses the Lucene Document Classification module, see {@link DocumentClassifier}.
 */
class ClassificationUpdateProcessor
    extends UpdateRequestProcessor {

  private final String trainingClassField;
  private final String predictedClassField;
  private final int maxOutputClasses;
  private DocumentClassifier<BytesRef> classifier;

  /**
   * Sole constructor
   *
   * @param classificationParams classification advanced params
   * @param next            next update processor in the chain
   * @param indexReader     index reader
   * @param schema          schema
   */
  public ClassificationUpdateProcessor(ClassificationUpdateProcessorParams classificationParams, UpdateRequestProcessor next, IndexReader indexReader, IndexSchema schema) {
    super(next);
    this.trainingClassField = classificationParams.getTrainingClassField();
    this.predictedClassField = classificationParams.getPredictedClassField();
    this.maxOutputClasses = classificationParams.getMaxPredictedClasses();
    String[] inputFieldNamesWithBoost = classificationParams.getInputFieldNames();
    Algorithm classificationAlgorithm = classificationParams.getAlgorithm();

    Map<String, Analyzer> field2analyzer = new HashMap<>();
    String[] inputFieldNames = this.removeBoost(inputFieldNamesWithBoost);
    for (String fieldName : inputFieldNames) {
      SchemaField fieldFromSolrSchema = schema.getField(fieldName);
      Analyzer indexAnalyzer = fieldFromSolrSchema.getType().getQueryAnalyzer();
      field2analyzer.put(fieldName, indexAnalyzer);
    }
    switch (classificationAlgorithm) {
      case KNN:
        classifier = new KNearestNeighborDocumentClassifier(indexReader, null, classificationParams.getTrainingFilterQuery(), classificationParams.getK(), classificationParams.getMinDf(), classificationParams.getMinTf(), trainingClassField, field2analyzer, inputFieldNamesWithBoost);
        break;
      case BAYES:
        classifier = new SimpleNaiveBayesDocumentClassifier(indexReader, null, trainingClassField, field2analyzer, inputFieldNamesWithBoost);
        break;
    }
  }

  private String[] removeBoost(String[] inputFieldNamesWithBoost) {
    String[] inputFieldNames = new String[inputFieldNamesWithBoost.length];
    for (int i = 0; i < inputFieldNamesWithBoost.length; i++) {
      String singleFieldNameWithBoost = inputFieldNamesWithBoost[i];
      String[] fieldName2boost = singleFieldNameWithBoost.split("\\^");
      inputFieldNames[i] = fieldName2boost[0];
    }
    return inputFieldNames;
  }

  /**
   * @param cmd the update command in input containing the Document to classify
   * @throws IOException If there is a low-level I/O error
   */
  @Override
  public void processAdd(AddUpdateCommand cmd)
      throws IOException {
    SolrInputDocument doc = cmd.getSolrInputDocument();
    Document luceneDocument = cmd.getLuceneDocument();
    String assignedClass;
    Object documentClass = doc.getFieldValue(trainingClassField);
    if (documentClass == null) {
      List<ClassificationResult<BytesRef>> assignedClassifications = classifier.getClasses(luceneDocument, maxOutputClasses);
      if (assignedClassifications != null) {
        for (ClassificationResult<BytesRef> singleClassification : assignedClassifications) {
          assignedClass = singleClassification.getAssignedClass().utf8ToString();
          doc.addField(predictedClassField, assignedClass);
        }
      }
    }
    super.processAdd(cmd);
  }
}
