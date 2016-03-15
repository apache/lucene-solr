package org.apache.solr.update.processor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.classification.ClassificationResult;
import org.apache.lucene.classification.document.DocumentClassifier;
import org.apache.lucene.classification.document.KNearestNeighborDocumentClassifier;
import org.apache.lucene.classification.document.SimpleNaiveBayesDocumentClassifier;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;

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
 * This Class is a Request Update Processor to classify the document in input and add a field
 * containing the class to the Document.
 * It uses the Lucene Document Classification module, see {@link DocumentClassifier}.
 */
class ClassificationUpdateProcessor
    extends UpdateRequestProcessor {

  private String classFieldName; // the field to index the assigned class

  private DocumentClassifier<BytesRef> classifier;

  /**
   * Sole constructor
   *
   * @param inputFieldNames fields to be used as classifier's inputs
   * @param classFieldName  field to be used as classifier's output
   * @param minDf           setting for {@link org.apache.lucene.queries.mlt.MoreLikeThis#minDocFreq}, in case algorithm is {@code "knn"}
   * @param minTf           setting for {@link org.apache.lucene.queries.mlt.MoreLikeThis#minTermFreq}, in case algorithm is {@code "knn"}
   * @param k               setting for k nearest neighbors to analyze, in case algorithm is {@code "knn"}
   * @param algorithm       the name of the classifier to use
   * @param next            next update processor in the chain
   * @param indexReader     index reader
   * @param schema          schema
   */
  public ClassificationUpdateProcessor(String[] inputFieldNames, String classFieldName, int minDf, int minTf, int k, String algorithm,
                                       UpdateRequestProcessor next, LeafReader indexReader, IndexSchema schema) {
    super(next);
    this.classFieldName = classFieldName;
    Map<String, Analyzer> field2analyzer = new HashMap<String, Analyzer>();
    for (String fieldName : inputFieldNames) {
      SchemaField fieldFromSolrSchema = schema.getField(fieldName);
      Analyzer indexAnalyzer = fieldFromSolrSchema.getType().getQueryAnalyzer();
      field2analyzer.put(fieldName, indexAnalyzer);
    }
    switch (algorithm) {
      case "knn":
        classifier = new KNearestNeighborDocumentClassifier(indexReader, null, null, k, minDf, minTf, classFieldName, field2analyzer, inputFieldNames);
        break;
      case "bayes":
        classifier = new SimpleNaiveBayesDocumentClassifier(indexReader, null, classFieldName, field2analyzer, inputFieldNames);
        break;
    }
  }

  /**
   * @param cmd the update command in input conaining the Document to classify
   * @throws IOException If there is a low-level I/O error
   */
  @Override
  public void processAdd(AddUpdateCommand cmd)
      throws IOException {
    SolrInputDocument doc = cmd.getSolrInputDocument();
    Document luceneDocument = cmd.getLuceneDocument();
    String assignedClass;
    Object documentClass = doc.getFieldValue(classFieldName);
    if (documentClass == null) {
      ClassificationResult<BytesRef> classificationResult = classifier.assignClass(luceneDocument);
      if (classificationResult != null) {
        assignedClass = classificationResult.getAssignedClass().utf8ToString();
        doc.addField(classFieldName, assignedClass);
      }
    }
    super.processAdd(cmd);
  }
}
