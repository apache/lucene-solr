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
package org.apache.lucene.classification.document;


import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.classification.ClassificationResult;
import org.apache.lucene.classification.KNearestNeighborClassifier;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

/**
 * A k-Nearest Neighbor Document classifier (see <code>http://en.wikipedia.org/wiki/K-nearest_neighbors</code>) based
 * on {@link org.apache.lucene.queries.mlt.MoreLikeThis} .
 *
 * @lucene.experimental
 */
public class KNearestNeighborDocumentClassifier extends KNearestNeighborClassifier implements DocumentClassifier<BytesRef> {

  /**
   * map of per field analyzers
   */
  protected Map<String, Analyzer> field2analyzer;

  /**
   * Creates a {@link KNearestNeighborClassifier}.
   *
   * @param indexReader     the reader on the index to be used for classification
   * @param similarity     the {@link Similarity} to be used by the underlying {@link IndexSearcher} or {@code null}
   *                       (defaults to {@link org.apache.lucene.search.similarities.ClassicSimilarity})
   * @param query          a {@link org.apache.lucene.search.Query} to eventually filter the docs used for training the classifier, or {@code null}
   *                       if all the indexed docs should be used
   * @param k              the no. of docs to select in the MLT results to find the nearest neighbor
   * @param minDocsFreq    {@link org.apache.lucene.queries.mlt.MoreLikeThis#minDocFreq} parameter
   * @param minTermFreq    {@link org.apache.lucene.queries.mlt.MoreLikeThis#minTermFreq} parameter
   * @param classFieldName the name of the field used as the output for the classifier
   * @param field2analyzer map with key a field name and the related {org.apache.lucene.analysis.Analyzer}
   * @param textFieldNames the name of the fields used as the inputs for the classifier, they can contain boosting indication e.g. title^10
   */
  public KNearestNeighborDocumentClassifier(IndexReader indexReader, Similarity similarity, Query query, int k, int minDocsFreq,
                                            int minTermFreq, String classFieldName, Map<String, Analyzer> field2analyzer, String... textFieldNames) {
    super(indexReader, similarity, null, query, k, minDocsFreq, minTermFreq, classFieldName, textFieldNames);
    this.field2analyzer = field2analyzer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassificationResult<BytesRef> assignClass(Document document) throws IOException {
    TopDocs knnResults = knnSearch(document);
    List<ClassificationResult<BytesRef>> assignedClasses = buildListFromTopDocs(knnResults);
    ClassificationResult<BytesRef> assignedClass = null;
    double maxscore = -Double.MAX_VALUE;
    for (ClassificationResult<BytesRef> cl : assignedClasses) {
      if (cl.getScore() > maxscore) {
        assignedClass = cl;
        maxscore = cl.getScore();
      }
    }
    return assignedClass;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ClassificationResult<BytesRef>> getClasses(Document document) throws IOException {
    TopDocs knnResults = knnSearch(document);
    List<ClassificationResult<BytesRef>> assignedClasses = buildListFromTopDocs(knnResults);
    Collections.sort(assignedClasses);
    return assignedClasses;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ClassificationResult<BytesRef>> getClasses(Document document, int max) throws IOException {
    TopDocs knnResults = knnSearch(document);
    List<ClassificationResult<BytesRef>> assignedClasses = buildListFromTopDocs(knnResults);
    Collections.sort(assignedClasses);
    max = Math.min(max, assignedClasses.size());
    return assignedClasses.subList(0, max);
  }

  /**
   * Returns the top k results from a More Like This query based on the input document
   *
   * @param document the document to use for More Like This search
   * @return the top results for the MLT query
   * @throws IOException If there is a low-level I/O error
   */
  private TopDocs knnSearch(Document document) throws IOException {
    BooleanQuery.Builder mltQuery = new BooleanQuery.Builder();

    for (String fieldName : textFieldNames) {
      String boost = null;
      if (fieldName.contains("^")) {
        String[] field2boost = fieldName.split("\\^");
        fieldName = field2boost[0];
        boost = field2boost[1];
      }
      String[] fieldValues = document.getValues(fieldName);
      mlt.setBoost(true); // we want always to use the boost coming from TF * IDF of the term
      if (boost != null) {
        mlt.setBoostFactor(Float.parseFloat(boost)); // this is an additional multiplicative boost coming from the field boost
      }
      mlt.setAnalyzer(field2analyzer.get(fieldName));
      for (String fieldContent : fieldValues) {
        mltQuery.add(new BooleanClause(mlt.like(fieldName, new StringReader(fieldContent)), BooleanClause.Occur.SHOULD));
      }
    }
    Query classFieldQuery = new WildcardQuery(new Term(classFieldName, "*"));
    mltQuery.add(new BooleanClause(classFieldQuery, BooleanClause.Occur.MUST));
    if (query != null) {
      mltQuery.add(query, BooleanClause.Occur.MUST);
    }
    return indexSearcher.search(mltQuery.build(), k);
  }
}
