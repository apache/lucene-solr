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
package org.apache.lucene.classification;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.queries.mlt.MoreLikeThisParameters;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

/**
 * A k-Nearest Neighbor classifier (see <code>http://en.wikipedia.org/wiki/K-nearest_neighbors</code>) based
 * on {@link MoreLikeThis}
 *
 * @lucene.experimental
 */
public class KNearestNeighborClassifier implements Classifier<BytesRef> {

  /**
   * a {@link MoreLikeThis} instance used to perform MLT queries
   */
  protected final MoreLikeThis mlt;

  /**
   * the name of the fields used as the input text
   */
  protected final String[] textFieldNames;

  /**
   * the name of the field used as the output text
   */
  protected final String classFieldName;

  /**
   * an {@link IndexSearcher} used to perform queries
   */
  protected final IndexSearcher indexSearcher;

  /**
   * the no. of docs to compare in order to find the nearest neighbor to the input text
   */
  protected final int k;

  /**
   * a {@link Query} used to filter the documents that should be used from this classifier's underlying {@link LeafReader}
   */
  protected final Query query;

  /**
   * Creates a {@link KNearestNeighborClassifier}.
   *
   * @param indexReader     the reader on the index to be used for classification
   * @param analyzer       an {@link Analyzer} used to analyze unseen text
   * @param similarity     the {@link Similarity} to be used by the underlying {@link IndexSearcher} or {@code null}
   *                       (defaults to {@link org.apache.lucene.search.similarities.BM25Similarity})
   * @param query          a {@link Query} to eventually filter the docs used for training the classifier, or {@code null}
   *                       if all the indexed docs should be used
   * @param k              the no. of docs to select in the MLT results to find the nearest neighbor
   * @param minDocsFreq    {@link MoreLikeThisParameters#minDocFreq} parameter
   * @param minTermFreq    {@link MoreLikeThisParameters#minTermFreq} parameter
   * @param classFieldName the name of the field used as the output for the classifier
   * @param textFieldNames the name of the fields used as the inputs for the classifier, they can contain boosting indication e.g. title^10
   */
  public KNearestNeighborClassifier(IndexReader indexReader, Similarity similarity, Analyzer analyzer, Query query, int k, int minDocsFreq,
                                    int minTermFreq, String classFieldName, String... textFieldNames) {
    this.textFieldNames = textFieldNames;
    this.classFieldName = classFieldName;
    this.mlt = new MoreLikeThis(indexReader);
    MoreLikeThisParameters mltParameters = new MoreLikeThisParameters();
    this.mlt.setParameters(mltParameters);
    mltParameters.setAnalyzer(analyzer);
    mltParameters.setFieldNames(textFieldNames);
    this.indexSearcher = new IndexSearcher(indexReader);
    if (similarity != null) {
      this.indexSearcher.setSimilarity(similarity);
    } else {
      this.indexSearcher.setSimilarity(new BM25Similarity());
    }
    if (minDocsFreq > 0) {
      mltParameters.setMinDocFreq(minDocsFreq);
    }
    if (minTermFreq > 0) {
      mltParameters.setMinTermFreq(minTermFreq);
    }
    this.query = query;
    this.k = k;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public ClassificationResult<BytesRef> assignClass(String text) throws IOException {
    return classifyFromTopDocs(knnSearch(text));
  }

  /**
   * TODO
   */
  protected ClassificationResult<BytesRef> classifyFromTopDocs(TopDocs knnResults) throws IOException {
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
  public List<ClassificationResult<BytesRef>> getClasses(String text) throws IOException {
    TopDocs knnResults = knnSearch(text);
    List<ClassificationResult<BytesRef>> assignedClasses = buildListFromTopDocs(knnResults);
    Collections.sort(assignedClasses);
    return assignedClasses;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ClassificationResult<BytesRef>> getClasses(String text, int max) throws IOException {
    TopDocs knnResults = knnSearch(text);
    List<ClassificationResult<BytesRef>> assignedClasses = buildListFromTopDocs(knnResults);
    Collections.sort(assignedClasses);
    return assignedClasses.subList(0, max);
  }

  private TopDocs knnSearch(String text) throws IOException {
    Document textDocument = new Document();
    for(String fieldName: textFieldNames){
      textDocument.add(new TextField(fieldName,text, Field.Store.YES));
    }
    return knnSearch(textDocument);
  }

  /**
   * Returns the top k results from a More Like This query based on the input document
   *
   * @param document the document to use for More Like This search
   * @return the top results for the MLT query
   * @throws IOException If there is a low-level I/O error
   */
  protected TopDocs knnSearch(Document document) throws IOException {
    MoreLikeThisParameters classificationMltParameters = mlt.getParameters();
    BooleanQuery.Builder mltQuery = new BooleanQuery.Builder();
    Map<String, Float> fieldToQueryTimeBoostFactor = classificationMltParameters.getFieldToQueryTimeBoostFactor();
    ArrayList<String> fieldNamesWithNoBoost = new ArrayList<>();
    for (String fieldName : textFieldNames) {
      String boost = null;
      if (fieldName.contains("^")) {
        String[] field2boost = fieldName.split("\\^");
        fieldName = field2boost[0];
        boost = field2boost[1];
      }
      fieldNamesWithNoBoost.add(fieldName);
      classificationMltParameters.enableBoost(true); // we want always to use the boost coming from TF * IDF of the term
      if (boost != null) {
        if(fieldToQueryTimeBoostFactor == null){
          fieldToQueryTimeBoostFactor = new HashMap<>();
          classificationMltParameters.setFieldToQueryTimeBoostFactor(fieldToQueryTimeBoostFactor);
        }
        fieldToQueryTimeBoostFactor.put(fieldName,Float.parseFloat(boost)); // this is an additional multiplicative boost coming from the field boost
      }
    }
    classificationMltParameters.setFieldNames(fieldNamesWithNoBoost.toArray(textFieldNames));
    mltQuery.add(mlt.like(document), BooleanClause.Occur.MUST);

    Query classFieldQuery = new WildcardQuery(new Term(classFieldName, "*"));
    mltQuery.add(new BooleanClause(classFieldQuery, BooleanClause.Occur.MUST));
    if (query != null) {
      mltQuery.add(query, BooleanClause.Occur.MUST);
    }
    return indexSearcher.search(mltQuery.build(), k);
  }

  //ranking of classes must be taken in consideration
  /**
   * build a list of classification results from search results
   * @param topDocs the search results as a {@link TopDocs} object
   * @return a {@link List} of {@link ClassificationResult}, one for each existing class
   * @throws IOException if it's not possible to get the stored value of class field
   */
  protected List<ClassificationResult<BytesRef>> buildListFromTopDocs(TopDocs topDocs) throws IOException {
    Map<BytesRef, Integer> classCounts = new HashMap<>();
    Map<BytesRef, Double> classBoosts = new HashMap<>(); // this is a boost based on class ranking positions in topDocs
    float maxScore = topDocs.getMaxScore();
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      IndexableField[] storableFields = indexSearcher.doc(scoreDoc.doc).getFields(classFieldName);
      for (IndexableField singleStorableField : storableFields) {
        if (singleStorableField != null) {
          BytesRef cl = new BytesRef(singleStorableField.stringValue());
        //update count
        Integer count = classCounts.get(cl);
        if (count != null) {
          classCounts.put(cl, count + 1);
        } else {
          classCounts.put(cl, 1);
        }
        //update boost, the boost is based on the best score
        Double totalBoost = classBoosts.get(cl);
        double singleBoost = scoreDoc.score / maxScore;
        if (totalBoost != null) {
          classBoosts.put(cl, totalBoost + singleBoost);
        } else {
          classBoosts.put(cl, singleBoost);
        }
        }
      }
    }
    List<ClassificationResult<BytesRef>> returnList = new ArrayList<>();
    List<ClassificationResult<BytesRef>> temporaryList = new ArrayList<>();
    int sumdoc = 0;
    for (Map.Entry<BytesRef, Integer> entry : classCounts.entrySet()) {
      Integer count = entry.getValue();
      Double normBoost = classBoosts.get(entry.getKey()) / count; //the boost is normalized to be 0<b<1
      temporaryList.add(new ClassificationResult<>(entry.getKey().clone(), (count * normBoost) / (double) k));
      sumdoc += count;
    }

    //correction
    if (sumdoc < k) {
      for (ClassificationResult<BytesRef> cr : temporaryList) {
        returnList.add(new ClassificationResult<>(cr.getAssignedClass(), cr.getScore() * k / (double) sumdoc));
      }
    } else {
      returnList = temporaryList;
    }
    return returnList;
  }

  @Override
  public String toString() {
    return "KNearestNeighborClassifier{" +
        "textFieldNames=" + Arrays.toString(textFieldNames) +
        ", classFieldName='" + classFieldName + '\'' +
        ", k=" + k +
        ", query=" + query +
        ", similarity=" + indexSearcher.getSimilarity(true) +
        '}';
  }
}
