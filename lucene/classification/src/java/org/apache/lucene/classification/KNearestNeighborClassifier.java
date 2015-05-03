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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;

/**
 * A k-Nearest Neighbor classifier (see <code>http://en.wikipedia.org/wiki/K-nearest_neighbors</code>) based
 * on {@link MoreLikeThis}
 *
 * @lucene.experimental
 */
public class KNearestNeighborClassifier implements Classifier<BytesRef> {

  private final MoreLikeThis mlt;
  private final String[] textFieldNames;
  private final String classFieldName;
  private final IndexSearcher indexSearcher;
  private final int k;
  private final Query query;

  /**
   * Creates a {@link KNearestNeighborClassifier}.
   *
   * @param leafReader     the reader on the index to be used for classification
   * @param analyzer       an {@link Analyzer} used to analyze unseen text
   * @param query          a {@link Query} to eventually filter the docs used for training the classifier, or {@code null}
   *                       if all the indexed docs should be used
   * @param k              the no. of docs to select in the MLT results to find the nearest neighbor
   * @param minDocsFreq    {@link MoreLikeThis#minDocFreq} parameter
   * @param minTermFreq    {@link MoreLikeThis#minTermFreq} parameter
   * @param classFieldName the name of the field used as the output for the classifier
   * @param textFieldNames the name of the fields used as the inputs for the classifier
   */
  public KNearestNeighborClassifier(LeafReader leafReader, Analyzer analyzer, Query query, int k, int minDocsFreq,
                                    int minTermFreq, String classFieldName, String... textFieldNames) {
    this.textFieldNames = textFieldNames;
    this.classFieldName = classFieldName;
    this.mlt = new MoreLikeThis(leafReader);
    this.mlt.setAnalyzer(analyzer);
    this.mlt.setFieldNames(textFieldNames);
    this.indexSearcher = new IndexSearcher(leafReader);
    if (minDocsFreq > 0) {
      mlt.setMinDocFreq(minDocsFreq);
    }
    if (minTermFreq > 0) {
      mlt.setMinTermFreq(minTermFreq);
    }
    this.query = query;
    this.k = k;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public ClassificationResult<BytesRef> assignClass(String text) throws IOException {
    TopDocs topDocs = knnSearch(text);
    List<ClassificationResult<BytesRef>> doclist = buildListFromTopDocs(topDocs);
    ClassificationResult<BytesRef> retval = null;
    double maxscore = -Double.MAX_VALUE;
    for (ClassificationResult<BytesRef> element : doclist) {
      if (element.getScore() > maxscore) {
        retval = element;
        maxscore = element.getScore();
      }
    }
    return retval;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ClassificationResult<BytesRef>> getClasses(String text) throws IOException {
    TopDocs topDocs = knnSearch(text);
    List<ClassificationResult<BytesRef>> doclist = buildListFromTopDocs(topDocs);
    Collections.sort(doclist);
    return doclist;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ClassificationResult<BytesRef>> getClasses(String text, int max) throws IOException {
    TopDocs topDocs = knnSearch(text);
    List<ClassificationResult<BytesRef>> doclist = buildListFromTopDocs(topDocs);
    Collections.sort(doclist);
    return doclist.subList(0, max);
  }

  private TopDocs knnSearch(String text) throws IOException {
    BooleanQuery mltQuery = new BooleanQuery();
    for (String textFieldName : textFieldNames) {
      mltQuery.add(new BooleanClause(mlt.like(textFieldName, new StringReader(text)), BooleanClause.Occur.SHOULD));
    }
    Query classFieldQuery = new WildcardQuery(new Term(classFieldName, "*"));
    mltQuery.add(new BooleanClause(classFieldQuery, BooleanClause.Occur.MUST));
    if (query != null) {
      mltQuery.add(query, BooleanClause.Occur.MUST);
    }
    return indexSearcher.search(mltQuery, k);
  }

  private List<ClassificationResult<BytesRef>> buildListFromTopDocs(TopDocs topDocs) throws IOException {
    Map<BytesRef, Integer> classCounts = new HashMap<>();
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      StorableField storableField = indexSearcher.doc(scoreDoc.doc).getField(classFieldName);
      if (storableField != null) {
        BytesRef cl = new BytesRef(storableField.stringValue());
        Integer count = classCounts.get(cl);
        if (count != null) {
          classCounts.put(cl, count + 1);
        } else {
          classCounts.put(cl, 1);
        }
      }
    }
    List<ClassificationResult<BytesRef>> returnList = new ArrayList<>();
    int sumdoc = 0;
    for (Map.Entry<BytesRef, Integer> entry : classCounts.entrySet()) {
      Integer count = entry.getValue();
      returnList.add(new ClassificationResult<>(entry.getKey().clone(), count / (double) k));
      sumdoc += count;
    }

    //correction
    if (sumdoc < k) {
      for (ClassificationResult<BytesRef> cr : returnList) {
        cr.setScore(cr.getScore() * (double) k / (double) sumdoc);
      }
    }
    return returnList;
  }

}
