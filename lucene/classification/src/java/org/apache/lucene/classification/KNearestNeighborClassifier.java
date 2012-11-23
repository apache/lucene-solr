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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

/**
 * A k-Nearest Neighbor classifier (see <code>http://en.wikipedia.org/wiki/K-nearest_neighbors</code>) based
 * on {@link MoreLikeThis}
 *
 * @lucene.experimental
 */
public class KNearestNeighborClassifier implements Classifier {

  private MoreLikeThis mlt;
  private String textFieldName;
  private String classFieldName;
  private IndexSearcher indexSearcher;
  private int k;

  /**
   * Create a {@link Classifier} using kNN algorithm
   *
   * @param k the number of neighbors to analyze as an <code>int</code>
   */
  public KNearestNeighborClassifier(int k) {
    this.k = k;
  }

  @Override
  public ClassificationResult assignClass(String text) throws IOException {
    Query q = mlt.like(new StringReader(text), textFieldName);
    TopDocs docs = indexSearcher.search(q, k);

    // TODO : improve the nearest neighbor selection
    Map<String, Integer> classCounts = new HashMap<String, Integer>();
    for (ScoreDoc scoreDoc : docs.scoreDocs) {
      String cl = indexSearcher.doc(scoreDoc.doc).getField(classFieldName).stringValue();
      Integer count = classCounts.get(cl);
      if (count != null) {
        classCounts.put(cl, count + 1);
      } else {
        classCounts.put(cl, 1);
      }
    }
    int max = 0;
    String assignedClass = null;
    for (String cl : classCounts.keySet()) {
      Integer count = classCounts.get(cl);
      if (count > max) {
        max = count;
        assignedClass = cl;
      }
    }
    double score = max / k;
    return new ClassificationResult(assignedClass, score);
  }

  @Override
  public void train(AtomicReader atomicReader, String textFieldName, String classFieldName, Analyzer analyzer) throws IOException {
    this.textFieldName = textFieldName;
    this.classFieldName = classFieldName;
    mlt = new MoreLikeThis(atomicReader);
    mlt.setAnalyzer(analyzer);
    mlt.setFieldNames(new String[]{textFieldName});
    indexSearcher = new IndexSearcher(atomicReader);
  }
}
