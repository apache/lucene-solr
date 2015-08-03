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

import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

/**
 * Testcase for {@link KNearestNeighborClassifier}
 */
public class KNearestNeighborClassifierTest extends ClassificationTestBase<BytesRef> {

  @Test
  public void testBasicUsage() throws Exception {
    // usage with default MLT min docs / term freq
    checkCorrectClassification(new KNearestNeighborClassifier(3), POLITICS_INPUT, POLITICS_RESULT, new MockAnalyzer(random()), textFieldName, categoryFieldName);
    // usage without custom min docs / term freq for MLT
    checkCorrectClassification(new KNearestNeighborClassifier(3, 2, 1), TECHNOLOGY_INPUT, TECHNOLOGY_RESULT, new MockAnalyzer(random()), textFieldName, categoryFieldName);
  }

  /**
   * This test is for the scenario where in the first topK results from the MLT query, we have the same number of results per class.
   * But the results for a class have a better ranking in comparison with the results of the second class.
   * So we would expect a greater score for the best ranked class.
   *
   * @throws Exception if any error happens
   */
  @Test
  public void testRankedClasses() throws Exception {
    LeafReader leafReader = null;
    try {
      Analyzer analyzer = new EnglishAnalyzer();
      leafReader = populateSampleIndex(analyzer);
      KNearestNeighborClassifier knnClassifier = new KNearestNeighborClassifier(6, 1, 1);
      knnClassifier.train(leafReader, textFieldName, categoryFieldName, analyzer);
      List<ClassificationResult<BytesRef>> classes = knnClassifier.getClasses(STRONG_TECHNOLOGY_INPUT);
      assertTrue(classes.get(0).getScore() > classes.get(1).getScore());
      checkCorrectClassification(knnClassifier, STRONG_TECHNOLOGY_INPUT, TECHNOLOGY_RESULT, analyzer, textFieldName, categoryFieldName);
    } finally {
      if (leafReader != null) {
        leafReader.close();
      }
    }
  }

  /**
   * This test is for the scenario where in the first topK results from the MLT query, we have less results
   * for the expected class than the results for the bad class.
   * But the results for the expected class have a better score in comparison with the results of the second class.
   * So we would expect a greater score for the best ranked class.
   *
   * @throws Exception if any error happens
   */
  @Test
  public void testUnbalancedClasses() throws Exception {
    LeafReader leafReader = null;
    try {
      Analyzer analyzer = new EnglishAnalyzer();
      leafReader = populateSampleIndex(analyzer);
      KNearestNeighborClassifier knnClassifier = new KNearestNeighborClassifier(3, 1, 1);
      knnClassifier.train(leafReader, textFieldName, categoryFieldName, analyzer);
      List<ClassificationResult<BytesRef>> classes = knnClassifier.getClasses(SUPER_STRONG_TECHNOLOGY_INPUT);
      assertTrue(classes.get(0).getScore() > classes.get(1).getScore());
      checkCorrectClassification(knnClassifier, SUPER_STRONG_TECHNOLOGY_INPUT, TECHNOLOGY_RESULT, analyzer, textFieldName, categoryFieldName);
    } finally {
      if (leafReader != null) {
        leafReader.close();
      }
    }
  }

  @Test
  public void testBasicUsageWithQuery() throws Exception {
    checkCorrectClassification(new KNearestNeighborClassifier(1), TECHNOLOGY_INPUT, TECHNOLOGY_RESULT, new MockAnalyzer(random()), textFieldName, categoryFieldName, new TermQuery(new Term(textFieldName, "it")));
  }

  @Test
  public void testPerformance() throws Exception {
    checkPerformance(new KNearestNeighborClassifier(100), new MockAnalyzer(random()), categoryFieldName);
  }

}
