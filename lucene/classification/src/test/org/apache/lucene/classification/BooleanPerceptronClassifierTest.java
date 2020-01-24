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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.classification.utils.ConfusionMatrixGenerator;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

/**
 * Testcase for {@link org.apache.lucene.classification.BooleanPerceptronClassifier}
 */
public class BooleanPerceptronClassifierTest extends ClassificationTestBase<Boolean> {

  @Test
  public void testBasicUsage() throws Exception {
    LeafReader leafReader = null;
    try {
      MockAnalyzer analyzer = new MockAnalyzer(random());
      leafReader = getSampleIndex(analyzer);
      BooleanPerceptronClassifier classifier = new BooleanPerceptronClassifier(leafReader, analyzer, null, 1, null, booleanFieldName, textFieldName);
      checkCorrectClassification(classifier, TECHNOLOGY_INPUT, false);
      checkCorrectClassification(classifier, POLITICS_INPUT, true);
    } finally {
      if (leafReader != null) {
        leafReader.close();
      }
    }
  }

  @Test
  public void testExplicitThreshold() throws Exception {
    LeafReader leafReader = null;
    try {
      MockAnalyzer analyzer = new MockAnalyzer(random());
      leafReader = getSampleIndex(analyzer);
      BooleanPerceptronClassifier classifier = new BooleanPerceptronClassifier(leafReader, analyzer, null, 1, 50d, booleanFieldName, textFieldName);
      checkCorrectClassification(classifier, TECHNOLOGY_INPUT, false);
      checkCorrectClassification(classifier, POLITICS_INPUT, true);
    } finally {
      if (leafReader != null) {
        leafReader.close();
      }
    }
  }

  @Test
  public void testBasicUsageWithQuery() throws Exception {
    TermQuery query = new TermQuery(new Term(textFieldName, "of"));
    LeafReader leafReader = null;
    try {
      MockAnalyzer analyzer = new MockAnalyzer(random());
      leafReader = getSampleIndex(analyzer);
      BooleanPerceptronClassifier classifier = new BooleanPerceptronClassifier(leafReader, analyzer, query, 1, null, booleanFieldName, textFieldName);
      checkCorrectClassification(classifier, TECHNOLOGY_INPUT, false);
      checkCorrectClassification(classifier, POLITICS_INPUT, true);
    } finally {
      if (leafReader != null) {
        leafReader.close();
      }
    }
  }

  @Test
  public void testPerformance() throws Exception {
    MockAnalyzer analyzer = new MockAnalyzer(random());
    int numDocs = atLeast(10);
    LeafReader leafReader = getRandomIndex(analyzer, numDocs);
    try {
      BooleanPerceptronClassifier classifier = new BooleanPerceptronClassifier(leafReader, analyzer, null, 1, null, booleanFieldName, textFieldName);

      ConfusionMatrixGenerator.ConfusionMatrix confusionMatrix = ConfusionMatrixGenerator.getConfusionMatrix(leafReader,
          classifier, booleanFieldName, textFieldName, -1);
      assertNotNull(confusionMatrix);

      double avgClassificationTime = confusionMatrix.getAvgClassificationTime();
      assertTrue(avgClassificationTime >= 0);

      double f1 = confusionMatrix.getF1Measure();
      assertTrue(f1 >= 0d);
      assertTrue(f1 <= 1d);

      double accuracy = confusionMatrix.getAccuracy();
      assertTrue(accuracy >= 0d);
      assertTrue(accuracy <= 1d);

      double recall = confusionMatrix.getRecall();
      assertTrue(recall >= 0d);
      assertTrue(recall <= 1d);

      double precision = confusionMatrix.getPrecision();
      assertTrue(precision >= 0d);
      assertTrue(precision <= 1d);

      Terms terms = MultiTerms.getTerms(leafReader, booleanFieldName);
      TermsEnum iterator = terms.iterator();
      BytesRef term;
      while ((term = iterator.next()) != null) {
        String s = term.utf8ToString();
        recall = confusionMatrix.getRecall(s);
        assertTrue(recall >= 0d);
        assertTrue(recall <= 1d);
        precision = confusionMatrix.getPrecision(s);
        assertTrue(precision >= 0d);
        assertTrue(precision <= 1d);
        double f1Measure = confusionMatrix.getF1Measure(s);
        assertTrue(f1Measure >= 0d);
        assertTrue(f1Measure <= 1d);
      }
    } finally {
      leafReader.close();
    }
  }

}
