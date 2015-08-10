package org.apache.lucene.classification.utils;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.classification.BooleanPerceptronClassifier;
import org.apache.lucene.classification.CachingNaiveBayesClassifier;
import org.apache.lucene.classification.ClassificationTestBase;
import org.apache.lucene.classification.Classifier;
import org.apache.lucene.classification.KNearestNeighborClassifier;
import org.apache.lucene.classification.SimpleNaiveBayesClassifier;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

/**
 * Tests for {@link ConfusionMatrixGenerator}
 */
public class ConfusionMatrixGeneratorTest extends ClassificationTestBase {

  @Test
  public void testGetConfusionMatrixWithSNB() throws Exception {
    LeafReader reader = null;
    try {
      MockAnalyzer analyzer = new MockAnalyzer(random());
      reader = populateSampleIndex(analyzer);
      Classifier<BytesRef> classifier = new SimpleNaiveBayesClassifier(reader, analyzer, null, categoryFieldName, textFieldName);
      ConfusionMatrixGenerator.ConfusionMatrix confusionMatrix = ConfusionMatrixGenerator.getConfusionMatrix(reader, classifier, categoryFieldName, textFieldName);
      assertNotNull(confusionMatrix);
      assertNotNull(confusionMatrix.getLinearizedMatrix());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Test
  public void testGetConfusionMatrixWithCNB() throws Exception {
    LeafReader reader = null;
    try {
      MockAnalyzer analyzer = new MockAnalyzer(random());
      reader = populateSampleIndex(analyzer);
      Classifier<BytesRef> classifier = new CachingNaiveBayesClassifier(reader, analyzer, null, categoryFieldName, textFieldName);
      ConfusionMatrixGenerator.ConfusionMatrix confusionMatrix = ConfusionMatrixGenerator.getConfusionMatrix(reader, classifier, categoryFieldName, textFieldName);
      assertNotNull(confusionMatrix);
      assertNotNull(confusionMatrix.getLinearizedMatrix());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Test
  public void testGetConfusionMatrixWithKNN() throws Exception {
    LeafReader reader = null;
    try {
      MockAnalyzer analyzer = new MockAnalyzer(random());
      reader = populateSampleIndex(analyzer);
      Classifier<BytesRef> classifier = new KNearestNeighborClassifier(reader, null, analyzer, null, 1, 0, 0, categoryFieldName, textFieldName);
      ConfusionMatrixGenerator.ConfusionMatrix confusionMatrix = ConfusionMatrixGenerator.getConfusionMatrix(reader, classifier, categoryFieldName, textFieldName);
      assertNotNull(confusionMatrix);
      assertNotNull(confusionMatrix.getLinearizedMatrix());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Test
  public void testGetConfusionMatrixWithBP() throws Exception {
    LeafReader reader = null;
    try {
      MockAnalyzer analyzer = new MockAnalyzer(random());
      reader = populateSampleIndex(analyzer);
      Classifier<Boolean> classifier = new BooleanPerceptronClassifier(reader, analyzer, null, 1, null, booleanFieldName, textFieldName);
      ConfusionMatrixGenerator.ConfusionMatrix confusionMatrix = ConfusionMatrixGenerator.getConfusionMatrix(reader, classifier, booleanFieldName, textFieldName);
      assertNotNull(confusionMatrix);
      assertNotNull(confusionMatrix.getLinearizedMatrix());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
}