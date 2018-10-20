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


import org.apache.lucene.util.BytesRef;
import org.junit.Test;

/**
 * Tests for {@link org.apache.lucene.classification.SimpleNaiveBayesClassifier}
 */
public class SimpleNaiveBayesDocumentClassifierTest extends DocumentClassificationTestBase<BytesRef> {

  @Test
  public void testBasicDocumentClassification() throws Exception {
    try {
      checkCorrectDocumentClassification(new SimpleNaiveBayesDocumentClassifier(indexReader, null, categoryFieldName,field2analyzer, new String[]{textFieldName, titleFieldName, authorFieldName}), getVideoGameDocument(), VIDEOGAME_ANALYZED_RESULT);
      checkCorrectDocumentClassification(new SimpleNaiveBayesDocumentClassifier(indexReader, null, categoryFieldName,field2analyzer,  new String[]{textFieldName, titleFieldName, authorFieldName}), getBatmanDocument(), BATMAN_RESULT);

      checkCorrectDocumentClassification(new SimpleNaiveBayesDocumentClassifier(indexReader, null, categoryFieldName,field2analyzer, new String[]{textFieldName}), getVideoGameDocument(), BATMAN_RESULT);
      checkCorrectDocumentClassification(new SimpleNaiveBayesDocumentClassifier(indexReader, null, categoryFieldName,field2analyzer,  new String[]{textFieldName}), getBatmanDocument(), VIDEOGAME_ANALYZED_RESULT);
    } finally {
      if (indexReader != null) {
        indexReader.close();
      }
    }
  }

  @Test
  public void testBasicDocumentClassificationScore() throws Exception {
    try {
      double score1 = checkCorrectDocumentClassification(new SimpleNaiveBayesDocumentClassifier(indexReader, null, categoryFieldName, field2analyzer, new String[]{textFieldName, titleFieldName, authorFieldName}), getVideoGameDocument(), VIDEOGAME_ANALYZED_RESULT);
      assertEquals(0.88,score1,0.01);
      double score2 = checkCorrectDocumentClassification(new SimpleNaiveBayesDocumentClassifier(indexReader, null, categoryFieldName,field2analyzer,  new String[]{textFieldName, titleFieldName, authorFieldName}), getBatmanDocument(), BATMAN_RESULT);
      assertEquals(0.89,score2,0.01);
      //taking in consideration only the text
      double score3 = checkCorrectDocumentClassification(new SimpleNaiveBayesDocumentClassifier(indexReader, null, categoryFieldName,field2analyzer, new String[]{textFieldName}), getVideoGameDocument(), BATMAN_RESULT);
      assertEquals(0.55,score3,0.01);
      double score4 = checkCorrectDocumentClassification(new SimpleNaiveBayesDocumentClassifier(indexReader, null, categoryFieldName,field2analyzer,  new String[]{textFieldName}), getBatmanDocument(), VIDEOGAME_ANALYZED_RESULT);
      assertEquals(0.52,score4,0.01);
    } finally {
      if (indexReader != null) {
        indexReader.close();
      }
    }
  }

  @Test
  public void testBoostedDocumentClassification() throws Exception {
    try {
      checkCorrectDocumentClassification(new SimpleNaiveBayesDocumentClassifier(indexReader, null, categoryFieldName,field2analyzer, new String[]{textFieldName, titleFieldName+"^100", authorFieldName}), getBatmanAmbiguosDocument(), BATMAN_RESULT);
      // considering without boost wrong classification will appear
      checkCorrectDocumentClassification(new SimpleNaiveBayesDocumentClassifier(indexReader, null, categoryFieldName,field2analyzer, new String[]{textFieldName, titleFieldName, authorFieldName}), getBatmanAmbiguosDocument(), VIDEOGAME_ANALYZED_RESULT);
    } finally {
      if (indexReader != null) {
        indexReader.close();
      }
    }
  }


}
