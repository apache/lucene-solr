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


import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

/**
 * Tests for {@link org.apache.lucene.classification.KNearestNeighborClassifier}
 */
public class KNearestNeighborDocumentClassifierTest extends DocumentClassificationTestBase<BytesRef> {

  @Test
  public void testBasicDocumentClassification() throws Exception {
    try {
      Document videoGameDocument = getVideoGameDocument();
      Document batmanDocument = getBatmanDocument();
      checkCorrectDocumentClassification(new KNearestNeighborDocumentClassifier(indexReader,null, null, 1, 1, 1, categoryFieldName, field2analyzer, new String[]{textFieldName, titleFieldName, authorFieldName}), videoGameDocument, VIDEOGAME_RESULT);
      checkCorrectDocumentClassification(new KNearestNeighborDocumentClassifier(indexReader,null, null, 1, 1, 1, categoryFieldName, field2analyzer, new String[]{textFieldName, titleFieldName, authorFieldName}), batmanDocument, BATMAN_RESULT);
      // considering only the text we have wrong classification because the text was ambiguos on purpose
      checkCorrectDocumentClassification(new KNearestNeighborDocumentClassifier(indexReader,null, null, 1, 1, 1, categoryFieldName, field2analyzer, new String[]{textFieldName}), videoGameDocument, BATMAN_RESULT);
      checkCorrectDocumentClassification(new KNearestNeighborDocumentClassifier(indexReader,null, null, 1, 1, 1, categoryFieldName, field2analyzer, new String[]{textFieldName}), batmanDocument, VIDEOGAME_RESULT);

    } finally {
      if (indexReader != null) {
        indexReader.close();
      }
    }
  }

  @Test
  public void testBasicDocumentClassificationScore() throws Exception {
    try {
      Document videoGameDocument = getVideoGameDocument();
      Document batmanDocument = getBatmanDocument();
      double score1 = checkCorrectDocumentClassification(new KNearestNeighborDocumentClassifier(indexReader,null, null, 1, 1, 1, categoryFieldName, field2analyzer, new String[]{textFieldName, titleFieldName, authorFieldName}), videoGameDocument, VIDEOGAME_RESULT);
      assertEquals(1.0,score1,0);
      double score2 = checkCorrectDocumentClassification(new KNearestNeighborDocumentClassifier(indexReader,null, null, 1, 1, 1, categoryFieldName, field2analyzer, new String[]{textFieldName, titleFieldName, authorFieldName}), batmanDocument, BATMAN_RESULT);
      assertEquals(1.0,score2,0);
      // considering only the text we have wrong classification because the text was ambiguos on purpose
      double score3 = checkCorrectDocumentClassification(new KNearestNeighborDocumentClassifier(indexReader,null, null, 1, 1, 1, categoryFieldName, field2analyzer, new String[]{textFieldName}), videoGameDocument, BATMAN_RESULT);
      assertEquals(1.0,score3,0);
      double score4 = checkCorrectDocumentClassification(new KNearestNeighborDocumentClassifier(indexReader,null, null, 1, 1, 1, categoryFieldName, field2analyzer, new String[]{textFieldName}), batmanDocument, VIDEOGAME_RESULT);
      assertEquals(1.0,score4,0);
    } finally {
      if (indexReader != null) {
        indexReader.close();
      }
    }
  }

  @Test
  public void testBoostedDocumentClassification() throws Exception {
    try {
      checkCorrectDocumentClassification(new KNearestNeighborDocumentClassifier(indexReader,null, null, 1, 1, 1, categoryFieldName, field2analyzer, new String[]{textFieldName, titleFieldName + "^100", authorFieldName}), getBatmanAmbiguosDocument(), BATMAN_RESULT);
      // considering without boost wrong classification will appear
      checkCorrectDocumentClassification(new KNearestNeighborDocumentClassifier(indexReader,null, null, 1, 1, 1, categoryFieldName, field2analyzer, new String[]{textFieldName, titleFieldName, authorFieldName}), getBatmanAmbiguosDocument(), VIDEOGAME_RESULT);
    } finally {
      if (indexReader != null) {
        indexReader.close();
      }
    }
  }

  @Test
  public void testBasicDocumentClassificationWithQuery() throws Exception {
    try {
      TermQuery query = new TermQuery(new Term(authorFieldName, "ign"));
      checkCorrectDocumentClassification(new KNearestNeighborDocumentClassifier(indexReader,null, query, 1, 1, 1, categoryFieldName, field2analyzer, new String[]{textFieldName, titleFieldName, authorFieldName}), getVideoGameDocument(), VIDEOGAME_RESULT);
      checkCorrectDocumentClassification(new KNearestNeighborDocumentClassifier(indexReader,null,query, 1, 1, 1, categoryFieldName, field2analyzer, new String[]{textFieldName, titleFieldName, authorFieldName}), getBatmanDocument(), VIDEOGAME_RESULT);
    } finally {
      if (indexReader != null) {
        indexReader.close();
      }
    }
  }

}
