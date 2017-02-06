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

package org.apache.lucene.queries.mlt.terms;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThisParameters;
import org.apache.lucene.queries.mlt.MoreLikeThisTestBase;
import org.apache.lucene.queries.mlt.terms.scorer.ScoredTerm;
import org.apache.lucene.util.PriorityQueue;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;

public class LocalDocumentTermsRetrieverTest extends MoreLikeThisTestBase {
  private LocalDocumentTermsRetriever toTest;

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void singleFieldDoc_KQueryTerms_shouldReturnTopKTerms() throws Exception {
    //More Like This parameters definition
    int topK = 26;
    MoreLikeThisParameters params = getDefaultParams();
    params.setMaxQueryTerms(topK);
    //Test preparation
    int lastDocId = initIndexWithSingleFieldDocuments();
    toTest = new LocalDocumentTermsRetriever(reader);
    toTest.setParameters(params);

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveTermsFromLocalDocument(lastDocId);

    assertEquals("Expected " + topK + " terms only!", topK, scoredTerms.size());
    //Expected terms preparation
    Term[] expectedTerms = new Term[topK];
    int idx = 0;
    for (String text : getArithmeticSeriesWithSuffix(numDocs - topK + 1, topK, "a")) {
      expectedTerms[idx++] = new Term(FIELD1, text);
    }
    //Expected terms assertions
    assertScoredTermsPriorityOrder(scoredTerms, expectedTerms);
  }

  @Test
  public void multiFieldDoc_KQueryTerms_shouldReturnTopKTerms() throws Exception {
    //More Like This parameters definition
    int topK = 26;
    MoreLikeThisParameters params = getDefaultParams();
    params.setMaxQueryTerms(topK);
    params.setFieldNames(new String[]{FIELD1, FIELD2});
    //Test preparation
    int lastDocId = initIndex();
    toTest = new LocalDocumentTermsRetriever(reader);
    toTest.setParameters(params);

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveTermsFromLocalDocument(lastDocId);

    assertEquals("Expected " + topK + " terms only!", topK, scoredTerms.size());
    //Expected terms preparation
    int perFieldTermsSize = (topK / params.getFieldNames().length);
    Term[] expectedField1Terms = new Term[perFieldTermsSize];
    int idx = 0;
    for (String text : getArithmeticSeriesWithSuffix(numDocs - perFieldTermsSize + 1, perFieldTermsSize, "a")) {
      expectedField1Terms[idx++] = new Term(FIELD1, text);
    }
    Term[] expectedField2Terms = new Term[perFieldTermsSize];
    idx = 0;
    for (String text : getArithmeticSeriesWithSuffix(numDocs - perFieldTermsSize + 1, perFieldTermsSize, "b")) {
      expectedField2Terms[idx++] = new Term(FIELD2, text);
    }
    //Expected terms assertions
    assertScoredTermsPriorityOrder(scoredTerms, expectedField1Terms, expectedField2Terms);
  }

  @Test
  public void singleFieldDoc_minTermFreq_shouldIgnoreTermsLessFrequent() throws Exception {
    //More Like This parameters definition
    int minTermFreq = 5;
    MoreLikeThisParameters params = getDefaultParams();
    params.setMinTermFreq(minTermFreq);
    //Test preparation
    initIndexWithSingleFieldDocuments();
    int termsCountPerField = 10;
    int testDocId = indexDocumentWithLinearTermFrequencies(termsCountPerField);
    toTest = new LocalDocumentTermsRetriever(reader);
    toTest.setParameters(params);
    int expectedScoredTermsSize = termsCountPerField - minTermFreq + 1;

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveTermsFromLocalDocument(testDocId);

    assertEquals("Expected " + expectedScoredTermsSize + " terms only!", expectedScoredTermsSize, scoredTerms.size());
    //Expected terms preparation
    Term[] expectedTerms = new Term[expectedScoredTermsSize];
    int idx = 0;
    for (String text : getArithmeticSeriesWithSuffix(minTermFreq, expectedScoredTermsSize, "a")) {
      expectedTerms[idx++] = new Term(FIELD1, text);
    }
    //Expected terms assertions
    assertScoredTermsPriorityOrder(scoredTerms, expectedTerms);
  }

  @Test
  public void multiFieldDoc_minTermFreq_shouldIgnoreTermsLessFrequent() throws Exception {
    //More Like This parameters definition
    int minTermFreq = 5;
    MoreLikeThisParameters params = getDefaultParams();
    params.setFieldNames(new String[]{FIELD1, FIELD2});
    params.setMinTermFreq(minTermFreq);
    //Test preparation
    initIndex();
    int termsCountPerField = 10;
    int testDocId = indexDocumentWithLinearTermFrequencies(termsCountPerField);
    toTest = new LocalDocumentTermsRetriever(reader);
    toTest.setParameters(params);
    int perFieldExpectedScoredTermsSize = termsCountPerField - minTermFreq + 1;
    int expectedScoredTermsSize = params.getFieldNames().length * perFieldExpectedScoredTermsSize;

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveTermsFromLocalDocument(testDocId);

    assertEquals("Expected " + expectedScoredTermsSize + " terms only!", expectedScoredTermsSize, scoredTerms.size());
    //Expected terms preparation
    Term[] expectedField1Terms = new Term[perFieldExpectedScoredTermsSize];
    int idx = 0;
    for (String text : getArithmeticSeriesWithSuffix(minTermFreq, perFieldExpectedScoredTermsSize, "a")) {
      expectedField1Terms[idx++] = new Term(FIELD1, text);
    }
    Term[] expectedField2Terms = new Term[perFieldExpectedScoredTermsSize];
    idx = 0;
    for (String text : getArithmeticSeriesWithSuffix(minTermFreq, perFieldExpectedScoredTermsSize, "b")) {
      expectedField2Terms[idx++] = new Term(FIELD2, text);
    }
    //Expected terms assertions
    assertScoredTermsPriorityOrder(scoredTerms, expectedField1Terms, expectedField2Terms);
  }

  @Test
  public void singleFieldDoc_minDocFreq_shouldIgnoreTermsLessFrequent() throws Exception {
    //More Like This parameters definition
    int minDocFreq = 91;
    MoreLikeThisParameters params = getDefaultParams();
    params.setMinDocFreq(minDocFreq);
    //Test preparation
    int lastDocId = initIndexWithSingleFieldDocuments();
    toTest = new LocalDocumentTermsRetriever(reader);
    toTest.setParameters(params);
    int expectedScoredTermsSize = 10;

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveTermsFromLocalDocument(lastDocId);

    assertEquals("Expected " + expectedScoredTermsSize + " terms only!", expectedScoredTermsSize, scoredTerms.size());
    //Expected terms preparation
    Term[] expectedTerms = new Term[expectedScoredTermsSize];
    int idx = 0;
    for (String text : getArithmeticSeriesWithSuffix(1, expectedScoredTermsSize, "a")) {
      expectedTerms[idx++] = new Term(FIELD1, text);
    }
    //Expected terms assertions
    assertScoredTermsPriorityOrder(scoredTerms, expectedTerms);
  }

  @Test
  public void multiFieldDoc_minDocFreq_shouldIgnoreTermsLessFrequent() throws Exception {
    //More Like This parameters definition
    int minDocFreq = 96;
    MoreLikeThisParameters params = getDefaultParams();
    params.setMinDocFreq(minDocFreq);
    params.setFieldNames(new String[]{FIELD1, FIELD2});
    //Test preparation
    int lastDocId = initIndex();
    toTest = new LocalDocumentTermsRetriever(reader);
    toTest.setParameters(params);
    int expectedScoredTermsSize = 10;

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveTermsFromLocalDocument(lastDocId);

    assertEquals("Expected " + expectedScoredTermsSize + " terms only!", expectedScoredTermsSize, scoredTerms.size());
    //Expected terms preparation
    int perFieldTermsSize = (expectedScoredTermsSize / params.getFieldNames().length);
    Term[] expectedField1Terms = new Term[perFieldTermsSize];
    int idx = 0;
    for (String text : getArithmeticSeriesWithSuffix(1, perFieldTermsSize, "a")) {
      expectedField1Terms[idx++] = new Term(FIELD1, text);
    }
    Term[] expectedField2Terms = new Term[perFieldTermsSize];
    idx = 0;
    for (String text : getArithmeticSeriesWithSuffix(1, perFieldTermsSize, "b")) {
      expectedField2Terms[idx++] = new Term(FIELD2, text);
    }
    //Expected terms assertions
    assertScoredTermsPriorityOrder(scoredTerms, expectedField1Terms, expectedField2Terms);
  }

  @Test
  public void singleFieldDoc_maxDocFreq_shouldIgnoreTermsTooFrequent() throws Exception {
    //More Like This parameters definition
    int maxDocFreq = 10;
    MoreLikeThisParameters params = getDefaultParams();
    params.setMaxDocFreq(maxDocFreq);
    //Test preparation
    int lastDocId = initIndexWithSingleFieldDocuments();
    toTest = new LocalDocumentTermsRetriever(reader);
    toTest.setParameters(params);
    int expectedScoredTermsSize = maxDocFreq;

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveTermsFromLocalDocument(lastDocId);

    assertEquals("Expected " + expectedScoredTermsSize + " terms only!", expectedScoredTermsSize, scoredTerms.size());
    //Expected terms preparation
    Term[] expectedTerms = new Term[expectedScoredTermsSize];
    int idx = 0;
    for (String text : getArithmeticSeriesWithSuffix(numDocs - expectedScoredTermsSize + 1, expectedScoredTermsSize, "a")) {
      expectedTerms[idx++] = new Term(FIELD1, text);
    }
    //Expected terms assertions
    assertScoredTermsPriorityOrder(scoredTerms, expectedTerms);
  }

  @Test
  public void multiFieldDoc_maxDocFreq_shouldIgnoreTermsTooFrequent() throws Exception {
    //More Like This parameters definition
    int maxDocFreq = 5;
    MoreLikeThisParameters params = getDefaultParams();
    params.setMaxDocFreq(maxDocFreq);
    params.setFieldNames(new String[]{FIELD1, FIELD2});
    //Test preparation
    int lastDocId = initIndex();
    toTest = new LocalDocumentTermsRetriever(reader);
    toTest.setParameters(params);
    int expectedScoredTermsSize = params.getFieldNames().length * maxDocFreq;

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveTermsFromLocalDocument(lastDocId);

    assertEquals("Expected " + expectedScoredTermsSize + " terms only!", expectedScoredTermsSize, scoredTerms.size());
    //Expected terms preparation
    int perFieldTermsSize = (expectedScoredTermsSize / params.getFieldNames().length);
    Term[] expectedField1Terms = new Term[perFieldTermsSize];
    int idx = 0;
    for (String text : getArithmeticSeriesWithSuffix(96, perFieldTermsSize, "a")) {
      expectedField1Terms[idx++] = new Term(FIELD1, text);
    }
    Term[] expectedField2Terms = new Term[perFieldTermsSize];
    idx = 0;
    for (String text : getArithmeticSeriesWithSuffix(96, perFieldTermsSize, "b")) {
      expectedField2Terms[idx++] = new Term(FIELD2, text);
    }
    //Expected terms assertions
    assertScoredTermsPriorityOrder(scoredTerms, expectedField1Terms, expectedField2Terms);
  }

  @Test
  public void multiFieldDoc_onlyField1Configured_shouldConsiderOnlyTermsFromField1() throws Exception {
    //More Like This parameters definition
    int topK = 26;
    MoreLikeThisParameters params = getDefaultParams();
    params.setMaxQueryTerms(topK);
    params.setFieldNames(new String[]{FIELD1});
    Map<String, Float> testBoostFactor = new HashMap<>();
    testBoostFactor.put(FIELD2, 2.0f);
    params.setFieldToQueryTimeBoostFactor(testBoostFactor);
    //Test preparation
    int lastDocId = initIndex();
    toTest = new LocalDocumentTermsRetriever(reader);
    toTest.setParameters(params);

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveTermsFromLocalDocument(lastDocId);

    assertEquals("Expected " + topK + " terms only!", topK, scoredTerms.size());
    int countField2 = 0;
    for (ScoredTerm term : scoredTerms) {
      if (term.field.equals(FIELD2)) {
        countField2++;
      }
    }

    assertThat(countField2, is(0));
  }

  /**
   * This method will index in the index a seed document.
   * The seed document will have 2 multi valued fields.
   * Each field will have terms of linearly increasing term frequency :
   * 1a - tf=1
   * 2a - tf=2
   * 3a - tf=3
   * ...
   *
   * @throws IOException
   */
  private int indexDocumentWithLinearTermFrequencies(int numTermsPerField) throws IOException {
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    writer.numDocs();

    Document doc = new Document();
    for (String value1 : getTriangularArithmeticSeriesWithSuffix(1, numTermsPerField, "a")) {
      doc.add(newTextField(FIELD1, value1, Field.Store.YES));
    }
    for (String value2 : getTriangularArithmeticSeriesWithSuffix(1, numTermsPerField, "b")) {
      doc.add(newTextField(FIELD2, value2, Field.Store.YES));
    }

    writer.addDocument(doc);
    int docId = writer.numDocs() - 1;
    reader.close();
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
    return docId;
  }
}


