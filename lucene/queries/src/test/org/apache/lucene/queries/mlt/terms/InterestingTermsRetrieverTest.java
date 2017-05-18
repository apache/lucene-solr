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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThisParameters;
import org.apache.lucene.queries.mlt.MoreLikeThisTestBase;
import org.apache.lucene.queries.mlt.terms.scorer.ScoredTerm;
import org.apache.lucene.util.PriorityQueue;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;


public class InterestingTermsRetrieverTest extends MoreLikeThisTestBase {

  private InterestingTermsRetriever toTest;

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
    initIndexWithSingleFieldDocuments();
    toTest = new MockInterestingTermsRetriever(reader);
    toTest.setParameters(params);
    DocumentTermFrequencies sampleTermFrequencies = initTermFrequencies(numDocs);

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveInterestingTerms(sampleTermFrequencies);

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
    initIndex();
    toTest = new MockInterestingTermsRetriever(reader);
    toTest.setParameters(params);

    DocumentTermFrequencies sampleTermFrequencies = initTermFrequencies(numDocs);
    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveInterestingTerms(sampleTermFrequencies);

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
    toTest = new MockInterestingTermsRetriever(reader);
    toTest.setParameters(params);
    int expectedScoredTermsSize = termsCountPerField - minTermFreq + 1;
    DocumentTermFrequencies sampleTermFrequencies = initTermFrequencies(termsCountPerField);

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveInterestingTerms(sampleTermFrequencies);

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
    int minTermFreq = 6;
    MoreLikeThisParameters params = getDefaultParams();
    params.setFieldNames(new String[]{FIELD1, FIELD2});
    params.setMinTermFreq(minTermFreq);
    //Test preparation
    initIndex();
    int termsCountPerField = 10;
    toTest = new MockInterestingTermsRetriever(reader);
    toTest.setParameters(params);
    int perFieldExpectedScoredTermsSize = termsCountPerField - minTermFreq + 1;
    int expectedScoredTermsSize = params.getFieldNames().length * perFieldExpectedScoredTermsSize;
    DocumentTermFrequencies sampleTermFrequencies = initTermFrequencies(termsCountPerField);

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveInterestingTerms(sampleTermFrequencies);

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
    initIndexWithSingleFieldDocuments();
    toTest = new MockInterestingTermsRetriever(reader);
    toTest.setParameters(params);
    int expectedScoredTermsSize = 10;
    DocumentTermFrequencies sampleTermFrequencies = initTermFrequencies(expectedScoredTermsSize + 1);

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveInterestingTerms(sampleTermFrequencies);

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
    initIndex();
    toTest = new MockInterestingTermsRetriever(reader);
    toTest.setParameters(params);
    int expectedScoredTermsSize = 10;
    DocumentTermFrequencies sampleTermFrequencies = initTermFrequencies(expectedScoredTermsSize);

    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveInterestingTerms(sampleTermFrequencies);

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
    initIndexWithSingleFieldDocuments();
    toTest = new MockInterestingTermsRetriever(reader);
    toTest.setParameters(params);
    int expectedScoredTermsSize = maxDocFreq;

    DocumentTermFrequencies sampleTermFrequencies = initTermFrequencies(numDocs);
    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveInterestingTerms(sampleTermFrequencies);

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
    initIndex();
    toTest = new MockInterestingTermsRetriever(reader);
    toTest.setParameters(params);
    int expectedScoredTermsSize = params.getFieldNames().length * maxDocFreq;

    DocumentTermFrequencies sampleTermFrequencies = initTermFrequencies(numDocs);
    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveInterestingTerms(sampleTermFrequencies);

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
  public void multiFieldDoc_field1Boosted_shouldConsiderMoreTermsFromField1() throws Exception {
    //More Like This parameters definition
    int topK = 26;
    MoreLikeThisParameters params = getDefaultParams();
    params.setMaxQueryTerms(topK);
    params.setFieldNames(new String[]{FIELD1, FIELD2});
    Map<String, Float> testBoostFactor = new HashMap<>();
    testBoostFactor.put(FIELD2, 2.0f);
    params.setFieldToQueryTimeBoostFactor(testBoostFactor);
    //Test preparation
    initIndex();
    toTest = new MockInterestingTermsRetriever(reader);
    toTest.setParameters(params);

    DocumentTermFrequencies sampleTermFrequencies = initTermFrequencies(numDocs);
    PriorityQueue<ScoredTerm> scoredTerms = toTest.retrieveInterestingTerms(sampleTermFrequencies);

    assertEquals("Expected " + topK + " terms only!", topK, scoredTerms.size());
    int countField1 = 0;
    int countField2 = 0;
    for (ScoredTerm term : scoredTerms) {
      if (term.field.equals(FIELD1)) {
        countField1++;
      } else if (term.field.equals(FIELD2)) {
        countField2++;
      }
    }

    assertThat(countField1 < countField2, is(true));
  }

  /**
   * Init a {@link DocumentTermFrequencies} structure with 2 fields.
   * Each field will have terms of linearly increasing frequency.
   * <p>
   * 1a - tf=1
   * 2a - tf=2
   * ...
   * na - tf=n
   *
   * @param numTermsPerField
   * @return
   */
  private DocumentTermFrequencies initTermFrequencies(int numTermsPerField) {
    DocumentTermFrequencies frequencies = new DocumentTermFrequencies();

    for (int i = 1; i <= numTermsPerField; i++) {
      frequencies.increment(FIELD1, i + "a", i);
    }
    for (int i = 1; i <= numTermsPerField; i++) {
      frequencies.increment(FIELD2, i + "b", i);
    }

    return frequencies;
  }

  private class MockInterestingTermsRetriever extends InterestingTermsRetriever {
    public MockInterestingTermsRetriever(IndexReader ir) {
      this.ir = ir;
    }
  }
}
