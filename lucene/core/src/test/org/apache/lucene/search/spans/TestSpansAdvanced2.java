package org.apache.lucene.search.spans;

/**
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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.DefaultSimilarity;

/*******************************************************************************
 * Some expanded tests to make sure my patch doesn't break other SpanTermQuery
 * functionality.
 * 
 */
public class TestSpansAdvanced2 extends TestSpansAdvanced {
  IndexSearcher searcher2;
  IndexReader reader2;
  
  /**
   * Initializes the tests by adding documents to the index.
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    // create test index
    final RandomIndexWriter writer = new RandomIndexWriter(random(), mDirectory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(),
            MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true))
            .setOpenMode(OpenMode.APPEND).setMergePolicy(newLogMergePolicy())
            .setSimilarity(new DefaultSimilarity()));
    addDocument(writer, "A", "Should we, could we, would we?");
    addDocument(writer, "B", "It should.  Should it?");
    addDocument(writer, "C", "It shouldn't.");
    addDocument(writer, "D", "Should we, should we, should we.");
    reader2 = writer.getReader();
    writer.close();
    
    // re-open the searcher since we added more docs
    searcher2 = newSearcher(reader2);
    searcher2.setSimilarity(new DefaultSimilarity());
  }
  
  @Override
  public void tearDown() throws Exception {
    reader2.close();
    super.tearDown();
  }
  
  /**
   * Verifies that the index has the correct number of documents.
   * 
   * @throws Exception
   */
  public void testVerifyIndex() throws Exception {
    final IndexReader reader = IndexReader.open(mDirectory);
    assertEquals(8, reader.numDocs());
    reader.close();
  }
  
  /**
   * Tests a single span query that matches multiple documents.
   * 
   * @throws IOException
   */
  public void testSingleSpanQuery() throws IOException {
    
    final Query spanQuery = new SpanTermQuery(new Term(FIELD_TEXT, "should"));
    final String[] expectedIds = new String[] {"B", "D", "1", "2", "3", "4",
        "A"};
    final float[] expectedScores = new float[] {0.625f, 0.45927936f,
        0.35355338f, 0.35355338f, 0.35355338f, 0.35355338f, 0.26516503f,};
    assertHits(searcher2, spanQuery, "single span query", expectedIds,
        expectedScores);
  }
  
  /**
   * Tests a single span query that matches multiple documents.
   * 
   * @throws IOException
   */
  public void testMultipleDifferentSpanQueries() throws IOException {
    
    final Query spanQuery1 = new SpanTermQuery(new Term(FIELD_TEXT, "should"));
    final Query spanQuery2 = new SpanTermQuery(new Term(FIELD_TEXT, "we"));
    final BooleanQuery query = new BooleanQuery();
    query.add(spanQuery1, BooleanClause.Occur.MUST);
    query.add(spanQuery2, BooleanClause.Occur.MUST);
    final String[] expectedIds = new String[] {"D", "A"};
    // these values were pre LUCENE-413
    // final float[] expectedScores = new float[] { 0.93163157f, 0.20698164f };
    final float[] expectedScores = new float[] {1.0191123f, 0.93163157f};
    assertHits(searcher2, query, "multiple different span queries",
        expectedIds, expectedScores);
  }
  
  /**
   * Tests two span queries.
   * 
   * @throws IOException
   */
  @Override
  public void testBooleanQueryWithSpanQueries() throws IOException {
    
    doTestBooleanQueryWithSpanQueries(searcher2, 0.73500174f);
  }
}
