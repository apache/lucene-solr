package org.apache.lucene.search.spans;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.Query;

/*******************************************************************************
 * Some expanded tests to make sure my patch doesn't break other SpanTermQuery
 * functionality.
 *
 * @author Reece Wilton
 */
public class TestSpansAdvanced2 extends TestSpansAdvanced {

    /**
     * Initializes the tests by adding documents to the index.
     */
    protected void setUp() throws Exception {
        super.setUp();

        // create test index
        final IndexWriter writer = new IndexWriter(mDirectory, new StandardAnalyzer(), false);
        addDocument(writer, "A", "Should we, could we, would we?");
        addDocument(writer, "B", "It should.  Should it?");
        addDocument(writer, "C", "It shouldn't.");
        addDocument(writer, "D", "Should we, should we, should we.");
        writer.close();
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
        final Hits hits = executeQuery(spanQuery);
        final String[] expectedIds = new String[] { "B", "D", "1", "2", "3", "4", "A" };
        final float[] expectedScores = new float[] { 0.625f, 0.45927936f, 0.35355338f, 0.35355338f, 0.35355338f,
                0.35355338f, 0.26516503f, };
        assertHits(hits, "single span query", expectedIds, expectedScores);
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
        final Hits hits = executeQuery(query);
        final String[] expectedIds = new String[] { "A", "D" };
        final float[] expectedScores = new float[] { 0.93163157f, 0.20698164f };
        assertHits(hits, "multiple different span queries", expectedIds, expectedScores);
    }

    /**
     * Tests two span queries.
     *
     * ERROR: Lucene returns the incorrect number of results and the scoring for
     * the results is incorrect.
     *
     * @throws IOException
     */
    public void testBooleanQueryWithSpanQueries() throws IOException {

        doTestBooleanQueryWithSpanQueries(0.73500174f);
    }
}
