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

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/*******************************************************************************
 * Tests the span query bug in Lucene. It demonstrates that SpanTermQuerys don't
 * work correctly in a BooleanQuery.
 *
 * @author Reece Wilton
 */
public class TestSpansAdvanced extends TestCase {

    // location to the index
    protected Directory mDirectory;;

    // field names in the index
    private final static String FIELD_ID = "ID";
    protected final static String FIELD_TEXT = "TEXT";

    /**
     * Initializes the tests by adding 4 identical documents to the index.
     */
    protected void setUp() throws Exception {

        super.setUp();

        // create test index
        mDirectory = new RAMDirectory();
        final IndexWriter writer = new IndexWriter(mDirectory, new StandardAnalyzer(), true);
        addDocument(writer, "1", "I think it should work.");
        addDocument(writer, "2", "I think it should work.");
        addDocument(writer, "3", "I think it should work.");
        addDocument(writer, "4", "I think it should work.");
        writer.close();
    }

    protected void tearDown() throws Exception {

        mDirectory.close();
        mDirectory = null;
    }

    /**
     * Adds the document to the index.
     *
     * @param writer the Lucene index writer
     * @param id the unique id of the document
     * @param text the text of the document
     * @throws IOException
     */
    protected void addDocument(final IndexWriter writer, final String id, final String text) throws IOException {

        final Document document = new Document();
        document.add(new Field(FIELD_ID, id, Field.Store.YES, Field.Index.UN_TOKENIZED));
        document.add(new Field(FIELD_TEXT, text, Field.Store.YES, Field.Index.TOKENIZED));
        writer.addDocument(document);
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

        doTestBooleanQueryWithSpanQueries(0.3884282f);
    }

    /**
     * Tests two span queries.
     *
     * ERROR: Lucene returns the incorrect number of results and the scoring for
     * the results is incorrect.
     *
     * @throws IOException
     */
    protected void doTestBooleanQueryWithSpanQueries(final float expectedScore) throws IOException {

        final Query spanQuery = new SpanTermQuery(new Term(FIELD_TEXT, "work"));
        final BooleanQuery query = new BooleanQuery();
        query.add(spanQuery, BooleanClause.Occur.MUST);
        query.add(spanQuery, BooleanClause.Occur.MUST);
        final Hits hits = executeQuery(query);
        final String[] expectedIds = new String[] { "1", "2", "3", "4" };
        final float[] expectedScores = new float[] { expectedScore, expectedScore, expectedScore, expectedScore };
        assertHits(hits, "two span queries", expectedIds, expectedScores);
    }

    /**
     * Executes the query and throws an assertion if the results don't match the
     * expectedHits.
     *
     * @param query the query to execute
     * @throws IOException
     */
    protected Hits executeQuery(final Query query) throws IOException {

        final IndexSearcher searcher = new IndexSearcher(mDirectory);
        final Hits hits = searcher.search(query);
        searcher.close();
        return hits;
    }

    /**
     * Checks to see if the hits are what we expected.
     *
     * @param hits the search results
     * @param description the description of the search
     * @param expectedIds the expected document ids of the hits
     * @param expectedScores the expected scores of the hits
     *
     * @throws IOException
     */
    protected void assertHits(final Hits hits, final String description, final String[] expectedIds,
            final float[] expectedScores) throws IOException {

        // display the hits
        System.out.println(hits.length() + " hits for search: \"" + description + '\"');
        for (int i = 0; i < hits.length(); i++) {
            System.out.println("  " + FIELD_ID + ':' + hits.doc(i).get(FIELD_ID) + " (score:" + hits.score(i) + ')');
        }

        // did we get the hits we expected
        assertEquals(expectedIds.length, hits.length());
        for (int i = 0; i < hits.length(); i++) {
            assertTrue(expectedIds[i].equals(hits.doc(i).get(FIELD_ID)));
            assertEquals(expectedScores[i], hits.score(i), 0);
        }
    }
}