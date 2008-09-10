package org.apache.lucene.search;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

public class TestTermScorer extends LuceneTestCase
{
    protected RAMDirectory directory;
    private static final String FIELD = "field";

    protected String[] values = new String[]{"all", "dogs dogs", "like", "playing", "fetch", "all"};
    protected IndexSearcher indexSearcher;
    protected IndexReader indexReader;


    public TestTermScorer(String s)
    {
        super(s);
    }

    protected void setUp() throws Exception
    {
        super.setUp();
        directory = new RAMDirectory();


        IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
        for (int i = 0; i < values.length; i++)
        {
            Document doc = new Document();
            doc.add(new Field(FIELD, values[i], Field.Store.YES, Field.Index.ANALYZED));
            writer.addDocument(doc);
        }
        writer.close();
        indexSearcher = new IndexSearcher(directory);
        indexReader = indexSearcher.getIndexReader();


    }

    public void test() throws IOException
    {

        Term allTerm = new Term(FIELD, "all");
        TermQuery termQuery = new TermQuery(allTerm);

        Weight weight = termQuery.weight(indexSearcher);

        TermScorer ts = new TermScorer(weight,
                                       indexReader.termDocs(allTerm), indexSearcher.getSimilarity(),
                                       indexReader.norms(FIELD));
        assertTrue("ts is null and it shouldn't be", ts != null);
        //we have 2 documents with the term all in them, one document for all the other values
        final List docs = new ArrayList();
        //must call next first


        ts.score(new HitCollector()
        {
            public void collect(int doc, float score)
            {
                docs.add(new TestHit(doc, score));
                assertTrue("score " + score + " is not greater than 0", score > 0);
                assertTrue("Doc: " + doc + " does not equal: " + 0 +
                        " or doc does not equaal: " + 5, doc == 0 || doc == 5);
            }
        });
        assertTrue("docs Size: " + docs.size() + " is not: " + 2, docs.size() == 2);
        TestHit doc0 = (TestHit) docs.get(0);
        TestHit doc5 = (TestHit) docs.get(1);
        //The scores should be the same
        assertTrue(doc0.score + " does not equal: " + doc5.score, doc0.score == doc5.score);
        /*
        Score should be (based on Default Sim.:
        All floats are approximate
        tf = 1
        numDocs = 6
        docFreq(all) = 2
        idf = ln(6/3) + 1 = 1.693147
        idf ^ 2 = 2.8667
        boost = 1
        lengthNorm = 1 //there is 1 term in every document
        coord = 1
        sumOfSquaredWeights = (idf * boost) ^ 2 = 1.693147 ^ 2 = 2.8667
        queryNorm = 1 / (sumOfSquaredWeights)^0.5 = 1 /(1.693147) = 0.590

         score = 1 * 2.8667 * 1 * 1 * 0.590 = 1.69

        */
        assertTrue(doc0.score + " does not equal: " + 1.6931472f, doc0.score == 1.6931472f);
    }

    public void testNext() throws Exception
    {

        Term allTerm = new Term(FIELD, "all");
        TermQuery termQuery = new TermQuery(allTerm);

        Weight weight = termQuery.weight(indexSearcher);

        TermScorer ts = new TermScorer(weight,
                                       indexReader.termDocs(allTerm), indexSearcher.getSimilarity(),
                                       indexReader.norms(FIELD));
        assertTrue("ts is null and it shouldn't be", ts != null);
        assertTrue("next did not return a doc", ts.next() == true);
        assertTrue("score is not correct", ts.score() == 1.6931472f);
        assertTrue("next did not return a doc", ts.next() == true);
        assertTrue("score is not correct", ts.score() == 1.6931472f);
        assertTrue("next returned a doc and it should not have", ts.next() == false);
    }

    public void testSkipTo() throws Exception
    {

        Term allTerm = new Term(FIELD, "all");
        TermQuery termQuery = new TermQuery(allTerm);

        Weight weight = termQuery.weight(indexSearcher);

        TermScorer ts = new TermScorer(weight,
                                       indexReader.termDocs(allTerm), indexSearcher.getSimilarity(),
                                       indexReader.norms(FIELD));
        assertTrue("ts is null and it shouldn't be", ts != null);
        assertTrue("Didn't skip", ts.skipTo(3) == true);
        //The next doc should be doc 5
        assertTrue("doc should be number 5", ts.doc() == 5);
    }

    public void testExplain() throws Exception
    {
        Term allTerm = new Term(FIELD, "all");
        TermQuery termQuery = new TermQuery(allTerm);

        Weight weight = termQuery.weight(indexSearcher);

        TermScorer ts = new TermScorer(weight,
                                       indexReader.termDocs(allTerm), indexSearcher.getSimilarity(),
                                       indexReader.norms(FIELD));
        assertTrue("ts is null and it shouldn't be", ts != null);
        Explanation explanation = ts.explain(0);
        assertTrue("explanation is null and it shouldn't be", explanation != null);
        //System.out.println("Explanation: " + explanation.toString());
        //All this Explain does is return the term frequency
        assertTrue("term frq is not 1", explanation.getValue() == 1);
        explanation = ts.explain(1);
        assertTrue("explanation is null and it shouldn't be", explanation != null);
        //System.out.println("Explanation: " + explanation.toString());
        //All this Explain does is return the term frequency
        assertTrue("term frq is not 0", explanation.getValue() == 0);

        Term dogsTerm = new Term(FIELD, "dogs");
        termQuery = new TermQuery(dogsTerm);
        weight = termQuery.weight(indexSearcher);

        ts = new TermScorer(weight, indexReader.termDocs(dogsTerm), indexSearcher.getSimilarity(),
                                       indexReader.norms(FIELD));
        assertTrue("ts is null and it shouldn't be", ts != null);
        explanation = ts.explain(1);
        assertTrue("explanation is null and it shouldn't be", explanation != null);
        //System.out.println("Explanation: " + explanation.toString());
        //All this Explain does is return the term frequency
        float sqrtTwo = (float)Math.sqrt(2.0f);
        assertTrue("term frq: " + explanation.getValue() + " is not the square root of 2", explanation.getValue() == sqrtTwo);

        explanation = ts.explain(10);//try a doc out of range
        assertTrue("explanation is null and it shouldn't be", explanation != null);
        //System.out.println("Explanation: " + explanation.toString());
        //All this Explain does is return the term frequency

        assertTrue("term frq: " + explanation.getValue() + " is not 0", explanation.getValue() == 0);

    }

    private class TestHit
    {
        public int doc;
        public float score;

        public TestHit(int doc, float score)
        {
            this.doc = doc;
            this.score = score;
        }

        public String toString()
        {
            return "TestHit{" +
                    "doc=" + doc +
                    ", score=" + score +
                    "}";
        }
    }

}
