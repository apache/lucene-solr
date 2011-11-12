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

import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;


/**
 * TestExplanations subclass focusing on basic query types
 */
public class TestSimpleExplanations extends TestExplanations {

  // we focus on queries that don't rewrite to other queries.
  // if we get those covered well, then the ones that rewrite should
  // also be covered.
  

  /* simple term tests */
  
  public void testT1() throws Exception {
    qtest("w1", new int[] { 0,1,2,3 });
  }
  public void testT2() throws Exception {
    qtest("w1^1000", new int[] { 0,1,2,3 });
  }
  
  /* MatchAllDocs */
  
  public void testMA1() throws Exception {
    qtest(new MatchAllDocsQuery(), new int[] { 0,1,2,3 });
  }
  public void testMA2() throws Exception {
    Query q=new MatchAllDocsQuery();
    q.setBoost(1000);
    qtest(q, new int[] { 0,1,2,3 });
  }

  /* some simple phrase tests */
  
  public void testP1() throws Exception {
    qtest("\"w1 w2\"", new int[] { 0 });
  }
  public void testP2() throws Exception {
    qtest("\"w1 w3\"", new int[] { 1,3 });
  }
  public void testP3() throws Exception {
    qtest("\"w1 w2\"~1", new int[] { 0,1,2 });
  }
  public void testP4() throws Exception {
    qtest("\"w2 w3\"~1", new int[] { 0,1,2,3 });
  }
  public void testP5() throws Exception {
    qtest("\"w3 w2\"~1", new int[] { 1,3 });
  }
  public void testP6() throws Exception {
    qtest("\"w3 w2\"~2", new int[] { 0,1,3 });
  }
  public void testP7() throws Exception {
    qtest("\"w3 w2\"~3", new int[] { 0,1,2,3 });
  }

  /* some simple filtered query tests */
  
  public void testFQ1() throws Exception {
    qtest(new FilteredQuery(qp.parse("w1"),
                            new ItemizedFilter(new int[] {0,1,2,3})),
          new int[] {0,1,2,3});
  }
  public void testFQ2() throws Exception {
    qtest(new FilteredQuery(qp.parse("w1"),
                            new ItemizedFilter(new int[] {0,2,3})),
          new int[] {0,2,3});
  }
  public void testFQ3() throws Exception {
    qtest(new FilteredQuery(qp.parse("xx"),
                            new ItemizedFilter(new int[] {1,3})),
          new int[] {3});
  }
  public void testFQ4() throws Exception {
    qtest(new FilteredQuery(qp.parse("xx^1000"),
                            new ItemizedFilter(new int[] {1,3})),
          new int[] {3});
  }
  public void testFQ6() throws Exception {
    Query q = new FilteredQuery(qp.parse("xx"),
                                new ItemizedFilter(new int[] {1,3}));
    q.setBoost(1000);
    qtest(q, new int[] {3});
  }

  /* ConstantScoreQueries */
  
  public void testCSQ1() throws Exception {
    Query q = new ConstantScoreQuery(new ItemizedFilter(new int[] {0,1,2,3}));
    qtest(q, new int[] {0,1,2,3});
  }
  public void testCSQ2() throws Exception {
    Query q = new ConstantScoreQuery(new ItemizedFilter(new int[] {1,3}));
    qtest(q, new int[] {1,3});
  }
  public void testCSQ3() throws Exception {
    Query q = new ConstantScoreQuery(new ItemizedFilter(new int[] {0,2}));
    q.setBoost(1000);
    qtest(q, new int[] {0,2});
  }
  
  /* DisjunctionMaxQuery */
  
  public void testDMQ1() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.0f);
    q.add(qp.parse("w1"));
    q.add(qp.parse("w5"));
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testDMQ2() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
    q.add(qp.parse("w1"));
    q.add(qp.parse("w5"));
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testDMQ3() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
    q.add(qp.parse("QQ"));
    q.add(qp.parse("w5"));
    qtest(q, new int[] { 0 });
  }
  public void testDMQ4() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
    q.add(qp.parse("QQ"));
    q.add(qp.parse("xx"));
    qtest(q, new int[] { 2,3 });
  }
  public void testDMQ5() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
    q.add(qp.parse("yy -QQ"));
    q.add(qp.parse("xx"));
    qtest(q, new int[] { 2,3 });
  }
  public void testDMQ6() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
    q.add(qp.parse("-yy w3"));
    q.add(qp.parse("xx"));
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testDMQ7() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
    q.add(qp.parse("-yy w3"));
    q.add(qp.parse("w2"));
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testDMQ8() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
    q.add(qp.parse("yy w5^100"));
    q.add(qp.parse("xx^100000"));
    qtest(q, new int[] { 0,2,3 });
  }
  public void testDMQ9() throws Exception {
    DisjunctionMaxQuery q = new DisjunctionMaxQuery(0.5f);
    q.add(qp.parse("yy w5^100"));
    q.add(qp.parse("xx^0"));
    qtest(q, new int[] { 0,2,3 });
  }
  
  /* MultiPhraseQuery */
  
  public void testMPQ1() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1"}));
    q.add(ta(new String[] {"w2","w3", "xx"}));
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testMPQ2() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1"}));
    q.add(ta(new String[] {"w2","w3"}));
    qtest(q, new int[] { 0,1,3 });
  }
  public void testMPQ3() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1","xx"}));
    q.add(ta(new String[] {"w2","w3"}));
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testMPQ4() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1"}));
    q.add(ta(new String[] {"w2"}));
    qtest(q, new int[] { 0 });
  }
  public void testMPQ5() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1"}));
    q.add(ta(new String[] {"w2"}));
    q.setSlop(1);
    qtest(q, new int[] { 0,1,2 });
  }
  public void testMPQ6() throws Exception {
    MultiPhraseQuery q = new MultiPhraseQuery();
    q.add(ta(new String[] {"w1","w3"}));
    q.add(ta(new String[] {"w2"}));
    q.setSlop(1);
    qtest(q, new int[] { 0,1,2,3 });
  }

  /* some simple tests of boolean queries containing term queries */
  
  public void testBQ1() throws Exception {
    qtest("+w1 +w2", new int[] { 0,1,2,3 });
  }
  public void testBQ2() throws Exception {
    qtest("+yy +w3", new int[] { 2,3 });
  }
  public void testBQ3() throws Exception {
    qtest("yy +w3", new int[] { 0,1,2,3 });
  }
  public void testBQ4() throws Exception {
    qtest("w1 (-xx w2)", new int[] { 0,1,2,3 });
  }
  public void testBQ5() throws Exception {
    qtest("w1 (+qq w2)", new int[] { 0,1,2,3 });
  }
  public void testBQ6() throws Exception {
    qtest("w1 -(-qq w5)", new int[] { 1,2,3 });
  }
  public void testBQ7() throws Exception {
    qtest("+w1 +(qq (xx -w2) (+w3 +w4))", new int[] { 0 });
  }
  public void testBQ8() throws Exception {
    qtest("+w1 (qq (xx -w2) (+w3 +w4))", new int[] { 0,1,2,3 });
  }
  public void testBQ9() throws Exception {
    qtest("+w1 (qq (-xx w2) -(+w3 +w4))", new int[] { 0,1,2,3 });
  }
  public void testBQ10() throws Exception {
    qtest("+w1 +(qq (-xx w2) -(+w3 +w4))", new int[] { 1 });
  }
  public void testBQ11() throws Exception {
    qtest("w1 w2^1000.0", new int[] { 0,1,2,3 });
  }
  public void testBQ14() throws Exception {
    BooleanQuery q = new BooleanQuery(true);
    q.add(qp.parse("QQQQQ"), BooleanClause.Occur.SHOULD);
    q.add(qp.parse("w1"), BooleanClause.Occur.SHOULD);
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testBQ15() throws Exception {
    BooleanQuery q = new BooleanQuery(true);
    q.add(qp.parse("QQQQQ"), BooleanClause.Occur.MUST_NOT);
    q.add(qp.parse("w1"), BooleanClause.Occur.SHOULD);
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testBQ16() throws Exception {
    BooleanQuery q = new BooleanQuery(true);
    q.add(qp.parse("QQQQQ"), BooleanClause.Occur.SHOULD);
    q.add(qp.parse("w1 -xx"), BooleanClause.Occur.SHOULD);
    qtest(q, new int[] { 0,1 });
  }
  public void testBQ17() throws Exception {
    BooleanQuery q = new BooleanQuery(true);
    q.add(qp.parse("w2"), BooleanClause.Occur.SHOULD);
    q.add(qp.parse("w1 -xx"), BooleanClause.Occur.SHOULD);
    qtest(q, new int[] { 0,1,2,3 });
  }
  public void testBQ19() throws Exception {
    qtest("-yy w3", new int[] { 0,1 });
  }
  
  public void testBQ20() throws Exception {
    BooleanQuery q = new BooleanQuery();
    q.setMinimumNumberShouldMatch(2);
    q.add(qp.parse("QQQQQ"), BooleanClause.Occur.SHOULD);
    q.add(qp.parse("yy"), BooleanClause.Occur.SHOULD);
    q.add(qp.parse("zz"), BooleanClause.Occur.SHOULD);
    q.add(qp.parse("w5"), BooleanClause.Occur.SHOULD);
    q.add(qp.parse("w4"), BooleanClause.Occur.SHOULD);
    
    qtest(q, new int[] { 0,3 });
    
  }
  
  public void testTermQueryMultiSearcherExplain() throws Exception {
    // creating two directories for indices
    Directory indexStoreA = newDirectory();
    Directory indexStoreB = newDirectory();

    Document lDoc = new Document();
    lDoc.add(newField("handle", "1 2", Field.Store.YES, Field.Index.ANALYZED));
    Document lDoc2 = new Document();
    lDoc2.add(newField("handle", "1 2", Field.Store.YES, Field.Index.ANALYZED));
    Document lDoc3 = new Document();
    lDoc3.add(newField("handle", "1 2", Field.Store.YES, Field.Index.ANALYZED));

    IndexWriter writerA = new IndexWriter(indexStoreA, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new StandardAnalyzer(
        TEST_VERSION_CURRENT)));
    IndexWriter writerB = new IndexWriter(indexStoreB, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new StandardAnalyzer(
        TEST_VERSION_CURRENT)));

    writerA.addDocument(lDoc);
    writerA.addDocument(lDoc2);
    writerA.forceMerge(1);
    writerA.close();

    writerB.addDocument(lDoc3);
    writerB.close();

    QueryParser parser = new QueryParser(TEST_VERSION_CURRENT, "fulltext", new StandardAnalyzer(TEST_VERSION_CURRENT));
    Query query = parser.parse("handle:1");

    Searcher[] searchers = new Searcher[2];
    searchers[0] = new IndexSearcher(indexStoreB, true);
    searchers[1] = new IndexSearcher(indexStoreA, true);
    Searcher mSearcher = new MultiSearcher(searchers);
    ScoreDoc[] hits = mSearcher.search(query, null, 1000).scoreDocs;

    assertEquals(3, hits.length);

    Explanation explain = mSearcher.explain(query, hits[0].doc);
    String exp = explain.toString(0);
    assertTrue(exp, exp.indexOf("maxDocs=3") > -1);
    assertTrue(exp, exp.indexOf("docFreq=3") > -1);
    
    query = parser.parse("handle:\"1 2\"");
    hits = mSearcher.search(query, null, 1000).scoreDocs;

    assertEquals(3, hits.length);

    explain = mSearcher.explain(query, hits[0].doc);
    exp = explain.toString(0);
    assertTrue(exp, exp.indexOf("1=3") > -1);
    assertTrue(exp, exp.indexOf("2=3") > -1);
    
    query = new SpanNearQuery(new SpanQuery[] {
        new SpanTermQuery(new Term("handle", "1")),
        new SpanTermQuery(new Term("handle", "2")) }, 0, true);
    hits = mSearcher.search(query, null, 1000).scoreDocs;

    assertEquals(3, hits.length);

    explain = mSearcher.explain(query, hits[0].doc);
    exp = explain.toString(0);
    assertTrue(exp, exp.indexOf("1=3") > -1);
    assertTrue(exp, exp.indexOf("2=3") > -1);
    mSearcher.close();
    indexStoreA.close();
    indexStoreB.close();
  }

  /* BQ of TQ: using alt so some fields have zero boost and some don't */
  
  public void testMultiFieldBQ1() throws Exception {
    qtest("+w1 +alt:w2", new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ2() throws Exception {
    qtest("+yy +alt:w3", new int[] { 2,3 });
  }
  public void testMultiFieldBQ3() throws Exception {
    qtest("yy +alt:w3", new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ4() throws Exception {
    qtest("w1 (-xx alt:w2)", new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ5() throws Exception {
    qtest("w1 (+alt:qq alt:w2)", new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ6() throws Exception {
    qtest("w1 -(-alt:qq alt:w5)", new int[] { 1,2,3 });
  }
  public void testMultiFieldBQ7() throws Exception {
    qtest("+w1 +(alt:qq (alt:xx -alt:w2) (+alt:w3 +alt:w4))", new int[] { 0 });
  }
  public void testMultiFieldBQ8() throws Exception {
    qtest("+alt:w1 (qq (alt:xx -w2) (+alt:w3 +w4))", new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ9() throws Exception {
    qtest("+w1 (alt:qq (-xx w2) -(+alt:w3 +w4))", new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQ10() throws Exception {
    qtest("+w1 +(alt:qq (-xx alt:w2) -(+alt:w3 +w4))", new int[] { 1 });
  }

  /* BQ of PQ: using alt so some fields have zero boost and some don't */
  
  public void testMultiFieldBQofPQ1() throws Exception {
    qtest("\"w1 w2\" alt:\"w1 w2\"", new int[] { 0 });
  }
  public void testMultiFieldBQofPQ2() throws Exception {
    qtest("\"w1 w3\" alt:\"w1 w3\"", new int[] { 1,3 });
  }
  public void testMultiFieldBQofPQ3() throws Exception {
    qtest("\"w1 w2\"~1 alt:\"w1 w2\"~1", new int[] { 0,1,2 });
  }
  public void testMultiFieldBQofPQ4() throws Exception {
    qtest("\"w2 w3\"~1 alt:\"w2 w3\"~1", new int[] { 0,1,2,3 });
  }
  public void testMultiFieldBQofPQ5() throws Exception {
    qtest("\"w3 w2\"~1 alt:\"w3 w2\"~1", new int[] { 1,3 });
  }
  public void testMultiFieldBQofPQ6() throws Exception {
    qtest("\"w3 w2\"~2 alt:\"w3 w2\"~2", new int[] { 0,1,3 });
  }
  public void testMultiFieldBQofPQ7() throws Exception {
    qtest("\"w3 w2\"~3 alt:\"w3 w2\"~3", new int[] { 0,1,2,3 });
  }

}
