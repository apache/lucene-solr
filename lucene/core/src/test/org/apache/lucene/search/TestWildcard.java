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

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;

import java.io.IOException;

/**
 * TestWildcard tests the '*' and '?' wildcard characters.
 */
public class TestWildcard
    extends LuceneTestCase {
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public void testEquals() {
    WildcardQuery wq1 = new WildcardQuery(new Term("field", "b*a"));
    WildcardQuery wq2 = new WildcardQuery(new Term("field", "b*a"));
    WildcardQuery wq3 = new WildcardQuery(new Term("field", "b*a"));

    // reflexive?
    assertEquals(wq1, wq2);
    assertEquals(wq2, wq1);

    // transitive?
    assertEquals(wq2, wq3);
    assertEquals(wq1, wq3);

    assertFalse(wq1.equals(null));

    FuzzyQuery fq = new FuzzyQuery(new Term("field", "b*a"));
    assertFalse(wq1.equals(fq));
    assertFalse(fq.equals(wq1));
  }
  
  /**
   * Tests if a WildcardQuery that has no wildcard in the term is rewritten to a single
   * TermQuery. The boost should be preserved, and the rewrite should return
   * a ConstantScoreQuery if the WildcardQuery had a ConstantScore rewriteMethod.
   */
  public void testTermWithoutWildcard() throws IOException {
      Directory indexStore = getIndexStore("field", new String[]{"nowildcard", "nowildcardx"});
      IndexReader reader = IndexReader.open(indexStore);
      IndexSearcher searcher = new IndexSearcher(reader);

      MultiTermQuery wq = new WildcardQuery(new Term("field", "nowildcard"));
      assertMatches(searcher, wq, 1);

      wq.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
      wq.setBoost(0.1F);
      Query q = searcher.rewrite(wq);
      assertTrue(q instanceof TermQuery);
      assertEquals(q.getBoost(), wq.getBoost(), 0);
      
      wq.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
      wq.setBoost(0.2F);
      q = searcher.rewrite(wq);
      assertTrue(q instanceof ConstantScoreQuery);
      assertEquals(q.getBoost(), wq.getBoost(), 0.1);
      
      wq.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT);
      wq.setBoost(0.3F);
      q = searcher.rewrite(wq);
      assertTrue(q instanceof ConstantScoreQuery);
      assertEquals(q.getBoost(), wq.getBoost(), 0.1);
      
      wq.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
      wq.setBoost(0.4F);
      q = searcher.rewrite(wq);
      assertTrue(q instanceof ConstantScoreQuery);
      assertEquals(q.getBoost(), wq.getBoost(), 0.1);
      searcher.close();
      reader.close();
      indexStore.close();
  }
  
  /**
   * Tests if a WildcardQuery with an empty term is rewritten to an empty BooleanQuery
   */
  public void testEmptyTerm() throws IOException {
    Directory indexStore = getIndexStore("field", new String[]{"nowildcard", "nowildcardx"});
    IndexReader reader = IndexReader.open(indexStore);
    IndexSearcher searcher = new IndexSearcher(reader);

    MultiTermQuery wq = new WildcardQuery(new Term("field", ""));
    wq.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    assertMatches(searcher, wq, 0);
    Query q = searcher.rewrite(wq);
    assertTrue(q instanceof BooleanQuery);
    assertEquals(0, ((BooleanQuery) q).clauses().size());
    searcher.close();
    reader.close();
    indexStore.close();
  }
  
  /**
   * Tests if a WildcardQuery that has only a trailing * in the term is
   * rewritten to a single PrefixQuery. The boost and rewriteMethod should be
   * preserved.
   */
  public void testPrefixTerm() throws IOException {
    Directory indexStore = getIndexStore("field", new String[]{"prefix", "prefixx"});
    IndexReader reader = IndexReader.open(indexStore);
    IndexSearcher searcher = new IndexSearcher(reader);

    MultiTermQuery wq = new WildcardQuery(new Term("field", "prefix*"));
    assertMatches(searcher, wq, 2);
    assertTrue(wq.getEnum(searcher.getIndexReader()) instanceof PrefixTermEnum);
   
    searcher.close();
    reader.close();
    indexStore.close();
  }

  /**
   * Tests Wildcard queries with an asterisk.
   */
  public void testAsterisk()
      throws IOException {
    Directory indexStore = getIndexStore("body", new String[]
    {"metal", "metals"});
    IndexReader reader = IndexReader.open(indexStore);
    IndexSearcher searcher = new IndexSearcher(reader);
    Query query1 = new TermQuery(new Term("body", "metal"));
    Query query2 = new WildcardQuery(new Term("body", "metal*"));
    Query query3 = new WildcardQuery(new Term("body", "m*tal"));
    Query query4 = new WildcardQuery(new Term("body", "m*tal*"));
    Query query5 = new WildcardQuery(new Term("body", "m*tals"));

    BooleanQuery query6 = new BooleanQuery();
    query6.add(query5, BooleanClause.Occur.SHOULD);

    BooleanQuery query7 = new BooleanQuery();
    query7.add(query3, BooleanClause.Occur.SHOULD);
    query7.add(query5, BooleanClause.Occur.SHOULD);

    // Queries do not automatically lower-case search terms:
    Query query8 = new WildcardQuery(new Term("body", "M*tal*"));

    assertMatches(searcher, query1, 1);
    assertMatches(searcher, query2, 2);
    assertMatches(searcher, query3, 1);
    assertMatches(searcher, query4, 2);
    assertMatches(searcher, query5, 1);
    assertMatches(searcher, query6, 1);
    assertMatches(searcher, query7, 2);
    assertMatches(searcher, query8, 0);
    assertMatches(searcher, new WildcardQuery(new Term("body", "*tall")), 0);
    assertMatches(searcher, new WildcardQuery(new Term("body", "*tal")), 1);
    assertMatches(searcher, new WildcardQuery(new Term("body", "*tal*")), 2);
    searcher.close();
    reader.close();
    indexStore.close();
  }

  /**
   * LUCENE-2620
   */
  public void testLotsOfAsterisks()
      throws IOException {
    Directory indexStore = getIndexStore("body", new String[]
    {"metal", "metals"});
    IndexSearcher searcher = new IndexSearcher(indexStore, true);
    StringBuilder term = new StringBuilder();
    term.append("m");
    for (int i = 0; i < 512; i++)
      term.append("*");
    term.append("tal");
    Query query3 = new WildcardQuery(new Term("body", term.toString()));

    assertMatches(searcher, query3, 1);
    searcher.close();
    indexStore.close();
  }
  
  /**
   * Tests Wildcard queries with a question mark.
   *
   * @throws IOException if an error occurs
   */
  public void testQuestionmark()
      throws IOException {
    Directory indexStore = getIndexStore("body", new String[]
    {"metal", "metals", "mXtals", "mXtXls"});
    IndexReader reader = IndexReader.open(indexStore);
    IndexSearcher searcher = new IndexSearcher(reader);
    Query query1 = new WildcardQuery(new Term("body", "m?tal"));
    Query query2 = new WildcardQuery(new Term("body", "metal?"));
    Query query3 = new WildcardQuery(new Term("body", "metals?"));
    Query query4 = new WildcardQuery(new Term("body", "m?t?ls"));
    Query query5 = new WildcardQuery(new Term("body", "M?t?ls"));
    Query query6 = new WildcardQuery(new Term("body", "meta??"));
    
    assertMatches(searcher, query1, 1); 
    assertMatches(searcher, query2, 1);
    assertMatches(searcher, query3, 0);
    assertMatches(searcher, query4, 3);
    assertMatches(searcher, query5, 0);
    assertMatches(searcher, query6, 1); // Query: 'meta??' matches 'metals' not 'metal'
    searcher.close();
    reader.close();
    indexStore.close();
  }

  private Directory getIndexStore(String field, String[] contents)
      throws IOException {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, indexStore);
    for (int i = 0; i < contents.length; ++i) {
      Document doc = new Document();
      doc.add(newField(field, contents[i], Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    writer.close();

    return indexStore;
  }

  private void assertMatches(IndexSearcher searcher, Query q, int expectedMatches)
      throws IOException {
    ScoreDoc[] result = searcher.search(q, null, 1000).scoreDocs;
    assertEquals(expectedMatches, result.length);
  }

  /**
   * Test that wild card queries are parsed to the correct type and are searched correctly.
   * This test looks at both parsing and execution of wildcard queries.
   * Although placed here, it also tests prefix queries, verifying that
   * prefix queries are not parsed into wild card queries, and viceversa.
   * @throws Exception
   */
  public void testParsingAndSearching() throws Exception {
    String field = "content";
    QueryParser qp = new QueryParser(TEST_VERSION_CURRENT, field, new MockAnalyzer(random));
    qp.setAllowLeadingWildcard(true);
    String docs[] = {
        "\\ abcdefg1",
        "\\79 hijklmn1",
        "\\\\ opqrstu1",
    };
    // queries that should find all docs
    String matchAll[] = {
        "*", "*1", "**1", "*?", "*?1", "?*1", "**", "***", "\\\\*"
    };
    // queries that should find no docs
    String matchNone[] = {
        "a*h", "a?h", "*a*h", "?a", "a?",
    };
    // queries that should be parsed to prefix queries
    String matchOneDocPrefix[][] = {
        {"a*", "ab*", "abc*", }, // these should find only doc 0 
        {"h*", "hi*", "hij*", "\\\\7*"}, // these should find only doc 1
        {"o*", "op*", "opq*", "\\\\\\\\*"}, // these should find only doc 2
    };
    // queries that should be parsed to wildcard queries
    String matchOneDocWild[][] = {
        {"*a*", "*ab*", "*abc**", "ab*e*", "*g?", "*f?1", "abc**"}, // these should find only doc 0
        {"*h*", "*hi*", "*hij**", "hi*k*", "*n?", "*m?1", "hij**"}, // these should find only doc 1
        {"*o*", "*op*", "*opq**", "op*q*", "*u?", "*t?1", "opq**"}, // these should find only doc 2
    };

    // prepare the index
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random, dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docs.length; i++) {
      Document doc = new Document();
      doc.add(newField(field,docs[i],Store.NO,Index.ANALYZED));
      iw.addDocument(doc);
    }
    iw.close();
    
    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    
    // test queries that must find all
    for (int i = 0; i < matchAll.length; i++) {
      String qtxt = matchAll[i];
      Query q = qp.parse(qtxt);
      if (VERBOSE) System.out.println("matchAll: qtxt="+qtxt+" q="+q+" "+q.getClass().getName());
      ScoreDoc[] hits = searcher.search(q, null, 1000).scoreDocs;
      assertEquals(docs.length,hits.length);
    }
    
    // test queries that must find none
    for (int i = 0; i < matchNone.length; i++) {
      String qtxt = matchNone[i];
      Query q = qp.parse(qtxt);
      if (VERBOSE) System.out.println("matchNone: qtxt="+qtxt+" q="+q+" "+q.getClass().getName());
      ScoreDoc[] hits = searcher.search(q, null, 1000).scoreDocs;
      assertEquals(0,hits.length);
    }

    // test queries that must be prefix queries and must find only one doc
    for (int i = 0; i < matchOneDocPrefix.length; i++) {
      for (int j = 0; j < matchOneDocPrefix[i].length; j++) {
        String qtxt = matchOneDocPrefix[i][j];
        Query q = qp.parse(qtxt);
        if (VERBOSE) System.out.println("match 1 prefix: doc="+docs[i]+" qtxt="+qtxt+" q="+q+" "+q.getClass().getName());
        assertEquals(PrefixQuery.class, q.getClass());
        ScoreDoc[] hits = searcher.search(q, null, 1000).scoreDocs;
        assertEquals(1,hits.length);
        assertEquals(i,hits[0].doc);
      }
    }

    // test queries that must be wildcard queries and must find only one doc
    for (int i = 0; i < matchOneDocPrefix.length; i++) {
      for (int j = 0; j < matchOneDocWild[i].length; j++) {
        String qtxt = matchOneDocWild[i][j];
        Query q = qp.parse(qtxt);
        if (VERBOSE) System.out.println("match 1 wild: doc="+docs[i]+" qtxt="+qtxt+" q="+q+" "+q.getClass().getName());
        assertEquals(WildcardQuery.class, q.getClass());
        ScoreDoc[] hits = searcher.search(q, null, 1000).scoreDocs;
        assertEquals(1,hits.length);
        assertEquals(i,hits[0].doc);
      }
    }

    searcher.close();
    reader.close();
    dir.close();
  }
  
}
