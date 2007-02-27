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

import junit.framework.TestCase;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

/**
 * TestWildcard tests the '*' and '?' wildcard characters.
 *
 * @version $Id$
 * @author Otis Gospodnetic
 */
public class TestWildcard
    extends TestCase {
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
   * TermQuery.
   */
  public void testTermWithoutWildcard() throws IOException {
      RAMDirectory indexStore = getIndexStore("field", new String[]{"nowildcard", "nowildcardx"});
      IndexSearcher searcher = new IndexSearcher(indexStore);

      Query wq = new WildcardQuery(new Term("field", "nowildcard"));
      assertMatches(searcher, wq, 1);

      wq = searcher.rewrite(wq);
      assertTrue(wq instanceof TermQuery);
  }

  /**
   * Tests Wildcard queries with an asterisk.
   */
  public void testAsterisk()
      throws IOException {
    RAMDirectory indexStore = getIndexStore("body", new String[]
    {"metal", "metals"});
    IndexSearcher searcher = new IndexSearcher(indexStore);
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
  }

  /**
   * Tests Wildcard queries with a question mark.
   *
   * @throws IOException if an error occurs
   */
  public void testQuestionmark()
      throws IOException {
    RAMDirectory indexStore = getIndexStore("body", new String[]
    {"metal", "metals", "mXtals", "mXtXls"});
    IndexSearcher searcher = new IndexSearcher(indexStore);
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
  }

  private RAMDirectory getIndexStore(String field, String[] contents)
      throws IOException {
    RAMDirectory indexStore = new RAMDirectory();
    IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true);
    for (int i = 0; i < contents.length; ++i) {
      Document doc = new Document();
      doc.add(new Field(field, contents[i], Field.Store.YES, Field.Index.TOKENIZED));
      writer.addDocument(doc);
    }
    writer.optimize();
    writer.close();

    return indexStore;
  }

  private void assertMatches(IndexSearcher searcher, Query q, int expectedMatches)
      throws IOException {
    Hits result = searcher.search(q);
    assertEquals(expectedMatches, result.length());
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
    boolean dbg = false;
    QueryParser qp = new QueryParser(field, new WhitespaceAnalyzer());
    qp.setAllowLeadingWildcard(true);
    String docs[] = {
        "abcdefg1",
        "hijklmn1",
        "opqrstu1",
    };
    // queries that should find all docs
    String matchAll[] = {
        "*", "*1", "**1", "*?", "*?1", "?*1", "**", "***",
    };
    // queries that should find no docs
    String matchNone[] = {
        "a*h", "a?h", "*a*h", "?a", "a?",
    };
    // queries that should be parsed to prefix queries
    String matchOneDocPrefix[][] = {
        {"a*", "ab*", "abc*"}, // these should find only doc 0 
        {"h*", "hi*", "hij*"}, // these should find only doc 1
        {"o*", "op*", "opq*"}, // these should find only doc 2
    };
    // queries that should be parsed to wildcard queries
    String matchOneDocWild[][] = {
        {"*a*", "*ab*", "*abc**", "ab*e*", "*g?", "*f?1", "abc**"}, // these should find only doc 0
        {"*h*", "*hi*", "*hij**", "hi*k*", "*n?", "*m?1", "hij**"}, // these should find only doc 1
        {"*o*", "*op*", "*opq**", "op*q*", "*u?", "*t?1", "opq**"}, // these should find only doc 2
    };

    // prepare the index
    RAMDirectory dir = new RAMDirectory();
    IndexWriter iw = new IndexWriter(dir, new WhitespaceAnalyzer());
    for (int i = 0; i < docs.length; i++) {
      Document doc = new Document();
      doc.add(new Field(field,docs[i],Store.NO,Index.UN_TOKENIZED));
      iw.addDocument(doc);
    }
    iw.close();
    
    IndexSearcher searcher = new IndexSearcher(dir);
    
    // test queries that must find all
    for (int i = 0; i < matchAll.length; i++) {
      String qtxt = matchAll[i];
      Query q = qp.parse(qtxt);
      if (dbg) System.out.println("matchAll: qtxt="+qtxt+" q="+q+" "+q.getClass().getName());
      Hits hits = searcher.search(q);
      assertEquals(docs.length,hits.length());
    }
    
    // test queries that must find none
    for (int i = 0; i < matchNone.length; i++) {
      String qtxt = matchNone[i];
      Query q = qp.parse(qtxt);
      if (dbg) System.out.println("matchNone: qtxt="+qtxt+" q="+q+" "+q.getClass().getName());
      Hits hits = searcher.search(q);
      assertEquals(0,hits.length());
    }

    // test queries that must be prefix queries and must find only one doc
    for (int i = 0; i < matchOneDocPrefix.length; i++) {
      for (int j = 0; j < matchOneDocPrefix[i].length; j++) {
        String qtxt = matchOneDocPrefix[i][j];
        Query q = qp.parse(qtxt);
        if (dbg) System.out.println("match 1 prefix: doc="+docs[i]+" qtxt="+qtxt+" q="+q+" "+q.getClass().getName());
        assertEquals(PrefixQuery.class, q.getClass());
        Hits hits = searcher.search(q);
        assertEquals(1,hits.length());
        assertEquals(i,hits.id(0));
      }
    }

    // test queries that must be wildcard queries and must find only one doc
    for (int i = 0; i < matchOneDocPrefix.length; i++) {
      for (int j = 0; j < matchOneDocWild[i].length; j++) {
        String qtxt = matchOneDocWild[i][j];
        Query q = qp.parse(qtxt);
        if (dbg) System.out.println("match 1 wild: doc="+docs[i]+" qtxt="+qtxt+" q="+q+" "+q.getClass().getName());
        assertEquals(WildcardQuery.class, q.getClass());
        Hits hits = searcher.search(q);
        assertEquals(1,hits.length());
        assertEquals(i,hits.id(0));
      }
    }

    searcher.close();
  }
  
}
