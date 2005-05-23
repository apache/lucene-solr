package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
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

import junit.framework.TestCase;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

/**
 * TestWildcard tests the '*' and '?' wildcard characters.
 *
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

    assertMatches(searcher, query1, 1);
    assertMatches(searcher, query2, 2);
    assertMatches(searcher, query3, 1);
    assertMatches(searcher, query4, 3);
    assertMatches(searcher, query5, 0);
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
}
