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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Tests IndexSearcher's searchAfter() method
 */
public class TestSearchAfter extends LuceneTestCase {
  private Directory dir;
  private IndexReader reader;
  private IndexSearcher searcher;
   
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int numDocs = atLeast(200);
    for (int i = 0; i < numDocs; i++) {
      Document document = new Document();
      document.add(newField("english", English.intToEnglish(i), TextField.TYPE_UNSTORED));
      document.add(newField("oddeven", (i % 2 == 0) ? "even" : "odd", TextField.TYPE_UNSTORED));
      iw.addDocument(document);
    }
    reader = iw.getReader();
    iw.close();
    searcher = newSearcher(reader);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  public void testQueries() throws Exception {
    // because the first page has a null 'after', we get a normal collector.
    // so we need to run the test a few times to ensure we will collect multiple
    // pages.
    int n = atLeast(10);
    for (int i = 0; i < n; i++) {
      Filter odd = new QueryWrapperFilter(new TermQuery(new Term("oddeven", "odd")));
      assertQuery(new MatchAllDocsQuery(), null);
      assertQuery(new TermQuery(new Term("english", "one")), null);
      assertQuery(new MatchAllDocsQuery(), odd);
      assertQuery(new TermQuery(new Term("english", "four")), odd);
      BooleanQuery bq = new BooleanQuery();
      bq.add(new TermQuery(new Term("english", "one")), BooleanClause.Occur.SHOULD);
      bq.add(new TermQuery(new Term("oddeven", "even")), BooleanClause.Occur.SHOULD);
      assertQuery(bq, null);
    }
  }
  
  void assertQuery(Query query, Filter filter) throws Exception {
    int maxDoc = searcher.getIndexReader().maxDoc();
    TopDocs all = searcher.search(query, filter, maxDoc);
    int pageSize = _TestUtil.nextInt(random(), 1, maxDoc*2);
    int pageStart = 0;
    ScoreDoc lastBottom = null;
    while (pageStart < all.totalHits) {
      TopDocs paged = searcher.searchAfter(lastBottom, query, filter, pageSize);
      if (paged.scoreDocs.length == 0) {
        break;
      }
      assertPage(pageStart, all, paged);
      pageStart += paged.scoreDocs.length;
      lastBottom = paged.scoreDocs[paged.scoreDocs.length - 1];
    }
    assertEquals(all.scoreDocs.length, pageStart);
  }

  static void assertPage(int pageStart, TopDocs all, TopDocs paged) {
    assertEquals(all.totalHits, paged.totalHits);
    for (int i = 0; i < paged.scoreDocs.length; i++) {
      assertEquals(all.scoreDocs[pageStart + i].doc, paged.scoreDocs[i].doc);
      assertEquals(all.scoreDocs[pageStart + i].score, paged.scoreDocs[i].score, 0f);
    }
  }
}
