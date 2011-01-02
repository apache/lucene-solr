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

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

public class TestRemoteSearchable extends RemoteTestCase {
  private static Directory indexStore;
  private static Searchable local;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    // construct an index
    indexStore = newDirectory();
    IndexWriter writer = new IndexWriter(indexStore, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new SimpleAnalyzer(TEST_VERSION_CURRENT)));
    Document doc = new Document();
    doc.add(newField("test", "test text", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(newField("other", "other test text", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.optimize();
    writer.close();
    local = new IndexSearcher(indexStore, true);
    startServer(local);
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    local.close();
    indexStore.close();
    indexStore = null;
  }
  
  private static void search(Query query) throws Exception {
    // try to search the published index
    Searchable[] searchables = { lookupRemote() };
    Searcher searcher = new MultiSearcher(searchables);
    ScoreDoc[] result = searcher.search(query, null, 1000).scoreDocs;

    assertEquals(1, result.length);
    Document document = searcher.doc(result[0].doc);
    assertTrue("document is null and it shouldn't be", document != null);
    assertEquals("test text", document.get("test"));
    assertTrue("document.getFields() Size: " + document.getFields().size() + " is not: " + 2, document.getFields().size() == 2);
    Set<String> ftl = new HashSet<String>();
    ftl.add("other");
    FieldSelector fs = new SetBasedFieldSelector(ftl, Collections.<String>emptySet());
    document = searcher.doc(0, fs);
    assertTrue("document is null and it shouldn't be", document != null);
    assertTrue("document.getFields() Size: " + document.getFields().size() + " is not: " + 1, document.getFields().size() == 1);
    fs = new MapFieldSelector(new String[]{"other"});
    document = searcher.doc(0, fs);
    assertTrue("document is null and it shouldn't be", document != null);
    assertTrue("document.getFields() Size: " + document.getFields().size() + " is not: " + 1, document.getFields().size() == 1);
  }

  @Test
  public void testTermQuery() throws Exception {
    search(new TermQuery(new Term("test", "test")));
  }

  @Test
  public void testBooleanQuery() throws Exception {
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("test", "test")), BooleanClause.Occur.MUST);
    search(query);
  }

  @Test
  public void testPhraseQuery() throws Exception {
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("test", "test"));
    query.add(new Term("test", "text"));
    search(query);
  }

  // Tests bug fix at http://nagoya.apache.org/bugzilla/show_bug.cgi?id=20290
  @Test
  public void testQueryFilter() throws Exception {
    // try to search the published index
    Searchable[] searchables = { lookupRemote() };
    Searcher searcher = new MultiSearcher(searchables);
    ScoreDoc[] hits = searcher.search(
          new TermQuery(new Term("test", "text")),
          new QueryWrapperFilter(new TermQuery(new Term("test", "test"))), 1000).scoreDocs;
    assertEquals(1, hits.length);
    ScoreDoc[] nohits = searcher.search(
          new TermQuery(new Term("test", "text")),
          new QueryWrapperFilter(new TermQuery(new Term("test", "non-existent-term"))), 1000).scoreDocs;
    assertEquals(0, nohits.length);
  }

  @Test
  public void testConstantScoreQuery() throws Exception {
    // try to search the published index
    Searchable[] searchables = { lookupRemote() };
    Searcher searcher = new MultiSearcher(searchables);
    ScoreDoc[] hits = searcher.search(
          new ConstantScoreQuery(new TermQuery(new Term("test", "test"))), null, 1000).scoreDocs;
    assertEquals(1, hits.length);
  }
}
