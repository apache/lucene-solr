/*
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
package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests MatchNoDocsQuery.
 */
public class TestMatchNoDocsQuery extends LuceneTestCase {
  private Analyzer analyzer;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new MockAnalyzer(random());
  }

  public void testSimple() throws Exception {
    MatchNoDocsQuery query = new MatchNoDocsQuery();
    assertEquals(query.toString(), "MatchNoDocsQuery(\"\")");
    query = new MatchNoDocsQuery("field 'title' not found");
    assertEquals(query.toString(), "MatchNoDocsQuery(\"field 'title' not found\")");
    Query rewrite = query.rewrite(null);
    assertTrue(rewrite instanceof MatchNoDocsQuery);
    assertEquals(rewrite.toString(), "MatchNoDocsQuery(\"field 'title' not found\")");
  }
  
  public void testQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(analyzer).setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy()));
    addDoc("one", iw);
    addDoc("two", iw);
    addDoc("three", iw);
    IndexReader ir = DirectoryReader.open(iw);
    IndexSearcher searcher = new IndexSearcher(ir);
    
    Query query = new MatchNoDocsQuery("field not found");
    assertEquals(searcher.count(query), 0);

    ScoreDoc[] hits;
    hits = searcher.search(new MatchNoDocsQuery(), 1000).scoreDocs;
    assertEquals(0, hits.length);
    assertEquals(query.toString(), "MatchNoDocsQuery(\"field not found\")");

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new BooleanClause(new TermQuery(new Term("key", "five")), BooleanClause.Occur.SHOULD));
    bq.add(new BooleanClause(new MatchNoDocsQuery("field not found"), BooleanClause.Occur.MUST));
    query = bq.build();
    assertEquals(searcher.count(query), 0);
    hits = searcher.search(new MatchNoDocsQuery(), 1000).scoreDocs;
    assertEquals(0, hits.length);
    assertEquals(query.toString(), "key:five +MatchNoDocsQuery(\"field not found\")");

    bq = new BooleanQuery.Builder();
    bq.add(new BooleanClause(new TermQuery(new Term("key", "one")), BooleanClause.Occur.SHOULD));
    bq.add(new BooleanClause(new MatchNoDocsQuery("field not found"), BooleanClause.Occur.SHOULD));
    query = bq.build();
    assertEquals(query.toString(), "key:one MatchNoDocsQuery(\"field not found\")");
    assertEquals(searcher.count(query), 1);
    hits = searcher.search(query, 1000).scoreDocs;
    Query rewrite = query.rewrite(ir);
    assertEquals(1, hits.length);
    assertEquals(rewrite.toString(), "key:one MatchNoDocsQuery(\"field not found\")");

    iw.close();
    ir.close();
    dir.close();
  }

  public void testEquals() {
    Query q1 = new MatchNoDocsQuery();
    Query q2 = new MatchNoDocsQuery();
    assertTrue(q1.equals(q2));
    QueryUtils.check(q1);
  }

  private void addDoc(String text, IndexWriter iw) throws IOException {
    Document doc = new Document();
    Field f = newTextField("key", text, Field.Store.YES);
    doc.add(f);
    iw.addDocument(doc);
  }
}
