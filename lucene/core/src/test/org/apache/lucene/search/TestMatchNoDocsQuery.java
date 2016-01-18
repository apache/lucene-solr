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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
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

  public void testQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(analyzer).setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy()));
    addDoc("one", iw);
    addDoc("two", iw);
    addDoc("three four", iw);
    IndexReader ir = DirectoryReader.open(iw);

    IndexSearcher is = newSearcher(ir);
    ScoreDoc[] hits;

    hits = is.search(new MatchNoDocsQuery(), 1000).scoreDocs;
    assertEquals(0, hits.length);

    // A MatchNoDocsQuery rewrites to an empty BooleanQuery
    MatchNoDocsQuery mndq = new MatchNoDocsQuery();
    Query rewritten = mndq.rewrite(ir);
    assertTrue(rewritten instanceof BooleanQuery);
    assertEquals(0, ((BooleanQuery) rewritten).clauses().size());
    hits = is.search(mndq, 1000).scoreDocs;
    assertEquals(0, hits.length);

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
