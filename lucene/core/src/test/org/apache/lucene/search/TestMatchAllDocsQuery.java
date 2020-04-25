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
 * Tests MatchAllDocsQuery.
 *
 */
public class TestMatchAllDocsQuery extends LuceneTestCase {
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
    
    hits = is.search(new MatchAllDocsQuery(), 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals("one", is.doc(hits[0].doc).get("key"));
    assertEquals("two", is.doc(hits[1].doc).get("key"));
    assertEquals("three four", is.doc(hits[2].doc).get("key"));

    // some artificial queries to trigger the use of skipTo():
    
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
    hits = is.search(bq.build(), 1000).scoreDocs;
    assertEquals(3, hits.length);

    bq = new BooleanQuery.Builder();
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("key", "three")), BooleanClause.Occur.MUST);
    hits = is.search(bq.build(), 1000).scoreDocs;
    assertEquals(1, hits.length);

    iw.deleteDocuments(new Term("key", "one"));
    ir.close();
    ir = DirectoryReader.open(iw);
    is = newSearcher(ir);
    
    hits = is.search(new MatchAllDocsQuery(), 1000).scoreDocs;
    assertEquals(2, hits.length);

    iw.close();
    ir.close();
    dir.close();
  }

  public void testEquals() {
    Query q1 = new MatchAllDocsQuery();
    Query q2 = new MatchAllDocsQuery();
    assertTrue(q1.equals(q2));
  }
  
  private void addDoc(String text, IndexWriter iw) throws IOException {
    Document doc = new Document();
    Field f = newTextField("key", text, Field.Store.YES);
    doc.add(f);
    iw.addDocument(doc);
  }

  public void testEarlyTermination() throws IOException {

    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(analyzer).setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy()));
    final int numDocs = 500;
    for (int i = 0; i < numDocs; i++) {
      addDoc("doc" + i, iw);
    }
    IndexReader ir = DirectoryReader.open(iw);

    IndexSearcher is = newSearcher(ir);

    final int totalHitsThreshold = 200;
    TopScoreDocCollector c = TopScoreDocCollector.create(10, null, totalHitsThreshold);

    is.search(new MatchAllDocsQuery(), c);
    assertEquals(totalHitsThreshold+1, c.totalHits);
    assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, c.totalHitsRelation);

    TopScoreDocCollector c1 = TopScoreDocCollector.create(10, null, numDocs);

    is.search(new MatchAllDocsQuery(), c1);
    assertEquals(numDocs, c1.totalHits);
    assertEquals(TotalHits.Relation.EQUAL_TO, c1.totalHitsRelation);

    iw.close();
    ir.close();
    dir.close();

  }

}
