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
package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.store.RAMDirectory;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;

/**
 * Tests MatchAllDocsQuery.
 *
 */
public class TestMatchAllDocsQuery extends LuceneTestCase {
  private Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
  
  public void testQuery() throws Exception {

    RAMDirectory dir = new RAMDirectory();
    IndexWriter iw = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
    iw.setMaxBufferedDocs(2);  // force multi-segment
    addDoc("one", iw, 1f);
    addDoc("two", iw, 20f);
    addDoc("three four", iw, 300f);
    iw.close();

    IndexReader ir = IndexReader.open(dir, false);
    IndexSearcher is = new IndexSearcher(ir);
    ScoreDoc[] hits;

    // assert with norms scoring turned off

    hits = is.search(new MatchAllDocsQuery(), null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals("one", ir.document(hits[0].doc).get("key"));
    assertEquals("two", ir.document(hits[1].doc).get("key"));
    assertEquals("three four", ir.document(hits[2].doc).get("key"));

    // assert with norms scoring turned on

    MatchAllDocsQuery normsQuery = new MatchAllDocsQuery("key");
    hits = is.search(normsQuery, null, 1000).scoreDocs;
    assertEquals(3, hits.length);

    assertEquals("three four", ir.document(hits[0].doc).get("key"));    
    assertEquals("two", ir.document(hits[1].doc).get("key"));
    assertEquals("one", ir.document(hits[2].doc).get("key"));

    // change norm & retest
    ir.setNorm(0, "key", 400f);
    normsQuery = new MatchAllDocsQuery("key");
    hits = is.search(normsQuery, null, 1000).scoreDocs;
    assertEquals(3, hits.length);

    assertEquals("one", ir.document(hits[0].doc).get("key"));
    assertEquals("three four", ir.document(hits[1].doc).get("key"));    
    assertEquals("two", ir.document(hits[2].doc).get("key"));
    
    // some artificial queries to trigger the use of skipTo():
    
    BooleanQuery bq = new BooleanQuery();
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
    hits = is.search(bq, null, 1000).scoreDocs;
    assertEquals(3, hits.length);

    bq = new BooleanQuery();
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term("key", "three")), BooleanClause.Occur.MUST);
    hits = is.search(bq, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    // delete a document:
    is.getIndexReader().deleteDocument(0);
    hits = is.search(new MatchAllDocsQuery(), null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    
    // test parsable toString()
    QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, "key", analyzer);
    hits = is.search(qp.parse(new MatchAllDocsQuery().toString()), null, 1000).scoreDocs;
    assertEquals(2, hits.length);

    // test parsable toString() with non default boost
    Query maq = new MatchAllDocsQuery();
    maq.setBoost(2.3f);
    Query pq = qp.parse(maq.toString());
    hits = is.search(pq, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    
    is.close();
    ir.close();
    dir.close();
  }

  public void testEquals() {
    Query q1 = new MatchAllDocsQuery();
    Query q2 = new MatchAllDocsQuery();
    assertTrue(q1.equals(q2));
    q1.setBoost(1.5f);
    assertFalse(q1.equals(q2));
  }
  
  private void addDoc(String text, IndexWriter iw, float boost) throws IOException {
    Document doc = new Document();
    Field f = new Field("key", text, Field.Store.YES, Field.Index.ANALYZED);
    f.setBoost(boost);
    doc.add(f);
    iw.addDocument(doc);
  }

}
