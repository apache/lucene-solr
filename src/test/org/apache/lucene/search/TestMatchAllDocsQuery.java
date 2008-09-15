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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests MatchAllDocsQuery.
 *
 */
public class TestMatchAllDocsQuery extends LuceneTestCase {

  public void testQuery() throws IOException {
    RAMDirectory dir = new RAMDirectory();
    IndexWriter iw = new IndexWriter(dir, new StandardAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    addDoc("one", iw);
    addDoc("two", iw);
    addDoc("three four", iw);
    iw.close();
    
    IndexSearcher is = new IndexSearcher(dir);
    ScoreDoc[] hits = is.search(new MatchAllDocsQuery(), null, 1000).scoreDocs;
    assertEquals(3, hits.length);

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
    
    is.close();
  }

  public void testEquals() {
    Query q1 = new MatchAllDocsQuery();
    Query q2 = new MatchAllDocsQuery();
    assertTrue(q1.equals(q2));
    q1.setBoost(1.5f);
    assertFalse(q1.equals(q2));
  }
  
  private void addDoc(String text, IndexWriter iw) throws IOException {
    Document doc = new Document();
    doc.add(new Field("key", text, Field.Store.YES, Field.Index.ANALYZED));
    iw.addDocument(doc);
  }

}
