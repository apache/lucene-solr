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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestSynonymQuery extends LuceneTestCase {

  public void testEquals() {
    QueryUtils.checkEqual(new SynonymQuery(), new SynonymQuery());
    QueryUtils.checkEqual(new SynonymQuery(new Term("foo", "bar")), 
                          new SynonymQuery(new Term("foo", "bar")));
    
    QueryUtils.checkEqual(new SynonymQuery(new Term("a", "a"), new Term("a", "b")), 
                          new SynonymQuery(new Term("a", "b"), new Term("a", "a")));
  }
  
  public void testBogusParams() {
    expectThrows(IllegalArgumentException.class, () -> {
      new SynonymQuery(new Term("field1", "a"), new Term("field2", "b"));
    });
  }

  public void testToString() {
    assertEquals("Synonym()", new SynonymQuery().toString());
    Term t1 = new Term("foo", "bar");
    assertEquals("Synonym(foo:bar)", new SynonymQuery(t1).toString());
    Term t2 = new Term("foo", "baz");
    assertEquals("Synonym(foo:bar foo:baz)", new SynonymQuery(t1, t2).toString());
  }

  public void testScores() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new StringField("f", "a", Store.NO));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new StringField("f", "b", Store.NO));
    for (int i = 0; i < 10; ++i) {
      w.addDocument(doc);
    }

    IndexReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);
    SynonymQuery query = new SynonymQuery(new Term("f", "a"), new Term("f", "b"));

    TopDocs topDocs = searcher.search(query, 20);
    assertEquals(11, topDocs.totalHits);
    // All docs must have the same score
    for (int i = 0; i < topDocs.scoreDocs.length; ++i) {
      assertEquals(topDocs.scoreDocs[0].score, topDocs.scoreDocs[i].score, 0.0f);
    }

    reader.close();
    w.close();
    dir.close();
  }

}
