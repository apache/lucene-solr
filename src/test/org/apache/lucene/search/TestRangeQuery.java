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

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

import junit.framework.TestCase;
import java.io.IOException;

/**
 * @author goller
 */
public class TestRangeQuery extends TestCase {

  private int docCount = 0;
  private RAMDirectory dir;

  public void setUp() {
    dir = new RAMDirectory();
  }

  public void testExclusive() throws Exception {
    Query query = new RangeQuery(new Term("content", "A"),
                                 new Term("content", "C"),
                                 false);
    initializeIndex(new String[] {"A", "B", "C", "D"});
    IndexSearcher searcher = new IndexSearcher(dir);
    Hits hits = searcher.search(query);
    assertEquals("A,B,C,D, only B in range", 1, hits.length());
    searcher.close();

    initializeIndex(new String[] {"A", "B", "D"});
    searcher = new IndexSearcher(dir);
    hits = searcher.search(query);
    assertEquals("A,B,D, only B in range", 1, hits.length());
    searcher.close();

    addDoc("C");
    searcher = new IndexSearcher(dir);
    hits = searcher.search(query);
    assertEquals("C added, still only B in range", 1, hits.length());
    searcher.close();
  }

  public void testInclusive() throws Exception {
    Query query = new RangeQuery(new Term("content", "A"),
                                 new Term("content", "C"),
                                 true);

    initializeIndex(new String[]{"A", "B", "C", "D"});
    IndexSearcher searcher = new IndexSearcher(dir);
    Hits hits = searcher.search(query);
    assertEquals("A,B,C,D - A,B,C in range", 3, hits.length());
    searcher.close();

    initializeIndex(new String[]{"A", "B", "D"});
    searcher = new IndexSearcher(dir);
    hits = searcher.search(query);
    assertEquals("A,B,D - A and B in range", 2, hits.length());
    searcher.close();

    addDoc("C");
    searcher = new IndexSearcher(dir);
    hits = searcher.search(query);
    assertEquals("C added - A, B, C in range", 3, hits.length());
    searcher.close();
  }

  public void testEqualsHashcode() {
    Query query = new RangeQuery(new Term("content", "A"),
                                 new Term("content", "C"),
                                 true);
    query.setBoost(1.0f);
    Query other = new RangeQuery(new Term("content", "A"),
                                 new Term("content", "C"),
                                 true);
    other.setBoost(1.0f);

    assertEquals("query equals itself is true", query, query);
    assertEquals("equivalent queries are equal", query, other);
    assertEquals("hashcode must return same value when equals is true", query.hashCode(), other.hashCode());

    other.setBoost(2.0f);
    assertFalse("Different boost queries are not equal", query.equals(other));

    other = new RangeQuery(new Term("notcontent", "A"), new Term("notcontent", "C"), true);
    assertFalse("Different fields are not equal", query.equals(other));

    other = new RangeQuery(new Term("content", "X"), new Term("content", "C"), true);
    assertFalse("Different lower terms are not equal", query.equals(other));

    other = new RangeQuery(new Term("content", "A"), new Term("content", "Z"), true);
    assertFalse("Different upper terms are not equal", query.equals(other));

    query = new RangeQuery(null, new Term("content", "C"), true);
    other = new RangeQuery(null, new Term("content", "C"), true);
    assertEquals("equivalent queries with null lowerterms are equal()", query, other);
    assertEquals("hashcode must return same value when equals is true", query.hashCode(), other.hashCode());

    query = new RangeQuery(new Term("content", "C"), null, true);
    other = new RangeQuery(new Term("content", "C"), null, true);
    assertEquals("equivalent queries with null upperterms are equal()", query, other);
    assertEquals("hashcode returns same value", query.hashCode(), other.hashCode());

    query = new RangeQuery(null, new Term("content", "C"), true);
    other = new RangeQuery(new Term("content", "C"), null, true);
    assertFalse("queries with different upper and lower terms are not equal", query.equals(other));

    query = new RangeQuery(new Term("content", "A"), new Term("content", "C"), false);
    other = new RangeQuery(new Term("content", "A"), new Term("content", "C"), true);
    assertFalse("queries with different inclusive are not equal", query.equals(other));
  }

  private void initializeIndex(String[] values) throws IOException {
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
    for (int i = 0; i < values.length; i++) {
      insertDoc(writer, values[i]);
    }
    writer.close();
  }

  private void addDoc(String content) throws IOException {
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
    insertDoc(writer, content);
    writer.close();
  }

  private void insertDoc(IndexWriter writer, String content) throws IOException {
    Document doc = new Document();

    doc.add(new Field("id", "id" + docCount, Field.Store.YES, Field.Index.UN_TOKENIZED));
    doc.add(new Field("content", content, Field.Store.NO, Field.Index.TOKENIZED));

    writer.addDocument(doc);
    docCount++;
  }
}


