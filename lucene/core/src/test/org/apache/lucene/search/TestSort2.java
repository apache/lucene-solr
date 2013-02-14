package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/*
 * Very simple tests of sorting.
 * 
 * THE RULES:
 * 1. keywords like 'abstract' and 'static' should not appear in this file.
 * 2. each test method should be self-contained and understandable. 
 * 3. no test methods should not share code with other test methods.
 * 4. no testing of things unrelated to sorting.
 * 5. no tracers.
 * 6. keyword 'class' should appear only once in this file, here ----
 *                                                                  |
 *        -----------------------------------------------------------
 *        |
 *       \./
 */
public class TestSort2 extends LuceneTestCase {

  public void testDemo() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("id", "1", Field.Store.YES));
    doc.add(newStringField("value", "foo", Field.Store.NO));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "2", Field.Store.YES));
    doc.add(newStringField("value", "bar", Field.Store.NO));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = new IndexSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.STRING));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 'bar' comes before 'foo'
    assertEquals("2", searcher.doc(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }
  
  public void testCountingCollector() throws Exception {
    Directory indexStore = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
    for(int i=0; i<5; i++) {
      Document doc = new Document();
      doc.add(new StringField("string", "a"+i, Field.Store.NO));
      doc.add(new StringField("string", "b"+i, Field.Store.NO));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);
    TotalHitCountCollector c = new TotalHitCountCollector();
    searcher.search(new MatchAllDocsQuery(), null, c);
    assertEquals(5, c.getTotalHits());
    reader.close();
    indexStore.close();
  }
  
  public void testEmptyStringVsNullStringSort() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(
                        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newStringField("f", "", Field.Store.NO));
    doc.add(newStringField("t", "1", Field.Store.NO));
    w.addDocument(doc);
    w.commit();
    doc = new Document();
    doc.add(newStringField("t", "1", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    IndexSearcher s = newSearcher(r);
    TopDocs hits = s.search(new TermQuery(new Term("t", "1")), null, 10, new Sort(new SortField("f", SortField.Type.STRING)));
    assertEquals(2, hits.totalHits);
    // null sorts first
    assertEquals(1, hits.scoreDocs[0].doc);
    assertEquals(0, hits.scoreDocs[1].doc);
    r.close();
    dir.close();
  }
  
  public void testLUCENE2142() throws IOException {
    Directory indexStore = newDirectory();
    IndexWriter writer = new IndexWriter(indexStore, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    for(int i=0; i<5; i++) {
        Document doc = new Document();
        doc.add(new StringField("string", "a"+i, Field.Store.NO));
        doc.add(new StringField("string", "b"+i, Field.Store.NO));
        writer.addDocument(doc);
    }
    writer.forceMerge(1); // enforce one segment to have a higher unique term count in all cases
    writer.close();
    Sort sort = new Sort(
        new SortField("string", SortField.Type.STRING),
        SortField.FIELD_DOC);
    // this should not throw AIOOBE or RuntimeEx
    IndexReader reader = DirectoryReader.open(indexStore);
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.search(new MatchAllDocsQuery(), null, 500, sort);
    reader.close();
    indexStore.close();
  }
}
