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
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
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
 * 3. no test methods should share code with other test methods.
 * 4. no testing of things unrelated to sorting.
 * 5. no tracers.
 * 6. keyword 'class' should appear only once in this file, here ----
 *                                                                  |
 *        -----------------------------------------------------------
 *        |
 *       \./
 */
public class TestSort2 extends LuceneTestCase {

  /** Tests sorting on type string */
  public void testString() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("value", "foo", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "bar", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = new IndexSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.STRING));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 'bar' comes before 'foo'
    assertEquals("bar", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("foo", searcher.doc(td.scoreDocs[1].doc).get("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on internal docid order */
  public void testFieldDoc() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("value", "foo", Field.Store.NO));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "bar", Field.Store.NO));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = new IndexSearcher(ir);
    Sort sort = new Sort(SortField.FIELD_DOC);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // docid 0, then docid 1
    assertEquals(0, td.scoreDocs[0].doc);
    assertEquals(1, td.scoreDocs[1].doc);

    ir.close();
    dir.close();
  }
  
  /** Tests default sort (by score) */
  public void testFieldScore() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("value", "foo bar bar bar bar", Field.Store.NO));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newTextField("value", "foo foo foo foo foo", Field.Store.NO));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = new IndexSearcher(ir);
    Sort sort = new Sort();

    TopDocs actual = searcher.search(new TermQuery(new Term("value", "foo")), 10, sort);
    assertEquals(2, actual.totalHits);

    TopDocs expected = searcher.search(new TermQuery(new Term("value", "foo")), 10);
    // the two topdocs should be the same
    assertEquals(expected.totalHits, actual.totalHits);
    for (int i = 0; i < actual.scoreDocs.length; i++) {
      assertEquals(actual.scoreDocs[i].doc, expected.scoreDocs[i].doc);
    }

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type byte */
  public void testByte() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("value", "23", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "-1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "4", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = new IndexSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.BYTE));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("23", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type short */
  public void testShort() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("value", "300", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "-1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "4", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = new IndexSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.SHORT));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("300", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type int */
  public void testInt() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("value", "300000", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "-1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "4", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = new IndexSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.INT));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("300000", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type long */
  public void testLong() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("value", "3000000000", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "-1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "4", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = new IndexSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.LONG));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("3000000000", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type float */
  public void testFloat() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("value", "30.1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "-1.3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "4.2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = new IndexSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.FLOAT));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("30.1", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type double */
  public void testDouble() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newStringField("value", "30.1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "-1.3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "4.2333333333333", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("value", "4.2333333333332", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = new IndexSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.DOUBLE));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(4, td.totalHits);
    // numeric order
    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2333333333332", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("4.2333333333333", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertEquals("30.1", searcher.doc(td.scoreDocs[3].doc).get("value"));

    ir.close();
    dir.close();
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
  
  public void testMaxScore() throws Exception {
    Directory d = newDirectory();
    // Not RIW because we need exactly 2 segs:
    IndexWriter w = new IndexWriter(d, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    int id = 0;
    for(int seg=0;seg<2;seg++) {
      for(int docIDX=0;docIDX<10;docIDX++) {
        Document doc = new Document();
        doc.add(newStringField("id", ""+docIDX, Field.Store.YES));
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<id;i++) {
          sb.append(' ');
          sb.append("text");
        }
        doc.add(newTextField("body", sb.toString(), Field.Store.NO));
        w.addDocument(doc);
        id++;
      }
      w.commit();
    }

    IndexReader r = DirectoryReader.open(w, true);
    w.close();
    Query q = new TermQuery(new Term("body", "text"));
    IndexSearcher s = newSearcher(r);
    float maxScore = s.search(q , 10).getMaxScore();
    assertEquals(maxScore, s.search(q, null, 3, Sort.INDEXORDER, random().nextBoolean(), true).getMaxScore(), 0.0);
    assertEquals(maxScore, s.search(q, null, 3, Sort.RELEVANCE, random().nextBoolean(), true).getMaxScore(), 0.0);
    assertEquals(maxScore, s.search(q, null, 3, new Sort(new SortField[] {new SortField("id", SortField.Type.INT, false)}), random().nextBoolean(), true).getMaxScore(), 0.0);
    assertEquals(maxScore, s.search(q, null, 3, new Sort(new SortField[] {new SortField("id", SortField.Type.INT, true)}), random().nextBoolean(), true).getMaxScore(), 0.0);
    r.close();
    d.close();
  }
  
  /** test sorts when there's nothing in the index */
  public void testEmptyIndex() throws Exception {
    IndexSearcher empty = new IndexSearcher(new MultiReader());
    Query query = new TermQuery(new Term("contents", "foo"));
  
    Sort sort = new Sort();
    TopDocs td = empty.search(query, null, 10, sort, true, true);
    assertEquals(0, td.totalHits);

    sort.setSort(SortField.FIELD_DOC);
    td = empty.search(query, null, 10, sort, true, true);
    assertEquals(0, td.totalHits);

    sort.setSort(new SortField("int", SortField.Type.INT), SortField.FIELD_DOC);
    td = empty.search(query, null, 10, sort, true, true);
    assertEquals(0, td.totalHits);
    
    sort.setSort(new SortField("string", SortField.Type.STRING, true), SortField.FIELD_DOC);
    td = empty.search(query, null, 10, sort, true, true);
    assertEquals(0, td.totalHits);
    
    sort.setSort(new SortField("string_val", SortField.Type.STRING_VAL, true), SortField.FIELD_DOC);
    td = empty.search(query, null, 10, sort, true, true);
    assertEquals(0, td.totalHits);

    sort.setSort(new SortField("float", SortField.Type.FLOAT), new SortField("string", SortField.Type.STRING));
    td = empty.search(query, null, 10, sort, true, true);
    assertEquals(0, td.totalHits);
  }
}
