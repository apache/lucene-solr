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

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

/** Tests basic sorting on docvalues fields.
 * These are mostly like TestSort's tests, except each test
 * indexes the field up-front as docvalues, and checks no fieldcaches were made */
@SuppressCodecs("Lucene3x")
public class TestSortDocValues extends LuceneTestCase {
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // ensure there is nothing in fieldcache before test starts
    FieldCache.DEFAULT.purgeAllCaches();
  }
  
  private void assertNoFieldCaches() {
    // docvalues sorting should NOT create any fieldcache entries!
    assertEquals(0, FieldCache.DEFAULT.getCacheEntries().length);
  }

  /** Tests sorting on type string */
  public void testString() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("value", new BytesRef("foo")));
    doc.add(newStringField("value", "foo", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("value", "bar", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.STRING));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 'bar' comes before 'foo'
    assertEquals("bar", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("foo", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNoFieldCaches();
    
    ir.close();
    dir.close();
  }
  
  /** Tests reverse sorting on type string */
  public void testStringReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("value", "bar", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("value", new BytesRef("foo")));
    doc.add(newStringField("value", "foo", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.STRING, true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 'foo' comes after 'bar' in reverse order
    assertEquals("foo", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("bar", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type string_val */
  public void testStringVal() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("value", new BytesRef("foo")));
    doc.add(newStringField("value", "foo", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new BinaryDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("value", "bar", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.STRING_VAL));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 'bar' comes before 'foo'
    assertEquals("bar", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("foo", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests reverse sorting on type string_val */
  public void testStringValReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("value", "bar", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new BinaryDocValuesField("value", new BytesRef("foo")));
    doc.add(newStringField("value", "foo", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.STRING_VAL, true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 'foo' comes after 'bar' in reverse order
    assertEquals("foo", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("bar", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type string_val, but with a SortedDocValuesField */
  public void testStringValSorted() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("value", new BytesRef("foo")));
    doc.add(newStringField("value", "foo", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("value", "bar", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.STRING_VAL));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 'bar' comes before 'foo'
    assertEquals("bar", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("foo", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests reverse sorting on type string_val, but with a SortedDocValuesField */
  public void testStringValReverseSorted() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("value", new BytesRef("bar")));
    doc.add(newStringField("value", "bar", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("value", new BytesRef("foo")));
    doc.add(newStringField("value", "foo", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.STRING_VAL, true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // 'foo' comes after 'bar' in reverse order
    assertEquals("foo", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("bar", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type byte */
  public void testByte() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("value", 23));
    doc.add(newStringField("value", "23", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", -1));
    doc.add(newStringField("value", "-1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", 4));
    doc.add(newStringField("value", "4", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.BYTE));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("23", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type byte in reverse */
  public void testByteReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("value", 23));
    doc.add(newStringField("value", "23", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", -1));
    doc.add(newStringField("value", "-1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", 4));
    doc.add(newStringField("value", "4", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.BYTE, true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // reverse numeric order
    assertEquals("23", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("-1", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type short */
  public void testShort() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("value", 300));
    doc.add(newStringField("value", "300", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", -1));
    doc.add(newStringField("value", "-1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", 4));
    doc.add(newStringField("value", "4", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.SHORT));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("300", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type short in reverse */
  public void testShortReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("value", 300));
    doc.add(newStringField("value", "300", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", -1));
    doc.add(newStringField("value", "-1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", 4));
    doc.add(newStringField("value", "4", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.SHORT, true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // reverse numeric order
    assertEquals("300", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("-1", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type int */
  public void testInt() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("value", 300000));
    doc.add(newStringField("value", "300000", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", -1));
    doc.add(newStringField("value", "-1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", 4));
    doc.add(newStringField("value", "4", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.INT));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("300000", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type int in reverse */
  public void testIntReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("value", 300000));
    doc.add(newStringField("value", "300000", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", -1));
    doc.add(newStringField("value", "-1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", 4));
    doc.add(newStringField("value", "4", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.INT, true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // reverse numeric order
    assertEquals("300000", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("-1", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type long */
  public void testLong() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("value", 3000000000L));
    doc.add(newStringField("value", "3000000000", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", -1));
    doc.add(newStringField("value", "-1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", 4));
    doc.add(newStringField("value", "4", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.LONG));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("3000000000", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type long in reverse */
  public void testLongReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new NumericDocValuesField("value", 3000000000L));
    doc.add(newStringField("value", "3000000000", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", -1));
    doc.add(newStringField("value", "-1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("value", 4));
    doc.add(newStringField("value", "4", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.LONG, true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // reverse numeric order
    assertEquals("3000000000", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("-1", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type float */
  public void testFloat() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new FloatDocValuesField("value", 30.1F));
    doc.add(newStringField("value", "30.1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FloatDocValuesField("value", -1.3F));
    doc.add(newStringField("value", "-1.3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FloatDocValuesField("value", 4.2F));
    doc.add(newStringField("value", "4.2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.FLOAT));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("30.1", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type float in reverse */
  public void testFloatReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new FloatDocValuesField("value", 30.1F));
    doc.add(newStringField("value", "30.1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FloatDocValuesField("value", -1.3F));
    doc.add(newStringField("value", "-1.3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FloatDocValuesField("value", 4.2F));
    doc.add(newStringField("value", "4.2", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.FLOAT, true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // reverse numeric order
    assertEquals("30.1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("-1.3", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type double */
  public void testDouble() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new DoubleDocValuesField("value", 30.1));
    doc.add(newStringField("value", "30.1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new DoubleDocValuesField("value", -1.3));
    doc.add(newStringField("value", "-1.3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new DoubleDocValuesField("value", 4.2333333333333));
    doc.add(newStringField("value", "4.2333333333333", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new DoubleDocValuesField("value", 4.2333333333332));
    doc.add(newStringField("value", "4.2333333333332", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.DOUBLE));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(4, td.totalHits);
    // numeric order
    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2333333333332", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("4.2333333333333", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertEquals("30.1", searcher.doc(td.scoreDocs[3].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type double with +/- zero */
  public void testDoubleSignedZero() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new DoubleDocValuesField("value", +0D));
    doc.add(newStringField("value", "+0", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new DoubleDocValuesField("value", -0D));
    doc.add(newStringField("value", "-0", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.DOUBLE));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // numeric order
    assertEquals("-0", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("+0", searcher.doc(td.scoreDocs[1].doc).get("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type double in reverse */
  public void testDoubleReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new DoubleDocValuesField("value", 30.1));
    doc.add(newStringField("value", "30.1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new DoubleDocValuesField("value", -1.3));
    doc.add(newStringField("value", "-1.3", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new DoubleDocValuesField("value", 4.2333333333333));
    doc.add(newStringField("value", "4.2333333333333", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new DoubleDocValuesField("value", 4.2333333333332));
    doc.add(newStringField("value", "4.2333333333332", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortField("value", SortField.Type.DOUBLE, true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(4, td.totalHits);
    // numeric order
    assertEquals("30.1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2333333333333", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("4.2333333333332", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertEquals("-1.3", searcher.doc(td.scoreDocs[3].doc).get("value"));
    assertNoFieldCaches();

    ir.close();
    dir.close();
  }
}
