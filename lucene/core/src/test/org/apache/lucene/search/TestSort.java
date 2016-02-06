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
public class TestSort extends LuceneTestCase {

  private void assertEquals(Sort a, Sort b) {
    LuceneTestCase.assertEquals(a, b);
    LuceneTestCase.assertEquals(b, a);
    LuceneTestCase.assertEquals(a.hashCode(), b.hashCode());
  }

  private void assertDifferent(Sort a, Sort b) {
    assertFalse(a.equals(b));
    assertFalse(b.equals(a));
    assertFalse(a.hashCode() == b.hashCode());
  }

  public void testEquals() {
    SortField sortField1 = new SortField("foo", SortField.Type.STRING);
    SortField sortField2 = new SortField("foo", SortField.Type.STRING);
    assertEquals(new Sort(sortField1), new Sort(sortField2));

    sortField2 = new SortField("bar", SortField.Type.STRING);
    assertDifferent(new Sort(sortField1), new Sort(sortField2));

    sortField2 = new SortField("foo", SortField.Type.LONG);
    assertDifferent(new Sort(sortField1), new Sort(sortField2));

    sortField2 = new SortField("foo", SortField.Type.STRING);
    sortField2.setMissingValue(SortField.STRING_FIRST);
    assertDifferent(new Sort(sortField1), new Sort(sortField2));

    sortField2 = new SortField("foo", SortField.Type.STRING, false);
    assertEquals(new Sort(sortField1), new Sort(sortField2));

    sortField2 = new SortField("foo", SortField.Type.STRING, true);
    assertDifferent(new Sort(sortField1), new Sort(sortField2));
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

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type int with a missing value */
  public void testIntMissing() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
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
    // null is treated as a 0
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type int, specifying the missing value should be treated as Integer.MAX_VALUE */
  public void testIntMissingLast() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
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
    SortField sortField = new SortField("value", SortField.Type.INT);
    sortField.setMissingValue(Integer.MAX_VALUE);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // null is treated as a Integer.MAX_VALUE
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).get("value"));

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

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type long with a missing value */
  public void testLongMissing() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
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
    // null is treated as 0
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type long, specifying the missing value should be treated as Long.MAX_VALUE */
  public void testLongMissingLast() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
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
    SortField sortField = new SortField("value", SortField.Type.LONG);
    sortField.setMissingValue(Long.MAX_VALUE);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // null is treated as Long.MAX_VALUE
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).get("value"));

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

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type float with a missing value */
  public void testFloatMissing() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
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
    // null is treated as 0
    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[2].doc).get("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type float, specifying the missing value should be treated as Float.MAX_VALUE */
  public void testFloatMissingLast() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
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
    SortField sortField = new SortField("value", SortField.Type.FLOAT);
    sortField.setMissingValue(Float.MAX_VALUE);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // null is treated as Float.MAX_VALUE
    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).get("value"));

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

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type double with a missing value */
  public void testDoubleMissing() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
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
    // null treated as a 0
    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("4.2333333333332", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertEquals("4.2333333333333", searcher.doc(td.scoreDocs[3].doc).get("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type double, specifying the missing value should be treated as Double.MAX_VALUE */
  public void testDoubleMissingLast() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
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
    SortField sortField = new SortField("value", SortField.Type.DOUBLE);
    sortField.setMissingValue(Double.MAX_VALUE);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(4, td.totalHits);
    // null treated as Double.MAX_VALUE
    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).get("value"));
    assertEquals("4.2333333333332", searcher.doc(td.scoreDocs[1].doc).get("value"));
    assertEquals("4.2333333333333", searcher.doc(td.scoreDocs[2].doc).get("value"));
    assertNull(searcher.doc(td.scoreDocs[3].doc).get("value"));

    ir.close();
    dir.close();
  }

  /** Tests sorting on multiple sort fields */
  public void testMultiSort() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new SortedDocValuesField("value1", new BytesRef("foo")));
    doc.add(new NumericDocValuesField("value2", 0));
    doc.add(newStringField("value1", "foo", Field.Store.YES));
    doc.add(newStringField("value2", "0", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("value1", new BytesRef("bar")));
    doc.add(new NumericDocValuesField("value2", 1));
    doc.add(newStringField("value1", "bar", Field.Store.YES));
    doc.add(newStringField("value2", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("value1", new BytesRef("bar")));
    doc.add(new NumericDocValuesField("value2", 0));
    doc.add(newStringField("value1", "bar", Field.Store.YES));
    doc.add(newStringField("value2", "0", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new SortedDocValuesField("value1", new BytesRef("foo")));
    doc.add(new NumericDocValuesField("value2", 1));
    doc.add(newStringField("value1", "foo", Field.Store.YES));
    doc.add(newStringField("value2", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(
        new SortField("value1", SortField.Type.STRING),
        new SortField("value2", SortField.Type.LONG));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(4, td.totalHits);
    // 'bar' comes before 'foo'
    assertEquals("bar", searcher.doc(td.scoreDocs[0].doc).get("value1"));
    assertEquals("bar", searcher.doc(td.scoreDocs[1].doc).get("value1"));
    assertEquals("foo", searcher.doc(td.scoreDocs[2].doc).get("value1"));
    assertEquals("foo", searcher.doc(td.scoreDocs[3].doc).get("value1"));
    // 0 comes before 1
    assertEquals("0", searcher.doc(td.scoreDocs[0].doc).get("value2"));
    assertEquals("1", searcher.doc(td.scoreDocs[1].doc).get("value2"));
    assertEquals("0", searcher.doc(td.scoreDocs[2].doc).get("value2"));
    assertEquals("1", searcher.doc(td.scoreDocs[3].doc).get("value2"));

    // Now with overflow
    td = searcher.search(new MatchAllDocsQuery(), 1, sort);
    assertEquals(4, td.totalHits);
    assertEquals("bar", searcher.doc(td.scoreDocs[0].doc).get("value1"));
    assertEquals("0", searcher.doc(td.scoreDocs[0].doc).get("value2"));

    ir.close();
    dir.close();
  }
}
