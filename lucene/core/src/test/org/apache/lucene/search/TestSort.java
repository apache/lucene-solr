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
import org.apache.lucene.document.Document2;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DocValuesType;
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
  
  /** Tests sorting on type string */
  public void testString() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    Document2 doc = writer.newDocument();
    doc.addAtom("value", "foo");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addAtom("value", "bar");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

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
    FieldTypes fieldTypes = writer.getFieldTypes();
    Document2 doc = writer.newDocument();
    doc.addAtom("value", "bar");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addAtom("value", "foo");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value", true);

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
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setDocValuesType("value", DocValuesType.BINARY);
    fieldTypes.enableSorting("value");
    
    Document2 doc = writer.newDocument();
    doc.addAtom("value", "foo");
    writer.addDocument(doc);
    doc = writer.newDocument();
    doc.addAtom("value", "bar");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

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
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setDocValuesType("value", DocValuesType.BINARY);
    fieldTypes.enableSorting("value");

    Document2 doc = writer.newDocument();
    doc.addAtom("value", "bar");
    writer.addDocument(doc);

    doc = writer.newDocument();
    doc.addAtom("value", "foo");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value", true);

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
    Document2 doc = writer.newDocument();
    doc.addAtom("value", "foo");
    writer.addDocument(doc);

    doc = writer.newDocument();
    doc.addAtom("value", "bar");
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);

    // NOTE: don't ask FieldTypes here, because we are forcing STRING_VAL even though we indexed SORTED DV:
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
    Document2 doc = writer.newDocument();
    doc.addAtom("value", "bar");
    writer.addDocument(doc);

    doc = writer.newDocument();
    doc.addAtom("value", "foo");
    writer.addDocument(doc);

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    // NOTE: don't ask FieldTypes here, because we are forcing STRING_VAL even though we indexed SORTED DV:
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
    FieldTypes fieldTypes = writer.getFieldTypes();
    
    for(int value : new int[] {300000, -1, 4}) {
      Document2 doc = writer.newDocument();
      doc.addInt("value", value);
      writer.addDocument(doc);
    }

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertEquals("300000", searcher.doc(td.scoreDocs[2].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type int in reverse */
  public void testIntReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    for(int value : new int[] {300000, -1, 4}) {
      Document2 doc = writer.newDocument();
      doc.addInt("value", value);
      writer.addDocument(doc);
    }

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value", true);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // reverse numeric order
    assertEquals("300000", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertEquals("-1", searcher.doc(td.scoreDocs[2].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type int with a missing value */
  public void testIntMissingDefault() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    writer.addDocument(writer.newDocument());
    for(int value : new int[] {-1, 4}) {
      Document2 doc = writer.newDocument();
      doc.addInt("value", value);
      writer.addDocument(doc);
    }

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // sort missing last by default:
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type int, specifying the missing value should be treated as Integer.MAX_VALUE */
  public void testIntMissingLast() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setSortMissingLast("value");
    writer.addDocument(writer.newDocument());
    for(int value : new int[] {-1, 4}) {
      Document2 doc = writer.newDocument();
      doc.addInt("value", value);
      writer.addDocument(doc);
    }

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type long */
  public void testLong() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    for(long value : new long[] {3000000000L, -1L, 4L}) {
      Document2 doc = writer.newDocument();
      doc.addLong("value", value);
      writer.addDocument(doc);
    }
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertEquals("3000000000", searcher.doc(td.scoreDocs[2].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type long in reverse */
  public void testLongReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    for(long value : new long[] {3000000000L, -1L, 4L}) {
      Document2 doc = writer.newDocument();
      doc.addLong("value", value);
      writer.addDocument(doc);
    }
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value", true);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // reverse numeric order
    assertEquals("3000000000", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertEquals("-1", searcher.doc(td.scoreDocs[2].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type long with a missing value */
  public void testLongMissingDefault() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    writer.addDocument(writer.newDocument());
    for(long value : new long[] {-1L, 4L}) {
      Document2 doc = writer.newDocument();
      doc.addLong("value", value);
      writer.addDocument(doc);
    }

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // sort missing last by default:
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type long, specifying the missing value should be treated as Long.MAX_VALUE */
  public void testLongMissingLast() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setSortMissingLast("value");
    writer.addDocument(writer.newDocument());
    for(long value : new long[] {-1L, 4L}) {
      Document2 doc = writer.newDocument();
      doc.addLong("value", value);
      writer.addDocument(doc);
    }

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    assertEquals("-1", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type float */
  public void testFloat() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);    
    FieldTypes fieldTypes = writer.getFieldTypes();
    for(float value : new float[] {30.1F, -1.3F, 4.2F}) {
      Document2 doc = writer.newDocument();
      doc.addFloat("value", value);
      writer.addDocument(doc);
    }

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertEquals("30.1", searcher.doc(td.scoreDocs[2].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type float in reverse */
  public void testFloatReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);    
    FieldTypes fieldTypes = writer.getFieldTypes();
    for(float value : new float[] {30.1F, -1.3F, 4.2F}) {
      Document2 doc = writer.newDocument();
      doc.addFloat("value", value);
      writer.addDocument(doc);
    }

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value", true);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);
    // numeric order
    assertEquals("30.1", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertEquals("-1.3", searcher.doc(td.scoreDocs[2].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type float with a missing value */
  public void testFloatMissingDefault() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);    
    FieldTypes fieldTypes = writer.getFieldTypes();
    writer.addDocument(writer.newDocument());
    for(float value : new float[] {-1.3F, 4.2F}) {
      Document2 doc = writer.newDocument();
      doc.addFloat("value", value);
      writer.addDocument(doc);
    }

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);

    // sort missing last by default:
    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type float, specifying the missing value should be treated as Float.MAX_VALUE */
  public void testFloatMissingLast() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);    
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.setSortMissingLast("value");
    writer.addDocument(writer.newDocument());
    for(float value : new float[] {-1.3F, 4.2F}) {
      Document2 doc = writer.newDocument();
      doc.addFloat("value", value);
      writer.addDocument(doc);
    }

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits);

    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4.2", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertNull(searcher.doc(td.scoreDocs[2].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type double */
  public void testDouble() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();

    for(double value : new double[] {30.1, -1.3, 4.2333333333333, 4.2333333333332}) {
      Document2 doc = writer.newDocument();
      doc.addDouble("value", value);
      writer.addDocument(doc);
    }
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(4, td.totalHits);
    // numeric order
    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4.2333333333332", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertEquals("4.2333333333333", searcher.doc(td.scoreDocs[2].doc).getString("value"));
    assertEquals("30.1", searcher.doc(td.scoreDocs[3].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type double with +/- zero */
  public void testDoubleSignedZero() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    for(double value : new double[] {+0D, -0D}) {
      Document2 doc = writer.newDocument();
      doc.addDouble("value", value);
      writer.addDocument(doc);
    }

    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits);
    // numeric order
    assertEquals(-0D, searcher.doc(td.scoreDocs[0].doc).getDouble("value").doubleValue(), 0.0D);
    assertEquals(+0D, searcher.doc(td.scoreDocs[1].doc).getDouble("value").doubleValue(), 0.0D);

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type double in reverse */
  public void testDoubleReverse() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();

    for(double value : new double[] {30.1, -1.3, 4.2333333333333, 4.2333333333332}) {
      Document2 doc = writer.newDocument();
      doc.addDouble("value", value);
      writer.addDocument(doc);
    }
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value", true);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(4, td.totalHits);
    // numeric order
    assertEquals("30.1", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4.2333333333333", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertEquals("4.2333333333332", searcher.doc(td.scoreDocs[2].doc).getString("value"));
    assertEquals("-1.3", searcher.doc(td.scoreDocs[3].doc).getString("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type double with a missing value */
  public void testDoubleMissingDefault() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    writer.addDocument(writer.newDocument());
    for(double value : new double[] {-1.3, 4.2333333333333, 4.2333333333332}) {
      Document2 doc = writer.newDocument();
      doc.addDouble("value", value);
      writer.addDocument(doc);
    }
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(4, td.totalHits);

    // sort missing last by default:
    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4.2333333333332", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertEquals("4.2333333333333", searcher.doc(td.scoreDocs[2].doc).getString("value"));
    assertNull(searcher.doc(td.scoreDocs[3].doc).get("value"));

    ir.close();
    dir.close();
  }
  
  /** Tests sorting on type double, specifying the missing value should be treated as Double.MAX_VALUE */
  public void testDoubleMissingLast() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    FieldTypes fieldTypes = writer.getFieldTypes();
    writer.addDocument(writer.newDocument());
    for(double value : new double[] {-1.3, 4.2333333333333, 4.2333333333332}) {
      Document2 doc = writer.newDocument();
      doc.addDouble("value", value);
      writer.addDocument(doc);
    }
    IndexReader ir = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(ir);
    Sort sort = fieldTypes.newSort("value");

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(4, td.totalHits);

    assertEquals("-1.3", searcher.doc(td.scoreDocs[0].doc).getString("value"));
    assertEquals("4.2333333333332", searcher.doc(td.scoreDocs[1].doc).getString("value"));
    assertEquals("4.2333333333333", searcher.doc(td.scoreDocs[2].doc).getString("value"));
    assertNull(searcher.doc(td.scoreDocs[3].doc).get("value"));

    ir.close();
    dir.close();
  }
}
