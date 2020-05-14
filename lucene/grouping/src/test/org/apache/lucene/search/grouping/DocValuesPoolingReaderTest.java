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
package org.apache.lucene.search.grouping;

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesPoolingReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class DocValuesPoolingReaderTest extends LuceneTestCase {
  
  private static RandomIndexWriter w;
  private static Directory dir;
  private static DirectoryReader reader;

  @BeforeClass
  public static void index() throws IOException {
    dir = newDirectory();
    w = new RandomIndexWriter(
        random(),
        dir,
        newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    for (int i=0; i<2; i++) {
      Document doc = new Document();
      doc.add(new BinaryDocValuesField("bin", new BytesRef("binary")));
      doc.add(new BinaryDocValuesField("bin2", new BytesRef("binary2")));
      
      doc.add(new NumericDocValuesField("num", 1L));
      doc.add(new NumericDocValuesField("num2", 2L));
      
      doc.add(new SortedNumericDocValuesField("sortnum", 3L));
      doc.add(new SortedNumericDocValuesField("sortnum2", 4L));
      
      doc.add(new SortedDocValuesField("sort",  new BytesRef("sorted")));
      doc.add(new SortedDocValuesField("sort2",  new BytesRef("sorted2")));
      
      doc.add(new SortedSetDocValuesField("sortset", new BytesRef("sortedset")));
      doc.add(new SortedSetDocValuesField("sortset2", new BytesRef("sortedset2")));
      
      w.addDocument(doc);
      w.commit();
    }
    reader = w.getReader();
    w.close();
  }
  
  public void testDVCache() throws IOException {
    assertFalse(reader.leaves().isEmpty());
    int baseDoc=0;
    for (LeafReaderContext leaf : reader.leaves()) {
      assertEquals(baseDoc, leaf.docBase);
      final DocValuesPoolingReader caching = new DocValuesPoolingReader(leaf);
      
      assertSame(assertBinaryDV(caching, "bin", "binary"), 
          caching.getBinaryDocValues("bin"));
      assertSame(assertBinaryDV(caching, "bin2", "binary2"), 
          caching.getBinaryDocValues("bin2"));
      
      assertSame(assertNumericDV(caching, "num", 1L), 
          caching.getNumericDocValues("num"));
      assertSame(assertNumericDV(caching, "num2", 2L), 
          caching.getNumericDocValues("num2"));
      
      assertSame(assertSortedNumericDV(caching, "sortnum", 3L), 
          caching.getSortedNumericDocValues("sortnum"));
      assertSame(assertSortedNumericDV(caching, "sortnum2", 4L), 
          caching.getSortedNumericDocValues("sortnum2"));
      
      assertSame(assertSortedDV(caching, "sort", "sorted"), 
          caching.getSortedDocValues("sort"));
      assertSame(assertSortedDV(caching, "sort2", "sorted2"), 
          caching.getSortedDocValues("sort2"));

      assertSame(assertSortedSetDV(caching, "sortset", "sortedset"), 
          caching.getSortedSetDocValues("sortset"));
      assertSame(assertSortedSetDV(caching, "sortset2", "sortedset2"), 
          caching.getSortedSetDocValues("sortset2"));
      baseDoc+=leaf.reader().maxDoc();
    }
  }

  static BinaryDocValues assertBinaryDV(LeafReader reader, String field, String val) throws IOException {
    final BinaryDocValues binaryDocValues = reader.getBinaryDocValues(field);
    assertEquals(0, binaryDocValues.nextDoc());
    assertEquals(new BytesRef(val), binaryDocValues.binaryValue());
    return binaryDocValues;
  }

  static NumericDocValues assertNumericDV(LeafReader reader, String field, long val) throws IOException {
    final NumericDocValues numericDocValues = reader.getNumericDocValues(field);
    assertEquals(0, numericDocValues.nextDoc());
    assertEquals(val, numericDocValues.longValue());
    return numericDocValues;
  }

  static SortedNumericDocValues assertSortedNumericDV(LeafReader reader, String field, long val) throws IOException {
    final SortedNumericDocValues numericDocValues = reader.getSortedNumericDocValues(field);
    assertEquals(0, numericDocValues.nextDoc());
    assertEquals(val, numericDocValues.nextValue());
    return numericDocValues;
  }
  
  static SortedDocValues assertSortedDV(LeafReader reader, String field, String val) throws IOException {
    final SortedDocValues sortedDocValues = reader.getSortedDocValues(field);
    assertEquals(0, sortedDocValues.nextDoc());
    assertEquals(new BytesRef(val), sortedDocValues.lookupOrd(sortedDocValues.ordValue()));
    return sortedDocValues;
  }
  
  static SortedSetDocValues assertSortedSetDV(LeafReader reader, String field, String val) throws IOException {
    final SortedSetDocValues sortedDocValues = reader.getSortedSetDocValues(field);
    assertEquals(0, sortedDocValues.nextDoc());
    assertEquals(new BytesRef(val), sortedDocValues.lookupOrd(sortedDocValues.nextOrd()));
    return sortedDocValues;
  }  

  @AfterClass
  public static void sweep() throws IOException {
    reader.close();
    dir.close();
  }
}
