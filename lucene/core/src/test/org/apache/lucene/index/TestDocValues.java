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
package org.apache.lucene.index;


import java.io.IOException;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

/** Tests helper methods in DocValues */
public class TestDocValues extends LuceneTestCase {

  /** 
   * If the field doesn't exist, we return empty instances:
   * it can easily happen that a segment just doesn't have any docs with the field.
   */
  public void testEmptyIndex() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    iw.addDocument(new Document());
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
    
    // ok
    assertNotNull(DocValues.getBinary(r, "bogus"));
    assertNotNull(DocValues.getNumeric(r, "bogus"));
    assertNotNull(DocValues.getSorted(r, "bogus"));
    assertNotNull(DocValues.getSortedSet(r, "bogus"));
    assertNotNull(DocValues.getSortedNumeric(r, "bogus"));
    
    dr.close();
    iw.close();
    dir.close();
  }
  
  /** 
   * field just doesnt have any docvalues at all: exception
   */
  public void testMisconfiguredField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
   
    // errors
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getBinary(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getNumeric(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSorted(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedSet(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedNumeric(r, "foo");
    });
    
    dr.close();
    iw.close();
    dir.close();
  }
  
  /** 
   * field with numeric docvalues
   */
  public void testNumericField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new NumericDocValuesField("foo", 3));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
    
    // ok
    assertNotNull(DocValues.getNumeric(r, "foo"));
    assertNotNull(DocValues.getSortedNumeric(r, "foo"));
    
    // errors
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getBinary(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSorted(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedSet(r, "foo");
    });
    
    dr.close();
    iw.close();
    dir.close();
  }

  // The LUCENE-8585 jump-tables enables O(1) skipping of IndexedDISI blocks, DENSE block lookup
  // and numeric multi blocks. This test focuses on testing these jumps.
  @Slow
  public void testNumericFieldJumpTables() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    final int maxDoc = atLeast(3*65536); // Must be above 3*65536 to trigger IndexedDISI block skips
    for (int i = 0 ; i < maxDoc ; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
      if (random().nextInt(100) > 10) { // Skip 10% to make DENSE blocks
        int value = random().nextInt(100000);
        doc.add(new NumericDocValuesField("dv", value));
        doc.add(new StringField("storedValue", Integer.toString(value), Field.Store.YES));
      }
      iw.addDocument(doc);
    }
    iw.flush();
    iw.forceMerge(1, true); // Single segment to force large enough structures
    iw.commit();

    try (DirectoryReader dr = DirectoryReader.open(iw)) {
        assertEquals("maxDoc should be as expected", maxDoc, dr.maxDoc());
        LeafReader r = getOnlyLeafReader(dr);

      // Check non-jumping first
      {
        NumericDocValues numDV = DocValues.getNumeric(r, "dv");
        for (int i = 0; i < maxDoc; i++) {
          IndexableField fieldValue = r.document(i).getField("storedValue");
          if (fieldValue == null) {
            assertFalse("There should be no DocValue for document #" + i, numDV.advanceExact(i));
          } else {
            assertTrue("There should be a DocValue for document #" + i, numDV.advanceExact(i));
            assertEquals("The value for document #" + i + " should be correct",
                fieldValue.stringValue(), Long.toString(numDV.longValue()));
          }
        }
      }

      // We use steps of 511 to have a non-divisor for the different possible jump-steps 512 (DENSE),
      // 16384 (variable bits per value) and 65536 (IndexedDISI blocks)
      {
        for (int jump = 511; jump < maxDoc; jump += 511) {
          // Create a new instance each time to ensure jumps from the beginning
          NumericDocValues numDV = DocValues.getNumeric(r, "dv");
          for (int index = 0; index < maxDoc; index += jump) {
            IndexableField fieldValue = r.document(index).getField("storedValue");
            if (fieldValue == null) {
              assertFalse("There should be no DocValue for document #" + jump, numDV.advanceExact(index));
            } else {
              assertTrue("There should be a DocValue for document #" + jump, numDV.advanceExact(index));
              assertEquals("The value for document #" + jump + " should be correct",
                  fieldValue.stringValue(), Long.toString(numDV.longValue()));
            }
          }
        }
      }
    }

    iw.close();
    dir.close();
  }

  /**
   * field with binary docvalues
   */
  public void testBinaryField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new BinaryDocValuesField("foo", new BytesRef("bar")));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
    
    // ok
    assertNotNull(DocValues.getBinary(r, "foo"));
    
    // errors
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getNumeric(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSorted(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedSet(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedNumeric(r, "foo");
    });
    
    dr.close();
    iw.close();
    dir.close();
  }
  
  /** 
   * field with sorted docvalues
   */
  public void testSortedField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new SortedDocValuesField("foo", new BytesRef("bar")));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
    
    // ok
    assertNotNull(DocValues.getBinary(r, "foo"));
    assertNotNull(DocValues.getSorted(r, "foo"));
    assertNotNull(DocValues.getSortedSet(r, "foo"));
    
    // errors
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getNumeric(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedNumeric(r, "foo");
    });
    
    dr.close();
    iw.close();
    dir.close();
  }
  
  /** 
   * field with sortedset docvalues
   */
  public void testSortedSetField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("foo", new BytesRef("bar")));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
    
    // ok
    assertNotNull(DocValues.getSortedSet(r, "foo"));
    
    // errors
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getBinary(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getNumeric(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSorted(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedNumeric(r, "foo");
    });
    
    dr.close();
    iw.close();
    dir.close();
  }
  
  /** 
   * field with sortednumeric docvalues
   */
  public void testSortedNumericField() throws Exception {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("foo", 3));
    iw.addDocument(doc);
    DirectoryReader dr = DirectoryReader.open(iw);
    LeafReader r = getOnlyLeafReader(dr);
    
    // ok
    assertNotNull(DocValues.getSortedNumeric(r, "foo"));
    
    // errors
    expectThrows(IllegalStateException.class, () -> {
        DocValues.getBinary(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getNumeric(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSorted(r, "foo");
    });
    expectThrows(IllegalStateException.class, () -> {
      DocValues.getSortedSet(r, "foo");
    });
    
    dr.close();
    iw.close();
    dir.close();
  }

  public void testAddNullNumericDocValues() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    Document doc = new Document();
    if (random().nextBoolean()) {
      doc.add(new NumericDocValuesField("foo", null));
    } else {
      doc.add(new BinaryDocValuesField("foo", null));
    }
    IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> iw.addDocument(doc));
    assertEquals("field=\"foo\": null value not allowed", iae.getMessage());
    IOUtils.close(iw, dir);
  }
}
