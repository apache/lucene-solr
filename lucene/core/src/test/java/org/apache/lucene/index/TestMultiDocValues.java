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
import java.util.ArrayList;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;


/** Tests MultiDocValues versus ordinary segment merging */
public class TestMultiDocValues extends LuceneTestCase {
  
  public void testNumerics() throws Exception {
    Directory dir = newDirectory();
    Document doc = new Document();
    Field field = new NumericDocValuesField("numbers", 0);
    doc.add(field);
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      field.setLongValue(random().nextLong());
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    
    NumericDocValues multi = MultiDocValues.getNumericValues(ir, "numbers");
    NumericDocValues single = merged.getNumericDocValues("numbers");
    for (int i = 0; i < numDocs; i++) {
      assertEquals(i, multi.nextDoc());
      assertEquals(i, single.nextDoc());
      assertEquals(single.longValue(), multi.longValue());
    }
    testRandomAdvance(merged.getNumericDocValues("numbers"), MultiDocValues.getNumericValues(ir, "numbers"));
    testRandomAdvanceExact(merged.getNumericDocValues("numbers"), MultiDocValues.getNumericValues(ir, "numbers"), merged.maxDoc());

    ir.close();
    ir2.close();
    dir.close();
  }
  
  public void testBinary() throws Exception {
    Directory dir = newDirectory();
    Document doc = new Document();
    Field field = new BinaryDocValuesField("bytes", new BytesRef());
    doc.add(field);
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);

    for (int i = 0; i < numDocs; i++) {
      BytesRef ref = new BytesRef(TestUtil.randomUnicodeString(random()));
      field.setBytesValue(ref);
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();

    BinaryDocValues multi = MultiDocValues.getBinaryValues(ir, "bytes");
    BinaryDocValues single = merged.getBinaryDocValues("bytes");
    for (int i = 0; i < numDocs; i++) {
      assertEquals(i, multi.nextDoc());
      assertEquals(i, single.nextDoc());
      final BytesRef expected = BytesRef.deepCopyOf(single.binaryValue());
      final BytesRef actual = multi.binaryValue();
      assertEquals(expected, actual);
    }
    testRandomAdvance(merged.getBinaryDocValues("bytes"), MultiDocValues.getBinaryValues(ir, "bytes"));
    testRandomAdvanceExact(merged.getBinaryDocValues("bytes"), MultiDocValues.getBinaryValues(ir, "bytes"), merged.maxDoc());

    ir.close();
    ir2.close();
    dir.close();
  }
  
  public void testSorted() throws Exception {
    Directory dir = newDirectory();
    Document doc = new Document();
    Field field = new SortedDocValuesField("bytes", new BytesRef());
    doc.add(field);
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      BytesRef ref = new BytesRef(TestUtil.randomUnicodeString(random()));
      field.setBytesValue(ref);
      if (random().nextInt(7) == 0) {
        iw.addDocument(new Document());
      }
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    SortedDocValues multi = MultiDocValues.getSortedValues(ir, "bytes");
    SortedDocValues single = merged.getSortedDocValues("bytes");
    assertEquals(single.getValueCount(), multi.getValueCount());
    while (true) {
      assertEquals(single.nextDoc(), multi.nextDoc());
      if (single.docID() == NO_MORE_DOCS) {
        break;
      }

      // check value
      final BytesRef expected = BytesRef.deepCopyOf(single.binaryValue());
      final BytesRef actual = multi.binaryValue();
      assertEquals(expected, actual);

      // check ord
      assertEquals(single.ordValue(), multi.ordValue());
    }
    testRandomAdvance(merged.getSortedDocValues("bytes"), MultiDocValues.getSortedValues(ir, "bytes"));
    testRandomAdvanceExact(merged.getSortedDocValues("bytes"), MultiDocValues.getSortedValues(ir, "bytes"), merged.maxDoc());
    ir.close();
    ir2.close();
    dir.close();
  }
  
  // tries to make more dups than testSorted
  public void testSortedWithLotsOfDups() throws Exception {
    Directory dir = newDirectory();
    Document doc = new Document();
    Field field = new SortedDocValuesField("bytes", new BytesRef());
    doc.add(field);
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      BytesRef ref = new BytesRef(TestUtil.randomSimpleString(random(), 2));
      field.setBytesValue(ref);
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    
    SortedDocValues multi = MultiDocValues.getSortedValues(ir, "bytes");
    SortedDocValues single = merged.getSortedDocValues("bytes");
    assertEquals(single.getValueCount(), multi.getValueCount());
    for (int i = 0; i < numDocs; i++) {
      assertEquals(i, multi.nextDoc());
      assertEquals(i, single.nextDoc());
      // check ord
      assertEquals(single.ordValue(), multi.ordValue());
      // check ord value
      final BytesRef expected = BytesRef.deepCopyOf(single.binaryValue());
      final BytesRef actual = multi.binaryValue();
      assertEquals(expected, actual);
    }
    testRandomAdvance(merged.getSortedDocValues("bytes"), MultiDocValues.getSortedValues(ir, "bytes"));
    testRandomAdvanceExact(merged.getSortedDocValues("bytes"), MultiDocValues.getSortedValues(ir, "bytes"), merged.maxDoc());
    
    ir.close();
    ir2.close();
    dir.close();
  }
  
  public void testSortedSet() throws Exception {
    Directory dir = newDirectory();
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numValues = random().nextInt(5);
      for (int j = 0; j < numValues; j++) {
        doc.add(new SortedSetDocValuesField("bytes", new BytesRef(TestUtil.randomUnicodeString(random()))));
      }
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    
    SortedSetDocValues multi = MultiDocValues.getSortedSetValues(ir, "bytes");
    SortedSetDocValues single = merged.getSortedSetDocValues("bytes");
    if (multi == null) {
      assertNull(single);
    } else {
      assertEquals(single.getValueCount(), multi.getValueCount());
      // check values
      for (long i = 0; i < single.getValueCount(); i++) {
        final BytesRef expected = BytesRef.deepCopyOf(single.lookupOrd(i));
        final BytesRef actual = multi.lookupOrd(i);
        assertEquals(expected, actual);
      }
      // check ord list
      while (true) {
        int docID = single.nextDoc();
        assertEquals(docID, multi.nextDoc());
        if (docID == NO_MORE_DOCS) {
          break;
        }

        ArrayList<Long> expectedList = new ArrayList<>();
        long ord;
        while ((ord = single.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          expectedList.add(ord);
        }
        
        int upto = 0;
        while ((ord = multi.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          assertEquals(expectedList.get(upto).longValue(), ord);
          upto++;
        }
        assertEquals(expectedList.size(), upto);
      }
    }
    testRandomAdvance(merged.getSortedSetDocValues("bytes"), MultiDocValues.getSortedSetValues(ir, "bytes"));
    testRandomAdvanceExact(merged.getSortedSetDocValues("bytes"), MultiDocValues.getSortedSetValues(ir, "bytes"), merged.maxDoc());
    
    ir.close();
    ir2.close();
    dir.close();
  }
  
  // tries to make more dups than testSortedSet
  public void testSortedSetWithDups() throws Exception {
    Directory dir = newDirectory();
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numValues = random().nextInt(5);
      for (int j = 0; j < numValues; j++) {
        doc.add(new SortedSetDocValuesField("bytes", new BytesRef(TestUtil.randomSimpleString(random(), 2))));
      }
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    
    SortedSetDocValues multi = MultiDocValues.getSortedSetValues(ir, "bytes");
    SortedSetDocValues single = merged.getSortedSetDocValues("bytes");
    if (multi == null) {
      assertNull(single);
    } else {
      assertEquals(single.getValueCount(), multi.getValueCount());
      // check values
      for (long i = 0; i < single.getValueCount(); i++) {
        final BytesRef expected = BytesRef.deepCopyOf(single.lookupOrd(i));
        final BytesRef actual = multi.lookupOrd(i);
        assertEquals(expected, actual);
      }
      // check ord list
      while (true) {
        int docID = single.nextDoc();
        assertEquals(docID, multi.nextDoc());
        if (docID == NO_MORE_DOCS) {
          break;
        }
        ArrayList<Long> expectedList = new ArrayList<>();
        long ord;
        while ((ord = single.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          expectedList.add(ord);
        }
        
        int upto = 0;
        while ((ord = multi.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          assertEquals(expectedList.get(upto).longValue(), ord);
          upto++;
        }
        assertEquals(expectedList.size(), upto);
      }
    }
    testRandomAdvance(merged.getSortedSetDocValues("bytes"), MultiDocValues.getSortedSetValues(ir, "bytes"));
    testRandomAdvanceExact(merged.getSortedSetDocValues("bytes"), MultiDocValues.getSortedSetValues(ir, "bytes"), merged.maxDoc());

    ir.close();
    ir2.close();
    dir.close();
  }
  
  public void testSortedNumeric() throws Exception {
    Directory dir = newDirectory();
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int numValues = random().nextInt(5);
      for (int j = 0; j < numValues; j++) {
        doc.add(new SortedNumericDocValuesField("nums", TestUtil.nextLong(random(), Long.MIN_VALUE, Long.MAX_VALUE)));
      }
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlyLeafReader(ir2);
    iw.close();
    
    SortedNumericDocValues multi = MultiDocValues.getSortedNumericValues(ir, "nums");
    SortedNumericDocValues single = merged.getSortedNumericDocValues("nums");
    if (multi == null) {
      assertNull(single);
    } else {
      // check values
      for (int i = 0; i < numDocs; i++) {
        if (i > single.docID()) {
          assertEquals(single.nextDoc(), multi.nextDoc());
        }
        if (i == single.docID()) {
          assertEquals(single.docValueCount(), multi.docValueCount());
          for (int j = 0; j < single.docValueCount(); j++) {
            assertEquals(single.nextValue(), multi.nextValue());
          }
        }
      }
    }
    testRandomAdvance(merged.getSortedNumericDocValues("nums"), MultiDocValues.getSortedNumericValues(ir, "nums"));
    testRandomAdvanceExact(merged.getSortedNumericDocValues("nums"), MultiDocValues.getSortedNumericValues(ir, "nums"), merged.maxDoc());
    
    ir.close();
    ir2.close();
    dir.close();
  }

  private void testRandomAdvance(DocIdSetIterator iter1, DocIdSetIterator iter2) throws IOException {
    assertEquals(-1, iter1.docID());
    assertEquals(-1, iter2.docID());

    while (iter1.docID() != NO_MORE_DOCS) {
      if (random().nextBoolean()) {
        assertEquals(iter1.nextDoc(), iter2.nextDoc());
      } else {
        int target = iter1.docID() + TestUtil.nextInt(random(), 1, 100);
        assertEquals(iter1.advance(target), iter2.advance(target));
      }
    }
  }

  private void testRandomAdvanceExact(DocValuesIterator iter1, DocValuesIterator iter2, int maxDoc) throws IOException {
    for (int target = random().nextInt(Math.min(maxDoc, 10)); target < maxDoc; target += random().nextInt(10)) {
      final boolean exists1 = iter1.advanceExact(target);
      final boolean exists2 = iter2.advanceExact(target);
      assertEquals(exists1, exists2);
    }
  }
}
