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


import java.util.ArrayList;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

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
    LeafReader merged = getOnlySegmentReader(ir2);
    iw.close();
    
    NumericDocValues multi = MultiDocValues.getNumericValues(ir, "numbers");
    NumericDocValues single = merged.getNumericDocValues("numbers");
    for (int i = 0; i < numDocs; i++) {
      assertEquals(single.get(i), multi.get(i));
    }
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
    LeafReader merged = getOnlySegmentReader(ir2);
    iw.close();
    
    BinaryDocValues multi = MultiDocValues.getBinaryValues(ir, "bytes");
    BinaryDocValues single = merged.getBinaryDocValues("bytes");
    for (int i = 0; i < numDocs; i++) {
      final BytesRef expected = BytesRef.deepCopyOf(single.get(i));
      final BytesRef actual = multi.get(i);
      assertEquals(expected, actual);
    }
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
    LeafReader merged = getOnlySegmentReader(ir2);
    iw.close();
    
    SortedDocValues multi = MultiDocValues.getSortedValues(ir, "bytes");
    SortedDocValues single = merged.getSortedDocValues("bytes");
    assertEquals(single.getValueCount(), multi.getValueCount());
    for (int i = 0; i < numDocs; i++) {
      // check ord
      assertEquals(single.getOrd(i), multi.getOrd(i));
      // check value
      final BytesRef expected = BytesRef.deepCopyOf(single.get(i));
      final BytesRef actual = multi.get(i);
      assertEquals(expected, actual);
    }
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
    LeafReader merged = getOnlySegmentReader(ir2);
    iw.close();
    
    SortedDocValues multi = MultiDocValues.getSortedValues(ir, "bytes");
    SortedDocValues single = merged.getSortedDocValues("bytes");
    assertEquals(single.getValueCount(), multi.getValueCount());
    for (int i = 0; i < numDocs; i++) {
      // check ord
      assertEquals(single.getOrd(i), multi.getOrd(i));
      // check ord value
      final BytesRef expected = BytesRef.deepCopyOf(single.get(i));
      final BytesRef actual = multi.get(i);
      assertEquals(expected, actual);
    }
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
    LeafReader merged = getOnlySegmentReader(ir2);
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
      for (int i = 0; i < numDocs; i++) {
        single.setDocument(i);
        ArrayList<Long> expectedList = new ArrayList<>();
        long ord;
        while ((ord = single.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          expectedList.add(ord);
        }
        
        multi.setDocument(i);
        int upto = 0;
        while ((ord = multi.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          assertEquals(expectedList.get(upto).longValue(), ord);
          upto++;
        }
        assertEquals(expectedList.size(), upto);
      }
    }
    
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
    LeafReader merged = getOnlySegmentReader(ir2);
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
      for (int i = 0; i < numDocs; i++) {
        single.setDocument(i);
        ArrayList<Long> expectedList = new ArrayList<>();
        long ord;
        while ((ord = single.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          expectedList.add(ord);
        }
        
        multi.setDocument(i);
        int upto = 0;
        while ((ord = multi.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
          assertEquals(expectedList.get(upto).longValue(), ord);
          upto++;
        }
        assertEquals(expectedList.size(), upto);
      }
    }
    
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
    LeafReader merged = getOnlySegmentReader(ir2);
    iw.close();
    
    SortedNumericDocValues multi = MultiDocValues.getSortedNumericValues(ir, "nums");
    SortedNumericDocValues single = merged.getSortedNumericDocValues("nums");
    if (multi == null) {
      assertNull(single);
    } else {
      // check values
      for (int i = 0; i < numDocs; i++) {
        single.setDocument(i);
        ArrayList<Long> expectedList = new ArrayList<>();
        for (int j = 0; j < single.count(); j++) {
          expectedList.add(single.valueAt(j));
        }
        
        multi.setDocument(i);
        assertEquals(expectedList.size(), multi.count());
        for (int j = 0; j < single.count(); j++) {
          assertEquals(expectedList.get(j).longValue(), multi.valueAt(j));
        }
      }
    }
    
    ir.close();
    ir2.close();
    dir.close();
  }
  
  public void testDocsWithField() throws Exception {
    Directory dir = newDirectory();
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = TEST_NIGHTLY ? atLeast(500) : atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      if (random().nextInt(4) >= 0) {
        doc.add(new NumericDocValuesField("numbers", random().nextLong()));
      }
      doc.add(new NumericDocValuesField("numbersAlways", random().nextLong()));
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    LeafReader merged = getOnlySegmentReader(ir2);
    iw.close();
    
    Bits multi = MultiDocValues.getDocsWithField(ir, "numbers");
    Bits single = merged.getDocsWithField("numbers");
    if (multi == null) {
      assertNull(single);
    } else {
      assertEquals(single.length(), multi.length());
      for (int i = 0; i < numDocs; i++) {
        assertEquals(single.get(i), multi.get(i));
      }
    }
    
    multi = MultiDocValues.getDocsWithField(ir, "numbersAlways");
    single = merged.getDocsWithField("numbersAlways");
    assertEquals(single.length(), multi.length());
    for (int i = 0; i < numDocs; i++) {
      assertEquals(single.get(i), multi.get(i));
    }
    ir.close();
    ir2.close();
    dir.close();
  }
}
