package org.apache.lucene.index;

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

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/** Tests MultiDocValues versus ordinary segment merging */
public class TestMultiDocValues extends LuceneTestCase {
  
  public void testNumerics() throws Exception {
    Directory dir = newDirectory();
    Document doc = new Document();
    Field field = new NumericDocValuesField("numbers", 0);
    doc.add(field);
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), TEST_VERSION_CURRENT, null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = atLeast(500);
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
    AtomicReader merged = getOnlySegmentReader(ir2);
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
    BytesRef ref = new BytesRef();
    Field field = new BinaryDocValuesField("bytes", ref);
    doc.add(field);
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), TEST_VERSION_CURRENT, null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      ref.copyChars(_TestUtil.randomUnicodeString(random()));
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    AtomicReader merged = getOnlySegmentReader(ir2);
    iw.close();
    
    BinaryDocValues multi = MultiDocValues.getBinaryValues(ir, "bytes");
    BinaryDocValues single = merged.getBinaryDocValues("bytes");
    BytesRef actual = new BytesRef();
    BytesRef expected = new BytesRef();
    for (int i = 0; i < numDocs; i++) {
      single.get(i, expected);
      multi.get(i, actual);
      assertEquals(expected, actual);
    }
    ir.close();
    ir2.close();
    dir.close();
  }
  
  public void testSorted() throws Exception {
    Directory dir = newDirectory();
    Document doc = new Document();
    BytesRef ref = new BytesRef();
    Field field = new SortedDocValuesField("bytes", ref);
    doc.add(field);
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), TEST_VERSION_CURRENT, null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      ref.copyChars(_TestUtil.randomUnicodeString(random()));
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    AtomicReader merged = getOnlySegmentReader(ir2);
    iw.close();
    
    SortedDocValues multi = MultiDocValues.getSortedValues(ir, "bytes");
    SortedDocValues single = merged.getSortedDocValues("bytes");
    assertEquals(single.getValueCount(), multi.getValueCount());
    BytesRef actual = new BytesRef();
    BytesRef expected = new BytesRef();
    for (int i = 0; i < numDocs; i++) {
      // check ord
      assertEquals(single.getOrd(i), multi.getOrd(i));
      // check ord value
      single.get(i, expected);
      multi.get(i, actual);
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
    BytesRef ref = new BytesRef();
    Field field = new SortedDocValuesField("bytes", ref);
    doc.add(field);
    
    IndexWriterConfig iwc = newIndexWriterConfig(random(), TEST_VERSION_CURRENT, null);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int numDocs = atLeast(500);
    for (int i = 0; i < numDocs; i++) {
      ref.copyChars(_TestUtil.randomSimpleString(random(), 2));
      iw.addDocument(doc);
      if (random().nextInt(17) == 0) {
        iw.commit();
      }
    }
    DirectoryReader ir = iw.getReader();
    iw.forceMerge(1);
    DirectoryReader ir2 = iw.getReader();
    AtomicReader merged = getOnlySegmentReader(ir2);
    iw.close();
    
    SortedDocValues multi = MultiDocValues.getSortedValues(ir, "bytes");
    SortedDocValues single = merged.getSortedDocValues("bytes");
    assertEquals(single.getValueCount(), multi.getValueCount());
    BytesRef actual = new BytesRef();
    BytesRef expected = new BytesRef();
    for (int i = 0; i < numDocs; i++) {
      // check ord
      assertEquals(single.getOrd(i), multi.getOrd(i));
      // check ord value
      single.get(i, expected);
      multi.get(i, actual);
      assertEquals(expected, actual);
    }
    ir.close();
    ir2.close();
    dir.close();
  }
}
