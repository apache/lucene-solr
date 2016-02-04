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
package org.apache.lucene.uninverting;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.uninverting.UninvertingReader.Type;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;

public class TestUninvertingReader extends LuceneTestCase {
  
  public void testSortedSetInteger() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    
    Document doc = new Document();
    doc.add(new IntField("foo", 5, Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new IntField("foo", 5, Field.Store.NO));
    doc.add(new IntField("foo", -3, Field.Store.NO));
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
                         Collections.singletonMap("foo", Type.SORTED_SET_INTEGER));
    LeafReader ar = ir.leaves().get(0).reader();
    SortedSetDocValues v = ar.getSortedSetDocValues("foo");
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(0, v.nextOrd());
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals(-3, NumericUtils.prefixCodedToInt(value));
    
    value = v.lookupOrd(1);
    assertEquals(5, NumericUtils.prefixCodedToInt(value));
    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }
  
  public void testSortedSetFloat() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    
    Document doc = new Document();
    doc.add(new IntField("foo", Float.floatToRawIntBits(5f), Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new IntField("foo", Float.floatToRawIntBits(5f), Field.Store.NO));
    doc.add(new IntField("foo", Float.floatToRawIntBits(-3f), Field.Store.NO));
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
                         Collections.singletonMap("foo", Type.SORTED_SET_FLOAT));
    LeafReader ar = ir.leaves().get(0).reader();
    
    SortedSetDocValues v = ar.getSortedSetDocValues("foo");
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(0, v.nextOrd());
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals(Float.floatToRawIntBits(-3f), NumericUtils.prefixCodedToInt(value));
    
    value = v.lookupOrd(1);
    assertEquals(Float.floatToRawIntBits(5f), NumericUtils.prefixCodedToInt(value));
    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }
  
  public void testSortedSetLong() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    
    Document doc = new Document();
    doc.add(new LongField("foo", 5, Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new LongField("foo", 5, Field.Store.NO));
    doc.add(new LongField("foo", -3, Field.Store.NO));
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
        Collections.singletonMap("foo", Type.SORTED_SET_LONG));
    LeafReader ar = ir.leaves().get(0).reader();
    SortedSetDocValues v = ar.getSortedSetDocValues("foo");
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(0, v.nextOrd());
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals(-3, NumericUtils.prefixCodedToLong(value));
    
    value = v.lookupOrd(1);
    assertEquals(5, NumericUtils.prefixCodedToLong(value));
    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }
  
  public void testSortedSetDouble() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    
    Document doc = new Document();
    doc.add(new LongField("foo", Double.doubleToRawLongBits(5d), Field.Store.NO));
    iw.addDocument(doc);
    
    doc = new Document();
    doc.add(new LongField("foo", Double.doubleToRawLongBits(5d), Field.Store.NO));
    doc.add(new LongField("foo", Double.doubleToRawLongBits(-3d), Field.Store.NO));
    iw.addDocument(doc);
    
    iw.forceMerge(1);
    iw.close();
    
    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
        Collections.singletonMap("foo", Type.SORTED_SET_DOUBLE));
    LeafReader ar = ir.leaves().get(0).reader();
    SortedSetDocValues v = ar.getSortedSetDocValues("foo");
    assertEquals(2, v.getValueCount());
    
    v.setDocument(0);
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    v.setDocument(1);
    assertEquals(0, v.nextOrd());
    assertEquals(1, v.nextOrd());
    assertEquals(SortedSetDocValues.NO_MORE_ORDS, v.nextOrd());
    
    BytesRef value = v.lookupOrd(0);
    assertEquals(Double.doubleToRawLongBits(-3d), NumericUtils.prefixCodedToLong(value));
    
    value = v.lookupOrd(1);
    assertEquals(Double.doubleToRawLongBits(5d), NumericUtils.prefixCodedToLong(value));
    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }


  /** Tests {@link Type#SORTED_SET_INTEGER} using Integer based fields, with and w/o precision steps */
  public void testSortedSetIntegerManyValues() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    
    final FieldType NO_TRIE_TYPE = new FieldType(IntField.TYPE_NOT_STORED);
    NO_TRIE_TYPE.setNumericPrecisionStep(Integer.MAX_VALUE);

    final Map<String,Type> UNINVERT_MAP = new LinkedHashMap<String,Type>();
    UNINVERT_MAP.put("notrie_single", Type.SORTED_SET_INTEGER);
    UNINVERT_MAP.put("notrie_multi", Type.SORTED_SET_INTEGER);
    UNINVERT_MAP.put("trie_single", Type.SORTED_SET_INTEGER);
    UNINVERT_MAP.put("trie_multi", Type.SORTED_SET_INTEGER);
    final Set<String> MULTI_VALUES = new LinkedHashSet<String>();
    MULTI_VALUES.add("trie_multi");
    MULTI_VALUES.add("notrie_multi");

    
    final int NUM_DOCS = TestUtil.nextInt(random(), 200, 1500);
    final int MIN = TestUtil.nextInt(random(), 10, 100);
    final int MAX = MIN + TestUtil.nextInt(random(), 10, 100);
    final long EXPECTED_VALSET_SIZE = 1 + MAX - MIN;

    { // (at least) one doc should have every value, so that at least one segment has every value
      final Document doc = new Document();
      for (int i = MIN; i <= MAX; i++) {
        doc.add(new IntField("trie_multi", i, Field.Store.NO));
        doc.add(new IntField("notrie_multi", i, NO_TRIE_TYPE));
      }
      iw.addDocument(doc);
    }

    // now add some more random docs (note: starting at i=1 because of previously added doc)
    for (int i = 1; i < NUM_DOCS; i++) {
      final Document doc = new Document();
      if (0 != TestUtil.nextInt(random(), 0, 9)) {
        int val = TestUtil.nextInt(random(), MIN, MAX);
        doc.add(new IntField("trie_single", val, Field.Store.NO));
        doc.add(new IntField("notrie_single", val, NO_TRIE_TYPE));
      }
      if (0 != TestUtil.nextInt(random(), 0, 9)) {
        int numMulti = atLeast(1);
        while (0 < numMulti--) {
          int val = TestUtil.nextInt(random(), MIN, MAX);
          doc.add(new IntField("trie_multi", val, Field.Store.NO));
          doc.add(new IntField("notrie_multi", val, NO_TRIE_TYPE));
        }
      }
      iw.addDocument(doc);
    }

    iw.close();
    
    final DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), UNINVERT_MAP);
    TestUtil.checkReader(ir);
    
    final int NUM_LEAVES = ir.leaves().size();
    
    // check the leaves: no more then total set size
    for (LeafReaderContext rc : ir.leaves()) {
      final LeafReader ar = rc.reader();
      for (String f : UNINVERT_MAP.keySet()) {
        final SortedSetDocValues v = DocValues.getSortedSet(ar, f);
        final long valSetSize = v.getValueCount();
        assertTrue(f + ": Expected no more then " + EXPECTED_VALSET_SIZE + " values per segment, got " +
                   valSetSize + " from: " + ar.toString(),
                   valSetSize <= EXPECTED_VALSET_SIZE);
        
        if (1 == NUM_LEAVES && MULTI_VALUES.contains(f)) {
          // tighter check on multi fields in single segment index since we know one doc has all of them
          assertEquals(f + ": Single segment LeafReader's value set should have had exactly expected size",
                       EXPECTED_VALSET_SIZE, valSetSize);
        }
      }
    }

    // check the composite of all leaves: exact expectation of set size
    final LeafReader composite = SlowCompositeReaderWrapper.wrap(ir);
    TestUtil.checkReader(composite);
    
    for (String f : MULTI_VALUES) {
      final SortedSetDocValues v = composite.getSortedSetDocValues(f);
      final long valSetSize = v.getValueCount();
      assertEquals(f + ": Composite reader value set should have had exactly expected size",
                   EXPECTED_VALSET_SIZE, valSetSize);
    }
    
    ir.close();
    dir.close();
  }
  
  public void testSortedSetEmptyIndex() throws IOException {
    final Directory dir = newDirectory();
    final IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));
    iw.close();
    
    final Map<String,Type> UNINVERT_MAP = new LinkedHashMap<String,Type>();
    for (Type t : EnumSet.allOf(Type.class)) {
      UNINVERT_MAP.put(t.name(), t);
    }

    final DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), UNINVERT_MAP);
    TestUtil.checkReader(ir);
    
    final LeafReader composite = SlowCompositeReaderWrapper.wrap(ir);
    TestUtil.checkReader(composite);
    
    for (String f : UNINVERT_MAP.keySet()) { 
      // check the leaves
      // (normally there are none for an empty index, so this is really just future
      // proofing in case that changes for some reason)
      for (LeafReaderContext rc : ir.leaves()) {
        final LeafReader ar = rc.reader();
        assertNull(f + ": Expected no doc values from empty index (leaf)",
                   ar.getSortedSetDocValues(f));
      }
      
      // check the composite
      assertNull(f + ": Expected no doc values from empty index (composite)",
                 composite.getSortedSetDocValues(f));
      
    }

    ir.close();
    dir.close();
  }

  public void testFieldInfos() throws IOException {
    Directory dir = newDirectory();
    IndexWriter iw = new IndexWriter(dir, newIndexWriterConfig(null));

    Document doc = new Document();
    BytesRef idBytes = new BytesRef("id");
    doc.add(new StringField("id", idBytes, Store.YES));
    doc.add(new IntField("int", 5, Store.YES));
    doc.add(new NumericDocValuesField("dv", 5));
    doc.add(new StoredField("stored", 5)); // not indexed
    iw.addDocument(doc);

    iw.forceMerge(1);
    iw.close();

    Map<String, Type> uninvertingMap = new HashMap<>();
    uninvertingMap.put("int", Type.INTEGER);
    uninvertingMap.put("dv", Type.INTEGER);

    DirectoryReader ir = UninvertingReader.wrap(DirectoryReader.open(dir), 
                         uninvertingMap);
    LeafReader leafReader = ir.leaves().get(0).reader();

    FieldInfo intFInfo = leafReader.getFieldInfos().fieldInfo("int");
    assertEquals(DocValuesType.NUMERIC, intFInfo.getDocValuesType());

    FieldInfo dvFInfo = leafReader.getFieldInfos().fieldInfo("dv");
    assertEquals(DocValuesType.NUMERIC, dvFInfo.getDocValuesType());

    FieldInfo storedFInfo = leafReader.getFieldInfos().fieldInfo("stored");
    assertEquals(DocValuesType.NONE, storedFInfo.getDocValuesType());

    TestUtil.checkReader(ir);
    ir.close();
    dir.close();
  }

}
