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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FailOnNonBulkMergesInfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.Test;

public class TestConsistentFieldNumbers extends LuceneTestCase {

  @Test
  public void testSameFieldNumbersAcrossSegments() throws Exception {
    for (int i = 0; i < 2; i++) {
      Directory dir = newDirectory();
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.COMPOUND_FILES));

      Document d1 = new Document();
      d1.add(new StringField("f1", "first field", Field.Store.YES));
      d1.add(new StringField("f2", "second field", Field.Store.YES));
      writer.addDocument(d1);

      if (i == 1) {
        writer.close();
        writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.COMPOUND_FILES));
      } else {
        writer.commit();
      }

      Document d2 = new Document();
      FieldType customType2 = new FieldType(TextField.TYPE_STORED);
      customType2.setStoreTermVectors(true);
      d2.add(new TextField("f2", "second field", Field.Store.NO));
      d2.add(new Field("f1", "first field", customType2));
      d2.add(new TextField("f3", "third field", Field.Store.NO));
      d2.add(new TextField("f4", "fourth field", Field.Store.NO));
      writer.addDocument(d2);

      writer.close();

      SegmentInfos sis = new SegmentInfos();
      sis.read(dir);
      assertEquals(2, sis.size());

      FieldInfos fis1 = _TestUtil.getFieldInfos(sis.info(0).info);
      FieldInfos fis2 = _TestUtil.getFieldInfos(sis.info(1).info);

      assertEquals("f1", fis1.fieldInfo(0).name);
      assertEquals("f2", fis1.fieldInfo(1).name);
      assertEquals("f1", fis2.fieldInfo(0).name);
      assertEquals("f2", fis2.fieldInfo(1).name);
      assertEquals("f3", fis2.fieldInfo(2).name);
      assertEquals("f4", fis2.fieldInfo(3).name);

      writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
      writer.forceMerge(1);
      writer.close();

      sis = new SegmentInfos();
      sis.read(dir);
      assertEquals(1, sis.size());

      FieldInfos fis3 = _TestUtil.getFieldInfos(sis.info(0).info);

      assertEquals("f1", fis3.fieldInfo(0).name);
      assertEquals("f2", fis3.fieldInfo(1).name);
      assertEquals("f3", fis3.fieldInfo(2).name);
      assertEquals("f4", fis3.fieldInfo(3).name);


      dir.close();
    }
  }

  @Test
  public void testAddIndexes() throws Exception {
    Directory dir1 = newDirectory();
    Directory dir2 = newDirectory();
    IndexWriter writer = new IndexWriter(dir1, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.COMPOUND_FILES));

    Document d1 = new Document();
    d1.add(new TextField("f1", "first field", Field.Store.YES));
    d1.add(new TextField("f2", "second field", Field.Store.YES));
    writer.addDocument(d1);

    writer.close();
    writer = new IndexWriter(dir2, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.COMPOUND_FILES));

    Document d2 = new Document();
    FieldType customType2 = new FieldType(TextField.TYPE_STORED);
    customType2.setStoreTermVectors(true);
    d2.add(new TextField("f2", "second field", Field.Store.YES));
    d2.add(new Field("f1", "first field", customType2));
    d2.add(new TextField("f3", "third field", Field.Store.YES));
    d2.add(new TextField("f4", "fourth field", Field.Store.YES));
    writer.addDocument(d2);

    writer.close();

    writer = new IndexWriter(dir1, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.COMPOUND_FILES));
    writer.addIndexes(dir2);
    writer.close();

    SegmentInfos sis = new SegmentInfos();
    sis.read(dir1);
    assertEquals(2, sis.size());

    FieldInfos fis1 = _TestUtil.getFieldInfos(sis.info(0).info);
    FieldInfos fis2 = _TestUtil.getFieldInfos(sis.info(1).info);

    assertEquals("f1", fis1.fieldInfo(0).name);
    assertEquals("f2", fis1.fieldInfo(1).name);
    // make sure the ordering of the "external" segment is preserved
    assertEquals("f2", fis2.fieldInfo(0).name);
    assertEquals("f1", fis2.fieldInfo(1).name);
    assertEquals("f3", fis2.fieldInfo(2).name);
    assertEquals("f4", fis2.fieldInfo(3).name);

    dir1.close();
    dir2.close();
  }
  
  public void testFieldNumberGaps() throws IOException {
    int numIters = atLeast(13);
    for (int i = 0; i < numIters; i++) {
      Directory dir = newDirectory();
      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(
            NoMergePolicy.NO_COMPOUND_FILES));
        Document d = new Document();
        d.add(new TextField("f1", "d1 first field", Field.Store.YES));
        d.add(new TextField("f2", "d1 second field", Field.Store.YES));
        writer.addDocument(d);
        writer.close();
        SegmentInfos sis = new SegmentInfos();
        sis.read(dir);
        assertEquals(1, sis.size());
        FieldInfos fis1 = _TestUtil.getFieldInfos(sis.info(0).info);
        assertEquals("f1", fis1.fieldInfo(0).name);
        assertEquals("f2", fis1.fieldInfo(1).name);
      }
      

      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(
            random().nextBoolean() ? NoMergePolicy.NO_COMPOUND_FILES
                : NoMergePolicy.COMPOUND_FILES));
        Document d = new Document();
        d.add(new TextField("f1", "d2 first field", Field.Store.YES));
        d.add(new StoredField("f3", new byte[] { 1, 2, 3 }));
        writer.addDocument(d);
        writer.close();
        SegmentInfos sis = new SegmentInfos();
        sis.read(dir);
        assertEquals(2, sis.size());
        FieldInfos fis1 = _TestUtil.getFieldInfos(sis.info(0).info);
        FieldInfos fis2 = _TestUtil.getFieldInfos(sis.info(1).info);
        assertEquals("f1", fis1.fieldInfo(0).name);
        assertEquals("f2", fis1.fieldInfo(1).name);
        assertEquals("f1", fis2.fieldInfo(0).name);
        assertNull(fis2.fieldInfo(1));
        assertEquals("f3", fis2.fieldInfo(2).name);
      }

      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(
            random().nextBoolean() ? NoMergePolicy.NO_COMPOUND_FILES
                : NoMergePolicy.COMPOUND_FILES));
        Document d = new Document();
        d.add(new TextField("f1", "d3 first field", Field.Store.YES));
        d.add(new TextField("f2", "d3 second field", Field.Store.YES));
        d.add(new StoredField("f3", new byte[] { 1, 2, 3, 4, 5 }));
        writer.addDocument(d);
        writer.close();
        SegmentInfos sis = new SegmentInfos();
        sis.read(dir);
        assertEquals(3, sis.size());
        FieldInfos fis1 = _TestUtil.getFieldInfos(sis.info(0).info);
        FieldInfos fis2 = _TestUtil.getFieldInfos(sis.info(1).info);
        FieldInfos fis3 = _TestUtil.getFieldInfos(sis.info(2).info);
        assertEquals("f1", fis1.fieldInfo(0).name);
        assertEquals("f2", fis1.fieldInfo(1).name);
        assertEquals("f1", fis2.fieldInfo(0).name);
        assertNull(fis2.fieldInfo(1));
        assertEquals("f3", fis2.fieldInfo(2).name);
        assertEquals("f1", fis3.fieldInfo(0).name);
        assertEquals("f2", fis3.fieldInfo(1).name);
        assertEquals("f3", fis3.fieldInfo(2).name);
      }

      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(
            random().nextBoolean() ? NoMergePolicy.NO_COMPOUND_FILES
                : NoMergePolicy.COMPOUND_FILES));
        writer.deleteDocuments(new Term("f1", "d1"));
        // nuke the first segment entirely so that the segment with gaps is
        // loaded first!
        writer.forceMergeDeletes();
        writer.close();
      }

      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(
          new LogByteSizeMergePolicy()).setInfoStream(new FailOnNonBulkMergesInfoStream()));
      writer.forceMerge(1);
      writer.close();

      SegmentInfos sis = new SegmentInfos();
      sis.read(dir);
      assertEquals(1, sis.size());
      FieldInfos fis1 = _TestUtil.getFieldInfos(sis.info(0).info);
      assertEquals("f1", fis1.fieldInfo(0).name);
      assertEquals("f2", fis1.fieldInfo(1).name);
      assertEquals("f3", fis1.fieldInfo(2).name);
      dir.close();
    }
  }

  @Test
  public void testManyFields() throws Exception {
    final int NUM_DOCS = atLeast(200);
    final int MAX_FIELDS = atLeast(50);

    int[][] docs = new int[NUM_DOCS][4];
    for (int i = 0; i < docs.length; i++) {
      for (int j = 0; j < docs[i].length;j++) {
        docs[i][j] = random().nextInt(MAX_FIELDS);
      }
    }

    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    for (int i = 0; i < NUM_DOCS; i++) {
      Document d = new Document();
      for (int j = 0; j < docs[i].length; j++) {
        d.add(getField(docs[i][j]));
      }

      writer.addDocument(d);
    }

    writer.forceMerge(1);
    writer.close();

    SegmentInfos sis = new SegmentInfos();
    sis.read(dir);
    for (SegmentInfoPerCommit si : sis) {
      FieldInfos fis = _TestUtil.getFieldInfos(si.info);

      for (FieldInfo fi : fis) {
        Field expected = getField(Integer.parseInt(fi.name));
        assertEquals(expected.fieldType().indexed(), fi.isIndexed());
        assertEquals(expected.fieldType().storeTermVectors(), fi.hasVectors());
      }
    }

    dir.close();
  }

  private Field getField(int number) {
    int mode = number % 16;
    String fieldName = "" + number;
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    
    FieldType customType2 = new FieldType(TextField.TYPE_STORED);
    customType2.setTokenized(false);
    
    FieldType customType3 = new FieldType(TextField.TYPE_NOT_STORED);
    customType3.setTokenized(false);
    
    FieldType customType4 = new FieldType(TextField.TYPE_NOT_STORED);
    customType4.setTokenized(false);
    customType4.setStoreTermVectors(true);
    customType4.setStoreTermVectorOffsets(true);
    
    FieldType customType5 = new FieldType(TextField.TYPE_NOT_STORED);
    customType5.setStoreTermVectors(true);
    customType5.setStoreTermVectorOffsets(true);

    FieldType customType6 = new FieldType(TextField.TYPE_STORED);
    customType6.setTokenized(false);
    customType6.setStoreTermVectors(true);
    customType6.setStoreTermVectorOffsets(true);

    FieldType customType7 = new FieldType(TextField.TYPE_NOT_STORED);
    customType7.setTokenized(false);
    customType7.setStoreTermVectors(true);
    customType7.setStoreTermVectorOffsets(true);

    FieldType customType8 = new FieldType(TextField.TYPE_STORED);
    customType8.setTokenized(false);
    customType8.setStoreTermVectors(true);
    customType8.setStoreTermVectorPositions(true);

    FieldType customType9 = new FieldType(TextField.TYPE_NOT_STORED);
    customType9.setStoreTermVectors(true);
    customType9.setStoreTermVectorPositions(true);

    FieldType customType10 = new FieldType(TextField.TYPE_STORED);
    customType10.setTokenized(false);
    customType10.setStoreTermVectors(true);
    customType10.setStoreTermVectorPositions(true);

    FieldType customType11 = new FieldType(TextField.TYPE_NOT_STORED);
    customType11.setTokenized(false);
    customType11.setStoreTermVectors(true);
    customType11.setStoreTermVectorPositions(true);

    FieldType customType12 = new FieldType(TextField.TYPE_STORED);
    customType12.setStoreTermVectors(true);
    customType12.setStoreTermVectorOffsets(true);
    customType12.setStoreTermVectorPositions(true);

    FieldType customType13 = new FieldType(TextField.TYPE_NOT_STORED);
    customType13.setStoreTermVectors(true);
    customType13.setStoreTermVectorOffsets(true);
    customType13.setStoreTermVectorPositions(true);

    FieldType customType14 = new FieldType(TextField.TYPE_STORED);
    customType14.setTokenized(false);
    customType14.setStoreTermVectors(true);
    customType14.setStoreTermVectorOffsets(true);
    customType14.setStoreTermVectorPositions(true);

    FieldType customType15 = new FieldType(TextField.TYPE_NOT_STORED);
    customType15.setTokenized(false);
    customType15.setStoreTermVectors(true);
    customType15.setStoreTermVectorOffsets(true);
    customType15.setStoreTermVectorPositions(true);
    
    switch (mode) {
      case 0: return new Field(fieldName, "some text", customType);
      case 1: return new TextField(fieldName, "some text", Field.Store.NO);
      case 2: return new Field(fieldName, "some text", customType2);
      case 3: return new Field(fieldName, "some text", customType3);
      case 4: return new Field(fieldName, "some text", customType4);
      case 5: return new Field(fieldName, "some text", customType5);
      case 6: return new Field(fieldName, "some text", customType6);
      case 7: return new Field(fieldName, "some text", customType7);
      case 8: return new Field(fieldName, "some text", customType8);
      case 9: return new Field(fieldName, "some text", customType9);
      case 10: return new Field(fieldName, "some text", customType10);
      case 11: return new Field(fieldName, "some text", customType11);
      case 12: return new Field(fieldName, "some text", customType12);
      case 13: return new Field(fieldName, "some text", customType13);
      case 14: return new Field(fieldName, "some text", customType14);
      case 15: return new Field(fieldName, "some text", customType15);
      default: return null;
    }
  }
}
