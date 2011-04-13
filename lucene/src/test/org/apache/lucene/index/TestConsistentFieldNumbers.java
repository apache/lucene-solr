package org.apache.lucene.index;

/**
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
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestConsistentFieldNumbers extends LuceneTestCase {

  @Test
  public void testSameFieldNumbersAcrossSegments() throws Exception {
    for (int i = 0; i < 2; i++) {
      Directory dir = newDirectory();
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(NoMergePolicy.COMPOUND_FILES));

      Document d1 = new Document();
      d1.add(new Field("f1", "first field", Store.YES, Index.ANALYZED, TermVector.NO));
      d1.add(new Field("f2", "second field", Store.YES, Index.ANALYZED, TermVector.NO));
      writer.addDocument(d1);

      if (i == 1) {
        writer.close();
        writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(NoMergePolicy.COMPOUND_FILES));
      } else {
        writer.commit();
      }

      Document d2 = new Document();
      d2.add(new Field("f2", "second field", Store.YES, Index.ANALYZED, TermVector.NO));
      d2.add(new Field("f1", "first field", Store.YES, Index.ANALYZED, TermVector.YES));
      d2.add(new Field("f3", "third field", Store.YES, Index.ANALYZED, TermVector.NO));
      d2.add(new Field("f4", "fourth field", Store.YES, Index.ANALYZED, TermVector.NO));
      writer.addDocument(d2);

      writer.close();

      SegmentInfos sis = new SegmentInfos();
      sis.read(dir);
      assertEquals(2, sis.size());

      FieldInfos fis1 = sis.info(0).getFieldInfos();
      FieldInfos fis2 = sis.info(1).getFieldInfos();

      assertEquals("f1", fis1.fieldInfo(0).name);
      assertEquals("f2", fis1.fieldInfo(1).name);
      assertEquals("f1", fis2.fieldInfo(0).name);
      assertEquals("f2", fis2.fieldInfo(1).name);
      assertEquals("f3", fis2.fieldInfo(2).name);
      assertEquals("f4", fis2.fieldInfo(3).name);

      writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      writer.optimize();
      writer.close();

      sis = new SegmentInfos();
      sis.read(dir);
      assertEquals(1, sis.size());

      FieldInfos fis3 = sis.info(0).getFieldInfos();

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
    IndexWriter writer = new IndexWriter(dir1, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(NoMergePolicy.COMPOUND_FILES));

    Document d1 = new Document();
    d1.add(new Field("f1", "first field", Store.YES, Index.ANALYZED, TermVector.NO));
    d1.add(new Field("f2", "second field", Store.YES, Index.ANALYZED, TermVector.NO));
    writer.addDocument(d1);

    writer.close();
    writer = new IndexWriter(dir2, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(NoMergePolicy.COMPOUND_FILES));

    Document d2 = new Document();
    d2.add(new Field("f2", "second field", Store.YES, Index.ANALYZED, TermVector.NO));
    d2.add(new Field("f1", "first field", Store.YES, Index.ANALYZED, TermVector.YES));
    d2.add(new Field("f3", "third field", Store.YES, Index.ANALYZED, TermVector.NO));
    d2.add(new Field("f4", "fourth field", Store.YES, Index.ANALYZED, TermVector.NO));
    writer.addDocument(d2);

    writer.close();

    writer = new IndexWriter(dir1, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(NoMergePolicy.COMPOUND_FILES));
    writer.addIndexes(dir2);
    writer.close();

    SegmentInfos sis = new SegmentInfos();
    sis.read(dir1);
    assertEquals(2, sis.size());

    FieldInfos fis1 = sis.info(0).getFieldInfos();
    FieldInfos fis2 = sis.info(1).getFieldInfos();

    assertEquals("f1", fis1.fieldInfo(0).name);
    assertEquals("f2", fis1.fieldInfo(1).name);
    // make sure the ordering of the "external" segment is preserved
    assertEquals("f2", fis2.fieldInfo(0).name);
    assertEquals("f1", fis2.fieldInfo(1).name);
    assertEquals("f3", fis2.fieldInfo(2).name);
    assertEquals("f4", fis2.fieldInfo(3).name);

    writer = new IndexWriter(dir1, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    writer.optimize();
    writer.close();

    sis = new SegmentInfos();
    sis.read(dir1);
    assertEquals(1, sis.size());

    FieldInfos fis3 = sis.info(0).getFieldInfos();

    // after merging the ordering should be identical to the first segment
    assertEquals("f1", fis3.fieldInfo(0).name);
    assertEquals("f2", fis3.fieldInfo(1).name);
    assertEquals("f3", fis3.fieldInfo(2).name);
    assertEquals("f4", fis3.fieldInfo(3).name);

    dir1.close();
    dir2.close();
  }
  
  public void testFieldNumberGaps() throws IOException {
    for (int i = 0; i < 39; i++) {
      Directory dir = newDirectory();
      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(
            NoMergePolicy.NO_COMPOUND_FILES));
        Document d = new Document();
        d.add(new Field("f1", "d1 first field", Store.YES, Index.ANALYZED,
            TermVector.NO));
        d.add(new Field("f2", "d1 second field", Store.YES, Index.ANALYZED,
            TermVector.NO));
        writer.addDocument(d);
        writer.close();
        SegmentInfos sis = new SegmentInfos();
        sis.read(dir);
        assertEquals(1, sis.size());
        FieldInfos fis1 = sis.info(0).getFieldInfos();
        assertEquals("f1", fis1.fieldInfo(0).name);
        assertEquals("f2", fis1.fieldInfo(1).name);
        assertTrue(dir.fileExists("1.fnx"));
      }
      

      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(
            random.nextBoolean() ? NoMergePolicy.NO_COMPOUND_FILES
                : NoMergePolicy.COMPOUND_FILES));
        Document d = new Document();
        d.add(new Field("f1", "d2 first field", Store.YES, Index.ANALYZED,
            TermVector.NO));
        d.add(new Field("f3", new byte[] { 1, 2, 3 }));
        writer.addDocument(d);
        writer.close();
        SegmentInfos sis = new SegmentInfos();
        sis.read(dir);
        assertEquals(2, sis.size());
        FieldInfos fis1 = sis.info(0).getFieldInfos();
        FieldInfos fis2 = sis.info(1).getFieldInfos();
        assertEquals("f1", fis1.fieldInfo(0).name);
        assertEquals("f2", fis1.fieldInfo(1).name);
        assertEquals("f1", fis2.fieldInfo(0).name);
        assertNull(fis2.fieldInfo(1));
        assertEquals("f3", fis2.fieldInfo(2).name);
        assertFalse(dir.fileExists("1.fnx"));
        assertTrue(dir.fileExists("2.fnx"));
      }

      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(
            random.nextBoolean() ? NoMergePolicy.NO_COMPOUND_FILES
                : NoMergePolicy.COMPOUND_FILES));
        Document d = new Document();
        d.add(new Field("f1", "d3 first field", Store.YES, Index.ANALYZED,
            TermVector.NO));
        d.add(new Field("f2", "d3 second field", Store.YES, Index.ANALYZED,
            TermVector.NO));
        d.add(new Field("f3", new byte[] { 1, 2, 3, 4, 5 }));
        writer.addDocument(d);
        writer.close();
        SegmentInfos sis = new SegmentInfos();
        sis.read(dir);
        assertEquals(3, sis.size());
        FieldInfos fis1 = sis.info(0).getFieldInfos();
        FieldInfos fis2 = sis.info(1).getFieldInfos();
        FieldInfos fis3 = sis.info(2).getFieldInfos();
        assertEquals("f1", fis1.fieldInfo(0).name);
        assertEquals("f2", fis1.fieldInfo(1).name);
        assertEquals("f1", fis2.fieldInfo(0).name);
        assertNull(fis2.fieldInfo(1));
        assertEquals("f3", fis2.fieldInfo(2).name);
        assertEquals("f1", fis3.fieldInfo(0).name);
        assertEquals("f2", fis3.fieldInfo(1).name);
        assertEquals("f3", fis3.fieldInfo(2).name);
        assertFalse(dir.fileExists("1.fnx"));
        assertTrue(dir.fileExists("2.fnx"));
        assertFalse(dir.fileExists("3.fnx"));
      }

      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(
            random.nextBoolean() ? NoMergePolicy.NO_COMPOUND_FILES
                : NoMergePolicy.COMPOUND_FILES));
        writer.deleteDocuments(new Term("f1", "d1"));
        // nuke the first segment entirely so that the segment with gaps is
        // loaded first!
        writer.expungeDeletes();
        writer.close();
      }

      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(
          new LogByteSizeMergePolicy()));
      writer.optimize();
      assertFalse(" field numbers got mixed up", writer.anyNonBulkMerges);
      writer.close();

      SegmentInfos sis = new SegmentInfos();
      sis.read(dir);
      assertEquals(1, sis.size());
      FieldInfos fis1 = sis.info(0).getFieldInfos();
      assertEquals("f1", fis1.fieldInfo(0).name);
      assertEquals("f2", fis1.fieldInfo(1).name);
      assertEquals("f3", fis1.fieldInfo(2).name);
      assertFalse(dir.fileExists("1.fnx"));
      assertTrue(dir.fileExists("2.fnx"));
      assertFalse(dir.fileExists("3.fnx"));
      dir.close();
    }
  }

  @Test
  public void testManyFields() throws Exception {
    final int NUM_DOCS = 2000;
    final int MAX_FIELDS = 50;

    int[][] docs = new int[NUM_DOCS][4];
    for (int i = 0; i < docs.length; i++) {
      for (int j = 0; j < docs[i].length;j++) {
        docs[i][j] = random.nextInt(MAX_FIELDS);
      }
    }

    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random)));

    for (int i = 0; i < NUM_DOCS; i++) {
      Document d = new Document();
      for (int j = 0; j < docs[i].length; j++) {
        d.add(getField(docs[i][j]));
      }

      writer.addDocument(d);
    }

    writer.optimize();
    writer.close();

    SegmentInfos sis = new SegmentInfos();
    sis.read(dir);
    for (SegmentInfo si : sis) {
      FieldInfos fis = si.getFieldInfos();

      for (FieldInfo fi : fis) {
        Field expected = getField(Integer.parseInt(fi.name));
        assertEquals(expected.isIndexed(), fi.isIndexed);
        assertEquals(expected.isTermVectorStored(), fi.storeTermVector);
        assertEquals(expected.isStorePositionWithTermVector(), fi.storePositionWithTermVector);
        assertEquals(expected.isStoreOffsetWithTermVector(), fi.storeOffsetWithTermVector);
      }
    }

    dir.close();
  }

  private Field getField(int number) {
    int mode = number % 16;
    String fieldName = "" + number;
    switch (mode) {
      case 0: return new Field(fieldName, "some text", Store.YES, Index.ANALYZED, TermVector.NO);
      case 1: return new Field(fieldName, "some text", Store.NO, Index.ANALYZED, TermVector.NO);
      case 2: return new Field(fieldName, "some text", Store.YES, Index.NOT_ANALYZED, TermVector.NO);
      case 3: return new Field(fieldName, "some text", Store.NO, Index.NOT_ANALYZED, TermVector.NO);
      case 4: return new Field(fieldName, "some text", Store.YES, Index.ANALYZED, TermVector.WITH_OFFSETS);
      case 5: return new Field(fieldName, "some text", Store.NO, Index.ANALYZED, TermVector.WITH_OFFSETS);
      case 6: return new Field(fieldName, "some text", Store.YES, Index.NOT_ANALYZED, TermVector.WITH_OFFSETS);
      case 7: return new Field(fieldName, "some text", Store.NO, Index.NOT_ANALYZED, TermVector.WITH_OFFSETS);
      case 8: return new Field(fieldName, "some text", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS);
      case 9: return new Field(fieldName, "some text", Store.NO, Index.ANALYZED, TermVector.WITH_POSITIONS);
      case 10: return new Field(fieldName, "some text", Store.YES, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS);
      case 11: return new Field(fieldName, "some text", Store.NO, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS);
      case 12: return new Field(fieldName, "some text", Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS);
      case 13: return new Field(fieldName, "some text", Store.NO, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS);
      case 14: return new Field(fieldName, "some text", Store.YES, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS);
      case 15: return new Field(fieldName, "some text", Store.NO, Index.NOT_ANALYZED, TermVector.WITH_POSITIONS_OFFSETS);
      default: return null;
    }
  }
}
