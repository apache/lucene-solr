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
import org.apache.lucene.document.FieldTypes;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FailOnNonBulkMergesInfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestConsistentFieldNumbers extends LuceneTestCase {

  @Test
  public void testAddIndexes() throws Exception {
    Directory dir1 = newDirectory();
    Directory dir2 = newDirectory();
    IndexWriter writer = new IndexWriter(dir1, newIndexWriterConfig(new MockAnalyzer(random()))
                                                 .setMergePolicy(NoMergePolicy.INSTANCE));

    FieldTypes fieldTypes = writer.getFieldTypes();
    Document d1 = writer.newDocument();
    d1.addLargeText("f1", "first field");
    d1.addLargeText("f2", "second field");
    writer.addDocument(d1);

    writer.close();
    writer = new IndexWriter(dir2, newIndexWriterConfig(new MockAnalyzer(random()))
                                     .setMergePolicy(NoMergePolicy.INSTANCE));

    fieldTypes = writer.getFieldTypes();
    Document d2 = writer.newDocument();
    fieldTypes.enableTermVectors("f1");
    d2.addLargeText("f2", "second field");
    d2.addLargeText("f1", "first field");
    d2.addLargeText("f3", "third field");
    d2.addLargeText("f4", "fourth field");
    writer.addDocument(d2);

    writer.close();

    writer = new IndexWriter(dir1, newIndexWriterConfig(new MockAnalyzer(random()))
                                     .setMergePolicy(NoMergePolicy.INSTANCE));
    writer.addIndexes(dir2);
    writer.close();

    SegmentInfos sis = SegmentInfos.readLatestCommit(dir1);
    assertEquals(2, sis.size());

    FieldInfos fis1 = IndexWriter.readFieldInfos(sis.info(0));
    FieldInfos fis2 = IndexWriter.readFieldInfos(sis.info(1));

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
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                    .setMergePolicy(NoMergePolicy.INSTANCE));
        FieldTypes fieldTypes = writer.getFieldTypes();
        fieldTypes.disableExistsFilters();

        Document d = writer.newDocument();
        d.addLargeText("f1", "d1 first field");
        d.addLargeText("f2", "d1 second field");
        writer.addDocument(d);
        writer.close();
        SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
        assertEquals(1, sis.size());
        FieldInfos fis1 = IndexWriter.readFieldInfos(sis.info(0));
        assertEquals("f1", fis1.fieldInfo(0).name);
        assertEquals("f2", fis1.fieldInfo(1).name);
      }
      

      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                    .setMergePolicy(NoMergePolicy.INSTANCE));
        Document d = writer.newDocument();
        d.addLargeText("f1", "d2 first field");
        d.addStored("f3", new byte[] { 1, 2, 3 });
        writer.addDocument(d);
        writer.close();
        SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
        assertEquals(2, sis.size());
        FieldInfos fis1 = IndexWriter.readFieldInfos(sis.info(0));
        FieldInfos fis2 = IndexWriter.readFieldInfos(sis.info(1));
        assertEquals("f1", fis1.fieldInfo(0).name);
        assertEquals("f2", fis1.fieldInfo(1).name);
        assertEquals("f1", fis2.fieldInfo(0).name);
        assertNull(fis2.fieldInfo(1));
        assertEquals("f3", fis2.fieldInfo(2).name);
      }

      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                    .setMergePolicy(NoMergePolicy.INSTANCE));
        Document d = writer.newDocument();
        d.addLargeText("f1", "d3 first field");
        d.addLargeText("f2", "d3 second field");
        d.addStored("f3", new byte[] { 1, 2, 3, 4, 5 });
        writer.addDocument(d);
        writer.close();
        SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
        assertEquals(3, sis.size());
        FieldInfos fis1 = IndexWriter.readFieldInfos(sis.info(0));
        FieldInfos fis2 = IndexWriter.readFieldInfos(sis.info(1));
        FieldInfos fis3 = IndexWriter.readFieldInfos(sis.info(2));
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
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                    .setMergePolicy(NoMergePolicy.INSTANCE));
        writer.deleteDocuments(new Term("f1", "d1"));
        // nuke the first segment entirely so that the segment with gaps is
        // loaded first!
        writer.forceMergeDeletes();
        writer.close();
      }

      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random()))
                                                  .setMergePolicy(new LogByteSizeMergePolicy())
                                                  .setInfoStream(new FailOnNonBulkMergesInfoStream()));
      writer.forceMerge(1);
      writer.close();

      SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
      assertEquals(1, sis.size());
      FieldInfos fis1 = IndexWriter.readFieldInfos(sis.info(0));
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
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    FieldTypes fieldTypes = writer.getFieldTypes();
    fieldTypes.disableExistsFilters();
    for(int i=0;i<MAX_FIELDS;i++) {
      fieldTypes.setMultiValued(""+i);
    }

    for (int i = 0; i < NUM_DOCS; i++) {
      Document d = writer.newDocument();
      for (int j = 0; j < docs[i].length; j++) {
        addField(fieldTypes, d, docs[i][j]);
      }

      writer.addDocument(d);
    }

    Document d = writer.newDocument();

    for(int i=0;i<MAX_FIELDS;i++) {
      addField(fieldTypes, d, i);
    }

    writer.forceMerge(1);
    writer.close();

    SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
    for (SegmentCommitInfo si : sis) {
      FieldInfos fis = IndexWriter.readFieldInfos(si);

      for (FieldInfo fi : fis) {
        IndexableField expected = d.getField(fi.name);
        assertEquals(expected.fieldType().indexOptions(), fi.getIndexOptions());
        assertEquals(expected.fieldType().storeTermVectors(), fi.hasVectors());
      }
    }

    dir.close();
  }

  private void addField(FieldTypes fieldTypes, Document d, int number) {
    String fieldName = "" + number;

    int mode = number % 16;
    switch (mode) {
    case 0:
      d.addStored(fieldName, "some text");
      return;
    case 1:
      d.addLargeText(fieldName, "some text");
      return;
    case 2:
      d.addAtom(fieldName, "some text");
      return;
    case 3:
      fieldTypes.disableStored(fieldName);
      d.addAtom(fieldName, "some text");
      return;
    case 4:
      fieldTypes.disableStored(fieldName);
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorOffsets(fieldName);
      d.addAtom(fieldName, "some text");
      return;
    case 5:
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorOffsets(fieldName);
      d.addLargeText(fieldName, "some text");
      return;
    case 6:
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorOffsets(fieldName);
      d.addAtom(fieldName, "some text");
      return;
    case 7:
      fieldTypes.disableStored(fieldName);
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorOffsets(fieldName);
      d.addAtom(fieldName, "some text");
      return;
    case 8:
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorPositions(fieldName);
      d.addAtom(fieldName, "some text");
      return;
    case 9:
      fieldTypes.disableStored(fieldName);
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorPositions(fieldName);
      d.addLargeText(fieldName, "some text");
      return;
    case 10:
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorPositions(fieldName);
      d.addAtom(fieldName, "some text");
      return;
    case 11:
      fieldTypes.disableStored(fieldName);
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorPositions(fieldName);
      d.addAtom(fieldName, "some text");
      return;
    case 12:
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorPositions(fieldName);
      fieldTypes.enableTermVectorOffsets(fieldName);
      d.addLargeText(fieldName, "some text");
      return;
    case 13:
      fieldTypes.disableStored(fieldName);
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorPositions(fieldName);
      fieldTypes.enableTermVectorOffsets(fieldName);
      d.addLargeText(fieldName, "some text");
      return;
    case 14:
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorPositions(fieldName);
      fieldTypes.enableTermVectorOffsets(fieldName);
      d.addAtom(fieldName, "some text");
      return;
    case 15:
      fieldTypes.disableStored(fieldName);
      fieldTypes.enableTermVectors(fieldName);
      fieldTypes.enableTermVectorPositions(fieldName);
      fieldTypes.enableTermVectorOffsets(fieldName);
      d.addAtom(fieldName, "some text");
      return;
    default:
      assert false;
    }
  }
}
