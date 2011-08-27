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
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfos.FieldNumberBiMap;
import org.apache.lucene.index.codecs.DefaultSegmentInfosWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestGlobalFieldNumbers extends LuceneTestCase {

  public void testGlobalFieldNumberFiles() throws IOException {
    int num = atLeast(3);
    for (int i = 0; i < num; i++) {
      Directory dir = newDirectory();
      {
        IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT,
            new MockAnalyzer(random));
        IndexWriter writer = new IndexWriter(dir, config);
        Document d = new Document();
        d.add(new Field("f1", TextField.TYPE_STORED, "d1 first field"));
        d.add(new Field("f2", TextField.TYPE_STORED, "d1 second field"));
        writer.addDocument(d);
        for (String string : writer.getIndexFileNames()) {
          assertFalse(string.endsWith(".fnx"));
        }
        writer.commit();
        Collection<String> files = writer.getIndexFileNames();
        files.remove("1.fnx");
        for (String string : files) {
          assertFalse(string.endsWith(".fnx"));
        }

        assertFNXFiles(dir, "1.fnx");
        d = new Document();
        d.add(new Field("f1", TextField.TYPE_STORED, "d2 first field"));
        d.add(new BinaryField("f3", new byte[] { 1, 2, 3 }));
        writer.addDocument(d);
        writer.commit();
        files = writer.getIndexFileNames();
        files.remove("2.fnx");
        for (String string : files) {
          assertFalse(string.endsWith(".fnx"));
        }
        assertFNXFiles(dir, "2.fnx");
        writer.close();
        assertFNXFiles(dir, "2.fnx");
      }

      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        Document d = new Document();
        d.add(new Field("f1", TextField.TYPE_STORED, "d3 first field"));
        d.add(new Field("f2", TextField.TYPE_STORED, "d3 second field"));
        d.add(new BinaryField("f3", new byte[] { 1, 2, 3, 4, 5 }));
        writer.addDocument(d);
        writer.close();
        Collection<String> files = writer.getIndexFileNames();
        files.remove("2.fnx");
        for (String string : files) {
          assertFalse(string.endsWith(".fnx"));
        }

        assertFNXFiles(dir, "2.fnx");
      }

      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      writer.optimize();
      assertFalse(" field numbers got mixed up", writer.anyNonBulkMerges);
      writer.close();
      assertFNXFiles(dir, "2.fnx");

      dir.close();
    }
  }

  public void testIndexReaderCommit() throws IOException {
    int num = atLeast(3);
    for (int i = 0; i < num; i++) {
      Directory dir = newDirectory();
      {
        IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT,
            new MockAnalyzer(random));
        IndexWriter writer = new IndexWriter(dir, config);
        Document d = new Document();
        d.add(new Field("f1", TextField.TYPE_STORED, "d1 first field"));
        d.add(new Field("f2", TextField.TYPE_STORED, "d1 second field"));
        writer.addDocument(d);
        writer.commit();
        assertFNXFiles(dir, "1.fnx");
        d = new Document();
        d.add(new Field("f1", TextField.TYPE_STORED, "d2 first field"));
        d.add(new BinaryField("f3", new byte[] { 1, 2, 3 }));
        writer.addDocument(d);
        writer.commit();
        assertFNXFiles(dir, "2.fnx");
        writer.close();
        assertFNXFiles(dir, "2.fnx");
      }
      IndexReader reader = IndexReader.open(dir, false);
      reader.deleteDocument(0);
      reader.commit();
      reader.close();
      // make sure this reader can not modify the field map
      assertFNXFiles(dir, "2.fnx");

      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      writer.optimize();
      assertFalse(" field numbers got mixed up", writer.anyNonBulkMerges);
      writer.close();
      assertFNXFiles(dir, "2.fnx");

      dir.close();
    }
  }

  public void testGlobalFieldNumberFilesAcrossCommits() throws IOException {
    int num = atLeast(3);
    for (int i = 0; i < num; i++) {
      Directory dir = newDirectory();
      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random)).setIndexDeletionPolicy(
            new KeepAllDeletionPolicy()));
        Document d = new Document();
        d.add(new Field("f1", TextField.TYPE_STORED, "d1 first field"));
        d.add(new Field("f2", TextField.TYPE_STORED, "d1 second field"));
        writer.addDocument(d);
        writer.commit();
        assertFNXFiles(dir, "1.fnx");
        d = new Document();
        d.add(new Field("f1", TextField.TYPE_STORED, "d2 first field"));
        d.add(new BinaryField("f3", new byte[] { 1, 2, 3 }));
        writer.addDocument(d);
        writer.commit();
        writer.commit();
        writer.commit();
        assertFNXFiles(dir, "1.fnx", "2.fnx");
        writer.close();
        assertFNXFiles(dir, "1.fnx", "2.fnx");
      }

      {
        IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        Document d = new Document();
        d.add(new Field("f1", TextField.TYPE_STORED, "d3 first field"));
        d.add(new Field("f2", TextField.TYPE_STORED, "d3 second field"));
        d.add(new BinaryField("f3", new byte[] { 1, 2, 3, 4, 5 }));
        writer.addDocument(d);
        writer.close();
        assertFNXFiles(dir, "2.fnx");
      }
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      writer.optimize();
      assertFalse(" field numbers got mixed up", writer.anyNonBulkMerges);
      writer.close();
      assertFNXFiles(dir, "2.fnx");
      dir.close();
    }
  }

  public void testGlobalFieldNumberOnOldCommit() throws IOException {
    int num = atLeast(3);
    for (int i = 0; i < num; i++) {
      Directory dir = newDirectory();
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)).setIndexDeletionPolicy(
          new KeepAllDeletionPolicy()));
      Document d = new Document();
      d.add(new Field("f1", TextField.TYPE_STORED, "d1 first field"));
      d.add(new Field("f2", TextField.TYPE_STORED, "d1 second field"));
      writer.addDocument(d);
      writer.commit();
      assertFNXFiles(dir, "1.fnx");
      d = new Document();
      d.add(new Field("f1", TextField.TYPE_STORED, "d2 first field"));
      d.add(new BinaryField("f3", new byte[] { 1, 2, 3 }));
      writer.addDocument(d);
      assertFNXFiles(dir, "1.fnx");
      writer.close();
      assertFNXFiles(dir, "1.fnx", "2.fnx");
      // open first commit
      List<IndexCommit> listCommits = IndexReader.listCommits(dir);
      assertEquals(2, listCommits.size());
      writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT,
          new MockAnalyzer(random)).setIndexDeletionPolicy(
          new KeepAllDeletionPolicy()).setIndexCommit(listCommits.get(0)));

      d = new Document();
      d.add(new Field("f1", TextField.TYPE_STORED, "d2 first field"));
      d.add(new BinaryField("f3", new byte[] { 1, 2, 3 }));
      writer.addDocument(d);
      writer.commit();
      // now we have 3 files since f3 is not present in the first commit
      assertFNXFiles(dir, "1.fnx", "2.fnx", "3.fnx");
      writer.close();
      assertFNXFiles(dir, "1.fnx", "2.fnx", "3.fnx");

      writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT,
          new MockAnalyzer(random)));
      writer.commit();
      listCommits = IndexReader.listCommits(dir);
      assertEquals(1, listCommits.size());
      assertFNXFiles(dir, "3.fnx");
      writer.close();
      assertFNXFiles(dir, "3.fnx");
      dir.close();
    }
  }

  private final Directory buildRandomIndex(String[] fieldNames, int numDocs,
      IndexWriterConfig conf) throws CorruptIndexException,
      LockObtainFailedException, IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, conf);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      final int numFields = 1 + random.nextInt(fieldNames.length);
      for (int j = 0; j < numFields; j++) {
        FieldType customType = new FieldType();
        customType.setIndexed(true);
        customType.setTokenized(random.nextBoolean());
        customType.setOmitNorms(random.nextBoolean());
        doc.add(newField(fieldNames[random.nextInt(fieldNames.length)],
            _TestUtil.randomRealisticUnicodeString(random),
            customType));

      }
      writer.addDocument(doc);
      if (random.nextInt(20) == 0) {
        writer.commit();
      }
    }
    writer.close();
    return dir;
  }

  public void testOptimize() throws IOException {
    for (int i = 0; i < 2*RANDOM_MULTIPLIER; i++) {
      Set<String> fieldNames = new HashSet<String>();
      final int numFields = 2 + (TEST_NIGHTLY ? random.nextInt(200) : random.nextInt(20));
      for (int j = 0; j < numFields; j++) {
        fieldNames.add("field_" + j);
      }
      Directory base = buildRandomIndex(fieldNames.toArray(new String[0]),
          20 + random.nextInt(100),
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      IndexWriter writer = new IndexWriter(base, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      FieldNumberBiMap globalFieldMap = writer.segmentInfos
          .getOrLoadGlobalFieldNumberMap(base);
      Set<Entry<String, Integer>> entries = globalFieldMap.entries();
      writer.optimize();
      writer.commit();
      writer.close();
      Set<Entry<String, Integer>> afterOptmize = globalFieldMap.entries();
      assertEquals(entries, afterOptmize);
      base.close();
    }
  }

  public void testAddIndexesStableFieldNumbers() throws IOException {
    for (int i = 0; i < 2*RANDOM_MULTIPLIER; i++) {
      Set<String> fieldNames = new HashSet<String>();
      final int numFields = 2 + (TEST_NIGHTLY ? random.nextInt(50) : random.nextInt(10));
      for (int j = 0; j < numFields; j++) {
        fieldNames.add("field_" + j);
      }

      Directory base = newDirectory();
      IndexWriter writer = new IndexWriter(base, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      Document doc = new Document();
      for (String string : fieldNames) {
        FieldType customType = new FieldType();
        customType.setIndexed(true);
        customType.setTokenized(random.nextBoolean());
        customType.setOmitNorms(random.nextBoolean());
        doc.add(newField(string,
            _TestUtil.randomRealisticUnicodeString(random),
            customType));

      }
      writer.addDocument(doc);
      writer.commit();
      FieldNumberBiMap globalFieldMap = writer.segmentInfos
          .getOrLoadGlobalFieldNumberMap(base);
      final Set<Entry<String, Integer>> entries = globalFieldMap.entries();
      assertEquals(entries.size(), fieldNames.size());
      for (Entry<String, Integer> entry : entries) {
        // all fields are in this fieldMap
        assertTrue(fieldNames.contains(entry.getKey()));
      }
      writer.close();

      int numIndexes = 1 + random.nextInt(10);
      for (int j = 0; j < numIndexes; j++) {
        Directory toAdd = buildRandomIndex(fieldNames.toArray(new String[0]),
            1 + random.nextInt(50),
            newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        IndexWriter w = new IndexWriter(base, newIndexWriterConfig(
            TEST_VERSION_CURRENT, new MockAnalyzer(random)));
        if (random.nextBoolean()) {
          IndexReader open = IndexReader.open(toAdd);
          w.addIndexes(open);
          open.close();
        } else {
          w.addIndexes(toAdd);
        }

        w.close();
        FieldNumberBiMap map = w.segmentInfos
            .getOrLoadGlobalFieldNumberMap(toAdd);
        assertEquals(entries, map.entries());
        toAdd.close();
      }
      IndexWriter w = new IndexWriter(base, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(
          new LogByteSizeMergePolicy()));
      w.optimize();
      w.close();
      SegmentInfos sis = new SegmentInfos();
      sis.read(base);
      SegmentInfo segmentInfo = sis.info(sis.size() - 1);// last segment must
                                                        // have all fields with
                                                        // consistent numbers
      FieldInfos fieldInfos = segmentInfo.getFieldInfos();
      assertEquals(fieldInfos.size(), entries.size());
      for (Entry<String, Integer> entry : entries) {
        assertEquals(entry.getValue(),
            Integer.valueOf(fieldInfos.fieldNumber(entry.getKey())));
        assertEquals(entry.getKey(), fieldInfos.fieldName(entry.getValue()));
      }
      base.close();
    }
  }

  final String[] oldNames = { "30.cfs", "30.nocfs", "31.cfs", "31.nocfs", };

  public void testAddOldIndex() throws IOException {
    int i = random.nextInt(oldNames.length);
    File oldIndxeDir = _TestUtil.getTempDir(oldNames[i]);
    Directory dir = null;
    try {
      _TestUtil
          .unzip(getDataFile("index." + oldNames[i] + ".zip"), oldIndxeDir);
      dir = newFSDirectory(oldIndxeDir);
      SegmentInfos infos = new SegmentInfos();
      infos.read(dir);
      SortedMap<Integer, String> sortedMap = new TreeMap<Integer, String>();

      FieldNumberBiMap biMap = new FieldNumberBiMap();
      int maxFieldNum = Integer.MIN_VALUE;
      for (SegmentInfo segmentInfo : infos) {
        for (FieldInfo fieldInfo : segmentInfo.getFieldInfos()) {
          int globNumber = biMap.addOrGet(fieldInfo.name, fieldInfo.number);
          maxFieldNum = Math.max(maxFieldNum, globNumber);
          sortedMap.put(globNumber, fieldInfo.name);
        }
      }
      Directory base = newDirectory();
      IndexWriter writer = new IndexWriter(base, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(
          NoMergePolicy.NO_COMPOUND_FILES));

      SortedMap<Integer, String> copySortedMap = new TreeMap<Integer, String>(
          sortedMap);
      while (!sortedMap.isEmpty()) { // add every field at least once
        Document doc = new Document();
        int nextField = random.nextInt(maxFieldNum + 1);
        sortedMap.remove(nextField);

        String name = copySortedMap.get(nextField);
        assertNotNull(name);

        FieldType customType = new FieldType();
        customType.setIndexed(true);
        customType.setTokenized(random.nextBoolean());
        customType.setOmitNorms(random.nextBoolean());
        doc.add(newField(name, _TestUtil.randomRealisticUnicodeString(random),
            customType));
        writer.addDocument(doc);
        if (random.nextInt(10) == 0) {
          writer.commit();
        }
      }
      Set<Entry<String, Integer>> expectedEntries = writer.segmentInfos
          .getOrLoadGlobalFieldNumberMap(base).entries();
      writer.addIndexes(dir); // add the old index
      writer.close();

      writer = new IndexWriter(base, newIndexWriterConfig(TEST_VERSION_CURRENT,
          new MockAnalyzer(random)).setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES));
      writer.commit(); // make sure the old index is the latest segment
      writer.close();

      // we don't merge here since we use NoMergePolicy
      SegmentInfos sis = new SegmentInfos();
      sis.read(base);
      // check that the latest global field numbers are consistent and carried
      // over from the 4.0 index
      FieldNumberBiMap actualGlobalMap = sis
          .getOrLoadGlobalFieldNumberMap(base);
      assertEquals(expectedEntries, actualGlobalMap.entries());
      base.close();
    } finally {
      if (dir != null)
        dir.close();
      _TestUtil.rmDir(oldIndxeDir);
    }
  }
  
  public void testFilesOnOldIndex() throws IOException {
    int i = random.nextInt(oldNames.length);
    File oldIndxeDir = _TestUtil.getTempDir(oldNames[i]);
    Directory dir = null;
    
    MergePolicy policy = random.nextBoolean() ? NoMergePolicy.COMPOUND_FILES : NoMergePolicy.NO_COMPOUND_FILES;
    try {
      _TestUtil
          .unzip(getDataFile("index." + oldNames[i] + ".zip"), oldIndxeDir);
      dir = newFSDirectory(oldIndxeDir);
      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(policy));
      SegmentInfos segmentInfos = writer.segmentInfos;
      assertTrue(DefaultSegmentInfosWriter.FORMAT_4_0 < segmentInfos.getFormat());
      assertEquals(0, segmentInfos.getGlobalFieldMapVersion());
      for (String string : writer.getIndexFileNames()) {
        assertFalse(string.endsWith(".fnx"));
      }
      writer.commit();
      
      assertTrue(DefaultSegmentInfosWriter.FORMAT_4_0 < segmentInfos.getFormat());
      assertEquals(0, segmentInfos.getGlobalFieldMapVersion());
      Collection<String> files = writer.getIndexFileNames();
      for (String string : files) {
        assertFalse(string.endsWith(".fnx"));
      }
      
      Document d = new Document();
      d.add(new Field("f1", TextField.TYPE_STORED, "d1 first field"));
      writer.addDocument(d);
      writer.prepareCommit();
      // the fnx file should still be under control of the SIS
      assertTrue(DefaultSegmentInfosWriter.FORMAT_4_0 < segmentInfos.getFormat());
      assertEquals(0, segmentInfos.getLastGlobalFieldMapVersion());
      assertEquals(1, segmentInfos.getGlobalFieldMapVersion());
      files = writer.getIndexFileNames();
      for (String string : files) {
        assertFalse(string.endsWith(".fnx"));
      }
      
      writer.commit();
      
      // now we should see the fnx file even if this is a 3.x segment
      assertTrue(DefaultSegmentInfosWriter.FORMAT_4_0 < segmentInfos.getFormat());
      assertEquals(1, segmentInfos.getGlobalFieldMapVersion());
      assertEquals(1, segmentInfos.getLastGlobalFieldMapVersion());
      files = writer.getIndexFileNames();
      assertTrue(files.remove("1.fnx"));
      for (String string : files) {
        assertFalse(string.endsWith(".fnx"));
      }
      writer.close();
    } finally {
      if (dir != null)
        dir.close();
      _TestUtil.rmDir(oldIndxeDir);
    }
  }

  class KeepAllDeletionPolicy implements IndexDeletionPolicy {
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
    }

    public void onCommit(List<? extends IndexCommit> commits)
        throws IOException {
    }
  }

  public static void assertFNXFiles(Directory dir, String... expectedFnxFiles)
      throws IOException {
    String[] listAll = dir.listAll();
    Set<String> fnxFiles = new HashSet<String>();
    for (String string : listAll) {
      if (string.endsWith(".fnx")) {
        fnxFiles.add(string);
      }
    }
    assertEquals("" + fnxFiles, expectedFnxFiles.length, fnxFiles.size());
    for (String string : expectedFnxFiles) {
      assertTrue(" missing fnx file: " + string, fnxFiles.contains(string));
    }
  }

}
