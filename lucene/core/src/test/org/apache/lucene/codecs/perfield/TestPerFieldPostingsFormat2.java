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
package org.apache.lucene.codecs.perfield;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.asserting.AssertingCodec;
import org.apache.lucene.codecs.blockterms.LuceneVarGapFixedInterval;
import org.apache.lucene.codecs.memory.DirectPostingsFormat;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;

/**
 * 
 *
 */
//TODO: would be better in this test to pull termsenums and instanceof or something?
// this way we can verify PFPF is doing the right thing.
// for now we do termqueries.
public class TestPerFieldPostingsFormat2 extends LuceneTestCase {

  private IndexWriter newWriter(Directory dir, IndexWriterConfig conf)
      throws IOException {
    LogDocMergePolicy logByteSizeMergePolicy = new LogDocMergePolicy();
    logByteSizeMergePolicy.setNoCFSRatio(0.0); // make sure we use plain
    // files
    conf.setMergePolicy(logByteSizeMergePolicy);

    final IndexWriter writer = new IndexWriter(dir, conf);
    return writer;
  }

  private void addDocs(IndexWriter writer, int numDocs) throws IOException {
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(newTextField("content", "aaa", Field.Store.NO));
      writer.addDocument(doc);
    }
  }

  private void addDocs2(IndexWriter writer, int numDocs) throws IOException {
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(newTextField("content", "bbb", Field.Store.NO));
      writer.addDocument(doc);
    }
  }

  private void addDocs3(IndexWriter writer, int numDocs) throws IOException {
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(newTextField("content", "ccc", Field.Store.NO));
      doc.add(newStringField("id", "" + i, Field.Store.YES));
      writer.addDocument(doc);
    }
  }

  /*
   * Test that heterogeneous index segments are merge successfully
   */
  @Test
  public void testMergeUnusedPerFieldCodec() throws IOException {
    Directory dir = newDirectory();
    IndexWriterConfig iwconf = newIndexWriterConfig(new MockAnalyzer(random()))
                                 .setOpenMode(OpenMode.CREATE).setCodec(new MockCodec());
    IndexWriter writer = newWriter(dir, iwconf);
    addDocs(writer, 10);
    writer.commit();
    addDocs3(writer, 10);
    writer.commit();
    addDocs2(writer, 10);
    writer.commit();
    assertEquals(30, writer.maxDoc());
    TestUtil.checkIndex(dir);
    writer.forceMerge(1);
    assertEquals(30, writer.maxDoc());
    writer.close();
    dir.close();
  }

  /*
   * Test that heterogeneous index segments are merged sucessfully
   */
  // TODO: not sure this test is that great, we should probably peek inside PerFieldPostingsFormat or something?!
  @Test
  public void testChangeCodecAndMerge() throws IOException {
    Directory dir = newDirectory();
    if (VERBOSE) {
      System.out.println("TEST: make new index");
    }
    IndexWriterConfig iwconf = newIndexWriterConfig(new MockAnalyzer(random()))
                                 .setOpenMode(OpenMode.CREATE).setCodec(new MockCodec());
    iwconf.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    //((LogMergePolicy) iwconf.getMergePolicy()).setMergeFactor(10);
    IndexWriter writer = newWriter(dir, iwconf);

    addDocs(writer, 10);
    writer.commit();
    assertQuery(new Term("content", "aaa"), dir, 10);
    if (VERBOSE) {
      System.out.println("TEST: addDocs3");
    }
    addDocs3(writer, 10);
    writer.commit();
    writer.close();

    assertQuery(new Term("content", "ccc"), dir, 10);
    assertQuery(new Term("content", "aaa"), dir, 10);
    Codec codec = iwconf.getCodec();

    iwconf = newIndexWriterConfig(new MockAnalyzer(random()))
        .setOpenMode(OpenMode.APPEND).setCodec(codec);
    //((LogMergePolicy) iwconf.getMergePolicy()).setNoCFSRatio(0.0);
    //((LogMergePolicy) iwconf.getMergePolicy()).setMergeFactor(10);
    iwconf.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);

    iwconf.setCodec(new MockCodec2()); // uses standard for field content
    writer = newWriter(dir, iwconf);
    // swap in new codec for currently written segments
    if (VERBOSE) {
      System.out.println("TEST: add docs w/ Standard codec for content field");
    }
    addDocs2(writer, 10);
    writer.commit();
    codec = iwconf.getCodec();
    assertEquals(30, writer.maxDoc());
    assertQuery(new Term("content", "bbb"), dir, 10);
    assertQuery(new Term("content", "ccc"), dir, 10);   ////
    assertQuery(new Term("content", "aaa"), dir, 10);

    if (VERBOSE) {
      System.out.println("TEST: add more docs w/ new codec");
    }
    addDocs2(writer, 10);
    writer.commit();
    assertQuery(new Term("content", "ccc"), dir, 10);
    assertQuery(new Term("content", "bbb"), dir, 20);
    assertQuery(new Term("content", "aaa"), dir, 10);
    assertEquals(40, writer.maxDoc());

    if (VERBOSE) {
      System.out.println("TEST: now optimize");
    }
    writer.forceMerge(1);
    assertEquals(40, writer.maxDoc());
    writer.close();
    assertQuery(new Term("content", "ccc"), dir, 10);
    assertQuery(new Term("content", "bbb"), dir, 20);
    assertQuery(new Term("content", "aaa"), dir, 10);

    dir.close();
  }

  public void assertQuery(Term t, Directory dir, int num)
      throws IOException {
    if (VERBOSE) {
      System.out.println("\nTEST: assertQuery " + t);
    }
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    TopDocs search = searcher.search(new TermQuery(t), num + 10);
    assertEquals(num, search.totalHits);
    reader.close();

  }

  public static class MockCodec extends AssertingCodec {
    final PostingsFormat luceneDefault = TestUtil.getDefaultPostingsFormat();
    final PostingsFormat direct = new DirectPostingsFormat();
    final PostingsFormat memory = new MemoryPostingsFormat();
    
    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      if (field.equals("id")) {
        return direct;
      } else if (field.equals("content")) {
        return memory;
      } else {
        return luceneDefault;
      }
    }
  }

  public static class MockCodec2 extends AssertingCodec {
    final PostingsFormat luceneDefault = TestUtil.getDefaultPostingsFormat();
    final PostingsFormat direct = new DirectPostingsFormat();
    
    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
      if (field.equals("id")) {
        return direct;
      } else {
        return luceneDefault;
      }
    }
  }

  /*
   * Test per field codec support - adding fields with random codecs
   */
  @Test
  public void testStressPerFieldCodec() throws IOException {
    Directory dir = newDirectory(random());
    final int docsPerRound = 97;
    int numRounds = atLeast(1);
    for (int i = 0; i < numRounds; i++) {
      int num = TestUtil.nextInt(random(), 30, 60);
      IndexWriterConfig config = newIndexWriterConfig(random(),
          new MockAnalyzer(random()));
      config.setOpenMode(OpenMode.CREATE_OR_APPEND);
      IndexWriter writer = newWriter(dir, config);
      for (int j = 0; j < docsPerRound; j++) {
        final Document doc = new Document();
        for (int k = 0; k < num; k++) {
          FieldType customType = new FieldType(TextField.TYPE_NOT_STORED);
          customType.setTokenized(random().nextBoolean());
          customType.setOmitNorms(random().nextBoolean());
          Field field = newField("" + k, TestUtil
              .randomRealisticUnicodeString(random(), 128), customType);
          doc.add(field);
        }
        writer.addDocument(doc);
      }
      if (random().nextBoolean()) {
        writer.forceMerge(1);
      }
      writer.commit();
      assertEquals((i + 1) * docsPerRound, writer.maxDoc());
      writer.close();
    }
    dir.close();
  }

  public void testSameCodecDifferentInstance() throws Exception {
    Codec codec = new AssertingCodec() {
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        if ("id".equals(field)) {
          return new MemoryPostingsFormat();
        } else if ("date".equals(field)) {
          return new MemoryPostingsFormat();
        } else {
          return super.getPostingsFormatForField(field);
        }
      }
    };
    doTestMixedPostings(codec);
  }
  
  public void testSameCodecDifferentParams() throws Exception {
    Codec codec = new AssertingCodec() {
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        if ("id".equals(field)) {
          return new LuceneVarGapFixedInterval(1);
        } else if ("date".equals(field)) {
          return new LuceneVarGapFixedInterval(2);
        } else {
          return super.getPostingsFormatForField(field);
        }
      }
    };
    doTestMixedPostings(codec);
  }
  
  private void doTestMixedPostings(Codec codec) throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(codec);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_NOT_STORED);
    // turn on vectors for the checkindex cross-check
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorOffsets(true);
    ft.setStoreTermVectorPositions(true);
    Field idField = new Field("id", "", ft);
    Field dateField = new Field("date", "", ft);
    doc.add(idField);
    doc.add(dateField);
    for (int i = 0; i < 100; i++) {
      idField.setStringValue(Integer.toString(random().nextInt(50)));
      dateField.setStringValue(Integer.toString(random().nextInt(100)));
      iw.addDocument(doc);
    }
    iw.close();
    dir.close(); // checkindex
  }

  @SuppressWarnings("deprecation")
  public void testMergeCalledOnTwoFormats() throws IOException {
    MergeRecordingPostingsFormatWrapper pf1 = new MergeRecordingPostingsFormatWrapper(TestUtil.getDefaultPostingsFormat());
    MergeRecordingPostingsFormatWrapper pf2 = new MergeRecordingPostingsFormatWrapper(TestUtil.getDefaultPostingsFormat());

    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(new AssertingCodec() {
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        switch (field) {
          case "f1":
          case "f2":
            return pf1;

          case "f3":
          case "f4":
            return pf2;

          default:
            return super.getPostingsFormatForField(field);
        }
      }
    });

    Directory directory = newDirectory();

    IndexWriter iwriter = new IndexWriter(directory, iwc);

    Document doc = new Document();
    doc.add(new StringField("f1", "val1", Field.Store.NO));
    doc.add(new StringField("f2", "val2", Field.Store.YES));
    doc.add(new IntPoint("f3", 3)); // Points are not indexed as postings and should not appear in the merge fields
    doc.add(new StringField("f4", "val4", Field.Store.NO));
    iwriter.addDocument(doc);
    iwriter.commit();

    doc = new Document();
    doc.add(new StringField("f1", "val5", Field.Store.NO));
    doc.add(new StringField("f2", "val6", Field.Store.YES));
    doc.add(new IntPoint("f3", 7));
    doc.add(new StringField("f4", "val8", Field.Store.NO));
    iwriter.addDocument(doc);
    iwriter.commit();

    iwriter.forceMerge(1, true);
    iwriter.close();

    assertEquals(1, pf1.nbMergeCalls);
    assertEquals(new HashSet<>(Arrays.asList("f1", "f2")), new HashSet<>(pf1.fieldNames));
    assertEquals(1, pf2.nbMergeCalls);
    assertEquals(Collections.singletonList("f4"), pf2.fieldNames);

    directory.close();
  }

  private static final class MergeRecordingPostingsFormatWrapper extends PostingsFormat {
    private final PostingsFormat delegate;
    final List<String> fieldNames = new ArrayList<>();
    int nbMergeCalls = 0;

    MergeRecordingPostingsFormatWrapper(PostingsFormat delegate) {
      super(delegate.getName());
      this.delegate = delegate;
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
      final FieldsConsumer consumer = delegate.fieldsConsumer(state);
      return new FieldsConsumer() {
        @Override
        public void write(Fields fields) throws IOException {
          consumer.write(fields);
        }

        @Override
        public void merge(MergeState mergeState) throws IOException {
          nbMergeCalls++;
          for (FieldInfo fi : mergeState.mergeFieldInfos) {
            fieldNames.add(fi.name);
          }
          consumer.merge(mergeState);
        }

        @Override
        public void close() throws IOException {
          consumer.close();
        }
      };
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
      return delegate.fieldsProducer(state);
    }
  }
}
