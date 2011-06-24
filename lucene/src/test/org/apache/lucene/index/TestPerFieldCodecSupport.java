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
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.index.CheckIndex.Status;
import org.apache.lucene.index.CheckIndex.Status.SegmentInfoStatus;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.mockintblock.MockFixedIntBlockCodec;
import org.apache.lucene.index.codecs.mockintblock.MockVariableIntBlockCodec;
import org.apache.lucene.index.codecs.mocksep.MockSepCodec;
import org.apache.lucene.index.codecs.pulsing.PulsingCodec;
import org.apache.lucene.index.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.index.codecs.standard.StandardCodec;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.Test;

/**
 * 
 *
 */
public class TestPerFieldCodecSupport extends LuceneTestCase {

  private IndexWriter newWriter(Directory dir, IndexWriterConfig conf)
      throws IOException {
    LogDocMergePolicy logByteSizeMergePolicy = new LogDocMergePolicy();
    logByteSizeMergePolicy.setUseCompoundFile(false); // make sure we use plain
    // files
    conf.setMergePolicy(logByteSizeMergePolicy);

    final IndexWriter writer = new IndexWriter(dir, conf);
    writer.setInfoStream(VERBOSE ? System.out : null);
    return writer;
  }

  private void addDocs(IndexWriter writer, int numDocs) throws IOException {
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(newField("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
  }

  private void addDocs2(IndexWriter writer, int numDocs) throws IOException {
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(newField("content", "bbb", Field.Store.NO, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
  }

  private void addDocs3(IndexWriter writer, int numDocs) throws IOException {
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(newField("content", "ccc", Field.Store.NO, Field.Index.ANALYZED));
      doc.add(newField("id", "" + i, Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
  }

  /*
   * Test is hetrogenous index segements are merge sucessfully
   */
  @Test
  public void testMergeUnusedPerFieldCodec() throws IOException {
    Directory dir = newDirectory();
    CodecProvider provider = new MockCodecProvider();
    IndexWriterConfig iwconf = newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random)).setOpenMode(OpenMode.CREATE).setCodecProvider(
        provider);
    IndexWriter writer = newWriter(dir, iwconf);
    addDocs(writer, 10);
    writer.commit();
    addDocs3(writer, 10);
    writer.commit();
    addDocs2(writer, 10);
    writer.commit();
    assertEquals(30, writer.maxDoc());
    _TestUtil.checkIndex(dir, provider);
    writer.optimize();
    assertEquals(30, writer.maxDoc());
    writer.close();
    dir.close();
  }

  /*
   * Test that heterogeneous index segments are merged sucessfully
   */
  @Test
  public void testChangeCodecAndMerge() throws IOException {
    Directory dir = newDirectory();
    CodecProvider provider = new MockCodecProvider();
    if (VERBOSE) {
      System.out.println("TEST: make new index");
    }
    IndexWriterConfig iwconf = newIndexWriterConfig(TEST_VERSION_CURRENT,
             new MockAnalyzer(random)).setOpenMode(OpenMode.CREATE).setCodecProvider(provider);
    iwconf.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    //((LogMergePolicy) iwconf.getMergePolicy()).setMergeFactor(10);
    IndexWriter writer = newWriter(dir, iwconf);

    addDocs(writer, 10);
    writer.commit();
    assertQuery(new Term("content", "aaa"), dir, 10, provider);
    if (VERBOSE) {
      System.out.println("TEST: addDocs3");
    }
    addDocs3(writer, 10);
    writer.commit();
    writer.close();

    assertQuery(new Term("content", "ccc"), dir, 10, provider);
    assertQuery(new Term("content", "aaa"), dir, 10, provider);
    assertCodecPerField(_TestUtil.checkIndex(dir, provider), "content",
        provider.lookup("MockSep"));

    iwconf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setOpenMode(OpenMode.APPEND).setCodecProvider(provider);
    //((LogMergePolicy) iwconf.getMergePolicy()).setUseCompoundFile(false);
    //((LogMergePolicy) iwconf.getMergePolicy()).setMergeFactor(10);
    iwconf.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);

    provider = new MockCodecProvider2(); // uses standard for field content
    iwconf.setCodecProvider(provider);
    writer = newWriter(dir, iwconf);
    // swap in new codec for currently written segments
    if (VERBOSE) {
      System.out.println("TEST: add docs w/ Standard codec for content field");
    }
    addDocs2(writer, 10);
    writer.commit();
    Codec origContentCodec = provider.lookup("MockSep");
    Codec newContentCodec = provider.lookup("Standard");
    assertHybridCodecPerField(_TestUtil.checkIndex(dir, provider), "content",
        origContentCodec, origContentCodec, newContentCodec);
    assertEquals(30, writer.maxDoc());
    assertQuery(new Term("content", "bbb"), dir, 10, provider);
    assertQuery(new Term("content", "ccc"), dir, 10, provider);   ////
    assertQuery(new Term("content", "aaa"), dir, 10, provider);

    if (VERBOSE) {
      System.out.println("TEST: add more docs w/ new codec");
    }
    addDocs2(writer, 10);
    writer.commit();
    assertQuery(new Term("content", "ccc"), dir, 10, provider);
    assertQuery(new Term("content", "bbb"), dir, 20, provider);
    assertQuery(new Term("content", "aaa"), dir, 10, provider);
    assertEquals(40, writer.maxDoc());

    if (VERBOSE) {
      System.out.println("TEST: now optimize");
    }
    writer.optimize();
    assertEquals(40, writer.maxDoc());
    writer.close();
    assertCodecPerFieldOptimized(_TestUtil.checkIndex(dir, provider),
        "content", newContentCodec);
    assertQuery(new Term("content", "ccc"), dir, 10, provider);
    assertQuery(new Term("content", "bbb"), dir, 20, provider);
    assertQuery(new Term("content", "aaa"), dir, 10, provider);

    dir.close();
  }

  public void assertCodecPerFieldOptimized(Status checkIndex, String field,
      Codec codec) {
    assertEquals(1, checkIndex.segmentInfos.size());
    final CodecProvider provider = checkIndex.segmentInfos.get(0).codec.provider;
    assertEquals(codec, provider.lookup(provider.getFieldCodec(field)));

  }

  public void assertCodecPerField(Status checkIndex, String field, Codec codec) {
    for (SegmentInfoStatus info : checkIndex.segmentInfos) {
      final CodecProvider provider = info.codec.provider;
      assertEquals(codec, provider.lookup(provider.getFieldCodec(field)));
    }
  }

  public void assertHybridCodecPerField(Status checkIndex, String field,
      Codec... codec) throws IOException {
    List<SegmentInfoStatus> segmentInfos = checkIndex.segmentInfos;
    assertEquals(segmentInfos.size(), codec.length);
    for (int i = 0; i < codec.length; i++) {
      SegmentCodecs codecInfo = segmentInfos.get(i).codec;
      FieldInfos fieldInfos = new FieldInfos(checkIndex.dir, IndexFileNames
          .segmentFileName(segmentInfos.get(i).name, "",
              IndexFileNames.FIELD_INFOS_EXTENSION));
      FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      assertEquals("faild for segment index: " + i, codec[i],
          codecInfo.codecs[fieldInfo.getCodecId()]);
    }
  }

  public void assertQuery(Term t, Directory dir, int num, CodecProvider codecs)
      throws CorruptIndexException, IOException {
    if (VERBOSE) {
      System.out.println("\nTEST: assertQuery " + t);
    }
    IndexReader reader = IndexReader.open(dir, null, true,
        IndexReader.DEFAULT_TERMS_INDEX_DIVISOR, codecs);
    IndexSearcher searcher = newSearcher(reader);
    TopDocs search = searcher.search(new TermQuery(t), num + 10);
    assertEquals(num, search.totalHits);
    searcher.close();
    reader.close();

  }

  public static class MockCodecProvider extends CodecProvider {

    public MockCodecProvider() {
      StandardCodec standardCodec = new StandardCodec();
      setDefaultFieldCodec(standardCodec.name);
      SimpleTextCodec simpleTextCodec = new SimpleTextCodec();
      MockSepCodec mockSepCodec = new MockSepCodec();
      register(standardCodec);
      register(mockSepCodec);
      register(simpleTextCodec);
      setFieldCodec("id", simpleTextCodec.name);
      setFieldCodec("content", mockSepCodec.name);
    }
  }

  public static class MockCodecProvider2 extends CodecProvider {

    public MockCodecProvider2() {
      StandardCodec standardCodec = new StandardCodec();
      setDefaultFieldCodec(standardCodec.name);
      SimpleTextCodec simpleTextCodec = new SimpleTextCodec();
      MockSepCodec mockSepCodec = new MockSepCodec();
      register(standardCodec);
      register(mockSepCodec);
      register(simpleTextCodec);
      setFieldCodec("id", simpleTextCodec.name);
      setFieldCodec("content", standardCodec.name);
    }
  }

  /*
   * Test per field codec support - adding fields with random codecs
   */
  @Test
  public void testStressPerFieldCodec() throws IOException {
    Directory dir = newDirectory(random);
    Index[] indexValue = new Index[] { Index.ANALYZED, Index.ANALYZED_NO_NORMS,
        Index.NOT_ANALYZED, Index.NOT_ANALYZED_NO_NORMS };
    final int docsPerRound = 97;
    int numRounds = atLeast(1);
    for (int i = 0; i < numRounds; i++) {
      CodecProvider provider = new CodecProvider();
      Codec[] codecs = new Codec[] { new StandardCodec(),
          new SimpleTextCodec(), new MockSepCodec(),
          new PulsingCodec(1 + random.nextInt(10)),
          new MockVariableIntBlockCodec(1 + random.nextInt(10)),
          new MockFixedIntBlockCodec(1 + random.nextInt(10)) };
      for (Codec codec : codecs) {
        provider.register(codec);
      }
      int num = _TestUtil.nextInt(random, 30, 60);
      for (int j = 0; j < num; j++) {
        provider.setFieldCodec("" + j, codecs[random.nextInt(codecs.length)].name);
      }
      IndexWriterConfig config = newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, new MockAnalyzer(random));
      config.setOpenMode(OpenMode.CREATE_OR_APPEND);
      config.setCodecProvider(provider);
      IndexWriter writer = newWriter(dir, config);
      for (int j = 0; j < docsPerRound; j++) {
        final Document doc = new Document();
        for (int k = 0; k < num; k++) {
          Field field = newField("" + k, _TestUtil
              .randomRealisticUnicodeString(random, 128), indexValue[random
              .nextInt(indexValue.length)]);
          doc.add(field);
        }
        writer.addDocument(doc);
      }
      if (random.nextBoolean()) {
        writer.optimize();
      }
      writer.commit();
      assertEquals((i + 1) * docsPerRound, writer.maxDoc());
      writer.close();
    }
    dir.close();
  }
}
