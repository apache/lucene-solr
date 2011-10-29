package org.apache.lucene.index.codecs.perfield;

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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.CheckIndex.Status.SegmentInfoStatus;
import org.apache.lucene.index.CheckIndex.Status;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.index.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.index.codecs.mocksep.MockSepPostingsFormat;
import org.apache.lucene.index.codecs.simpletext.SimpleTextPostingsFormat;
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
public class TestPerFieldPostingsFormat extends LuceneTestCase {

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
      doc.add(newField("content", "aaa", TextField.TYPE_UNSTORED));
      writer.addDocument(doc);
    }
  }

  private void addDocs2(IndexWriter writer, int numDocs) throws IOException {
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(newField("content", "bbb", TextField.TYPE_UNSTORED));
      writer.addDocument(doc);
    }
  }

  private void addDocs3(IndexWriter writer, int numDocs) throws IOException {
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      doc.add(newField("content", "ccc", TextField.TYPE_UNSTORED));
      doc.add(newField("id", "" + i, StringField.TYPE_STORED));
      writer.addDocument(doc);
    }
  }

  /*
   * Test that heterogeneous index segments are merge successfully
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
  // TODO: not sure this test is that great, we should probably peek inside PerFieldPostingsFormat or something?!
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
    Lucene40Codec codec = (Lucene40Codec)provider.getDefaultCodec();
    assertCodecPerField(_TestUtil.checkIndex(dir, provider), "content",
        codec.getPostingsFormat("MockSep"));

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
    codec = (Lucene40Codec)provider.getDefaultCodec();
    PostingsFormat origContentCodec = codec.getPostingsFormat("MockSep");
    PostingsFormat newContentCodec = codec.getPostingsFormat("Lucene40");
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
      PostingsFormat codec) {
    assertEquals(1, checkIndex.segmentInfos.size());
    final Lucene40Codec codecInfo = (Lucene40Codec) checkIndex.segmentInfos.get(0).codec;
    assertEquals(codec.name, codecInfo.getPostingsFormatForField(field));

  }

  public void assertCodecPerField(Status checkIndex, String field, PostingsFormat codec) {
    for (SegmentInfoStatus info : checkIndex.segmentInfos) {
      final Lucene40Codec codecInfo = (Lucene40Codec) info.codec;
      assertEquals(codec.name, codecInfo.getPostingsFormatForField(field));
    }
  }

  public void assertHybridCodecPerField(Status checkIndex, String field,
      PostingsFormat... codec) throws IOException {
    List<SegmentInfoStatus> segmentInfos = checkIndex.segmentInfos;
    assertEquals(segmentInfos.size(), codec.length);
    for (int i = 0; i < codec.length; i++) {
      Lucene40Codec codecInfo = (Lucene40Codec) segmentInfos.get(i).codec;
      assertEquals("failed for segment index: " + i, codec[i].name,
          codecInfo.getPostingsFormatForField(field));
    }
  }

  public void assertQuery(Term t, Directory dir, int num, CodecProvider codecs)
      throws CorruptIndexException, IOException {
    if (VERBOSE) {
      System.out.println("\nTEST: assertQuery " + t);
    }
    IndexReader reader = IndexReader.open(dir, null, true, 1, codecs);
    IndexSearcher searcher = newSearcher(reader);
    TopDocs search = searcher.search(new TermQuery(t), num + 10);
    assertEquals(num, search.totalHits);
    searcher.close();
    reader.close();

  }

  public static class MockCodecProvider extends CodecProvider {
    final PostingsFormat lucene40 = new Lucene40PostingsFormat();
    final PostingsFormat simpleText = new SimpleTextPostingsFormat();
    final PostingsFormat mockSep = new MockSepPostingsFormat();
    
    final Codec codec = new Lucene40Codec() {
      @Override
      public PostingsFormat getPostingsFormat(String formatName) {
        if (formatName.equals(lucene40.name)) {
          return lucene40;
        } else if (formatName.equals(simpleText.name)) {
          return simpleText;
        } else if (formatName.equals(mockSep.name)) {
          return mockSep;
        } else {
          throw new IllegalArgumentException("unknown postings format: " + formatName);
        }
      }

      @Override
      public String getPostingsFormatForField(String field) {
        if (field.equals("id")) {
          return simpleText.name;
        } else if (field.equals("content")) {
          return mockSep.name;
        } else {
          return lucene40.name;
        }
      }   
    };
    
    @Override
    public Codec lookup(String name) {
      return codec;
    }

    @Override
    public Codec getDefaultCodec() {
      return codec;
    }
  }

  public static class MockCodecProvider2 extends CodecProvider {
    final PostingsFormat lucene40 = new Lucene40PostingsFormat();
    final PostingsFormat simpleText = new SimpleTextPostingsFormat();
    
    final Codec codec = new Lucene40Codec() {
      @Override
      public PostingsFormat getPostingsFormat(String formatName) {
        if (formatName.equals(lucene40.name)) {
          return lucene40;
        } else if (formatName.equals(simpleText.name)) {
          return simpleText;
        } else {
          throw new IllegalArgumentException("unknown postings format: " + formatName);
        }
      }

      @Override
      public String getPostingsFormatForField(String field) {
        if (field.equals("id")) {
          return simpleText.name;
        } else {
          return lucene40.name;
        }
      }   
    };
    
    @Override
    public Codec lookup(String name) {
      return codec;
    }

    @Override
    public Codec getDefaultCodec() {
      return codec;
    }
  }

  /*
   * Test per field codec support - adding fields with random codecs
   */
  @Test
  public void testStressPerFieldCodec() throws IOException {
    Directory dir = newDirectory(random);
    final int docsPerRound = 97;
    int numRounds = atLeast(1);
    for (int i = 0; i < numRounds; i++) {
      int num = _TestUtil.nextInt(random, 30, 60);
      IndexWriterConfig config = newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, new MockAnalyzer(random));
      config.setOpenMode(OpenMode.CREATE_OR_APPEND);
      IndexWriter writer = newWriter(dir, config);
      for (int j = 0; j < docsPerRound; j++) {
        final Document doc = new Document();
        for (int k = 0; k < num; k++) {
          FieldType customType = new FieldType(TextField.TYPE_UNSTORED);
          customType.setTokenized(random.nextBoolean());
          customType.setOmitNorms(random.nextBoolean());
          Field field = newField("" + k, _TestUtil
              .randomRealisticUnicodeString(random, 128), customType);
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
