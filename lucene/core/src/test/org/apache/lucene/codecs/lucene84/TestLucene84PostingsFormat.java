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
package org.apache.lucene.codecs.lucene84;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.codecs.blocktree.FieldReader;
import org.apache.lucene.codecs.blocktree.Stats;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat;
import org.apache.lucene.codecs.lucene84.Lucene84ScoreSkipReader;
import org.apache.lucene.codecs.lucene84.Lucene84SkipWriter;
import org.apache.lucene.codecs.lucene84.Lucene84ScoreSkipReader.MutableImpactList;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.TestUtil;

public class TestLucene84PostingsFormat extends BasePostingsFormatTestCase {
  private final Codec codec = TestUtil.alwaysPostingsFormat(new Lucene84PostingsFormat());

  @Override
  protected Codec getCodec() {
    return codec;
  }

  public void testFstOffHeap() throws IOException {
    Path tempDir = createTempDir();
    try (Directory d = FSDirectory.open(tempDir)) {
      assumeTrue("only works with mmap directory", d instanceof MMapDirectory);
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new MockAnalyzer(random())))) {
        DirectoryReader readerFromWriter = DirectoryReader.open(w);
        for (int i = 0; i < 50; i++) {
          Document doc = new Document();
          doc.add(newStringField("id", "" + i, Field.Store.NO));
          doc.add(newStringField("field", Character.toString((char) (97 + i)), Field.Store.NO));
          doc.add(newStringField("field", Character.toString((char) (98 + i)), Field.Store.NO));
          if (rarely()) {
            w.addDocument(doc);
          } else {
            w.updateDocument(new Term("id", "" + i), doc);
          }
          if (random().nextBoolean()) {
            w.commit();
          }

          if (random().nextBoolean()) {
            DirectoryReader newReader = DirectoryReader.openIfChanged(readerFromWriter);
            if (newReader != null) {
              readerFromWriter.close();
              readerFromWriter = newReader;
            }
            for (LeafReaderContext leaf : readerFromWriter.leaves()) {
              FieldReader field = (FieldReader) leaf.reader().terms("field");
              FieldReader id = (FieldReader) leaf.reader().terms("id");
              assertFalse(id.isFstOffHeap());
              assertTrue(field.isFstOffHeap());
            }
          }
        }
        readerFromWriter.close();

        w.forceMerge(1);
        try (DirectoryReader r = DirectoryReader.open(w)) {
          assertEquals(1, r.leaves().size());
          FieldReader field = (FieldReader) r.leaves().get(0).reader().terms("field");
          FieldReader id = (FieldReader) r.leaves().get(0).reader().terms("id");
          assertFalse(id.isFstOffHeap());
          assertTrue(field.isFstOffHeap());
        }
        w.commit();
        try (DirectoryReader r = DirectoryReader.open(d)) {
          assertEquals(1, r.leaves().size());
          FieldReader field = (FieldReader) r.leaves().get(0).reader().terms("field");
          FieldReader id = (FieldReader) r.leaves().get(0).reader().terms("id");
          assertTrue(id.isFstOffHeap());
          assertTrue(field.isFstOffHeap());
        }
      }
    }

    try (Directory d = new SimpleFSDirectory(tempDir)) {
      // test auto
      try (DirectoryReader r = DirectoryReader.open(d)) {
        assertEquals(1, r.leaves().size());
        FieldReader field = (FieldReader) r.leaves().get(0).reader().terms("field");
        FieldReader id = (FieldReader) r.leaves().get(0).reader().terms("id");
        assertFalse(id.isFstOffHeap());
        assertFalse(field.isFstOffHeap());
      }
    }

    try (Directory d = new SimpleFSDirectory(tempDir)) {
      // test per field
      Map<String, String> readerAttributes = new HashMap<>();
      readerAttributes.put(BlockTreeTermsReader.FST_MODE_KEY, BlockTreeTermsReader.FSTLoadMode.OFF_HEAP.name());
      readerAttributes.put(BlockTreeTermsReader.FST_MODE_KEY + ".field", BlockTreeTermsReader.FSTLoadMode.ON_HEAP.name());
      try (DirectoryReader r = DirectoryReader.open(d, readerAttributes)) {
        assertEquals(1, r.leaves().size());
        FieldReader field = (FieldReader) r.leaves().get(0).reader().terms("field");
        FieldReader id = (FieldReader) r.leaves().get(0).reader().terms("id");
        assertTrue(id.isFstOffHeap());
        assertFalse(field.isFstOffHeap());
      }
    }

    IllegalArgumentException invalid = expectThrows(IllegalArgumentException.class, () -> {
      try (Directory d = new SimpleFSDirectory(tempDir)) {
        Map<String, String> readerAttributes = new HashMap<>();
        readerAttributes.put(BlockTreeTermsReader.FST_MODE_KEY, "invalid");
        DirectoryReader.open(d, readerAttributes);
      }
    });

    assertEquals("Invalid value for blocktree.terms.fst expected one of: [OFF_HEAP, ON_HEAP, OPTIMIZE_UPDATES_OFF_HEAP, AUTO] but was: invalid", invalid.getMessage());
  }

  public void testDisableFSTOffHeap() throws IOException {
    Path tempDir = createTempDir();
    try (Directory d = MMapDirectory.open(tempDir)) {
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new MockAnalyzer(random()))
          .setReaderAttributes(Collections.singletonMap(BlockTreeTermsReader.FST_MODE_KEY, BlockTreeTermsReader.FSTLoadMode.ON_HEAP.name())))) {
        assumeTrue("only works with mmap directory", d instanceof MMapDirectory);
        DirectoryReader readerFromWriter = DirectoryReader.open(w);
        for (int i = 0; i < 50; i++) {
          Document doc = new Document();
          doc.add(newStringField("id", "" + i, Field.Store.NO));
          doc.add(newStringField("field", Character.toString((char) (97 + i)), Field.Store.NO));
          doc.add(newStringField("field", Character.toString((char) (98 + i)), Field.Store.NO));
          if (rarely()) {
            w.addDocument(doc);
          } else {
            w.updateDocument(new Term("id", "" + i), doc);
          }
          if (random().nextBoolean()) {
            w.commit();
          }
          if (random().nextBoolean()) {
            DirectoryReader newReader = DirectoryReader.openIfChanged(readerFromWriter);
            if (newReader != null) {
              readerFromWriter.close();
              readerFromWriter = newReader;
            }
            for (LeafReaderContext leaf : readerFromWriter.leaves()) {
              FieldReader field = (FieldReader) leaf.reader().terms("field");
              FieldReader id = (FieldReader) leaf.reader().terms("id");
              assertFalse(id.isFstOffHeap());
              assertFalse(field.isFstOffHeap());
            }
          }
        }
        readerFromWriter.close();
        w.forceMerge(1);
        w.commit();
      }
      try (DirectoryReader r = DirectoryReader.open(d, Collections.singletonMap(BlockTreeTermsReader.FST_MODE_KEY, BlockTreeTermsReader.FSTLoadMode.ON_HEAP.name()))) {
        assertEquals(1, r.leaves().size());
        FieldReader field = (FieldReader) r.leaves().get(0).reader().terms("field");
        FieldReader id = (FieldReader) r.leaves().get(0).reader().terms("id");
        assertFalse(id.isFstOffHeap());
        assertFalse(field.isFstOffHeap());
      }
    }
  }

  public void testAlwaysFSTOffHeap() throws IOException {
    boolean alsoLoadIdOffHeap = random().nextBoolean();
    BlockTreeTermsReader.FSTLoadMode loadMode;
    if (alsoLoadIdOffHeap) {
      loadMode = BlockTreeTermsReader.FSTLoadMode.OFF_HEAP;
    } else {
      loadMode = BlockTreeTermsReader.FSTLoadMode.OPTIMIZE_UPDATES_OFF_HEAP;
    }
    try (Directory d = newDirectory()) { // any directory should work now
      try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new MockAnalyzer(random()))
          .setReaderAttributes(Collections.singletonMap(BlockTreeTermsReader.FST_MODE_KEY, loadMode.name())))) {
        DirectoryReader readerFromWriter = DirectoryReader.open(w);
        for (int i = 0; i < 50; i++) {
          Document doc = new Document();
          doc.add(newStringField("id", "" + i, Field.Store.NO));
          doc.add(newStringField("field", Character.toString((char) (97 + i)), Field.Store.NO));
          doc.add(newStringField("field", Character.toString((char) (98 + i)), Field.Store.NO));
          if (rarely()) {
            w.addDocument(doc);
          } else {
            w.updateDocument(new Term("id", "" + i), doc);
          }
          if (random().nextBoolean()) {
            w.commit();
          }
          if (random().nextBoolean()) {
            DirectoryReader newReader = DirectoryReader.openIfChanged(readerFromWriter);
            if (newReader != null) {
              readerFromWriter.close();
              readerFromWriter = newReader;
            }
            for (LeafReaderContext leaf : readerFromWriter.leaves()) {
              FieldReader field = (FieldReader) leaf.reader().terms("field");
              FieldReader id = (FieldReader) leaf.reader().terms("id");
              if (alsoLoadIdOffHeap) {
                assertTrue(id.isFstOffHeap());
              } else {
                assertFalse(id.isFstOffHeap());
              }
              assertTrue(field.isFstOffHeap());
            }
          }
        }
        readerFromWriter.close();
        w.forceMerge(1);
        w.commit();
      }
      try (DirectoryReader r = DirectoryReader.open(d, Collections.singletonMap(BlockTreeTermsReader.FST_MODE_KEY, loadMode.name()))) {
        assertEquals(1, r.leaves().size());
        FieldReader field = (FieldReader) r.leaves().get(0).reader().terms("field");
        FieldReader id = (FieldReader) r.leaves().get(0).reader().terms("id");
        assertTrue(id.isFstOffHeap());
        assertTrue(field.isFstOffHeap());
      }
    }
  }
  
  /** Make sure the final sub-block(s) are not skipped. */
  public void testFinalBlock() throws Exception {
    Directory d = newDirectory();
    IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new MockAnalyzer(random())));
    for(int i=0;i<25;i++) {
      Document doc = new Document();
      doc.add(newStringField("field", Character.toString((char) (97+i)), Field.Store.NO));
      doc.add(newStringField("field", "z" + Character.toString((char) (97+i)), Field.Store.NO));
      w.addDocument(doc);
    }
    w.forceMerge(1);

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.leaves().size());
    FieldReader field = (FieldReader) r.leaves().get(0).reader().terms("field");
    // We should see exactly two blocks: one root block (prefix empty string) and one block for z* terms (prefix z):
    Stats stats = field.getStats();
    assertEquals(0, stats.floorBlockCount);
    assertEquals(2, stats.nonFloorBlockCount);
    r.close();
    w.close();
    d.close();
  }

  private void shouldFail(int minItemsInBlock, int maxItemsInBlock) {
    expectThrows(IllegalArgumentException.class, () -> {
      new Lucene84PostingsFormat(minItemsInBlock, maxItemsInBlock, BlockTreeTermsReader.FSTLoadMode.AUTO);
    });
  }

  public void testInvalidBlockSizes() throws Exception {
    shouldFail(0, 0);
    shouldFail(10, 8);
    shouldFail(-1, 10);
    shouldFail(10, -1);
    shouldFail(10, 12);
  }

  public void testImpactSerialization() throws IOException {
    // omit norms and omit freqs
    doTestImpactSerialization(Collections.singletonList(new Impact(1, 1L)));

    // omit freqs
    doTestImpactSerialization(Collections.singletonList(new Impact(1, 42L)));
    // omit freqs with very large norms
    doTestImpactSerialization(Collections.singletonList(new Impact(1, -100L)));

    // omit norms
    doTestImpactSerialization(Collections.singletonList(new Impact(30, 1L)));
    // omit norms with large freq
    doTestImpactSerialization(Collections.singletonList(new Impact(500, 1L)));

    // freqs and norms, basic
    doTestImpactSerialization(
        Arrays.asList(
            new Impact(1, 7L),
            new Impact(3, 9L),
            new Impact(7, 10L),
            new Impact(15, 11L),
            new Impact(20, 13L),
            new Impact(28, 14L)));

    // freqs and norms, high values
    doTestImpactSerialization(
        Arrays.asList(
            new Impact(2, 2L),
            new Impact(10, 10L),
            new Impact(12, 50L),
            new Impact(50, -100L),
            new Impact(1000, -80L),
            new Impact(1005, -3L)));
  }

  private void doTestImpactSerialization(List<Impact> impacts) throws IOException {
    CompetitiveImpactAccumulator acc = new CompetitiveImpactAccumulator();
    for (Impact impact : impacts) {
      acc.add(impact.freq, impact.norm);
    }
    try(Directory dir = newDirectory()) {
      try (IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT)) {
        Lucene84SkipWriter.writeImpacts(acc, out);
      }
      try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
        byte[] b = new byte[Math.toIntExact(in.length())];
        in.readBytes(b, 0, b.length);
        List<Impact> impacts2 = Lucene84ScoreSkipReader.readImpacts(new ByteArrayDataInput(b), new MutableImpactList());
        assertEquals(impacts, impacts2);
      }
    }
  }
}
