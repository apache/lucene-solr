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
package org.apache.lucene.codecs.lucene50;


import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompetitiveFreqNormAccumulator;
import org.apache.lucene.codecs.blocktree.FieldReader;
import org.apache.lucene.codecs.blocktree.Stats;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.TestUtil;

/**
 * Tests BlockPostingsFormat
 */
public class TestBlockPostingsFormat extends BasePostingsFormatTestCase {
  private final Codec codec = TestUtil.alwaysPostingsFormat(new Lucene50PostingsFormat());

  @Override
  protected Codec getCodec() {
    return codec;
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
      new Lucene50PostingsFormat(minItemsInBlock, maxItemsInBlock);
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
    doTestImpactSerialization(new int[] { 1 }, new long[] { 1L });

    // omit freqs
    doTestImpactSerialization(new int[] { 1 }, new long[] { 42L });
    // omit freqs with very large norms
    doTestImpactSerialization(new int[] { 1 }, new long[] { -100L });

    // omit norms
    doTestImpactSerialization(new int[] { 30 }, new long[] { 1L });
    // omit norms with large freq
    doTestImpactSerialization(new int[] { 500 }, new long[] { 1L });

    // freqs and norms, basic
    doTestImpactSerialization(
        new int[] { 1, 3, 7, 15, 20, 28 },
        new long[] { 7L, 9L, 10L, 11L, 13L, 14L });

    // freqs and norms, high values
    doTestImpactSerialization(
        new int[] { 2, 10, 12, 50, 1000, 1005 },
        new long[] { 2L, 10L, 50L, -100L, -80L, -3L });
  }

  private void doTestImpactSerialization(int[] freqs, long[] norms) throws IOException {
    CompetitiveFreqNormAccumulator acc = new CompetitiveFreqNormAccumulator();
    for (int i = 0; i < freqs.length; ++i) {
      acc.add(freqs[i], norms[i]);
    }
    try(Directory dir = newDirectory()) {
      try (IndexOutput out = dir.createOutput("foo", IOContext.DEFAULT)) {
        Lucene50SkipWriter.writeImpacts(acc, out);
      }
      try (IndexInput in = dir.openInput("foo", IOContext.DEFAULT)) {
        byte[] b = new byte[Math.toIntExact(in.length())];
        in.readBytes(b, 0, b.length);
        Lucene50ScoreSkipReader.readImpacts(new ByteArrayDataInput(b), new SimScorer("") {
          int i = 0;

          @Override
          public float score(float freq, long norm) {
            assert freq == freqs[i];
            assert norm == norms[i];
            i++;
            return 0;
          }
        });
      }
    }
  }
}
