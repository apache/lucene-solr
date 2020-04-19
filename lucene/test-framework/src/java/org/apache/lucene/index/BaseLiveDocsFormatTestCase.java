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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Collections;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;

/**
 * Abstract class that performs basic testing of a codec's
 * {@link LiveDocsFormat}.
 */
public abstract class BaseLiveDocsFormatTestCase extends LuceneTestCase {

  /** Returns the codec to run tests against */
  protected abstract Codec getCodec();

  private Codec savedCodec;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    // set the default codec, so adding test cases to this isn't fragile
    savedCodec = Codec.getDefault();
    Codec.setDefault(getCodec());
  }

  @Override
  public void tearDown() throws Exception {
    Codec.setDefault(savedCodec); // restore
    super.tearDown();
  }

  public void testDenseLiveDocs() throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 3, 1000);
    testSerialization(maxDoc, maxDoc - 1, false);
    testSerialization(maxDoc, maxDoc - 1, true);
  }

  public void testEmptyLiveDocs() throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 3, 1000);
    testSerialization(maxDoc, 0, false);
    testSerialization(maxDoc, 0, true);
  }

  public void testSparseLiveDocs() throws IOException {
    final int maxDoc = TestUtil.nextInt(random(), 3, 1000);
    testSerialization(maxDoc, 1, false);
    testSerialization(maxDoc, 1, true);
  }

  @Monster("Uses lots of memory")
  public void testOverflow() throws IOException {
    testSerialization(IndexWriter.MAX_DOCS, IndexWriter.MAX_DOCS - 7, false);
  }

  private void testSerialization(int maxDoc, int numLiveDocs, boolean fixedBitSet) throws IOException {
    final Codec codec = Codec.getDefault();
    final LiveDocsFormat format = codec.liveDocsFormat();

    final FixedBitSet liveDocs = new FixedBitSet(maxDoc);
    if (numLiveDocs > maxDoc / 2) {
      liveDocs.set(0, maxDoc);
      for (int i = 0; i < maxDoc - numLiveDocs; ++i) {
        int clearBit;
        do {
          clearBit = random().nextInt(maxDoc);
        } while (liveDocs.get(clearBit) == false);
        liveDocs.clear(clearBit);
      }
    } else {
      for (int i = 0; i < numLiveDocs; ++i) {
        int setBit;
        do {
          setBit = random().nextInt(maxDoc);
        } while (liveDocs.get(setBit));
        liveDocs.set(setBit);
      }
    }

    final Bits bits;
    if (fixedBitSet) {
      bits = liveDocs;
    } else {
      // Make sure the impl doesn't only work with a FixedBitSet
      bits = new Bits() {

        @Override
        public boolean get(int index) {
          return liveDocs.get(index);
        }

        @Override
        public int length() {
          return liveDocs.length();
        }

      };
    }

    final Directory dir = newDirectory();
    final SegmentInfo si = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "foo", maxDoc, random().nextBoolean(),
        codec, Collections.emptyMap(), StringHelper.randomId(), Collections.emptyMap(), null);
    SegmentCommitInfo sci = new SegmentCommitInfo(si, 0, 0, 0, -1, -1, StringHelper.randomId());
    format.writeLiveDocs(bits, dir, sci, maxDoc - numLiveDocs, IOContext.DEFAULT);

    sci = new SegmentCommitInfo(si, maxDoc - numLiveDocs, 0, 1, -1, -1, StringHelper.randomId());
    final Bits bits2 = format.readLiveDocs(dir, sci, IOContext.READONCE);
    assertEquals(maxDoc, bits2.length());
    for (int i = 0; i < maxDoc; ++i) {
      assertEquals(bits.get(i), bits2.get(i));
    }
    dir.close();
  }
}
