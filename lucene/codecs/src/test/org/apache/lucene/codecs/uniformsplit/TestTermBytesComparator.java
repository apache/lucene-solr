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

package org.apache.lucene.codecs.uniformsplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests the {@link TermBytes} comparator.
 */
public class TestTermBytesComparator extends LuceneTestCase {

  public void testComparison() throws IOException {
    TermBytes[] vocab = new TermBytes[]{
        termBytes(1, "abaco"),
        termBytes(2, "amiga"),
        termBytes(5, "amigo"),
        termBytes(2, "arco"),
        termBytes(1, "bloom"),
        termBytes(1, "frien"),
        termBytes(6, "frienchies"),
        termBytes(6, "friend"),
        termBytes(7, "friendalan"),
        termBytes(7, "friende"),
        termBytes(8, "friendez"),
    };
    List<BlockLine> lines = generateBlockLines(vocab);
    Directory directory = new ByteBuffersDirectory();
    try (IndexOutput indexOutput = directory.createOutput("temp.bin", IOContext.DEFAULT)) {
      indexOutput.writeVInt(5);
    }

    MockBlockReader blockReader = new MockBlockReader(lines, directory);

    assertAlwaysGreater(blockReader, new BytesRef("z"));

    assertGreaterUntil(1, blockReader, new BytesRef("abacu"));

    assertGreaterUntil(4, blockReader, new BytesRef("bar"));

    assertGreaterUntil(2, blockReader, new BytesRef("amigas"));

    assertGreaterUntil(10, blockReader, new BytesRef("friendez"));

  }

  private TermsEnum.SeekStatus assertGreaterUntil(int expectedPosition, MockBlockReader blockReader, BytesRef lookedTerm) throws IOException {
    TermsEnum.SeekStatus seekStatus = blockReader.seekInBlock(lookedTerm);
    assertEquals("looked Term: " + lookedTerm.utf8ToString(), expectedPosition, blockReader.lineIndexInBlock - 1);

    //reset the state
    blockReader.reset();
    return seekStatus;
  }

  private void assertAlwaysGreater(MockBlockReader blockReader, BytesRef lookedTerm) throws IOException {
    TermsEnum.SeekStatus seekStatus = assertGreaterUntil(-1, blockReader, lookedTerm);
    assertEquals(TermsEnum.SeekStatus.END, seekStatus);
  }

  private List<BlockLine> generateBlockLines(TermBytes[] words) {
    List<BlockLine> lines = new ArrayList<>(words.length);
    for (TermBytes word : words) {
      lines.add(new BlockLine(word, null));
    }
    return lines;
  }

  class MockBlockReader extends BlockReader {

    private List<BlockLine> lines;

    MockBlockReader(List<BlockLine> lines, Directory directory) throws IOException {
      super(null, directory.openInput("temp.bin", IOContext.DEFAULT),
          createMockPostingReaderBase(), new FieldMetadata(null, 1), null);
      this.lines = lines;
    }

    @Override
    protected int compareToMiddleAndJump(BytesRef searchedTerm) {
      // Do not jump in test.
      return -1;
    }

    @Override
    protected BlockLine readLineInBlock() {
      if (lineIndexInBlock >= lines.size()) {
        lineIndexInBlock = 0;
        return blockLine = null;
      }
      return blockLine = lines.get(lineIndexInBlock++);
    }

    @Override
    protected void initializeHeader(BytesRef searchedTerm, long targetBlockStartFP) throws IOException {
      // Force blockStartFP to an impossible value so we never trigger the optimization
      // that keeps the current block with our mock block reader.
      blockStartFP = Long.MIN_VALUE;
      super.initializeHeader(searchedTerm, targetBlockStartFP);
    }

    @Override
    protected BlockHeader readHeader() {
      return blockHeader = lineIndexInBlock >= lines.size() ? null : new BlockHeader(lines.size(), 0, 0, 0, 0, 0);
    }

    void reset() {
      lineIndexInBlock = 0;
      blockHeader = null;
      blockLine = null;
    }
  }

  private static TermBytes termBytes(int mdpLength, String term) {
    return new TermBytes(mdpLength, new BytesRef(term));
  }

  private static PostingsReaderBase createMockPostingReaderBase() {
    return new PostingsReaderBase() {
      @Override
      public void init(IndexInput termsIn, SegmentReadState state) {
      }

      @Override
      public BlockTermState newTermState() {
        return null;
      }

      @Override
      public void decodeTerm(long[] longs, DataInput in, FieldInfo fieldInfo, BlockTermState state, boolean absolute) {
      }

      @Override
      public PostingsEnum postings(FieldInfo fieldInfo, BlockTermState state, PostingsEnum reuse, int flags) {
        return null;
      }

      @Override
      public ImpactsEnum impacts(FieldInfo fieldInfo, BlockTermState state, int flags) {
        return null;
      }

      @Override
      public void checkIntegrity() {
      }

      @Override
      public void close() {
      }

      @Override
      public long ramBytesUsed() {
        return 0;
      }
    };
  }
}
