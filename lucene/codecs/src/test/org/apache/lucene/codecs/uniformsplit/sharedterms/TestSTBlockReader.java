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

package org.apache.lucene.codecs.uniformsplit.sharedterms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.lucene84.MockTermStateFactory;
import org.apache.lucene.codecs.uniformsplit.BlockHeader;
import org.apache.lucene.codecs.uniformsplit.BlockLine;
import org.apache.lucene.codecs.uniformsplit.FSTDictionary;
import org.apache.lucene.codecs.uniformsplit.FieldMetadata;
import org.apache.lucene.codecs.uniformsplit.IndexDictionary;
import org.apache.lucene.codecs.uniformsplit.TermBytes;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestSTBlockReader extends LuceneTestCase {

  private static final String MOCK_BLOCK_OUTPUT_NAME = "TestSTBlockReader.tmp";

  private FieldInfos fieldInfos;
  private List<MockSTBlockLine> blockLines;
  private IndexDictionary.BrowserSupplier supplier;
  private ByteBuffersDirectory directory;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    fieldInfos = mockFieldInfos();
    List<BlockLineDefinition> vocab = new ArrayList<>();
    vocab.add(blockLineDef(1, "abaco", "f1", "f3"));
    vocab.add(blockLineDef(2, "amiga", "f1", "f2", "f4"));
    vocab.add(blockLineDef(5, "amigo", "f1", "f2", "f3", "f4"));
    vocab.add(blockLineDef(2, "arco", "f1"));
    vocab.add(blockLineDef(1, "bloom", "f2"));
    vocab.add(blockLineDef(1, "frien", "f2"));
    vocab.add(blockLineDef(6, "frienchies", "f3"));

    blockLines = generateBlockLines(vocab);
    directory = new ByteBuffersDirectory();
    try (IndexOutput blockOutput = directory.createOutput(MOCK_BLOCK_OUTPUT_NAME, IOContext.DEFAULT)) {
      blockOutput.writeVInt(5);
    }
    IndexDictionary.Builder builder = new FSTDictionary.Builder();
    builder.add(new BytesRef("a"), 0);
    IndexDictionary indexDictionary = builder.build();
    supplier = new IndexDictionary.BrowserSupplier() {
      @Override
      public IndexDictionary.Browser get() throws IOException {
        return indexDictionary.browser();
      }
      @Override
      public long ramBytesUsed() {
        return indexDictionary.ramBytesUsed();
      }
    };
  }

  @Override
  public void tearDown() throws Exception {
    try {
      blockLines.clear();
      directory.close();
    } finally {
      super.tearDown();
    }
  }

  public void testSeekExactIgnoreFieldF1() throws IOException {
    // when block reader for field 1 -> f1
    MockSTBlockReader blockReader = new MockSTBlockReader(
        supplier,
        blockLines,
        directory,
        fieldInfos.fieldInfo("f1"), //last term "arco"
        fieldInfos
    );

    // when seekCeil
    blockReader.seekCeil(new BytesRef("arco2"));
    // then
    assertNull(blockReader.term());

    // when seekCeilIgnoreField
    blockReader.seekCeilIgnoreField(new BytesRef("arco2"));
    // then
    assertEquals("bloom", blockReader.term().utf8ToString());
  }

  public void testSeekExactIgnoreFieldF2() throws IOException {
    MockSTBlockReader blockReader = new MockSTBlockReader(
        supplier,
        blockLines,
        directory,
        fieldInfos.fieldInfo("f2"),//last term "frien"
        fieldInfos
    );

    // when seekCeil
    blockReader.seekCeilIgnoreField(new BytesRef("arco2"));
    // then
    assertEquals("bloom", blockReader.term().utf8ToString());
  }

  public void testSeekExactIgnoreFieldF3() throws IOException {
    MockSTBlockReader blockReader = new MockSTBlockReader(
        supplier,
        blockLines,
        directory,
        fieldInfos.fieldInfo("f3"),//last term "frienchies"
        fieldInfos
    );

    // when seekCeilIgnoreField
    blockReader.seekCeilIgnoreField(new BytesRef("arco2"));
    // then
    assertEquals("bloom", blockReader.term().utf8ToString());

    // when seekCeil
    blockReader.seekCeil(new BytesRef("arco2"));
    // then
    assertEquals("frienchies", blockReader.term().utf8ToString());
  }

  public void testSeekExactIgnoreFieldF4() throws IOException {
    MockSTBlockReader blockReader = new MockSTBlockReader(
        supplier,
        blockLines,
        directory,
        fieldInfos.fieldInfo("f4"),//last term "amigo"
        fieldInfos
    );

    // when seekCeilIgnoreField
    blockReader.seekCeilIgnoreField(new BytesRef("abaco"));
    // then
    assertEquals("abaco", blockReader.term().utf8ToString());

    // when seekCeil
    blockReader.seekCeil(new BytesRef("abaco"));
    // then
    assertEquals("amiga", blockReader.term().utf8ToString());
  }

  private static FieldInfos mockFieldInfos() {
    return new FieldInfos(
        new FieldInfo[]{
            mockFieldInfo("f1", 0),
            mockFieldInfo("f2", 1),
            mockFieldInfo("f3", 2),
            mockFieldInfo("f4", 3),
        });
  }

  private static FieldInfo mockFieldInfo(String fieldName, int number) {
    return new FieldInfo(fieldName,
        number,
        false,
        false,
        true,
        IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
        DocValuesType.NONE,
        -1,
        Collections.emptyMap(),
        0,
        0,
        0,
        false
    );
  }

  private BlockLineDefinition blockLineDef(int mdpLength, String term, String... fields) {
    return new BlockLineDefinition(new TermBytes(mdpLength, new BytesRef(term)), Arrays.asList(fields));
  }

  private static List<MockSTBlockLine> generateBlockLines(Iterable<BlockLineDefinition> blockLineDefinitions) {
    List<MockSTBlockLine> lines = new ArrayList<>();
    for (BlockLineDefinition blockLineDefinition : blockLineDefinitions) {
      lines.add(new MockSTBlockLine(blockLineDefinition.termBytes, blockLineDefinition.fields));
    }
    return lines;
  }

  private static class BlockLineDefinition {
    final TermBytes termBytes;
    final List<String> fields;

    BlockLineDefinition(TermBytes termBytes, List<String> fields) {
      this.termBytes = termBytes;
      this.fields = fields;
    }
  }

  private static class MockSTBlockLine extends STBlockLine {

    final Map<String, BlockTermState> termStates;

    MockSTBlockLine(TermBytes termBytes, List<String> fields) {
      super(termBytes, Collections.singletonList(new FieldMetadataTermState(null, null)));
      this.termStates = new HashMap<>();
      for (String field : fields) {
        termStates.put(field, MockTermStateFactory.create());
      }
    }

    Set<String> getFields() {
      return termStates.keySet();
    }
  }

  private static class MockSTBlockReader extends STBlockReader {

    List<MockSTBlockLine> lines;

    MockSTBlockReader(IndexDictionary.BrowserSupplier supplier, List<MockSTBlockLine> lines, Directory directory, FieldInfo fieldInfo, FieldInfos fieldInfos) throws IOException {
      super(supplier, directory.openInput(MOCK_BLOCK_OUTPUT_NAME, IOContext.DEFAULT),
          getMockPostingReaderBase(), mockFieldMetadata(fieldInfo, getLastTermForField(lines, fieldInfo.name)), null, fieldInfos);
      this.lines = lines;
    }

    static PostingsReaderBase getMockPostingReaderBase() {
      return new PostingsReaderBase() {
        @Override
        public void init(IndexInput termsIn, SegmentReadState state) {
        }

        @Override
        public BlockTermState newTermState() {
          return null;
        }

        @Override
        public void decodeTerm(DataInput in, FieldInfo fieldInfo, BlockTermState state, boolean absolute) {
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

    static FieldMetadata mockFieldMetadata(FieldInfo fieldInfo, BytesRef lastTerm) {
      FieldMetadata fieldMetadata = new FieldMetadata(fieldInfo, 1);
      fieldMetadata.setLastTerm(lastTerm);
      fieldMetadata.setLastBlockStartFP(1);
      return fieldMetadata;
    }

    static BytesRef getLastTermForField(List<MockSTBlockLine> lines, String fieldName) {
      BytesRef lastTerm = null;
      for (MockSTBlockLine line : lines) {
        if (line.getFields().contains(fieldName)) {
          lastTerm = line.getTermBytes().getTerm();
        }
      }
      return lastTerm;
    }

    @Override
    protected BlockTermState readTermState() {
      return termState = lines.get(lineIndexInBlock - 1).termStates.get(fieldMetadata.getFieldInfo().name);
    }

    @Override
    protected int compareToMiddleAndJump(BytesRef searchedTerm) {
      blockLine = lines.get(lines.size() >> 1);
      lineIndexInBlock = blockHeader.getMiddleLineIndex();
      int compare = searchedTerm.compareTo(term());
      if (compare < 0) {
        lineIndexInBlock = 0;
      }
      return compare;
    }

    @Override
    protected BlockLine readLineInBlock() {
      if (lineIndexInBlock >= lines.size()) {
        return blockLine = null;
      }
      return blockLine = lines.get(lineIndexInBlock++);
    }

    @Override
    protected void initializeHeader(BytesRef searchedTerm, long startBlockLinePos) throws IOException {
      // Force blockStartFP to an impossible value so we never trigger the optimization
      // that keeps the current block with our mock block reader.
      blockStartFP = -1;
      super.initializeHeader(searchedTerm, startBlockLinePos);
    }

    @Override
    protected BlockHeader readHeader() {
      return blockHeader = lineIndexInBlock >= lines.size() ? null : new MockBlockHeader(lines.size());
    }
  }

  private static class MockBlockHeader extends BlockHeader {

    MockBlockHeader(int linesCount) {
      super(linesCount, 0, 0, 0, 1, 0);
    }
  }
}
