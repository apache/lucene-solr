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
import java.util.Collections;

import org.apache.lucene.codecs.lucene84.MockTermStateFactory;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests {@link BlockWriter}.
 */
public class TestBlockWriter extends LuceneTestCase {

  private BlockWriter blockWriter;
  private ByteBuffersIndexOutput blockOutput;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    blockOutput = new ByteBuffersIndexOutput(ByteBuffersDataOutput.newResettableInstance(), "Test", "Test");
    blockWriter = new BlockWriter(blockOutput, 10, 2, null);
  }

  public void testAddLine() throws IOException {
    BytesRef term = new BytesRef("mike");
    blockWriter.addLine(term, MockTermStateFactory.create(), null);
    assertEquals(1, blockWriter.blockLines.size());
    assertEquals(term, blockWriter.lastTerm);
  }

  public void testAddMultipleLinesSingleBlock() throws IOException {
    String[] terms = new String[]{
        "ana",
        "bark",
        "condor",
        "dice",
        "elephant"
    };
    for (String t : terms) {
      blockWriter.addLine(new BytesRef(t), MockTermStateFactory.create(), null);
    }
    assertEquals(terms.length, blockWriter.blockLines.size());
    assertEquals(new BytesRef(terms[terms.length - 1]), blockWriter.lastTerm);
  }

  public void testAddMultipleLinesMultiBlock() throws IOException {
    String[] terms = new String[]{
        "ana",
        "bark",
        "condor",
        "dice",
        "elephant",
        "fork",
        "gain",
        "hyper",
        "identifier",
        "judge",
        "ko",
        "large",
    };
    // in order to build a block a FieldMetadata must be set
    blockWriter.setField(new FieldMetadata(getMockFieldInfo("content", 0), 0));

    FSTDictionary.Builder dictionaryBuilder = new FSTDictionary.Builder();

    for (String t : terms) {
      blockWriter.addLine(new BytesRef(t), MockTermStateFactory.create(), dictionaryBuilder);
    }
    //at least one block was flushed
    assertTrue(blockOutput.getFilePointer() > 0);

    // last term is always the last term to be writen
    assertEquals(new BytesRef(terms[terms.length - 1]), blockWriter.lastTerm);

    // remains 'large' to be flushed
    assertEquals(1, blockWriter.blockLines.size());

    blockWriter.finishLastBlock(dictionaryBuilder);

    // we release memory
    assertTrue(blockWriter.blockLines.isEmpty());
    assertNull(blockWriter.lastTerm);
    assertEquals(0, blockWriter.blockLinesWriteBuffer.size());
    assertEquals(0, blockWriter.termStatesWriteBuffer.size());
  }

  private static FieldInfo getMockFieldInfo(String fieldName, int number) {
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
        true
    );
  }
}
