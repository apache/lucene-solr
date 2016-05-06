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
package org.apache.lucene.codecs.lucene41;

import static org.apache.lucene.util.BitUtil.zigZagEncode;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Writer for 4.1 stored fields/term vectors index for testing
 * @deprecated for test purposes only
 */
@Deprecated
public final class Lucene41StoredFieldsIndexWriter implements Closeable {
  
  static final int BLOCK_SIZE = 1024; // number of chunks to serialize at once

  final IndexOutput fieldsIndexOut;
  int totalDocs;
  int blockDocs;
  int blockChunks;
  long firstStartPointer;
  long maxStartPointer;
  final int[] docBaseDeltas;
  final long[] startPointerDeltas;

  public Lucene41StoredFieldsIndexWriter(IndexOutput indexOutput) throws IOException {
    this.fieldsIndexOut = indexOutput;
    reset();
    totalDocs = 0;
    docBaseDeltas = new int[BLOCK_SIZE];
    startPointerDeltas = new long[BLOCK_SIZE];
    fieldsIndexOut.writeVInt(PackedInts.VERSION_CURRENT);
  }

  private void reset() {
    blockChunks = 0;
    blockDocs = 0;
    firstStartPointer = -1; // means unset
  }

  private void writeBlock() throws IOException {
    assert blockChunks > 0;
    fieldsIndexOut.writeVInt(blockChunks);

    // The trick here is that we only store the difference from the average start
    // pointer or doc base, this helps save bits per value.
    // And in order to prevent a few chunks that would be far from the average to
    // raise the number of bits per value for all of them, we only encode blocks
    // of 1024 chunks at once
    // See LUCENE-4512

    // doc bases
    final int avgChunkDocs;
    if (blockChunks == 1) {
      avgChunkDocs = 0;
    } else {
      avgChunkDocs = Math.round((float) (blockDocs - docBaseDeltas[blockChunks - 1]) / (blockChunks - 1));
    }
    fieldsIndexOut.writeVInt(totalDocs - blockDocs); // docBase
    fieldsIndexOut.writeVInt(avgChunkDocs);
    int docBase = 0;
    long maxDelta = 0;
    for (int i = 0; i < blockChunks; ++i) {
      final int delta = docBase - avgChunkDocs * i;
      maxDelta |= zigZagEncode(delta);
      docBase += docBaseDeltas[i];
    }

    final int bitsPerDocBase = PackedInts.bitsRequired(maxDelta);
    fieldsIndexOut.writeVInt(bitsPerDocBase);
    PackedInts.Writer writer = PackedInts.getWriterNoHeader(fieldsIndexOut,
        PackedInts.Format.PACKED, blockChunks, bitsPerDocBase, 1);
    docBase = 0;
    for (int i = 0; i < blockChunks; ++i) {
      final long delta = docBase - avgChunkDocs * i;
      assert PackedInts.bitsRequired(zigZagEncode(delta)) <= writer.bitsPerValue();
      writer.add(zigZagEncode(delta));
      docBase += docBaseDeltas[i];
    }
    writer.finish();

    // start pointers
    fieldsIndexOut.writeVLong(firstStartPointer);
    final long avgChunkSize;
    if (blockChunks == 1) {
      avgChunkSize = 0;
    } else {
      avgChunkSize = (maxStartPointer - firstStartPointer) / (blockChunks - 1);
    }
    fieldsIndexOut.writeVLong(avgChunkSize);
    long startPointer = 0;
    maxDelta = 0;
    for (int i = 0; i < blockChunks; ++i) {
      startPointer += startPointerDeltas[i];
      final long delta = startPointer - avgChunkSize * i;
      maxDelta |= zigZagEncode(delta);
    }

    final int bitsPerStartPointer = PackedInts.bitsRequired(maxDelta);
    fieldsIndexOut.writeVInt(bitsPerStartPointer);
    writer = PackedInts.getWriterNoHeader(fieldsIndexOut, PackedInts.Format.PACKED,
        blockChunks, bitsPerStartPointer, 1);
    startPointer = 0;
    for (int i = 0; i < blockChunks; ++i) {
      startPointer += startPointerDeltas[i];
      final long delta = startPointer - avgChunkSize * i;
      assert PackedInts.bitsRequired(zigZagEncode(delta)) <= writer.bitsPerValue();
      writer.add(zigZagEncode(delta));
    }
    writer.finish();
  }

  public void writeIndex(int numDocs, long startPointer) throws IOException {
    if (blockChunks == BLOCK_SIZE) {
      writeBlock();
      reset();
    }

    if (firstStartPointer == -1) {
      firstStartPointer = maxStartPointer = startPointer;
    }
    assert firstStartPointer > 0 && startPointer >= firstStartPointer;

    docBaseDeltas[blockChunks] = numDocs;
    startPointerDeltas[blockChunks] = startPointer - maxStartPointer;

    ++blockChunks;
    blockDocs += numDocs;
    totalDocs += numDocs;
    maxStartPointer = startPointer;
  }

  public void finish(int numDocs, long maxPointer) throws IOException {
    if (numDocs != totalDocs) {
      throw new IllegalStateException("Expected " + numDocs + " docs, but got " + totalDocs);
    }
    if (blockChunks > 0) {
      writeBlock();
    }
    fieldsIndexOut.writeVInt(0); // end marker
    fieldsIndexOut.writeVLong(maxPointer);
    CodecUtil.writeFooter(fieldsIndexOut);
  }

  @Override
  public void close() throws IOException {
    fieldsIndexOut.close();
  }

}
