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
package org.apache.lucene.codecs.compressing;

import static org.apache.lucene.util.BitUtil.zigZagDecode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Random-access reader for {@link FieldsIndexWriter}.
 *
 * @lucene.internal
 */
final class LegacyFieldsIndexReader extends FieldsIndex {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(LegacyFieldsIndexReader.class);

  final int maxDoc;
  final int[] docBases;
  final long[] startPointers;
  final int[] avgChunkDocs;
  final long[] avgChunkSizes;
  final PackedInts.Reader[] docBasesDeltas; // delta from the avg
  final PackedInts.Reader[] startPointersDeltas; // delta from the avg

  // It is the responsibility of the caller to close fieldsIndexIn after this constructor
  // has been called
  LegacyFieldsIndexReader(IndexInput fieldsIndexIn, SegmentInfo si) throws IOException {
    maxDoc = si.maxDoc();
    int[] docBases = new int[16];
    long[] startPointers = new long[16];
    int[] avgChunkDocs = new int[16];
    long[] avgChunkSizes = new long[16];
    PackedInts.Reader[] docBasesDeltas = new PackedInts.Reader[16];
    PackedInts.Reader[] startPointersDeltas = new PackedInts.Reader[16];

    final int packedIntsVersion = fieldsIndexIn.readVInt();

    int blockCount = 0;

    for (; ; ) {
      final int numChunks = fieldsIndexIn.readVInt();
      if (numChunks == 0) {
        break;
      }
      if (blockCount == docBases.length) {
        final int newSize = ArrayUtil.oversize(blockCount + 1, 8);
        docBases = ArrayUtil.growExact(docBases, newSize);
        startPointers = ArrayUtil.growExact(startPointers, newSize);
        avgChunkDocs = ArrayUtil.growExact(avgChunkDocs, newSize);
        avgChunkSizes = ArrayUtil.growExact(avgChunkSizes, newSize);
        docBasesDeltas = ArrayUtil.growExact(docBasesDeltas, newSize);
        startPointersDeltas = ArrayUtil.growExact(startPointersDeltas, newSize);
      }

      // doc bases
      docBases[blockCount] = fieldsIndexIn.readVInt();
      avgChunkDocs[blockCount] = fieldsIndexIn.readVInt();
      final int bitsPerDocBase = fieldsIndexIn.readVInt();
      if (bitsPerDocBase > 32) {
        throw new CorruptIndexException(
            "Corrupted bitsPerDocBase: " + bitsPerDocBase, fieldsIndexIn);
      }
      docBasesDeltas[blockCount] =
          PackedInts.getReaderNoHeader(
              fieldsIndexIn,
              PackedInts.Format.PACKED,
              packedIntsVersion,
              numChunks,
              bitsPerDocBase);

      // start pointers
      startPointers[blockCount] = fieldsIndexIn.readVLong();
      avgChunkSizes[blockCount] = fieldsIndexIn.readVLong();
      final int bitsPerStartPointer = fieldsIndexIn.readVInt();
      if (bitsPerStartPointer > 64) {
        throw new CorruptIndexException(
            "Corrupted bitsPerStartPointer: " + bitsPerStartPointer, fieldsIndexIn);
      }
      startPointersDeltas[blockCount] =
          PackedInts.getReaderNoHeader(
              fieldsIndexIn,
              PackedInts.Format.PACKED,
              packedIntsVersion,
              numChunks,
              bitsPerStartPointer);

      ++blockCount;
    }

    this.docBases = ArrayUtil.copyOfSubArray(docBases, 0, blockCount);
    this.startPointers = ArrayUtil.copyOfSubArray(startPointers, 0, blockCount);
    this.avgChunkDocs = ArrayUtil.copyOfSubArray(avgChunkDocs, 0, blockCount);
    this.avgChunkSizes = ArrayUtil.copyOfSubArray(avgChunkSizes, 0, blockCount);
    this.docBasesDeltas = ArrayUtil.copyOfSubArray(docBasesDeltas, 0, blockCount);
    this.startPointersDeltas = ArrayUtil.copyOfSubArray(startPointersDeltas, 0, blockCount);
  }

  private int block(int docID) {
    int lo = 0, hi = docBases.length - 1;
    while (lo <= hi) {
      final int mid = (lo + hi) >>> 1;
      final int midValue = docBases[mid];
      if (midValue == docID) {
        return mid;
      } else if (midValue < docID) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return hi;
  }

  private int relativeDocBase(int block, int relativeChunk) {
    final int expected = avgChunkDocs[block] * relativeChunk;
    final long delta = zigZagDecode(docBasesDeltas[block].get(relativeChunk));
    return expected + (int) delta;
  }

  private long relativeStartPointer(int block, int relativeChunk) {
    final long expected = avgChunkSizes[block] * relativeChunk;
    final long delta = zigZagDecode(startPointersDeltas[block].get(relativeChunk));
    return expected + delta;
  }

  private int relativeChunk(int block, int relativeDoc) {
    int lo = 0, hi = docBasesDeltas[block].size() - 1;
    while (lo <= hi) {
      final int mid = (lo + hi) >>> 1;
      final int midValue = relativeDocBase(block, mid);
      if (midValue == relativeDoc) {
        return mid;
      } else if (midValue < relativeDoc) {
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return hi;
  }

  long getStartPointer(int docID) {
    if (docID < 0 || docID >= maxDoc) {
      throw new IllegalArgumentException("docID out of range [0-" + maxDoc + "]: " + docID);
    }
    final int block = block(docID);
    final int relativeChunk = relativeChunk(block, docID - docBases[block]);
    return startPointers[block] + relativeStartPointer(block, relativeChunk);
  }

  @Override
  public LegacyFieldsIndexReader clone() {
    return this;
  }

  @Override
  public long ramBytesUsed() {
    long res = BASE_RAM_BYTES_USED;

    res += RamUsageEstimator.shallowSizeOf(docBasesDeltas);
    for (PackedInts.Reader r : docBasesDeltas) {
      res += r.ramBytesUsed();
    }
    res += RamUsageEstimator.shallowSizeOf(startPointersDeltas);
    for (PackedInts.Reader r : startPointersDeltas) {
      res += r.ramBytesUsed();
    }

    res += RamUsageEstimator.sizeOf(docBases);
    res += RamUsageEstimator.sizeOf(startPointers);
    res += RamUsageEstimator.sizeOf(avgChunkDocs);
    res += RamUsageEstimator.sizeOf(avgChunkSizes);

    return res;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>();

    long docBaseDeltaBytes = RamUsageEstimator.shallowSizeOf(docBasesDeltas);
    for (PackedInts.Reader r : docBasesDeltas) {
      docBaseDeltaBytes += r.ramBytesUsed();
    }
    resources.add(Accountables.namedAccountable("doc base deltas", docBaseDeltaBytes));

    long startPointerDeltaBytes = RamUsageEstimator.shallowSizeOf(startPointersDeltas);
    for (PackedInts.Reader r : startPointersDeltas) {
      startPointerDeltaBytes += r.ramBytesUsed();
    }
    resources.add(Accountables.namedAccountable("start pointer deltas", startPointerDeltaBytes));

    return Collections.unmodifiableList(resources);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(blocks=" + docBases.length + ")";
  }

  @Override
  public void close() throws IOException {
    // nothing to do
  }

  @Override
  void checkIntegrity() throws IOException {
    // nothing to do, the index is checked at open time
  }
}
