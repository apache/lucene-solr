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

import static org.apache.lucene.codecs.compressing.FieldsIndexWriter.VERSION_CURRENT;
import static org.apache.lucene.codecs.compressing.FieldsIndexWriter.VERSION_START;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.FutureObjects;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.DirectMonotonicReader;

final class FieldsIndexReader extends FieldsIndex {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FieldsIndexReader.class);

  private final int maxDoc;
  private final int blockShift;
  private final int numChunks;
  private final DirectMonotonicReader.Meta docsMeta;
  private final DirectMonotonicReader.Meta startPointersMeta;
  private final IndexInput indexInput;
  private final long docsStartPointer, docsEndPointer, startPointersStartPointer, startPointersEndPointer;
  private final DirectMonotonicReader docs, startPointers;
  private final long maxPointer;

  FieldsIndexReader(Directory dir, String name, String suffix, String extension, String codecName, byte[] id, IndexInput metaIn) throws IOException {
    maxDoc = metaIn.readInt();
    blockShift = metaIn.readInt();
    numChunks = metaIn.readInt();
    docsStartPointer = metaIn.readLong();
    docsMeta = DirectMonotonicReader.loadMeta(metaIn, numChunks, blockShift);
    docsEndPointer = startPointersStartPointer = metaIn.readLong();
    startPointersMeta = DirectMonotonicReader.loadMeta(metaIn, numChunks, blockShift);
    startPointersEndPointer = metaIn.readLong();
    maxPointer = metaIn.readLong();

    indexInput = dir.openInput(IndexFileNames.segmentFileName(name, suffix, extension), IOContext.READ);
    boolean success = false;
    try {
      CodecUtil.checkIndexHeader(indexInput, codecName + "Idx", VERSION_START, VERSION_CURRENT, id, suffix);
      CodecUtil.retrieveChecksum(indexInput);
      success = true;
    } finally {
      if (success == false) {
        indexInput.close();
      }
    }
    final RandomAccessInput docsSlice = indexInput.randomAccessSlice(docsStartPointer, docsEndPointer - docsStartPointer);
    final RandomAccessInput startPointersSlice = indexInput.randomAccessSlice(startPointersStartPointer, startPointersEndPointer - startPointersStartPointer);
    docs = DirectMonotonicReader.getInstance(docsMeta, docsSlice);
    startPointers = DirectMonotonicReader.getInstance(startPointersMeta, startPointersSlice);
  }

  private FieldsIndexReader(FieldsIndexReader other) throws IOException {
    maxDoc = other.maxDoc;
    numChunks = other.numChunks;
    blockShift = other.blockShift;
    docsMeta = other.docsMeta;
    startPointersMeta = other.startPointersMeta;
    indexInput = other.indexInput.clone();
    docsStartPointer = other.docsStartPointer;
    docsEndPointer = other.docsEndPointer;
    startPointersStartPointer = other.startPointersStartPointer;
    startPointersEndPointer = other.startPointersEndPointer;
    maxPointer = other.maxPointer;
    final RandomAccessInput docsSlice = indexInput.randomAccessSlice(docsStartPointer, docsEndPointer - docsStartPointer);
    final RandomAccessInput startPointersSlice = indexInput.randomAccessSlice(startPointersStartPointer, startPointersEndPointer - startPointersStartPointer);
    docs = DirectMonotonicReader.getInstance(docsMeta, docsSlice);
    startPointers = DirectMonotonicReader.getInstance(startPointersMeta, startPointersSlice);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + docsMeta.ramBytesUsed() + startPointersMeta.ramBytesUsed() +
        docs.ramBytesUsed() + startPointers.ramBytesUsed();
  }

  @Override
  public void close() throws IOException {
    indexInput.close();
  }

  @Override
  long getStartPointer(int docID) {
    FutureObjects.checkIndex(docID, maxDoc);
    long blockIndex = docs.binarySearch(0, numChunks, docID);
    if (blockIndex < 0) {
      blockIndex = -2 - blockIndex;
    }
    return startPointers.get(blockIndex);
  }

  @Override
  public FieldsIndex clone() {
    try {
      return new FieldsIndexReader(this);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public long getMaxPointer() {
    return maxPointer;
  }

  @Override
  void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(indexInput);
  }
}
