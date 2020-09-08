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
package org.apache.lucene.util.bkd;

import java.io.EOFException;
import java.io.IOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

/**
 * Reads points from disk in a fixed-with format, previously written with {@link OfflinePointWriter}.
 *
 * @lucene.internal
 * */
public final class OfflinePointReader implements PointReader {

  long countLeft;
  final IndexInput in;
  byte[] onHeapBuffer;
  int offset;
  private boolean checked;
  private final BKDConfig config;
  private int pointsInBuffer;
  private final int maxPointOnHeap;
  // File name we are reading
  final String name;
  private final OfflinePointValue pointValue;

  public OfflinePointReader(BKDConfig config, Directory tempDir, String tempFileName, long start, long length, byte[] reusableBuffer) throws IOException {
    this.config = config;

    if ((start + length) * config.bytesPerDoc + CodecUtil.footerLength() > tempDir.fileLength(tempFileName)) {
      throw new IllegalArgumentException("requested slice is beyond the length of this file: start=" + start + " length=" + length + " bytesPerDoc=" + config.bytesPerDoc + " fileLength=" + tempDir.fileLength(tempFileName) + " tempFileName=" + tempFileName);
    }
    if (reusableBuffer == null) {
      throw new IllegalArgumentException("[reusableBuffer] cannot be null");
    }
    if (reusableBuffer.length < config.bytesPerDoc) {
      throw new IllegalArgumentException("Length of [reusableBuffer] must be bigger than " + config.bytesPerDoc);
    }

    this.maxPointOnHeap =  reusableBuffer.length / config.bytesPerDoc;
    // Best-effort checksumming:
    if (start == 0 && length*config.bytesPerDoc == tempDir.fileLength(tempFileName) - CodecUtil.footerLength()) {
      // If we are going to read the entire file, e.g. because BKDWriter is now
      // partitioning it, we open with checksums:
      in = tempDir.openChecksumInput(tempFileName, IOContext.READONCE);
    } else {
      // Since we are going to seek somewhere in the middle of a possibly huge
      // file, and not read all bytes from there, don't use ChecksumIndexInput here.
      // This is typically fine, because this same file will later be read fully,
      // at another level of the BKDWriter recursion
      in = tempDir.openInput(tempFileName, IOContext.READONCE);
    }

    name = tempFileName;

    long seekFP = start * config.bytesPerDoc;
    in.seek(seekFP);
    countLeft = length;
    this.onHeapBuffer = reusableBuffer;
    this.pointValue = new OfflinePointValue(config, onHeapBuffer);
  }

  @Override
  public boolean next() throws IOException {
    if (this.pointsInBuffer == 0) {
      if (countLeft >= 0) {
        if (countLeft == 0) {
          return false;
        }
      }
      try {
        if (countLeft > maxPointOnHeap) {
          in.readBytes(onHeapBuffer, 0, maxPointOnHeap * config.bytesPerDoc);
          pointsInBuffer = maxPointOnHeap - 1;
          countLeft -= maxPointOnHeap;
        } else {
          in.readBytes(onHeapBuffer, 0, (int) countLeft * config.bytesPerDoc);
          pointsInBuffer = Math.toIntExact(countLeft - 1);
          countLeft = 0;
        }
        this.offset = 0;
      } catch (EOFException eofe) {
        assert countLeft == -1;
        return false;
      }
    } else {
      this.pointsInBuffer--;
      this.offset += config.bytesPerDoc;
    }
    return true;
  }

  @Override
  public PointValue pointValue() {
    pointValue.setOffset(offset);
   return pointValue;
  }

  @Override
  public void close() throws IOException {
    try {
      if (countLeft == 0 && in instanceof ChecksumIndexInput && checked == false) {
        //System.out.println("NOW CHECK: " + name);
        checked = true;
        CodecUtil.checkFooter((ChecksumIndexInput) in);
      }
    } finally {
      in.close();
    }
  }

  /**
   * Reusable implementation for a point value offline
   */
  static class OfflinePointValue implements PointValue {

    final BytesRef packedValue;
    final BytesRef packedValueDocID;
    final int packedValueLength;

    OfflinePointValue(BKDConfig config, byte[] value) {
      this.packedValueLength = config.packedBytesLength;
      this.packedValue = new BytesRef(value, 0, packedValueLength);
      this.packedValueDocID = new BytesRef(value, 0, config.bytesPerDoc);
    }

    /**
     * Sets a new value by changing the offset.
     */
    public void setOffset(int offset) {
      packedValue.offset = offset;
      packedValueDocID.offset = offset;
    }

    @Override
    public BytesRef packedValue() {
      return packedValue;
    }

    @Override
    public int docID() {
      int position = packedValueDocID.offset + packedValueLength;
      return ((packedValueDocID.bytes[position] & 0xFF) << 24) | ((packedValueDocID.bytes[++position] & 0xFF) << 16)
          | ((packedValueDocID.bytes[++position] & 0xFF) <<  8) |  (packedValueDocID.bytes[++position] & 0xFF);
    }

    @Override
    public BytesRef packedValueDocIDBytes() {
      return packedValueDocID;
    }
  }

}

