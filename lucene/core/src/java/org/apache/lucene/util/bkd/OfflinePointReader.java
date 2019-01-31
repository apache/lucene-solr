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

/** Reads points from disk in a fixed-with format, previously written with {@link OfflinePointWriter}.
 *
 * @lucene.internal */
public final class OfflinePointReader extends PointReader {
  long countLeft;
  final IndexInput in;
  private final byte[] packedValue;
  final int bytesPerDoc;
  private int docID;
  private boolean checked;

  // File name we are reading
  final String name;

  public OfflinePointReader(Directory tempDir, String tempFileName, int packedBytesLength, long start, long length) throws IOException {
    int bytesPerDoc = packedBytesLength + Integer.BYTES;
    this.bytesPerDoc = bytesPerDoc;

    if ((start + length) * bytesPerDoc + CodecUtil.footerLength() > tempDir.fileLength(tempFileName)) {
      throw new IllegalArgumentException("requested slice is beyond the length of this file: start=" + start + " length=" + length + " bytesPerDoc=" + bytesPerDoc + " fileLength=" + tempDir.fileLength(tempFileName) + " tempFileName=" + tempFileName);
    }

    // Best-effort checksumming:
    if (start == 0 && length*bytesPerDoc == tempDir.fileLength(tempFileName) - CodecUtil.footerLength()) {
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

    long seekFP = start * bytesPerDoc;
    in.seek(seekFP);
    countLeft = length;
    packedValue = new byte[packedBytesLength];
  }

  @Override
  public boolean next() throws IOException {
    if (countLeft >= 0) {
      if (countLeft == 0) {
        return false;
      }
      countLeft--;
    }
    try {
      in.readBytes(packedValue, 0, packedValue.length);
    } catch (EOFException eofe) {
      assert countLeft == -1;
      return false;
    }
    docID = in.readInt();
    return true;
  }

  @Override
  public byte[] packedValue() {
    return packedValue;
  }

  @Override
  public int docID() {
    return docID;
  }

  @Override
  public void buildHistogram(int bytePosition, int[] histogram) throws IOException {
    if (bytePosition > bytesPerDoc) {
      throw new IllegalStateException("document has " + bytesPerDoc + " bytes , but " + bytePosition + " were requested");
    }
    long fp = in.getFilePointer() + bytePosition;
    for(long i = countLeft; i > 0 ;i--) {
      in.seek(fp);
      histogram[in.readByte() & 0xff]++;
      fp += bytesPerDoc;
    }
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
}

