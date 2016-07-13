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

package org.apache.lucene.replicator.nrt;

import java.io.Closeable;
import java.io.IOException;
import java.util.Locale;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

/** Copies one file from an incoming DataInput to a dest filename in a local Directory */

public class CopyOneFile implements Closeable {
  private final DataInput in;
  private final IndexOutput out;
  private final ReplicaNode dest;
  public final String name;
  public final String tmpName;
  public final FileMetaData metaData;
  public final long bytesToCopy;
  private final long copyStartNS;
  private final byte[] buffer;

  private long bytesCopied;

  public CopyOneFile(DataInput in, ReplicaNode dest, String name, FileMetaData metaData, byte[] buffer) throws IOException {
    this.in = in;
    this.name = name;
    this.dest = dest;
    this.buffer = buffer;
    // TODO: pass correct IOCtx, e.g. seg total size
    out = dest.createTempOutput(name, "copy", IOContext.DEFAULT);
    tmpName = out.getName();

    // last 8 bytes are checksum:
    bytesToCopy = metaData.length - 8;

    if (Node.VERBOSE_FILES) {
      dest.message("file " + name + ": start copying to tmp file " + tmpName + " length=" + (8+bytesToCopy));
    }

    copyStartNS = System.nanoTime();
    this.metaData = metaData;
    dest.startCopyFile(name);
  }

  /** Transfers this file copy to another input, continuing where the first one left off */
  public CopyOneFile(CopyOneFile other, DataInput in) {
    this.in = in;
    this.dest = other.dest;
    this.name = other.name;
    this.out = other.out;
    this.tmpName = other.tmpName;
    this.metaData = other.metaData;
    this.bytesCopied = other.bytesCopied;
    this.bytesToCopy = other.bytesToCopy;
    this.copyStartNS = other.copyStartNS;
    this.buffer = other.buffer;
  }

  public void close() throws IOException {
    out.close();
    dest.finishCopyFile(name);
  }

  /** Copy another chunk of bytes, returning true once the copy is done */
  public boolean visit() throws IOException {
    // Copy up to 640 KB per visit:
    for(int i=0;i<10;i++) {
      long bytesLeft = bytesToCopy - bytesCopied;
      if (bytesLeft == 0) {
        long checksum = out.getChecksum();
        if (checksum != metaData.checksum) {
          // Bits flipped during copy!
          dest.message("file " + tmpName + ": checksum mismatch after copy (bits flipped during network copy?) after-copy checksum=" + checksum + " vs expected=" + metaData.checksum + "; cancel job");
          throw new IOException("file " + name + ": checksum mismatch after file copy");
        }

        // Paranoia: make sure the primary node is not smoking crack, by somehow sending us an already corrupted file whose checksum (in its
        // footer) disagrees with reality:
        long actualChecksumIn = in.readLong();
        if (actualChecksumIn != checksum) {
          dest.message("file " + tmpName + ": checksum claimed by primary disagrees with the file's footer: claimed checksum=" + checksum + " vs actual=" + actualChecksumIn);
          throw new IOException("file " + name + ": checksum mismatch after file copy");
        }
        out.writeLong(checksum);
        close();

        if (Node.VERBOSE_FILES) {
          dest.message(String.format(Locale.ROOT, "file %s: done copying [%s, %.3fms]",
                                     name,
                                     Node.bytesToString(metaData.length),
                                     (System.nanoTime() - copyStartNS)/1000000.0));
        }

        return true;
      }

      int toCopy = (int) Math.min(bytesLeft, buffer.length);
      in.readBytes(buffer, 0, toCopy);
      out.writeBytes(buffer, 0, toCopy);

      // TODO: rsync will fsync a range of the file; maybe we should do that here for large files in case we crash/killed
      bytesCopied += toCopy;
    }

    return false;
  }

  public long getBytesCopied() {
    return bytesCopied;
  }
}
