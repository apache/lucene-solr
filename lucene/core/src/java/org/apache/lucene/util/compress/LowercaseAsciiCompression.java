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
package org.apache.lucene.util.compress;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;

/**
 * Utility class that can efficiently compress arrays that mostly contain
 * characters in the [0x1F,0x3F) or [0x5F,0x7F) ranges, which notably
 * include all digits, lowercase characters, '.', '-' and '_'.
 */
public final class LowercaseAsciiCompression {

  private static final boolean isCompressible(int b) {
    final int high3Bits = (b + 1) & ~0x1F;
    return high3Bits == 0x20 || high3Bits == 0x60;
  }

  private LowercaseAsciiCompression() {}

  /**
   * Compress {@code in[0:len]} into {@code out}.
   * This returns {@code false} if the content cannot be compressed. The number
   * of bytes written is guaranteed to be less than {@code len} otherwise.
   */
  public static boolean compress(byte[] in, int len, byte[] tmp, DataOutput out) throws IOException {
    if (len < 8) {
      return false;
    }

    // 1. Count exceptions and fail compression if there are too many of them.
    final int maxExceptions = len >>> 5;
    int previousExceptionIndex = 0;
    int numExceptions = 0;
    for (int i = 0; i < len; ++i) {
      final int b = in[i] & 0xFF;
      if (isCompressible(b) == false) {
        while (i - previousExceptionIndex > 0xFF) {
          ++numExceptions;
          previousExceptionIndex += 0xFF;
        }
        if (++numExceptions > maxExceptions) {
          return false;
        }
        previousExceptionIndex = i;
      }
    }
    assert numExceptions <= maxExceptions;

    // 2. Now move all bytes to the [0,0x40) range (6 bits). This loop gets auto-vectorized on JDK13+.
    final int compressedLen = len - (len >>> 2); // ignores exceptions
    assert compressedLen < len;
    for (int i = 0; i < len; ++i) {
      int b = (in[i] & 0xFF) + 1;
      tmp[i] = (byte) ((b & 0x1F) | ((b & 0x40) >>> 1));
    }

    // 3. Now pack the bytes so that we record 4 ASCII chars in 3 bytes
    int o = 0;
    for (int i = compressedLen; i < len; ++i) {
      tmp[o++] |= (tmp[i] & 0x30) << 2; // bits 4-5
    }
    for (int i = compressedLen; i < len; ++i) {
      tmp[o++] |= (tmp[i] & 0x0C) << 4; // bits 2-3
    }
    for (int i = compressedLen; i < len; ++i) {
      tmp[o++] |= (tmp[i] & 0x03) << 6; // bits 0-1
    }
    assert o <= compressedLen;

    out.writeBytes(tmp, 0, compressedLen);

    // 4. Finally record exceptions
    out.writeVInt(numExceptions);
    if (numExceptions > 0) {
      previousExceptionIndex = 0;
      int numExceptions2 = 0;
      for (int i = 0; i < len; ++i) {
        int b = in[i] & 0xFF;
        if (isCompressible(b) == false) {
          while (i - previousExceptionIndex > 0xFF) {
            // We record deltas between exceptions as bytes, so we need to create
            // "artificial" exceptions if the delta between two of them is greater
            // than the maximum unsigned byte value.
            out.writeByte((byte) 0xFF);
            previousExceptionIndex += 0xFF;
            out.writeByte(in[previousExceptionIndex]);
            numExceptions2++;
          }
          out.writeByte((byte) (i - previousExceptionIndex));
          previousExceptionIndex = i;
          out.writeByte((byte) b);
          numExceptions2++;
        }
      }
      if (numExceptions != numExceptions2) {
        throw new IllegalStateException("" + numExceptions + " <> " + numExceptions2 + " " + new BytesRef(in, 0, len).utf8ToString());
      }
    }

    return true;
  }

  /**
   * Decompress data that has been compressed with {@link #compress(byte[], int, byte[], DataOutput)}.
   * {@code len} must be the original length, not the compressed length.
   */
  public static void decompress(DataInput in, byte[] out, int len) throws IOException {
    final int saved = len >>> 2;
    int compressedLen = len - saved;

    // 1. Copy the packed bytes
    in.readBytes(out, 0, compressedLen);

    // 2. Restore the leading 2 bits of each packed byte into whole bytes
    for (int i = 0; i < saved; ++i) {
      out[compressedLen + i] = (byte) (((out[i] & 0xC0) >>> 2) | ((out[saved + i] & 0xC0) >>> 4) | ((out[(saved<<1) + i] & 0xC0) >>> 6));
    }

    // 3. Move back to the original range. This loop gets auto-vectorized on JDK13+.
    for (int i = 0; i < len; ++i) {
      final byte b = out[i];
      out[i] = (byte) (((b & 0x1F) | 0x20 | ((b & 0x20) << 1)) - 1);
    }

    // 4. Restore exceptions
    final int numExceptions = in.readVInt();
    int i = 0;
    for (int exception = 0; exception < numExceptions; ++exception) {
      i += in.readByte() & 0xFF;
      out[i] = in.readByte();
    }
  }
}
