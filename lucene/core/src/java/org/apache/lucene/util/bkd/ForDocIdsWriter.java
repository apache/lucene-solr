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

// This file has been automatically generated, DO NOT EDIT

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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ForPrimitives;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.lucene.util.ForPrimitives.BLOCK_SIZE;

final class ForDocIdsWriter {

  private final long[] tmp1 = new long[BLOCK_SIZE];
  private final long[] tmp2 = new long[BLOCK_SIZE];

  /** sole cxtor */
  ForDocIdsWriter() {
  }

  /**
   * Encode 128 integers from {@code ints} into {@code out}.
   */
  void encode(int[] ints, int start, DataOutput out) throws IOException {
    boolean sorted = true;
    for (int i = 1; i < BLOCK_SIZE; ++i) {
      if (ints[start + i - 1] > ints[start + i]) {
        sorted = false;
        break;
      }
    }
    if (sorted) {
      if (ints[start] == ints[start + BLOCK_SIZE - 1]) {
        // all equal, it might happen for multi-value points
        out.writeByte((byte) 0);
        out.writeVInt(ints[start]);
      } else {
        int base  = ints[start];
        tmp1[0] = 0;
        tmp2[0] = ints[start];
        long max = ints[start];
        long maxDelta = 0;
        for (int i = 1; i < BLOCK_SIZE; ++i) {
          final int delta = ints[start + i] - ints[start + i - 1];
          maxDelta |= Integer.toUnsignedLong(delta);
          max |= Integer.toUnsignedLong(ints[start + i]);
          tmp1[i] = delta;
          tmp2[i] = ints[start + i];
        }
        final int bpvDelta = PackedInts.bitsRequired(maxDelta);
        if (bpvDelta == 1 && allEqualOne(tmp1, 1, BLOCK_SIZE)) {
          // special case for consecutive integers
          out.writeByte(Byte.MAX_VALUE);
          out.writeVInt(base);
        } else {
          final int bpv = PackedInts.bitsRequired(max);
          if (bpvDelta < bpv) {
            // for delta encoding we add 32 to bpv
            out.writeByte((byte) (32 + bpvDelta));
            encode(tmp1, bpvDelta, out, tmp2);
            out.writeVInt(base);
          } else {
            // standard encoding, no benefit from delta encoding
            out.writeByte((byte) bpv);
            encode(tmp2, bpv, out, tmp1);
          }
        }
      }
    } else {
      long max = 0;
      int maxVal = Integer.MIN_VALUE;
      int minVal = Integer.MAX_VALUE;
      for (int j = 0; j < BLOCK_SIZE; ++j) {
        max |= Integer.toUnsignedLong(ints[start + j]);
        tmp1[j] = ints[start + j];
        minVal = Math.min(minVal, ints[start + j]);
        maxVal = Math.max(maxVal, ints[start + j]);
      }
      final int bpv = PackedInts.bitsRequired(max);
      final int bvpDiff = PackedInts.bitsRequired(Integer.toUnsignedLong(maxVal - minVal));
      if (bpv > bvpDiff) {
        // for base encoding we add 64 to bvp
        out.writeByte((byte) (64 + bvpDiff));
        for (int i = 0; i < BLOCK_SIZE; ++i) {
          tmp1[i] = ints[start + i] - minVal;
        }
        encode(tmp1, bvpDiff, out, tmp2);
        out.writeVInt(minVal);
      } else {
        // standard encoding
        out.writeByte((byte) bpv);
        encode(tmp1, bpv, out, tmp2);
      }
    }
  }

  private static boolean allEqualOne(long[] longs, int start, int end) {
    for (int i = start; i < end; ++i) {
      if (longs[i] != 1) {
        return false;
      }
    }
    return true;
  }

  /**
   * Decode 128 integers into {@code ints}.
   */
  void decode(DataInput in, int[] ints, int offset) throws IOException {
    final int bitsPerValue = in.readByte();
    decode(bitsPerValue, in, ints, offset, tmp1, tmp2);
  }

  /**
   * Decode 128 integers and feed the result directly to
   * {@link org.apache.lucene.index.PointValues.IntersectVisitor#visit(int)}.
   */
  void decode(DataInput in, PointValues.IntersectVisitor visitor) throws IOException {
    final int code = in.readByte();
    decode(code, in, visitor, tmp1, tmp2);
  }

  private static void encode(long[] longs, int bitsPerValue, DataOutput out, long[] tmp) throws IOException {
    // re-order values for easier decoding
    if (bitsPerValue <= 8) {
      for (int i = 0, j = 0; i < 16; ++i, j += 8) {
        tmp[i] = longs[j];
        tmp[16+i] = longs[j+1];
        tmp[32+i] = longs[j+2];
        tmp[48+i] = longs[j+3];
        tmp[64+i] = longs[j+4];
        tmp[80+i] = longs[j+5];
        tmp[96+i] = longs[j+6];
        tmp[112+i] = longs[j+7];
      }
    } else if (bitsPerValue <= 16) {
      for (int i = 0, j = 0; i < 32; ++i, j += 4) {
        tmp[i] = longs[j];
        tmp[32+i] = longs[j+1];
        tmp[64+i] = longs[j+2];
        tmp[96+i] = longs[j+3];
      }
    } else {
      for (int i = 0, j = 0; i < 64; ++i, j += 2) {
        tmp[i] = longs[j];
        tmp[64+i] = longs[j+1];
      }
    }
    ForPrimitives.encode(tmp, bitsPerValue, out, longs);
  }

  /**
   * Decode 128 integers into {@code longs}.
   */
  private static void decode(int code, DataInput in, int[] ints, int offset, long[] longs, long[] tmp) throws IOException {
    switch (code) {
      case 0:
        final int base = in.readVInt();
        Arrays.fill(ints, offset, offset + BLOCK_SIZE, base);
        break;
      case 1:
        ForPrimitives.decode1(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 2:
        ForPrimitives.decode2(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 3:
        ForPrimitives.decode3(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 4:
        ForPrimitives.decode4(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 5:
        ForPrimitives.decode5(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 6:
        ForPrimitives.decode6(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 7:
        ForPrimitives.decode7(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 8:
        ForPrimitives.decode8(in, tmp, longs);
        expand8(longs, ints, offset);
        break;
      case 9:
        ForPrimitives.decode9(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 10:
        ForPrimitives.decode10(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 11:
        ForPrimitives.decode11(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 12:
        ForPrimitives.decode12(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 13:
        ForPrimitives.decode13(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 14:
        ForPrimitives.decode14(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 15:
        ForPrimitives.decode15(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 16:
        ForPrimitives.decode16(in, tmp, longs);
        expand16(longs, ints, offset);
        break;
      case 17:
        ForPrimitives.decode17(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 18:
        ForPrimitives.decode18(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 19:
        ForPrimitives.decode19(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 20:
        ForPrimitives.decode20(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 21:
        ForPrimitives.decode21(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 22:
        ForPrimitives.decode22(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 23:
        ForPrimitives.decode23(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 24:
        ForPrimitives.decode24(in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 25:
      case 26:
      case 27:
      case 28:
      case 29:
      case 30:
      case 31:
      case 32:
        ForPrimitives.decodeSlow(code, in, tmp, longs);
        expand32(longs, ints, offset);
        break;
      case 33:
        ForPrimitives.decode1(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 34:
        ForPrimitives.decode2(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 35:
        ForPrimitives.decode3(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 36:
        ForPrimitives.decode4(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 37:
        ForPrimitives.decode5(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 38:
        ForPrimitives.decode6(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 39:
        ForPrimitives.decode7(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 40:
        ForPrimitives.decode8(in, tmp, longs);
        expand8Delta(in, longs, ints, offset);
        break;
      case 41:
        ForPrimitives.decode9(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 42:
        ForPrimitives.decode10(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 43:
        ForPrimitives.decode11(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 44:
        ForPrimitives.decode12(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 45:
        ForPrimitives.decode13(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 46:
        ForPrimitives.decode14(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 47:
        ForPrimitives.decode15(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 48:
        ForPrimitives.decode16(in, tmp, longs);
        expand16Delta(in, longs, ints, offset);
        break;
      case 49:
        ForPrimitives.decode17(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 50:
        ForPrimitives.decode18(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 51:
        ForPrimitives.decode19(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 52:
        ForPrimitives.decode20(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 53:
        ForPrimitives.decode21(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 54:
        ForPrimitives.decode22(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 55:
        ForPrimitives.decode23(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 56:
        ForPrimitives.decode24(in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 57:
      case 58:
      case 59:
      case 60:
      case 61:
      case 62:
      case 63:
      case 64:
        ForPrimitives.decodeSlow(code - 32, in, tmp, longs);
        expand32Delta(in, longs, ints, offset);
        break;
      case 65:
        ForPrimitives.decode1(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 66:
        ForPrimitives.decode2(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 67:
        ForPrimitives.decode3(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 68:
        ForPrimitives.decode4(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 69:
        ForPrimitives.decode5(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 70:
        ForPrimitives.decode6(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 71:
        ForPrimitives.decode7(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 72:
        ForPrimitives.decode8(in, tmp, longs);
        expand8Base(in, longs, ints, offset);
        break;
      case 73:
        ForPrimitives.decode9(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 74:
        ForPrimitives.decode10(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 75:
        ForPrimitives.decode11(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 76:
        ForPrimitives.decode12(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 77:
        ForPrimitives.decode13(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 78:
        ForPrimitives.decode14(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 79:
        ForPrimitives.decode15(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 80:
        ForPrimitives.decode16(in, tmp, longs);
        expand16Base(in, longs, ints, offset);
        break;
      case 81:
        ForPrimitives.decode17(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 82:
        ForPrimitives.decode18(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 83:
        ForPrimitives.decode19(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 84:
        ForPrimitives.decode20(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 85:
        ForPrimitives.decode21(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 86:
        ForPrimitives.decode22(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 87:
        ForPrimitives.decode23(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 88:
        ForPrimitives.decode24(in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case 89:
      case 90:
      case 91:
      case 92:
      case 93:
      case 94:
      case 95:
      case 96:
        ForPrimitives.decodeSlow(code - 64, in, tmp, longs);
        expand32Base(in, longs, ints, offset);
        break;
      case Byte.MAX_VALUE:
        consecutiveIntegers(in, ints, offset);
        break;
      default:
        throw new IllegalArgumentException("Invalid code: " + code);
    }
  }

  private static void decode(int code, DataInput in, PointValues.IntersectVisitor visitor, long[] longs, long[] tmp) throws IOException {
    switch (code) {
      case 0:
        final int base = in.readVInt();
        for (int i = 0; i < BLOCK_SIZE; i++) {
          visitor.visit(base);
        }
        break;
      case 1:
        ForPrimitives.decode1(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 2:
        ForPrimitives.decode2(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 3:
        ForPrimitives.decode3(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 4:
        ForPrimitives.decode4(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 5:
        ForPrimitives.decode5(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 6:
        ForPrimitives.decode6(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 7:
        ForPrimitives.decode7(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 8:
        ForPrimitives.decode8(in, tmp, longs);
        expand8(longs, visitor);
        break;
      case 9:
        ForPrimitives.decode9(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 10:
        ForPrimitives.decode10(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 11:
        ForPrimitives.decode11(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 12:
        ForPrimitives.decode12(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 13:
        ForPrimitives.decode13(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 14:
        ForPrimitives.decode14(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 15:
        ForPrimitives.decode15(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 16:
        ForPrimitives.decode16(in, tmp, longs);
        expand16(longs, visitor);
        break;
      case 17:
        ForPrimitives.decode17(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 18:
        ForPrimitives.decode18(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 19:
        ForPrimitives.decode19(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 20:
        ForPrimitives.decode20(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 21:
        ForPrimitives.decode21(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 22:
        ForPrimitives.decode22(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 23:
        ForPrimitives.decode23(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 24:
        ForPrimitives.decode24(in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 25:
      case 26:
      case 27:
      case 28:
      case 29:
      case 30:
      case 31:
      case 32:
        ForPrimitives.decodeSlow(code, in, tmp, longs);
        expand32(longs, visitor);
        break;
      case 33:
        ForPrimitives.decode1(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 34:
        ForPrimitives.decode2(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 35:
        ForPrimitives.decode3(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 36:
        ForPrimitives.decode4(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 37:
        ForPrimitives.decode5(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 38:
        ForPrimitives.decode6(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 39:
        ForPrimitives.decode7(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 40:
        ForPrimitives.decode8(in, tmp, longs);
        expand8Delta(in, longs, visitor);
        break;
      case 41:
        ForPrimitives.decode9(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 42:
        ForPrimitives.decode10(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 43:
        ForPrimitives.decode11(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 44:
        ForPrimitives.decode12(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 45:
        ForPrimitives.decode13(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 46:
        ForPrimitives.decode14(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 47:
        ForPrimitives.decode15(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 48:
        ForPrimitives.decode16(in, tmp, longs);
        expand16Delta(in, longs, visitor);
        break;
      case 49:
        ForPrimitives.decode17(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 50:
        ForPrimitives.decode18(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 51:
        ForPrimitives.decode19(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 52:
        ForPrimitives.decode20(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 53:
        ForPrimitives.decode21(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 54:
        ForPrimitives.decode22(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 55:
        ForPrimitives.decode23(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 56:
        ForPrimitives.decode24(in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 57:
      case 58:
      case 59:
      case 60:
      case 61:
      case 62:
      case 63:
      case 64:
        ForPrimitives.decodeSlow(code - 32, in, tmp, longs);
        expand32Delta(in, longs, visitor);
        break;
      case 65:
        ForPrimitives.decode1(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 66:
        ForPrimitives.decode2(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 67:
        ForPrimitives.decode3(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 68:
        ForPrimitives.decode4(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 69:
        ForPrimitives.decode5(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 70:
        ForPrimitives.decode6(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 71:
        ForPrimitives.decode7(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 72:
        ForPrimitives.decode8(in, tmp, longs);
        expand8Base(in, longs, visitor);
        break;
      case 73:
        ForPrimitives.decode9(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 74:
        ForPrimitives.decode10(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 75:
        ForPrimitives.decode11(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 76:
        ForPrimitives.decode12(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 77:
        ForPrimitives.decode13(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 78:
        ForPrimitives.decode14(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 79:
        ForPrimitives.decode15(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 80:
        ForPrimitives.decode16(in, tmp, longs);
        expand16Base(in, longs, visitor);
        break;
      case 81:
        ForPrimitives.decode17(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 82:
        ForPrimitives.decode18(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 83:
        ForPrimitives.decode19(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 84:
        ForPrimitives.decode20(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 85:
        ForPrimitives.decode21(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 86:
        ForPrimitives.decode22(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 87:
        ForPrimitives.decode23(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 88:
        ForPrimitives.decode24(in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case 89:
      case 90:
      case 91:
      case 92:
      case 93:
      case 94:
      case 95:
      case 96:
        ForPrimitives.decodeSlow(code - 64, in, tmp, longs);
        expand32Base(in, longs, visitor);
        break;
      case Byte.MAX_VALUE:
        consecutiveIntegers(in, visitor);
        break;
      default:
        throw new IllegalArgumentException("Invalid code: " + code);
    }
  }

  private static void expand8(long[] arr, int[] ints, int offset) {
    for (int i = 0, j = offset; i < 16; ++i, j += 8) {
      long l = arr[i];
      ints[j]   = (int) ((l >>> 56) & 0xFF);
      ints[j+1] = (int) ((l >>> 48) & 0xFF);
      ints[j+2] = (int) ((l >>> 40) & 0xFF);
      ints[j+3] = (int) ((l >>> 32) & 0xFF);
      ints[j+4] = (int) ((l >>> 24) & 0xFF);
      ints[j+5] = (int) ((l >>> 16) & 0xFF);
      ints[j+6] = (int) ((l >>> 8) & 0xFF);
      ints[j+7] = (int) (l & 0xFF);
    }
  }

  private static void expand8Delta(DataInput in, long[] arr, int[] ints, int offset) throws IOException{
    int base = in.readVInt();
    for (int i = 0, j = offset; i < 16; ++i, j += 8) {
      long l = arr[i];
      ints[j]   = base += (int) ((l >>> 56) & 0xFF);
      ints[j+1] = base += (int) ((l >>> 48) & 0xFF);
      ints[j+2] = base += (int) ((l >>> 40) & 0xFF);
      ints[j+3] = base += (int) ((l >>> 32) & 0xFF);
      ints[j+4] = base += (int) ((l >>> 24) & 0xFF);
      ints[j+5] = base += (int) ((l >>> 16) & 0xFF);
      ints[j+6] = base += (int) ((l >>> 8) & 0xFF);
      ints[j+7] = base += (int) (l & 0xFF);
    }
  }

  private static void expand8Base(DataInput in, long[] arr, int[] ints, int offset) throws IOException{
    final int base = in.readVInt();
    for (int i = 0, j = offset; i < 16; ++i, j += 8) {
      long l = arr[i];
      ints[j]   = base + (int) ((l >>> 56) & 0xFF);
      ints[j+1] = base + (int) ((l >>> 48) & 0xFF);
      ints[j+2] = base + (int) ((l >>> 40) & 0xFF);
      ints[j+3] = base + (int) ((l >>> 32) & 0xFF);
      ints[j+4] = base + (int) ((l >>> 24) & 0xFF);
      ints[j+5] = base + (int) ((l >>> 16) & 0xFF);
      ints[j+6] = base + (int) ((l >>> 8) & 0xFF);
      ints[j+7] = base + (int) (l & 0xFF);
    }
  }

  private static void expand8(long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      visitor.visit((int) ((l >>> 56) & 0xFF));
      visitor.visit((int) ((l >>> 48) & 0xFF));
      visitor.visit((int) ((l >>> 40) & 0xFF));
      visitor.visit((int) ((l >>> 32) & 0xFF));
      visitor.visit((int) ((l >>> 24) & 0xFF));
      visitor.visit((int) ((l >>> 16) & 0xFF));
      visitor.visit((int) ((l >>> 8) & 0xFF));
      visitor.visit((int) (l & 0xFF));
    }
  }

  private static void expand8Delta(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    int base = in.readVInt();
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      visitor.visit(base += (int) ((l >>> 56) & 0xFF));
      visitor.visit(base += (int) ((l >>> 48) & 0xFF));
      visitor.visit(base += (int) ((l >>> 40) & 0xFF));
      visitor.visit(base += (int) ((l >>> 32) & 0xFF));
      visitor.visit(base += (int) ((l >>> 24) & 0xFF));
      visitor.visit(base += (int) ((l >>> 16) & 0xFF));
      visitor.visit(base += (int) ((l >>> 8) & 0xFF));
      visitor.visit(base += (int) (l & 0xFF));
    }
  }

  private static void expand8Base(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    final int base = in.readVInt();
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      visitor.visit(base + (int) ((l >>> 56) & 0xFF));
      visitor.visit(base + (int) ((l >>> 48) & 0xFF));
      visitor.visit(base + (int) ((l >>> 40) & 0xFF));
      visitor.visit(base + (int) ((l >>> 32) & 0xFF));
      visitor.visit(base + (int) ((l >>> 24) & 0xFF));
      visitor.visit(base + (int) ((l >>> 16) & 0xFF));
      visitor.visit(base + (int) ((l >>> 8) & 0xFF));
      visitor.visit(base + (int) (l & 0xFF));
    }
  }

  private static void expand16(long[] arr, int[] ints, int offset) {
    for (int i = 0, j = offset; i < 32; ++i, j += 4) {
      long l = arr[i];
      ints[j]   = (int) ((l >>> 48) & 0xFFFF);
      ints[j+1] = (int) ((l >>> 32) & 0xFFFF);
      ints[j+2] = (int) ((l >>> 16) & 0xFFFF);
      ints[j+3] = (int) (l & 0xFFFF);
    }
  }

  private static void expand16Delta(DataInput in, long[] arr, int[] ints, int offset) throws IOException {
    int base = in.readVInt();
    for (int i = 0, j = offset; i < 32; ++i, j += 4) {
      long l = arr[i];
      ints[j]   = base += (int) ((l >>> 48) & 0xFFFF);
      ints[j+1] = base += (int) ((l >>> 32) & 0xFFFF);
      ints[j+2] = base += (int) ((l >>> 16) & 0xFFFF);
      ints[j+3] = base += (int) (l & 0xFFFF);
    }
  }

  private static void expand16Base(DataInput in, long[] arr, int[] ints, int offset) throws IOException {
    final int base = in.readVInt();
    for (int i = 0, j = offset; i < 32; ++i, j += 4) {
      long l = arr[i];
      ints[j]   = base + (int) ((l >>> 48) & 0xFFFF);
      ints[j+1] = base + (int) ((l >>> 32) & 0xFFFF);
      ints[j+2] = base + (int) ((l >>> 16) & 0xFFFF);
      ints[j+3] = base + (int) (l & 0xFFFF);
    }
  }

  private static void expand16(long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      visitor.visit((int) ((l >>> 48) & 0xFFFF));
      visitor.visit((int) ((l >>> 32) & 0xFFFF));
      visitor.visit((int) ((l >>> 16) & 0xFFFF));
      visitor.visit((int) (l & 0xFFFF));
    }
  }

  private static void expand16Delta(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    int base = in.readVInt();
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      visitor.visit(base += (int) ((l >>> 48) & 0xFFFF));
      visitor.visit(base += (int) ((l >>> 32) & 0xFFFF));
      visitor.visit(base += (int) ((l >>> 16) & 0xFFFF));
      visitor.visit(base += (int) (l & 0xFFFF));
    }
  }

  private static void expand16Base(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    final int base = in.readVInt();
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      visitor.visit(base + (int) ((l >>> 48) & 0xFFFF));
      visitor.visit(base + (int) ((l >>> 32) & 0xFFFF));
      visitor.visit(base + (int) ((l >>> 16) & 0xFFFF));
      visitor.visit(base + (int) (l & 0xFFFF));
    }
  }

  private static void expand32(long[] arr, int[] ints, int offset) {
    for (int i = 0, j = offset; i < 64; i++, j+=2) {
      long l = arr[i];
      ints[j] = (int) (l >>> 32);
      ints[j+1] = (int) l;
    }
  }

  private static void expand32Delta(DataInput in, long[] arr, int[] ints, int offset) throws IOException {
    int base = in.readVInt();
    for (int i = 0, j = offset; i < 64; i++, j+=2) {
      long l = arr[i];
      ints[j]   = base += (int) (l >>> 32);
      ints[j+1] = base += (int) l;
    }
  }

  private static void expand32Base(DataInput in, long[] arr, int[] ints, int offset) throws IOException {
    final int base = in.readVInt();
    for (int i = 0, j = offset; i < 64; i++, j+=2) {
      long l = arr[i];
      ints[j]   = base + (int) (l >>> 32);
      ints[j+1] = base + (int) l;
    }
  }

  private static void expand32(long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    for (int i = 0; i < 64; ++i) {
      long l = arr[i];
      visitor.visit((int) (l >>> 32));
      visitor.visit((int) l);
    }
  }

  private static void expand32Delta(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    int base = in.readVInt();
    for (int i = 0; i < 64; ++i) {
      long l = arr[i];
      visitor.visit(base += (int) (l >>> 32));
      visitor.visit(base += (int) l);
    }
  }

  private static void expand32Base(DataInput in, long[] arr, PointValues.IntersectVisitor visitor) throws IOException {
    final int base = in.readVInt();
    for (int i = 0; i < 64; ++i) {
      long l = arr[i];
      visitor.visit(base + (int) (l >>> 32));
      visitor.visit(base + (int) l);
    }
  }

  private static void consecutiveIntegers(DataInput in, int[] ints, int offset) throws IOException {
    int base = in.readVInt();
    for (int i = 0, j = offset; i < BLOCK_SIZE; i++, j++) {
      ints[j] = base + i;
    }
  }

  private static void consecutiveIntegers(DataInput in, PointValues.IntersectVisitor visitor) throws IOException {
    int base = in.readVInt();
    for (int i = 0; i < BLOCK_SIZE; ++i) {
     visitor.visit(base + i);
    }
  }
}
