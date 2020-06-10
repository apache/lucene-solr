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

import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

class DocIdsWriter {

  private DocIdsWriter() {
  }

  static void writeDocIds(int[] docIds, int start, int count, DataOutput out) throws IOException {
    // docs can be sorted either when all docs in a block have the same value
    // or when a segment is sorted
    boolean sorted = true;
    for (int i = 1; i < count; ++i) {
      if (docIds[start + i - 1] > docIds[start + i]) {
        sorted = false;
        break;
      }
    }
    if (sorted) {
      out.writeByte((byte) 0);
      int previous = 0;
      for (int i = 0; i < count; ++i) {
        int doc = docIds[start + i];
        out.writeVInt(doc - previous);
        previous = doc;
      }
    } else {
      long max = 0;
      for (int i = 0; i < count; ++i) {
        max |= Integer.toUnsignedLong(docIds[start + i]);
      }
      if (max <= 0xffffff) {
        out.writeByte((byte) 24);
        int i;
        for (i = 0; i < count - 7; i += 8) {
          // 1st long
          out.writeShort(Short.reverseBytes((short) (docIds[start + i + 2] >>> 8)));
          out.writeByte((byte) docIds[start + i + 1]);
          out.writeShort(Short.reverseBytes((short) (docIds[start + i + 1] >>> 8)));
          out.writeByte((byte) docIds[start + i]);
          out.writeShort(Short.reverseBytes((short) (docIds[start + i] >>> 8)));
          // 2nd long
          out.writeByte((byte) (docIds[start + i + 5] >>> 16));
          out.writeByte((byte) docIds[start + i + 4]);
          out.writeShort(Short.reverseBytes((short) (docIds[start + i + 4] >>> 8)));
          out.writeByte((byte) docIds[start + i + 3]);
          out.writeShort(Short.reverseBytes((short) (docIds[start + i + 3] >>> 8)));
          out.writeByte((byte) docIds[start + i + 2]);
          // 3rd long
          out.writeByte((byte) docIds[start + i + 7]);
          out.writeShort(Short.reverseBytes((short) (docIds[start + i + 7] >>> 8)));
          out.writeByte((byte) docIds[start + i + 6]);
          out.writeShort(Short.reverseBytes((short) (docIds[start + i + 6] >>> 8)));
          out.writeShort(Short.reverseBytes((short) (docIds[start + i + 5])));
        }
        for (; i < count; ++i) {
          out.writeShort((short) (docIds[start + i] >>> 8));
          out.writeByte((byte) docIds[start + i]);
        }
      } else {
        out.writeByte((byte) 32);
        int i;
        for (i = 0; i < count - 1; i += 2) {
          out.writeInt(Integer.reverseBytes(docIds[start + i + 1]));
          out.writeInt(Integer.reverseBytes(docIds[start + i]));
        }
        for (; i < count; ++i) {
          out.writeInt(docIds[start + i]);
        }
      }
    }
  }

  /** Read {@code count} integers into {@code docIDs}. */
  static void readInts(IndexInput in, int count, int[] docIDs, long[] tmp, int version) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
      case 0:
        readDeltaVInts(in, count, docIDs);
        break;
      case 32:
        readInts32(in, count, docIDs, tmp, version);
        break;
      case 24:
        readInts24(in, count, docIDs, tmp, version);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private static void readDeltaVInts(IndexInput in, int count, int[] docIDs) throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      doc += in.readVInt();
      docIDs[i] = doc;
    }
  }

  private static void readInts32(IndexInput in, int count, int[] docIDs, long[] tmp, int version) throws IOException {
    assert tmp.length >= count / 2;
    in.readLELongs(tmp, 0, count / 2);
    int i, j;
    for ( i = 0, j = 0 ; i < count - 1; i += 2, j++) {
      final long l1;
      if (version < BKDWriter.VERSION_DOCIDS_LITTLE_ENDIAN) {
        l1 = Long.reverseBytes(tmp[j]);
      } else {
        l1 = tmp[j];
      }
      docIDs[i] = (int)(l1 >>> 32);
      docIDs[i+1] = (int)(l1 & 0xffffffff);
    }
    for (;i < count; i++) {
      docIDs[i] = in.readInt();
    }
  }

  private static void readInts24(IndexInput in, int count, int[] docIDs, long[] tmp, int version) throws IOException {
    assert tmp.length >= 3 * (count / 8);
    in.readLELongs(tmp, 0, 3 * (count / 8));
    int i, j;
    for (i = 0, j= 0; i < count - 7; i += 8, j += 3) {
      final long l1, l2, l3;
      if (version < BKDWriter.VERSION_DOCIDS_LITTLE_ENDIAN) {
        l1 = Long.reverseBytes(tmp[j]);
        l2 = Long.reverseBytes(tmp[j+1]);
        l3 = Long.reverseBytes(tmp[j+2]);
      } else {
        l1 = tmp[j];
        l2 = tmp[j+1];
        l3 = tmp[j+2];
      }
      docIDs[i] =   (int) (l1 >>> 40);
      docIDs[i+1] = (int) ((l1 >>> 16) & 0xffffff);
      docIDs[i+2] = (int) (((l1 & 0xffff) << 8) | (l2 >>> 56));
      docIDs[i+3] = (int) ((l2 >>> 32) & 0xffffff);
      docIDs[i+4] = (int) ((l2 >>> 8) & 0xffffff);
      docIDs[i+5] = (int) (((l2 & 0xff) << 16) | (l3 >>> 48));
      docIDs[i+6] = (int) ((l3 >>> 24) & 0xffffff);
      docIDs[i+7] = (int) (l3 & 0xffffff);
    }
    for (; i < count; ++i) {
      docIDs[i] = (Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte());
    }
  }

  /** Read {@code count} integers and feed the result directly to {@link IntersectVisitor#visit(int)}. */
  static void readInts(IndexInput in, int count, IntersectVisitor visitor, long[] tmp, int version) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
      case 0:
        readDeltaVInts(in, count, visitor);
        break;
      case 32:
        readInts32(in, count, visitor, tmp, version);
        break;
      case 24:
        readInts24(in, count, visitor, tmp, version);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private static void readDeltaVInts(IndexInput in, int count, IntersectVisitor visitor) throws IOException {
    int doc = 0;
    for (int i = 0; i < count; i++) {
      doc += in.readVInt();
      visitor.visit(doc);
    }
  }

  private static void readInts32(IndexInput in, int count, IntersectVisitor visitor, long[] tmp, int version) throws IOException {
    assert tmp.length >= count / 2;
    in.readLELongs(tmp, 0, count / 2);
    int i, j;
    for ( i = 0, j = 0 ; i < count - 1; i += 2, j++) {
      final long l1;
      if (version < BKDWriter.VERSION_DOCIDS_LITTLE_ENDIAN) {
        l1 = Long.reverseBytes(tmp[j]);
      } else {
        l1 = tmp[j];
      }
      visitor.visit((int)(l1 >>> 32));
      visitor.visit((int)(l1 & 0xffffffff));
    }
    for (;i < count; i++) {
      visitor.visit(in.readInt());
    }
  }

  private static void readInts24(IndexInput in, int count, IntersectVisitor visitor, long[] tmp, int version) throws IOException {
    assert tmp.length >= 3 * (count / 8);
    in.readLELongs(tmp, 0, 3 * (count / 8));
    int i, j;
    for (i = 0, j= 0; i < count - 7; i += 8, j += 3) {
      final long l1, l2, l3;
      if (version < BKDWriter.VERSION_DOCIDS_LITTLE_ENDIAN) {
        l1 = Long.reverseBytes(tmp[j]);
        l2 = Long.reverseBytes(tmp[j+1]);
        l3 = Long.reverseBytes(tmp[j+2]);
      } else {
        l1 = tmp[j];
        l2 = tmp[j+1];
        l3 = tmp[j+2];
      }
      visitor.visit((int) (l1 >>> 40));
      visitor.visit((int) (l1 >>> 16) & 0xffffff);
      visitor.visit((int) (((l1 & 0xffff) << 8) | (l2 >>> 56)));
      visitor.visit((int) (l2 >>> 32) & 0xffffff);
      visitor.visit((int) (l2 >>> 8) & 0xffffff);
      visitor.visit((int) (((l2 & 0xff) << 16) | (l3 >>> 48)));
      visitor.visit((int) (l3 >>> 24) & 0xffffff);
      visitor.visit((int) l3 & 0xffffff);
    }
    for (; i < count; ++i) {
      visitor.visit((Short.toUnsignedInt(in.readShort()) << 8) | Byte.toUnsignedInt(in.readByte()));
    }
  }
}
