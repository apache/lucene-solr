package org.apache.lucene.codecs.temp;

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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.LongsRef;

// NOTE: outputs should be per-field, since
// longsSize is fixed for each field
public class TempTermOutputs extends Outputs<TempTermOutputs.TempMetaData> {
  private final static TempMetaData NO_OUTPUT = new TempMetaData();
  private static boolean DEBUG = false;
  private final boolean hasPos;
  private final int longsSize;

  public static class TempMetaData {
    long[] longs;
    byte[] bytes;
    int docFreq;
    long totalTermFreq;
    TempMetaData() {
      this.longs = null;
      this.bytes = null;
      this.docFreq = 0;
      this.totalTermFreq = -1;
    }
    TempMetaData(long[] longs, byte[] bytes, int docFreq, long totalTermFreq) {
      this.longs = longs;
      this.bytes = bytes;
      this.docFreq = docFreq;
      this.totalTermFreq = totalTermFreq;
    }

    // NOTE: actually, FST nodes are seldom 
    // identical when outputs on their arcs 
    // aren't NO_OUTPUTs.
    @Override
    public int hashCode() {
      int hash = 0;
      if (longs != null) {
        final int end = longs.length;
        for (int i = 0; i < end; i++) {
          hash -= longs[i];
        }
      }
      if (bytes != null) {
        hash = -hash;
        final int end = bytes.length;
        for (int i = 0; i < end; i++) {
          hash += bytes[i];
        }
      }
      hash += docFreq + totalTermFreq;
      return hash;
    }

    @Override
    public boolean equals(Object other_) {
      if (other_ == this) {
        return true;
      } else if (!(other_ instanceof TempTermOutputs.TempMetaData)) {
        return false;
      }
      TempMetaData other = (TempMetaData) other_;
      return statsEqual(this, other) && 
             longsEqual(this, other) && 
             bytesEqual(this, other);

    }
    public String toString() {
      if (this == NO_OUTPUT) {
        return "no_output";
      }
      StringBuffer sb = new StringBuffer();
      if (longs != null) {
        sb.append("[ ");
        for (int i = 0; i < longs.length; i++) {
          sb.append(longs[i]+" ");
        }
        sb.append("]");
      } else {
        sb.append("null");
      }
      if (bytes != null) {
        sb.append(" [ ");
        for (int i = 0; i < bytes.length; i++) {
          sb.append(Integer.toHexString((int)bytes[i] & 0xff)+" ");
        }
        sb.append("]");
      } else {
        sb.append(" null");
      }
      sb.append(" "+docFreq);
      sb.append(" "+totalTermFreq);
      return sb.toString();
    }
  }
  
  protected TempTermOutputs(FieldInfo fieldInfo, int longsSize) {
    this.hasPos = (fieldInfo.getIndexOptions() != IndexOptions.DOCS_ONLY);
    this.longsSize = longsSize;
  }

  @Override
  //
  // The return value will be the smaller one, when these two are 
  // 'comparable', i.e. every value in long[] fits the same ordering.
  //
  // NOTE: 
  // Only long[] part is 'shared' and pushed towards root.
  // byte[] and term stats will be on deeper arcs.
  //
  public TempMetaData common(TempMetaData t1, TempMetaData t2) {
    if (DEBUG) System.out.print("common("+t1+", "+t2+") = ");
    if (t1 == NO_OUTPUT || t2 == NO_OUTPUT) {
      if (DEBUG) System.out.println("ret:"+NO_OUTPUT);
      return NO_OUTPUT;
    }
    assert t1.longs.length == t2.longs.length;

    long[] min = t1.longs, max = t2.longs;
    int pos = 0;
    TempMetaData ret;

    while (pos < longsSize && min[pos] == max[pos]) {
      pos++;
    }
    if (pos < longsSize) {  // unequal long[]
      if (min[pos] > max[pos]) {
        min = t2.longs;
        max = t1.longs;
      }
      // check whether strictly smaller
      while (pos < longsSize && min[pos] <= max[pos]) {
        pos++;
      }
      if (pos < longsSize || allZero(min)) {  // not comparable or all-zero
        ret = NO_OUTPUT;
      } else {
        ret = new TempMetaData(min, null, 0, -1);
      }
    } else {  // equal long[]
      if (statsEqual(t1, t2) && bytesEqual(t1, t2)) {
        ret = t1;
      } else if (allZero(min)) {
        ret = NO_OUTPUT;
      } else {
        ret = new TempMetaData(min, null, 0, -1);
      }
    }
    if (DEBUG) System.out.println("ret:"+ret);
    return ret;
  }

  @Override
  public TempMetaData subtract(TempMetaData t1, TempMetaData t2) {
    if (DEBUG) System.out.print("subtract("+t1+", "+t2+") = ");
    if (t2 == NO_OUTPUT) {
      if (DEBUG) System.out.println("ret:"+t1);
      return t1;
    }
    assert t1.longs.length == t2.longs.length;

    int pos = 0;
    long diff = 0;
    long[] share = new long[longsSize];

    while (pos < longsSize) {
      share[pos] = t1.longs[pos] - t2.longs[pos];
      diff += share[pos];
      pos++;
    }

    TempMetaData ret;
    if (diff == 0 && statsEqual(t1, t2) && bytesEqual(t1, t2)) {
      ret = NO_OUTPUT;
    } else {
      ret = new TempMetaData(share, t1.bytes, t1.docFreq, t1.totalTermFreq);
    }
    if (DEBUG) System.out.println("ret:"+ret);
    return ret;
  }

  // nocommit: we might refactor out an 'addSelf' later, 
  // which improves 5~7% for fuzzy queries
  @Override
  public TempMetaData add(TempMetaData t1, TempMetaData t2) {
    if (DEBUG) System.out.print("add("+t1+", "+t2+") = ");
    if (t1 == NO_OUTPUT) {
      if (DEBUG) System.out.println("ret:"+t2);
      return t2;
    } else if (t2 == NO_OUTPUT) {
      if (DEBUG) System.out.println("ret:"+t1);
      return t1;
    }
    assert t1.longs.length == t2.longs.length;

    int pos = 0;
    long[] accum = new long[longsSize];

    while (pos < longsSize) {
      accum[pos] = t1.longs[pos] + t2.longs[pos];
      pos++;
    }

    TempMetaData ret;
    if (t2.bytes != null || t2.docFreq > 0) {
      ret = new TempMetaData(accum, t2.bytes, t2.docFreq, t2.totalTermFreq);
    } else {
      ret = new TempMetaData(accum, t1.bytes, t1.docFreq, t1.totalTermFreq);
    }
    if (DEBUG) System.out.println("ret:"+ret);
    return ret;
  }

  @Override
  public void write(TempMetaData data, DataOutput out) throws IOException {
    int bit0 = allZero(data.longs) ? 0 : 1;
    int bit1 = ((data.bytes == null || data.bytes.length == 0) ? 0 : 1) << 1;
    int bit2 = ((data.docFreq == 0)  ? 0 : 1) << 2;
    int bits = bit0 | bit1 | bit2;
    if (bit1 > 0) {  // determine extra length
      if (data.bytes.length < 32) {
        bits |= (data.bytes.length << 3);
        out.writeByte((byte)bits);
      } else {
        out.writeByte((byte)bits);
        out.writeVInt(data.bytes.length);
      }
    } else {
      out.writeByte((byte)bits);
    }
    if (bit0 > 0) {  // not all-zero case
      for (int pos = 0; pos < longsSize; pos++) {
        out.writeVLong(data.longs[pos]);
      }
    }
    if (bit1 > 0) {  // bytes exists
      out.writeBytes(data.bytes, 0, data.bytes.length);
    }
    if (bit2 > 0) {  // stats exist
      if (hasPos) {
        if (data.docFreq == data.totalTermFreq) {
          out.writeVInt((data.docFreq << 1) | 1);
        } else {
          out.writeVInt((data.docFreq << 1));
          out.writeVLong(data.totalTermFreq - data.docFreq);
        }
      } else {
        out.writeVInt(data.docFreq);
      }
    }
  }

  @Override
  public TempMetaData read(DataInput in) throws IOException {
    long[] longs = new long[longsSize];
    byte[] bytes = null;
    int docFreq = 0;
    long totalTermFreq = -1;
    int bits = in.readByte() & 0xff;
    int bit0 = bits & 1;
    int bit1 = bits & 2;
    int bit2 = bits & 4;
    int bytesSize = (bits >>> 3);
    if (bit1 > 0 && bytesSize == 0) {  // determine extra length
      bytesSize = in.readVInt();
    }
    if (bit0 > 0) {  // not all-zero case
      for (int pos = 0; pos < longsSize; pos++) {
        longs[pos] = in.readVLong();
      }
    }
    if (bit1 > 0) {  // bytes exists
      bytes = new byte[bytesSize];
      in.readBytes(bytes, 0, bytesSize);
    }
    if (bit2 > 0) {  // stats exist
      int code = in.readVInt();
      if (hasPos) {
        totalTermFreq = docFreq = code >>> 1;
        if ((code & 1) == 0) {
          totalTermFreq += in.readVLong();
        }
      } else {
        docFreq = code;
      }
    }
    return new TempMetaData(longs, bytes, docFreq, totalTermFreq);
  }

  @Override
  public TempMetaData getNoOutput() {
    return NO_OUTPUT;
  }

  @Override
  public String outputToString(TempMetaData data) {
    return data.toString();
  }

  static boolean statsEqual(final TempMetaData t1, final TempMetaData t2) {
    return t1.docFreq == t2.docFreq && t1.totalTermFreq == t2.totalTermFreq;
  }
  static boolean bytesEqual(final TempMetaData t1, final TempMetaData t2) {
    if (t1.bytes == null && t2.bytes == null) {
      return true;
    }
    return t1.bytes != null && t2.bytes != null && Arrays.equals(t1.bytes, t2.bytes);
  }
  static boolean longsEqual(final TempMetaData t1, final TempMetaData t2) {
    if (t1.longs == null && t2.longs == null) {
      return true;
    }
    return t1.longs != null && t2.longs != null && Arrays.equals(t1.longs, t2.longs);
  }
  static boolean allZero(final long[] l) {
    for (int i = 0; i < l.length; i++) {
      if (l[i] != 0) {
        return false;
      }
    }
    return true;
  }
}
