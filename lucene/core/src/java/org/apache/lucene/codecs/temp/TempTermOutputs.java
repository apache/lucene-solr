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
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.LongsRef;


// NOTE: outputs should be per-field, since
// longsSize is fixed for each field
public class TempTermOutputs extends Outputs<TempTermOutputs.TempMetaData> {
  private final static TempMetaData NO_OUTPUT = new TempMetaData();
  private static boolean DEBUG = false;
  private FieldInfo fieldInfo;
  private int longsSize;

  public static class TempMetaData {
    public long[] longs;
    public byte[] bytes;
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
      return hash;
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
  
  private TempTermOutputs() {
  }

  protected TempTermOutputs(FieldInfo fieldInfo, int longsSize) {
    this.fieldInfo = fieldInfo;
    this.longsSize = longsSize;
  }

  @Override
  //
  // Since longs blob is fixed length, when these two are 'comparable'
  // i.e. when every value in long[] fits the same ordering, the smaller one 
  // will be the result.
  //
  // NOTE: only long[] is 'shared', i.e. if there are two byte[] on the successive
  // arcs, only the last byte[] is valid. (this somewhat saves nodes, but might affect
  // compression, since we'll have to load metadata block for other terms as well, currently,
  // we don't support this)
  //
  // nocommit: get the byte[] from smaller one as well, so that
  // byte[] is actually inherited
  //
  public TempMetaData common(TempMetaData t1, TempMetaData t2) {
    if (DEBUG) System.out.print("common("+t1+", "+t2+") = ");
    if (t1 == NO_OUTPUT || t2 == NO_OUTPUT) {
      if (DEBUG) System.out.println("ret:"+NO_OUTPUT);
      return NO_OUTPUT;
    }
    assert t1.longs != null;
    assert t2.longs != null;
    assert t1.longs.length == t2.longs.length;

    long accum = 0;
    long[] longs1 = t1.longs, longs2 = t2.longs;
    int pos = 0;
    boolean order = true;
    TempMetaData ret;

    while (pos < longsSize && longs1[pos] == longs2[pos]) {
      pos++;
    }
    if (pos < longsSize) {
      // unequal
      order = (longs1[pos] > longs2[pos]);
      if (order) {
        // check whether strictly longs1 >= longs2 
        while (pos < longsSize && longs1[pos] >= longs2[pos]) {
          accum += longs2[pos];
          pos++;
        }
      } else {
        // check whether strictly longs1 <= longs2 
        while (pos < longsSize && longs1[pos] <= longs2[pos]) {
          accum += longs1[pos];
          pos++;
        }
      }
      if (pos < longsSize || accum == 0) {
        ret = NO_OUTPUT;
      } else if (order) {
        ret = new TempMetaData(longs2, null, 0, -1);
      } else {
        ret = new TempMetaData(longs1, null, 0, -1);
      }
    } else {
      // equal
      if (t1.bytes!= null && bytesEqual(t1, t2) && statsEqual(t1, t2)) {  // all fields are equal
        ret = t1;
      } else if (accum == 0) { // all zero case
        ret = NO_OUTPUT;
      } else {
        ret = new TempMetaData(longs1, null, 0, -1);
      }
    }
    if (DEBUG) System.out.println("ret:"+ret);
    return ret;
  }

  @Override
  // nocommit: 
  // this *actually* always assume that t2 <= t1 before calling the method
  public TempMetaData subtract(TempMetaData t1, TempMetaData t2) {
    if (DEBUG) System.out.print("subtract("+t1+", "+t2+") = ");
    if (t2 == NO_OUTPUT) {
      if (DEBUG) System.out.println("ret:"+t1);
      return t1;
    }
    assert t1.longs != null;
    assert t2.longs != null;

    int pos = 0;
    long diff = 0;
    long[] share = new long[longsSize];  // nocommit: reuse

    while (pos < longsSize) {
      share[pos] = t1.longs[pos] - t2.longs[pos];
      diff += share[pos];
      pos++;
    }

    TempMetaData ret;
    if (diff == 0 && bytesEqual(t1, t2) && statsEqual(t1, t2)) {
      ret = NO_OUTPUT;
    } else {
      ret = new TempMetaData(share, t1.bytes, t1.docFreq, t1.totalTermFreq);
    }
    if (DEBUG) System.out.println("ret:"+ret);
    return ret;
  }

  static boolean statsEqual(final TempMetaData t1, final TempMetaData t2) {
    return t1.docFreq == t2.docFreq && t1.totalTermFreq == t2.totalTermFreq;
  }
  static boolean bytesEqual(final TempMetaData t1, final TempMetaData t2) {
    return Arrays.equals(t1.bytes, t2.bytes);
  }

  @Override
  // nocommit: need to check all-zero case?
  // so we can reuse one long[] 
  public TempMetaData add(TempMetaData t1, TempMetaData t2) {
    if (DEBUG) System.out.print("add("+t1+", "+t2+") = ");
    if (t1 == NO_OUTPUT) {
      if (DEBUG) System.out.println("ret:"+t2);
      return t2;
    } else if (t2 == NO_OUTPUT) {
      if (DEBUG) System.out.println("ret:"+t1);
      return t1;
    }
    assert t1.longs != null;
    assert t2.longs != null;

    int pos = 0;
    long[] accum = new long[longsSize];  // nocommit: reuse?
    while (pos < longsSize) {
      accum[pos] = t1.longs[pos] + t2.longs[pos];
      assert(accum[pos] >= 0);
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
    for (int pos = 0; pos < longsSize; pos++) {
      out.writeVLong(data.longs[pos]);
    }
    int code = data.docFreq == 0 ? 0 : 1;
    if (data.bytes != null) {
      out.writeVInt((data.bytes.length << 1) | code);
      out.writeBytes(data.bytes, 0, data.bytes.length);
    } else {
      out.writeVInt(code);
    }
    if (data.docFreq > 0) {
      out.writeVInt(data.docFreq);
      if (fieldInfo.getIndexOptions() != IndexOptions.DOCS_ONLY) {
        out.writeVLong(data.totalTermFreq - data.docFreq);
      }
    }
  }

  @Override
  public TempMetaData read(DataInput in) throws IOException {
    long[] longs = new long[longsSize];
    for (int pos = 0; pos < longsSize; pos++) {
      longs[pos] = in.readVLong();
    }
    int code = in.readVInt();
    int bytesSize = code >>> 1;
    int docFreq = 0;
    long totalTermFreq = -1;
    byte[] bytes = null;
    if (bytesSize > 0) {
      bytes = new byte[bytesSize];
      in.readBytes(bytes, 0, bytes.length);
    }
    if ((code & 1) == 1) {
      docFreq = in.readVInt();
      if (fieldInfo.getIndexOptions() != IndexOptions.DOCS_ONLY) {
        totalTermFreq = docFreq + in.readVLong();
      }
    }
    TempMetaData meta = new TempMetaData(longs, bytes, docFreq, totalTermFreq);
    return meta;
  }

  @Override
  public TempMetaData getNoOutput() {
    return NO_OUTPUT;
  }

  @Override
  public String outputToString(TempMetaData data) {
    return data.toString();
  }
}
  
