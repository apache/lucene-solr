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

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.LongsRef;


// NOTE: outputs should be per-field, since
// longsSize is fixed for each field
public class TempTermOutputs extends Outputs<TempTermOutputs.TempMetaData> {
  private final static TempMetaData NO_OUTPUT = new TempMetaData();
  private static boolean DEBUG = false;
  private int longsSize;

  public static class TempMetaData {
    public long[] longs;
    public byte[] bytes;
    TempMetaData() {
      this.longs = null;
      this.bytes = null;
    }
    TempMetaData(long[] longs, byte[] bytes) {
      this.longs = longs;
      this.bytes = bytes;
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
          sb.append(bytes[i]+" ");
        }
        sb.append("]");
      } else {
        sb.append(" null");
      }
      return sb.toString();
    }
  }
  
  private TempTermOutputs() {
  }

  protected TempTermOutputs(int longsSize) {
    this.longsSize = longsSize;
  }

  @Override
  //
  // Since longs blob is fixed length, when these two are 'comparable'
  // i.e. when every value in long[] fits the same ordering, the smaller one 
  // will be the result.
  //
  // NOTE: only long[] is 'shared', i.e. after sharing common value,
  // the output of smaller one will be a all-zero long[] with original byte[] blob.
  //
  // nocommit: Builder.add() doesn't immediatelly consumes the output data, 
  // which means, the longs after one add() should all be deeply copied 
  // instead of being reused? quite hairly to detect it here, so the caller 
  // must be careful about this.
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
        ret = new TempMetaData(longs2, null);
      } else {
        ret = new TempMetaData(longs1, null);
      }
    } else {
      // equal
      if (t1.bytes!= null && Arrays.equals(t1.bytes, t2.bytes)) {  // all fields are equal
        ret = t1;
      } else if (accum == 0) { // all zero case
        ret = NO_OUTPUT;
      } else {
        ret = new TempMetaData(longs1, null);
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
    if (diff == 0 && (t1.bytes == null || t1.bytes.length == 0)) {
      ret = NO_OUTPUT;
    } else {
      ret = new TempMetaData(share, t1.bytes);
    }
    if (DEBUG) System.out.println("ret:"+ret);
    return ret;
  }

  @Override
  // nocommit: need to check all-zero case?
  // so we can reuse one long[] 
  public TempMetaData add(TempMetaData t1, TempMetaData t2) {
    if (DEBUG) System.out.print("add("+t1+", "+t2+") = ");
    // nocommit: necessary?
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
    long[] accum = new long[longsSize];  // nocommit: reuse
    while (pos < longsSize) {
      accum[pos] = t1.longs[pos] + t2.longs[pos];
      assert(accum[pos] >= 0);
      pos++;
    }
    TempMetaData ret;
    if (t2.bytes != null) {
      ret = new TempMetaData(accum, t2.bytes);
    } else {
      ret = new TempMetaData(accum, t1.bytes);
    }
    if (DEBUG) System.out.println("ret:"+ret);
    return ret;
  }

  @Override
  public void write(TempMetaData data, DataOutput out) throws IOException {
    for (int pos = 0; pos < longsSize; pos++) {
      out.writeVLong(data.longs[pos]);
    }
    if (data.bytes != null) {
      out.writeVInt(data.bytes.length);
      out.writeBytes(data.bytes, 0, data.bytes.length);
    } else {
      out.writeVInt(0);
    }
  }
  // nocommit: can this non-null byte case be used in Final Output?

  @Override
  public TempMetaData read(DataInput in) throws IOException {
    long[] longs = new long[longsSize];
    for (int pos = 0; pos < longsSize; pos++) {
      longs[pos] = in.readVLong();
    }
    int bytesSize = in.readVInt();
    byte[] bytes = null;
    if (bytesSize > 0) {
      bytes = new byte[bytesSize];
      in.readBytes(bytes, 0, bytes.length);
    }
    TempMetaData meta = new TempMetaData(longs, bytes);
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
  
