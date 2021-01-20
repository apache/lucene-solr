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
package org.apache.lucene.codecs.memory;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.fst.Outputs;

/**
 * An FST {@link Outputs} implementation for {@link FSTTermsWriter}.
 *
 * @lucene.experimental
 */

// NOTE: outputs should be per-field, since
// longsSize is fixed for each field
class FSTTermOutputs extends Outputs<FSTTermOutputs.TermData> {
  private static final TermData NO_OUTPUT = new TermData();
  // private static boolean TEST = false;
  private final boolean hasPos;

  /**
   * Represents the metadata for one term. On an FST, only long[] part is 'shared' and pushed
   * towards root. byte[] and term stats will be kept on deeper arcs.
   */
  static class TermData implements Accountable {
    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(TermData.class);
    byte[] bytes;
    int docFreq;
    long totalTermFreq;

    TermData() {
      this.bytes = null;
      this.docFreq = 0;
      this.totalTermFreq = -1;
    }

    TermData(byte[] bytes, int docFreq, long totalTermFreq) {
      this.bytes = bytes;
      this.docFreq = docFreq;
      this.totalTermFreq = totalTermFreq;
    }

    @Override
    public long ramBytesUsed() {
      long ramBytesUsed = BASE_RAM_BYTES_USED;
      if (bytes != null) {
        ramBytesUsed += RamUsageEstimator.sizeOf(bytes);
      }
      return ramBytesUsed;
    }

    // NOTE: actually, FST nodes are seldom
    // identical when outputs on their arcs
    // aren't NO_OUTPUTs.
    @Override
    public int hashCode() {
      int hash = 0;
      if (bytes != null) {
        final int end = bytes.length;
        for (int i = 0; i < end; i++) {
          hash += bytes[i];
        }
      }
      hash += docFreq + totalTermFreq;
      return hash;
    }

    @Override
    public String toString() {
      return "FSTTermOutputs$TermData bytes="
          + Arrays.toString(bytes)
          + " docFreq="
          + docFreq
          + " totalTermFreq="
          + totalTermFreq;
    }

    @Override
    public boolean equals(Object other_) {
      if (other_ == this) {
        return true;
      } else if (!(other_ instanceof FSTTermOutputs.TermData)) {
        return false;
      }
      TermData other = (TermData) other_;
      return statsEqual(this, other) && bytesEqual(this, other);
    }
  }

  protected FSTTermOutputs(FieldInfo fieldInfo) {
    this.hasPos = fieldInfo.getIndexOptions() != IndexOptions.DOCS;
  }

  @Override
  public long ramBytesUsed(TermData output) {
    return output.ramBytesUsed();
  }

  @Override
  //
  // The return value will be the smaller one, when these two are
  // 'comparable', i.e.
  // 1. every value in t1 is not larger than in t2, or
  // 2. every value in t1 is not smaller than t2.
  //
  public TermData common(TermData t1, TermData t2) {
    // if (TEST) System.out.print("common("+t1+", "+t2+") = ");
    if (t1 == NO_OUTPUT || t2 == NO_OUTPUT) {
      // if (TEST) System.out.println("ret:"+NO_OUTPUT);
      return NO_OUTPUT;
    }

    TermData ret;

    if (statsEqual(t1, t2) && bytesEqual(t1, t2)) {
      ret = t1;
    } else {
      ret = NO_OUTPUT;
    }
    // if (TEST) System.out.println("ret:"+ret);
    return ret;
  }

  @Override
  public TermData subtract(TermData t1, TermData t2) {
    // if (TEST) System.out.print("subtract("+t1+", "+t2+") = ");
    if (t2 == NO_OUTPUT) {
      // if (TEST) System.out.println("ret:"+t1);
      return t1;
    }

    TermData ret;
    if (statsEqual(t1, t2) && bytesEqual(t1, t2)) {
      ret = NO_OUTPUT;
    } else {
      ret = new TermData(t1.bytes, t1.docFreq, t1.totalTermFreq);
    }
    // if (TEST) System.out.println("ret:"+ret);
    return ret;
  }

  // TODO: if we refactor a 'addSelf(TermData other)',
  // we can gain about 5~7% for fuzzy queries, however this also
  // means we are putting too much stress on FST Outputs decoding?
  @Override
  public TermData add(TermData t1, TermData t2) {
    // if (TEST) System.out.print("add("+t1+", "+t2+") = ");
    if (t1 == NO_OUTPUT) {
      // if (TEST) System.out.println("ret:"+t2);
      return t2;
    } else if (t2 == NO_OUTPUT) {
      // if (TEST) System.out.println("ret:"+t1);
      return t1;
    }

    TermData ret;
    if (t2.bytes != null || t2.docFreq > 0) {
      ret = new TermData(t2.bytes, t2.docFreq, t2.totalTermFreq);
    } else {
      ret = new TermData(t1.bytes, t1.docFreq, t1.totalTermFreq);
    }
    // if (TEST) System.out.println("ret:"+ret);
    return ret;
  }

  @Override
  public void write(TermData data, DataOutput out) throws IOException {
    assert hasPos || data.totalTermFreq == -1;
    int bit0 = ((data.bytes == null || data.bytes.length == 0) ? 0 : 1);
    int bit1 = ((data.docFreq == 0) ? 0 : 1) << 1;
    int bits = bit0 | bit1;
    if (bit0 > 0) { // determine extra length
      if (data.bytes.length < 32) {
        bits |= (data.bytes.length << 2);
        out.writeByte((byte) bits);
      } else {
        out.writeByte((byte) bits);
        out.writeVInt(data.bytes.length);
      }
    } else {
      out.writeByte((byte) bits);
    }
    if (bit0 > 0) { // bytes exists
      out.writeBytes(data.bytes, 0, data.bytes.length);
    }
    if (bit1 > 0) { // stats exist
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
  public TermData read(DataInput in) throws IOException {
    byte[] bytes = null;
    int docFreq = 0;
    long totalTermFreq = -1;
    int bits = in.readByte() & 0xff;
    int bit0 = bits & 1;
    int bit1 = bits & 2;
    int bytesSize = (bits >>> 2);
    if (bit0 > 0 && bytesSize == 0) { // determine extra length
      bytesSize = in.readVInt();
    }
    if (bit0 > 0) { // bytes exists
      bytes = new byte[bytesSize];
      in.readBytes(bytes, 0, bytesSize);
    }
    if (bit1 > 0) { // stats exist
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
    return new TermData(bytes, docFreq, totalTermFreq);
  }

  @Override
  public void skipOutput(DataInput in) throws IOException {
    int bits = in.readByte() & 0xff;
    int bit0 = bits & 1;
    int bit1 = bits & 2;
    int bytesSize = (bits >>> 2);
    if (bit0 > 0 && bytesSize == 0) { // determine extra length
      bytesSize = in.readVInt();
    }
    if (bit0 > 0) { // bytes exists
      in.skipBytes(bytesSize);
    }
    if (bit1 > 0) { // stats exist
      int code = in.readVInt();
      if (hasPos && (code & 1) == 0) {
        in.readVLong();
      }
    }
  }

  @Override
  public TermData getNoOutput() {
    return NO_OUTPUT;
  }

  @Override
  public String outputToString(TermData data) {
    return data.toString();
  }

  static boolean statsEqual(final TermData t1, final TermData t2) {
    return t1.docFreq == t2.docFreq && t1.totalTermFreq == t2.totalTermFreq;
  }

  static boolean bytesEqual(final TermData t1, final TermData t2) {
    if (t1.bytes == null && t2.bytes == null) {
      return true;
    }
    return t1.bytes != null && t2.bytes != null && Arrays.equals(t1.bytes, t2.bytes);
  }
}
