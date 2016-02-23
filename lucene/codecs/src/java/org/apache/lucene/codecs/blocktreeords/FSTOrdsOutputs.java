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
package org.apache.lucene.codecs.blocktreeords;


import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.fst.Outputs;

/** A custom FST outputs implementation that stores block data
 *  (BytesRef), long ordStart, long numTerms. */

final class FSTOrdsOutputs extends Outputs<FSTOrdsOutputs.Output> {

  public static final Output NO_OUTPUT = new Output(new BytesRef(), 0, 0);

  private static final BytesRef NO_BYTES = new BytesRef();

  public static final class Output {
    public final BytesRef bytes;
    // Inclusive:
    public final long startOrd;
    // Inclusive:
    public final long endOrd;

    public Output(BytesRef bytes, long startOrd, long endOrd) {
      assert startOrd >= 0: "startOrd=" + startOrd;
      assert endOrd >= 0: "endOrd=" + endOrd;
      this.bytes = bytes;
      this.startOrd = startOrd;
      this.endOrd = endOrd;
    }

    @Override
    public String toString() {
      long x;
      if (endOrd > Long.MAX_VALUE/2) {
        x = Long.MAX_VALUE-endOrd;
      } else {
        assert endOrd >= 0;
        x = -endOrd;
      }
      return startOrd + " to " + x;
    }

    @Override
    public int hashCode() {
      int hash = bytes.hashCode();
      hash = (int) (hash ^ startOrd);
      hash = (int) (hash ^ endOrd);
      return hash;
    }

    @Override
    public boolean equals(Object _other) {
      if (_other instanceof Output) {
        Output other = (Output) _other;
        return bytes.equals(other.bytes) && startOrd == other.startOrd && endOrd == other.endOrd;
      } else {
        return false;
      }
    }
  }

  @Override
  public Output common(Output output1, Output output2) {
    BytesRef bytes1 = output1.bytes;
    BytesRef bytes2 = output2.bytes;

    assert bytes1 != null;
    assert bytes2 != null;

    int pos1 = bytes1.offset;
    int pos2 = bytes2.offset;
    int stopAt1 = pos1 + Math.min(bytes1.length, bytes2.length);
    while(pos1 < stopAt1) {
      if (bytes1.bytes[pos1] != bytes2.bytes[pos2]) {
        break;
      }
      pos1++;
      pos2++;
    }

    BytesRef prefixBytes;

    if (pos1 == bytes1.offset) {
      // no common prefix
      prefixBytes = NO_BYTES;
    } else if (pos1 == bytes1.offset + bytes1.length) {
      // bytes1 is a prefix of bytes2
      prefixBytes = bytes1;
    } else if (pos2 == bytes2.offset + bytes2.length) {
      // bytes2 is a prefix of bytes1
      prefixBytes = bytes2;
    } else {
      prefixBytes = new BytesRef(bytes1.bytes, bytes1.offset, pos1-bytes1.offset);
    }

    return newOutput(prefixBytes,
                     Math.min(output1.startOrd, output2.startOrd),
                     Math.min(output1.endOrd, output2.endOrd));
  }

  @Override
  public Output subtract(Output output, Output inc) {
    assert output != null;
    assert inc != null;
    if (inc == NO_OUTPUT) {
      // no prefix removed
      return output;
    } else {
      assert StringHelper.startsWith(output.bytes, inc.bytes);
      BytesRef suffix;
      if (inc.bytes.length == output.bytes.length) {
        // entire output removed
        suffix = NO_BYTES;
      } else if (inc.bytes.length == 0) {
        suffix = output.bytes;
      } else {
        assert inc.bytes.length < output.bytes.length: "inc.length=" + inc.bytes.length + " vs output.length=" + output.bytes.length;
        assert inc.bytes.length > 0;
        suffix = new BytesRef(output.bytes.bytes, output.bytes.offset + inc.bytes.length, output.bytes.length-inc.bytes.length);
      }
      assert output.startOrd >= inc.startOrd;
      assert output.endOrd >= inc.endOrd;
      return newOutput(suffix, output.startOrd-inc.startOrd, output.endOrd - inc.endOrd);
    }
  }

  @Override
  public Output add(Output prefix, Output output) {
    assert prefix != null;
    assert output != null;
    if (prefix == NO_OUTPUT) {
      return output;
    } else if (output == NO_OUTPUT) {
      return prefix;
    } else {
      BytesRef bytes = new BytesRef(prefix.bytes.length + output.bytes.length);
      System.arraycopy(prefix.bytes.bytes, prefix.bytes.offset, bytes.bytes, 0, prefix.bytes.length);
      System.arraycopy(output.bytes.bytes, output.bytes.offset, bytes.bytes, prefix.bytes.length, output.bytes.length);
      bytes.length = prefix.bytes.length + output.bytes.length;
      return newOutput(bytes, prefix.startOrd + output.startOrd, prefix.endOrd + output.endOrd);
    }
  }

  @Override
  public void write(Output prefix, DataOutput out) throws IOException {
    out.writeVInt(prefix.bytes.length);
    out.writeBytes(prefix.bytes.bytes, prefix.bytes.offset, prefix.bytes.length);
    out.writeVLong(prefix.startOrd);
    out.writeVLong(prefix.endOrd);
  }

  @Override
  public Output read(DataInput in) throws IOException {
    int len = in.readVInt();
    BytesRef bytes;
    if (len == 0) {
      bytes = NO_BYTES;
    } else {
      bytes = new BytesRef(len);
      in.readBytes(bytes.bytes, 0, len);
      bytes.length = len;
    }

    long startOrd = in.readVLong();
    long endOrd = in.readVLong();

    Output result = newOutput(bytes, startOrd, endOrd);

    return result;
  }

  @Override
  public void skipOutput(DataInput in) throws IOException {
    int len = in.readVInt();
    in.skipBytes(len);
    in.readVLong();
    in.readVLong();
  }

  @Override
  public void skipFinalOutput(DataInput in) throws IOException {
    skipOutput(in);
  }

  @Override
  public Output getNoOutput() {
    return NO_OUTPUT;
  }

  @Override
  public String outputToString(Output output) {
    if ((output.endOrd == 0 || output.endOrd == Long.MAX_VALUE) && output.startOrd == 0) {
      return "";
    } else {
      return output.toString();
    }
  }

  public Output newOutput(BytesRef bytes, long startOrd, long endOrd) {
    if (bytes.length == 0 && startOrd == 0 && endOrd == 0) {
      return NO_OUTPUT;
    } else {
      return new Output(bytes, startOrd, endOrd);
    }
  }

  @Override
  public long ramBytesUsed(Output output) {
    return 2 * RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 2 * Long.BYTES + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 2 * Integer.BYTES + output.bytes.length;
  }
}
