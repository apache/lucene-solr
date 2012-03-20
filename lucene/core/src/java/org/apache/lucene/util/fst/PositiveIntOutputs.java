package org.apache.lucene.util.fst;

/**
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

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

/**
 * An FST {@link Outputs} implementation where each output
 * is a non-negative long value.
 *
 * @lucene.experimental
 */

public final class PositiveIntOutputs extends Outputs<Long> {
  
  private final static Long NO_OUTPUT = new Long(0);

  private final boolean doShare;

  private final static PositiveIntOutputs singletonShare = new PositiveIntOutputs(true);
  private final static PositiveIntOutputs singletonNoShare = new PositiveIntOutputs(false);

  private PositiveIntOutputs(boolean doShare) {
    this.doShare = doShare;
  }

  public static PositiveIntOutputs getSingleton(boolean doShare) {
    return doShare ? singletonShare : singletonNoShare;
  }

  @Override
  public Long common(Long output1, Long output2) {
    assert valid(output1);
    assert valid(output2);
    if (output1 == NO_OUTPUT || output2 == NO_OUTPUT) {
      return NO_OUTPUT;
    } else if (doShare) {
      assert output1 > 0;
      assert output2 > 0;
      return Math.min(output1, output2);
    } else if (output1.equals(output2)) {
      return output1;
    } else {
      return NO_OUTPUT;
    }
  }

  @Override
  public Long subtract(Long output, Long inc) {
    assert valid(output);
    assert valid(inc);
    assert output >= inc;

    if (inc == NO_OUTPUT) {
      return output;
    } else if (output.equals(inc)) {
      return NO_OUTPUT;
    } else {
      return output - inc;
    }
  }

  @Override
  public Long add(Long prefix, Long output) {
    assert valid(prefix);
    assert valid(output);
    if (prefix == NO_OUTPUT) {
      return output;
    } else if (output == NO_OUTPUT) {
      return prefix;
    } else {
      return prefix + output;
    }
  }

  @Override
  public void write(Long output, DataOutput out) throws IOException {
    assert valid(output);
    out.writeVLong(output);
  }

  @Override
  public Long read(DataInput in) throws IOException {
    long v = in.readVLong();
    if (v == 0) {
      return NO_OUTPUT;
    } else {
      return v;
    }
  }

  private boolean valid(Long o) {
    assert o != null;
    assert o instanceof Long;
    assert o == NO_OUTPUT || o > 0;
    return true;
  }

  @Override
  public Long getNoOutput() {
    return NO_OUTPUT;
  }

  @Override
  public String outputToString(Long output) {
    return output.toString();
  }

  @Override
  public String toString() {
    return "PositiveIntOutputs(doShare=" + doShare + ")";
  }
}
