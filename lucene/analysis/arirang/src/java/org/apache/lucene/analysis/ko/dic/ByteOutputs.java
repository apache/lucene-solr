package org.apache.lucene.analysis.ko.dic;

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
import org.apache.lucene.util.fst.Outputs;

/**
 * Output is a byte, for each input term.
 * doesn't share anything.
 *
 * @lucene.experimental
 */
final class ByteOutputs extends Outputs<Byte> {
  private final static Byte NO_OUTPUT = new Byte((byte)0);
  private final static ByteOutputs singleton = new ByteOutputs();

  public static ByteOutputs getSingleton() {
    return singleton;
  }

  @Override
  public Byte common(Byte output1, Byte output2) {
    assert valid(output1);
    assert valid(output2);
    return NO_OUTPUT;
  }

  @Override
  public Byte subtract(Byte output, Byte inc) {
    assert valid(output);
    assert valid(inc);
    assert inc == NO_OUTPUT;
    return output;
  }

  @Override
  public Byte add(Byte prefix, Byte output) {
    assert valid(prefix);
    assert valid(output);
    assert output == NO_OUTPUT || prefix == NO_OUTPUT;
    
    if (prefix == NO_OUTPUT) {
      return output;
    } else {
      return prefix;
    }
  }

  @Override
  public void write(Byte output, DataOutput out) throws IOException {
    assert valid(output);
    assert output != 0;
    out.writeByte(output);
  }

  @Override
  public Byte read(DataInput in) throws IOException {
    byte v = in.readByte();
    assert v != 0;
    return v;
  }

  private boolean valid(Byte o) {
    assert o != null;
    assert o instanceof Byte;
    assert o == NO_OUTPUT || o != 0;
    return true;
  }

  @Override
  public Byte getNoOutput() {
    return NO_OUTPUT;
  }

  @Override
  public String outputToString(Byte output) {
    return output.toString();
  }
}

