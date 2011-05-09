package org.apache.lucene.util.automaton.fst;

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
 * Pairs up two outputs into one.
 * @lucene.experimental
 */


public class PairOutputs<A,B> extends Outputs<PairOutputs.Pair<A,B>> {

  private final Pair<A,B> NO_OUTPUT;
  private final Outputs<A> outputs1;
  private final Outputs<B> outputs2;

  public static class Pair<A,B> {
    public final A output1;
    public final B output2;

    public Pair(A output1, B output2) {
      this.output1 = output1;
      this.output2 = output2;
    }

    @Override @SuppressWarnings("rawtypes")
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Pair) {
        Pair pair = (Pair) other;
        return output1.equals(pair.output1) && output2.equals(pair.output2);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return output1.hashCode() + output2.hashCode();
    }
  };

  public PairOutputs(Outputs<A> outputs1, Outputs<B> outputs2) {
    this.outputs1 = outputs1;
    this.outputs2 = outputs2;
    NO_OUTPUT = new Pair<A,B>(outputs1.getNoOutput(), outputs2.getNoOutput());
  }
  
  public Pair<A,B> get(A output1, B output2) {
    if (output1 == outputs1.getNoOutput() && output2 == outputs2.getNoOutput()) {
      return NO_OUTPUT;
    } else {
      return new Pair<A,B>(output1, output2);
    }
  }
 
  @Override
  public Pair<A,B> common(Pair<A,B> pair1, Pair<A,B> pair2) {
    return get(outputs1.common(pair1.output1, pair2.output1),
               outputs2.common(pair1.output2, pair2.output2));
  }

  @Override
  public Pair<A,B> subtract(Pair<A,B> output, Pair<A,B> inc) {
    return get(outputs1.subtract(output.output1, inc.output1),
               outputs2.subtract(output.output2, inc.output2));
  }

  @Override
  public Pair<A,B> add(Pair<A,B> prefix, Pair<A,B> output) {
    return get(outputs1.add(prefix.output1, output.output1),
               outputs2.add(prefix.output2, output.output2));
  }

  @Override
  public void write(Pair<A,B> output, DataOutput writer) throws IOException {
    outputs1.write(output.output1, writer);
    outputs2.write(output.output2, writer);
  }

  @Override
  public Pair<A,B> read(DataInput in) throws IOException {
    A output1 = outputs1.read(in);
    B output2 = outputs2.read(in);
    return get(output1, output2);
  }

  @Override
  public Pair<A,B> getNoOutput() {
    return NO_OUTPUT;
  }

  @Override
  public String outputToString(Pair<A,B> output) {
    return "<pair:" + outputs1.outputToString(output.output1) + "," + outputs2.outputToString(output.output2) + ">";
  }
}
