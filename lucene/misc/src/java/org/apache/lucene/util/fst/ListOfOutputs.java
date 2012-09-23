package org.apache.lucene.util.fst;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.IntsRef; // javadocs

/**
 * Wraps another Outputs implementation and encodes one or
 * more of its output values.  You can use this when a single
 * input may need to map to more than one output,
 * maintaining order: pass the same input with a different
 * output by calling {@link Builder#add(IntsRef,Object)} multiple
 * times.  The builder will then combine the outputs using
 * the {@link Outputs#merge(Object,Object)} method.
 *
 * <p>The resulting FST may not be minimal when an input has
 * more than one output, as this requires pushing all
 * multi-output values to a final state.
 *
 * <p>NOTE: this cannot wrap itself (ie you cannot make an
 * FST with List&lt;List&lt;Object&gt;&gt; outputs using this).
 *
 * @lucene.experimental
 */


// NOTE: i think we could get a more compact FST if, instead
// of adding the same input multiple times with a different
// output each time, we added it only once with a
// pre-constructed List<T> output.  This way the "multiple
// values" is fully opaque to the Builder/FST.  It would
// require implementing the full algebra using set
// arithmetic (I think?); maybe SetOfOutputs is a good name.

@SuppressWarnings("unchecked")
public final class ListOfOutputs<T> extends Outputs<Object> {
  
  private final Outputs<T> outputs;

  public ListOfOutputs(Outputs<T> outputs) {
    this.outputs = outputs;
  }

  @Override
  public Object common(Object output1, Object output2) {
    // These will never be a list:
    return outputs.common((T) output1, (T) output2);
  }

  @Override
  public Object subtract(Object object, Object inc) {
    // These will never be a list:
    return outputs.subtract((T) object, (T) inc);
  }

  @Override
  public Object add(Object prefix, Object output) {
    assert !(prefix instanceof List);
    if (!(output instanceof List)) {
      return outputs.add((T) prefix, (T) output);
    } else {
      List<T> outputList = (List<T>) output;
      List<T> addedList = new ArrayList<T>(outputList.size());
      for(T _output : outputList) {
        addedList.add(outputs.add((T) prefix, _output));
      }
      return addedList;
    }
  }

  @Override
  public void write(Object output, DataOutput out) throws IOException {
    assert !(output instanceof List);
    outputs.write((T) output, out);
  }

  @Override
  public void writeFinalOutput(Object output, DataOutput out) throws IOException {
    if (!(output instanceof List)) {
      out.writeVInt(1);
      outputs.write((T) output, out);
    } else {
      List<T> outputList = (List<T>) output;
      out.writeVInt(outputList.size());
      for(T eachOutput : outputList) {
        outputs.write(eachOutput, out);
      }
    }
  }

  @Override
  public Object read(DataInput in) throws IOException {
    return outputs.read(in);
  }

  @Override
  public Object readFinalOutput(DataInput in) throws IOException {
    int count = in.readVInt();
    if (count == 1) {
      return outputs.read(in);
    } else {
      List<T> outputList = new ArrayList<T>(count);
      for(int i=0;i<count;i++) {
        outputList.add(outputs.read(in));
      }
      return outputList;
    }
  }

  @Override
  public Object getNoOutput() {
    return outputs.getNoOutput();
  }

  @Override
  public String outputToString(Object output) {
    if (!(output instanceof List)) {
      return outputs.outputToString((T) output);
    } else {
      List<T> outputList = (List<T>) output;

      StringBuilder b = new StringBuilder();
      b.append('[');
      
      for(int i=0;i<outputList.size();i++) {
        if (i > 0) {
          b.append(", ");
        }
        b.append(outputs.outputToString(outputList.get(i)));
      }
      b.append(']');
      return b.toString();
    }
  }

  @Override
  public Object merge(Object first, Object second) {
    List<T> outputList = new ArrayList<T>();
    if (!(first instanceof List)) {
      outputList.add((T) first);
    } else {
      outputList.addAll((List<T>) first);
    }
    if (!(second instanceof List)) {
      outputList.add((T) second);
    } else {
      outputList.addAll((List<T>) second);
    }
    //System.out.println("MERGE: now " + outputList.size() + " first=" + outputToString(first) + " second=" + outputToString(second));
    //System.out.println("  return " + outputToString(outputList));
    return outputList;
  }

  @Override
  public String toString() {
    return "OneOrMoreOutputs(" + outputs + ")";
  }

  public List<T> asList(Object output) { 
    if (!(output instanceof List)) {
      List<T> result = new ArrayList<T>(1);
      result.add((T) output);
      return result;
    } else {
      return (List<T>) output;
    }
  }
}
