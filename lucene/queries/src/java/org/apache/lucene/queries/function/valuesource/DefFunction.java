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
package org.apache.lucene.queries.function.valuesource;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRefBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * {@link ValueSource} implementation which only returns the values from the provided
 * ValueSources which are available for a particular docId.  Consequently, when combined
 * with a {@link ConstValueSource}, this function serves as a way to return a default
 * value when the values for a field are unavailable.
 */
public class DefFunction extends MultiFunction {
  public DefFunction(List<ValueSource> sources) {
    super(sources);
  }

  @Override
  protected String name() {
    return "def";
  }


  @Override
  public FunctionValues getValues(Map fcontext, LeafReaderContext readerContext) throws IOException {


    return new Values(valsArr(sources, fcontext, readerContext)) {
      final int upto = valsArr.length - 1;

      private FunctionValues get(int doc) throws IOException {
        for (int i=0; i<upto; i++) {
          FunctionValues vals = valsArr[i];
          if (vals.exists(doc)) {
            return vals;
          }
        }
        return valsArr[upto];
      }

      @Override
      public byte byteVal(int doc) throws IOException {
        return get(doc).byteVal(doc);
      }

      @Override
      public short shortVal(int doc) throws IOException {
        return get(doc).shortVal(doc);
      }

      @Override
      public float floatVal(int doc) throws IOException {
        return get(doc).floatVal(doc);
      }

      @Override
      public int intVal(int doc) throws IOException {
        return get(doc).intVal(doc);
      }

      @Override
      public long longVal(int doc) throws IOException {
        return get(doc).longVal(doc);
      }

      @Override
      public double doubleVal(int doc) throws IOException {
        return get(doc).doubleVal(doc);
      }

      @Override
      public String strVal(int doc) throws IOException {
        return get(doc).strVal(doc);
      }

      @Override
      public boolean boolVal(int doc) throws IOException {
        return get(doc).boolVal(doc);
      }

      @Override
      public boolean bytesVal(int doc, BytesRefBuilder target) throws IOException {
        return get(doc).bytesVal(doc, target);
      }

      @Override
      public Object objectVal(int doc) throws IOException {
        return get(doc).objectVal(doc);
      }

      @Override
      public boolean exists(int doc) throws IOException {
        // return true if any source is exists?
        for (FunctionValues vals : valsArr) {
          if (vals.exists(doc)) {
            return true;
          }
        }
        return false;
      }

      @Override
      public ValueFiller getValueFiller() {
        // TODO: need ValueSource.type() to determine correct type
        return super.getValueFiller();
      }
    };
  }
}
