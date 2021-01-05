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
package org.apache.lucene.queries.function.docvalues;

import java.io.IOException;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueStr;

/**
 * Abstract {@link FunctionValues} implementation which supports retrieving String values.
 * Implementations can control how the String values are loaded through {@link #strVal(int)}}
 */
public abstract class StrDocValues extends FunctionValues {
  protected final ValueSource vs;

  public StrDocValues(ValueSource vs) {
    this.vs = vs;
  }

  @Override
  public abstract String strVal(int doc) throws IOException;

  @Override
  public Object objectVal(int doc) throws IOException {
    return exists(doc) ? strVal(doc) : null;
  }

  @Override
  public boolean boolVal(int doc) throws IOException {
    return exists(doc);
  }

  @Override
  public String toString(int doc) throws IOException {
    return vs.description() + "='" + strVal(doc) + "'";
  }

  @Override
  public ValueFiller getValueFiller() {
    return new ValueFiller() {
      private final MutableValueStr mval = new MutableValueStr();

      @Override
      public MutableValue getValue() {
        return mval;
      }

      @Override
      public void fillValue(int doc) throws IOException {
        mval.exists = bytesVal(doc, mval.value);
      }
    };
  }
}
