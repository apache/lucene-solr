package org.apache.lucene.queries.function.valuesource;

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


import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A {@link ValueSource} implementation which concatenates existing string
 * values from the provided ValueSources into one string.
 */
public class ConcatenateFunction extends MultiFunction {

  public ConcatenateFunction(List<ValueSource> sources) {
    super(sources);
  }

  @Override
  protected String name() {
    return "concat";
  }

  @Override
  public FunctionValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    return new Values(valsArr(sources, context, readerContext)) {

      @Override
      public String strVal(int doc) {
        StringBuilder stringBuilder = new StringBuilder();
        for (FunctionValues functionValues : valsArr) {
          if (functionValues.exists(doc)) {
            stringBuilder.append(functionValues.strVal(doc));
          }
        }
        return stringBuilder.toString();
      }

      @Override
      public Object objectVal(int doc) {
        return strVal(doc);
      }

      @Override
      public boolean exists(int doc) {
        for (FunctionValues vals : valsArr) {
          if (vals.exists(doc)) {
            return true;
          }
        }
        return false;
      }
    };
  }
}
