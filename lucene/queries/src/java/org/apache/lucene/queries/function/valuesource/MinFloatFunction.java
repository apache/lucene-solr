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

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

/**
 * <code>MinFloatFunction</code> returns the min of it's components.
 */
public class MinFloatFunction extends MultiFloatFunction {
  public MinFloatFunction(ValueSource[] sources) {
    super(sources);
  }

  @Override  
  protected String name() {
    return "min";
  }

  @Override
  protected float func(int doc, FunctionValues[] valsArr) {
    boolean first = true;
    float val = 0.0f;
    for (FunctionValues vals : valsArr) {
      if (first) {
        first = false;
        val = vals.floatVal(doc);
      } else {
        val = Math.min(vals.floatVal(doc),val);
      }
    }
    return val;
  }
}
