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
package org.apache.solr.search.function;

import java.io.IOException;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

/**
 * <code>ConcatStringFunction</code> concatenates the string values of its 
 * components in the order given.
 */
public class ConcatStringFunction extends MultiStringFunction {
  public final static String NAME = "concat";

  public ConcatStringFunction(ValueSource[] sources) {
    super(sources);
  }

  protected String name() {
    return NAME;
  }

  @Override
  protected String func(int doc, FunctionValues[] valsArr) throws IOException {
    StringBuilder sb = new StringBuilder();
    for (FunctionValues val : valsArr) {
      String v = val.strVal(doc);
      if(v == null){
        return null;
      } else {
        sb.append(v);
      }
    }
    return sb.toString();
  }

}
