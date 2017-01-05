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
package org.apache.solr.analytics.util.valuesource;

import java.io.IOException;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.analytics.util.AnalyticsParams;

/**
 * <code>MultiplyDoubleFunction</code> returns the product of its components.
 */
public class MultiplyDoubleFunction extends MultiDoubleFunction {
  public final static String NAME = AnalyticsParams.MULTIPLY;

  public MultiplyDoubleFunction(ValueSource[] sources) {
    super(sources);
  }

  @Override
  protected String name() {
    return NAME;
  }

  @Override
  protected double func(int doc, FunctionValues[] valsArr) throws IOException {
    double product = 1d;
    for (FunctionValues val : valsArr) {
      product *= val.doubleVal(doc);
    }
    return product;
  }

}
