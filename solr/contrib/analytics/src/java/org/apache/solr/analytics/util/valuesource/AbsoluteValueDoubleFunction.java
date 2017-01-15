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
 * <code>AbsoluteValueDoubleFunction</code> takes the absolute value of the double value of the source it contains.
 */
public class AbsoluteValueDoubleFunction extends SingleDoubleFunction {
  public final static String NAME = AnalyticsParams.ABSOLUTE_VALUE;
  
  public AbsoluteValueDoubleFunction(ValueSource source) {
    super(source);
  }

  protected String name() {
    return NAME;
  }

  @Override
  public String description() {
    return name()+"("+source.description()+")";
  }

  protected double func(int doc, FunctionValues vals) throws IOException {
    double d = vals.doubleVal(doc);
    if (d<0) {
      return d*-1;
    } else {
      return d;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (getClass() != o.getClass()) return false;
    AbsoluteValueDoubleFunction other = (AbsoluteValueDoubleFunction)o;
    return this.source.equals(other.source);
  }

}
