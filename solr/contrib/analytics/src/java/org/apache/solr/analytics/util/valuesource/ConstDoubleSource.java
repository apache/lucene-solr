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
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.ConstNumberSource;
import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.solr.analytics.util.AnalyticsParams;

/**
 * <code>ConstDoubleSource</code> returns a constant double for all documents
 */
public class ConstDoubleSource extends ConstNumberSource {
  public final static String NAME = AnalyticsParams.CONSTANT_NUMBER;
  final double constant;

  public ConstDoubleSource(double constant) {
    this.constant = constant;
  }

  @Override
  public String description() {
    return name()+"(" + getFloat() + ")";
  }

  protected String name() {
    return NAME;
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    return new DoubleDocValues(this) {
      @Override
      public double doubleVal(int doc) {
        return constant;
      }
      @Override
      public boolean exists(int doc) {
        return true;
      }
    };
  }

  @Override
  public int hashCode() {
    return (int)Double.doubleToLongBits(constant) * 31;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ConstValueSource)) return false;
    ConstDoubleSource other = (ConstDoubleSource)o;
    return  this.constant == other.constant;
  }

  @Override
  public int getInt() {
    return (int)constant;
  }

  @Override
  public long getLong() {
    return (long)constant;
  }

  @Override
  public float getFloat() {
    return (float)constant;
  }

  @Override
  public double getDouble() {
    return constant;
  }

  @Override
  public Number getNumber() {
    return new Double(constant);
  }

  @Override
  public boolean getBool() {
    return constant != 0.0f;
  }

}
