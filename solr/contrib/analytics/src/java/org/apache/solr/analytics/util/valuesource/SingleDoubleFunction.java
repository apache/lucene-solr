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
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;

/**
 * Abstract {@link ValueSource} implementation which wraps one ValueSource
 * and applies an extendible double function to its values.
 */
public abstract class SingleDoubleFunction extends ValueSource {
  protected final ValueSource source;
  
  public SingleDoubleFunction(ValueSource source) {
    this.source = source;
  }

  @Override
  public String description() {
    return name()+"("+source.description()+")";
  }

  abstract String name();
  abstract double func(int doc, FunctionValues vals);

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues vals =  source.getValues(context, readerContext);
    return new DoubleDocValues(this) {
      @Override
      public double doubleVal(int doc) {
        return func(doc, vals);
      }
      
      @Override
      public boolean exists(int doc) {
        return vals.exists(doc);
      }

      @Override
      public String toString(int doc) {
        return name() + '(' + vals.toString(doc) + ')';
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (getClass() != o.getClass()) return false;
    SingleDoubleFunction other = (SingleDoubleFunction)o;
    return this.source.equals(other.source);
  }

  @Override
  public int hashCode() {
    return source.hashCode()+name().hashCode();
  }

}
