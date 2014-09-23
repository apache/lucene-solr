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
import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.search.IndexSearcher;

/**
 * Abstract {@link ValueSource} implementation which wraps multiple ValueSources
 * and applies an extendible double function to their values.
 **/
public abstract class MultiDoubleFunction extends ValueSource {
  protected final ValueSource[] sources;

  public MultiDoubleFunction(ValueSource[] sources) {
    this.sources = sources;
  }

  abstract protected String name();
  abstract protected double func(int doc, FunctionValues[] valsArr);

  @Override
  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append(name()).append('(');
    boolean firstTime=true;
    for (ValueSource source : sources) {
      if (firstTime) {
        firstTime=false;
      } else {
        sb.append(',');
      }
      sb.append(source);
    }
    sb.append(')');
    return sb.toString();
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues[] valsArr = new FunctionValues[sources.length];
    for (int i=0; i<sources.length; i++) {
      valsArr[i] = sources[i].getValues(context, readerContext);
    }

    return new DoubleDocValues(this) {
      @Override
      public double doubleVal(int doc) {
        return func(doc, valsArr);
      }
      
      @Override
      public boolean exists(int doc) {
        boolean exists = true;
        for (FunctionValues val : valsArr) {
          exists = exists & val.exists(doc);
        }
        return exists;
      }
       
      @Override
      public String toString(int doc) {
        StringBuilder sb = new StringBuilder();
        sb.append(name()).append('(');
        boolean firstTime=true;
        for (FunctionValues vals : valsArr) {
          if (firstTime) {
            firstTime=false;
          } else {
            sb.append(',');
          }
          sb.append(vals.toString(doc));
        }
        sb.append(')');
        return sb.toString();
      }
    };
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    for (ValueSource source : sources)
      source.createWeight(context, searcher);
  }

  @Override
  public boolean equals(Object o) {
    if (getClass() != o.getClass()) return false;
    MultiDoubleFunction other = (MultiDoubleFunction)o;
    return this.name().equals(other.name())
            && Arrays.equals(this.sources, other.sources);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(sources) + name().hashCode();
  }
}
