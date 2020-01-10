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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.BoolDocValues;
import org.apache.lucene.search.IndexSearcher;

/**
 * Abstract {@link ValueSource} implementation which wraps multiple ValueSources
 * and applies an extendible boolean function to their values.
 **/
public abstract class MultiBoolFunction extends BoolFunction {
  protected final List<ValueSource> sources;

  public MultiBoolFunction(List<ValueSource> sources) {
    this.sources = sources;
  }

  protected abstract String name();

  protected abstract boolean func(int doc, FunctionValues[] vals) throws IOException;

  @Override
  public BoolDocValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues[] vals =  new FunctionValues[sources.size()];
    int i=0;
    for (ValueSource source : sources) {
      vals[i++] = source.getValues(context, readerContext);
    }

    return new BoolDocValues(this) {
      @Override
      public boolean boolVal(int doc) throws IOException {
        return func(doc, vals);
      }

      @Override
      public String toString(int doc) throws IOException {
        StringBuilder sb = new StringBuilder(name());
        sb.append('(');
        boolean first = true;
        for (FunctionValues dv : vals) {
          if (first) {
            first = false;
          } else {
            sb.append(',');
          }
          sb.append(dv.toString(doc));
        }
        sb.append(')');
        return sb.toString();
      }
    };
  }

  @Override
  public String description() {
    StringBuilder sb = new StringBuilder(name());
    sb.append('(');
    boolean first = true;
    for (ValueSource source : sources) {
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      sb.append(source.description());
    }
    sb.append(')');
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return sources.hashCode() + name().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    MultiBoolFunction other = (MultiBoolFunction)o;
    return this.sources.equals(other.sources);
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    for (ValueSource source : sources) {
      source.createWeight(context, searcher);
    }
  }
}
