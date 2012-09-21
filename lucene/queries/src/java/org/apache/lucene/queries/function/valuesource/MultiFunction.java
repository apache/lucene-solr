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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Abstract parent class for {@link ValueSource} implementations that wrap multiple
 * ValueSources and apply their own logic.
 */
public abstract class MultiFunction extends ValueSource {
  protected final List<ValueSource> sources;

  public MultiFunction(List<ValueSource> sources) {
    this.sources = sources;
  }

  abstract protected String name();

  @Override
  public String description() {
    return description(name(), sources);
  }

  public static String description(String name, List<ValueSource> sources) {
    StringBuilder sb = new StringBuilder();
    sb.append(name).append('(');
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

  public static FunctionValues[] valsArr(List<ValueSource> sources, Map fcontext, AtomicReaderContext readerContext) throws IOException {
    final FunctionValues[] valsArr = new FunctionValues[sources.size()];
    int i=0;
    for (ValueSource source : sources) {
      valsArr[i++] = source.getValues(fcontext, readerContext);
    }
    return valsArr;
  }

  public class Values extends FunctionValues {
    final FunctionValues[] valsArr;

    public Values(FunctionValues[] valsArr) {
      this.valsArr = valsArr;
    }

    @Override
    public String toString(int doc) {
      return MultiFunction.toString(name(), valsArr, doc);
    }

    @Override
    public ValueFiller getValueFiller() {
      // TODO: need ValueSource.type() to determine correct type
      return super.getValueFiller();
    }
  }


  public static String toString(String name, FunctionValues[] valsArr, int doc) {
    StringBuilder sb = new StringBuilder();
    sb.append(name).append('(');
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

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    for (ValueSource source : sources)
      source.createWeight(context, searcher);
  }

  @Override
  public int hashCode() {
    return sources.hashCode() + name().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    MultiFunction other = (MultiFunction)o;
    return this.sources.equals(other.sources);
  }
}

