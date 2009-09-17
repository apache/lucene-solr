/**
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Searcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * <code>SumFloatFunction</code> returns the sum of it's components.
 */
public class SumFloatFunction extends MultiFloatFunction {
  public SumFloatFunction(ValueSource[] sources) {
    super(sources);
  }

  @Override  
  protected String name() {
    return "sum";
  }

  protected float func(int doc, DocValues[] valsArr) {
    float val = 0.0f;
    for (DocValues vals : valsArr) {
      val += vals.floatVal(doc);
    }
    return val;
  }
}

// a simple function of multiple sources
abstract class MultiFloatFunction extends ValueSource {
  protected final ValueSource[] sources;

  public MultiFloatFunction(ValueSource[] sources) {
    this.sources = sources;
  }

  abstract protected String name();
  abstract protected float func(int doc, DocValues[] valsArr);

  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append(name()+'(');
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

  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final DocValues[] valsArr = new DocValues[sources.length];
    for (int i=0; i<sources.length; i++) {
      valsArr[i] = sources[i].getValues(context, reader);
    }

    return new DocValues() {
      public float floatVal(int doc) {
        return func(doc, valsArr);
      }
      public int intVal(int doc) {
        return (int)floatVal(doc);
      }
      public long longVal(int doc) {
        return (long)floatVal(doc);
      }
      public double doubleVal(int doc) {
        return (double)floatVal(doc);
      }
      public String strVal(int doc) {
        return Float.toString(floatVal(doc));
      }
      public String toString(int doc) {
        StringBuilder sb = new StringBuilder();
        sb.append(name()+'(');
        boolean firstTime=true;
        for (DocValues vals : valsArr) {
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
  public void createWeight(Map context, Searcher searcher) throws IOException {
    for (ValueSource source : sources)
      source.createWeight(context, searcher);
  }

  public int hashCode() {
    return Arrays.hashCode(sources) + name().hashCode();
  }

  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    MultiFloatFunction other = (MultiFloatFunction)o;
    return this.name().equals(other.name())
            && Arrays.equals(this.sources, other.sources);
  }
}

