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
import org.apache.lucene.search.IndexSearcher;

/**
 * Converts individual ValueSource instances to leverage the FunctionValues *Val functions that work
 * with multiple values, i.e. {@link
 * org.apache.lucene.queries.function.FunctionValues#doubleVal(int, double[])}
 */
// Not crazy about the name, but...
public class VectorValueSource extends MultiValueSource {
  protected final List<ValueSource> sources;

  public VectorValueSource(List<ValueSource> sources) {
    this.sources = sources;
  }

  public List<ValueSource> getSources() {
    return sources;
  }

  @Override
  public int dimension() {
    return sources.size();
  }

  public String name() {
    return "vector";
  }

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {
    int size = sources.size();

    // special-case x,y and lat,lon since it's so common
    if (size == 2) {
      final FunctionValues x = sources.get(0).getValues(context, readerContext);
      final FunctionValues y = sources.get(1).getValues(context, readerContext);
      return new FunctionValues() {
        @Override
        public void byteVal(int doc, byte[] vals) throws IOException {
          vals[0] = x.byteVal(doc);
          vals[1] = y.byteVal(doc);
        }

        @Override
        public void shortVal(int doc, short[] vals) throws IOException {
          vals[0] = x.shortVal(doc);
          vals[1] = y.shortVal(doc);
        }

        @Override
        public void intVal(int doc, int[] vals) throws IOException {
          vals[0] = x.intVal(doc);
          vals[1] = y.intVal(doc);
        }

        @Override
        public void longVal(int doc, long[] vals) throws IOException {
          vals[0] = x.longVal(doc);
          vals[1] = y.longVal(doc);
        }

        @Override
        public void floatVal(int doc, float[] vals) throws IOException {
          vals[0] = x.floatVal(doc);
          vals[1] = y.floatVal(doc);
        }

        @Override
        public void doubleVal(int doc, double[] vals) throws IOException {
          vals[0] = x.doubleVal(doc);
          vals[1] = y.doubleVal(doc);
        }

        @Override
        public void strVal(int doc, String[] vals) throws IOException {
          vals[0] = x.strVal(doc);
          vals[1] = y.strVal(doc);
        }

        @Override
        public String toString(int doc) throws IOException {
          return name() + "(" + x.toString(doc) + "," + y.toString(doc) + ")";
        }
      };
    }

    final FunctionValues[] valsArr = new FunctionValues[size];
    for (int i = 0; i < size; i++) {
      valsArr[i] = sources.get(i).getValues(context, readerContext);
    }

    return new FunctionValues() {
      @Override
      public void byteVal(int doc, byte[] vals) throws IOException {
        for (int i = 0; i < valsArr.length; i++) {
          vals[i] = valsArr[i].byteVal(doc);
        }
      }

      @Override
      public void shortVal(int doc, short[] vals) throws IOException {
        for (int i = 0; i < valsArr.length; i++) {
          vals[i] = valsArr[i].shortVal(doc);
        }
      }

      @Override
      public void floatVal(int doc, float[] vals) throws IOException {
        for (int i = 0; i < valsArr.length; i++) {
          vals[i] = valsArr[i].floatVal(doc);
        }
      }

      @Override
      public void intVal(int doc, int[] vals) throws IOException {
        for (int i = 0; i < valsArr.length; i++) {
          vals[i] = valsArr[i].intVal(doc);
        }
      }

      @Override
      public void longVal(int doc, long[] vals) throws IOException {
        for (int i = 0; i < valsArr.length; i++) {
          vals[i] = valsArr[i].longVal(doc);
        }
      }

      @Override
      public void doubleVal(int doc, double[] vals) throws IOException {
        for (int i = 0; i < valsArr.length; i++) {
          vals[i] = valsArr[i].doubleVal(doc);
        }
      }

      @Override
      public void strVal(int doc, String[] vals) throws IOException {
        for (int i = 0; i < valsArr.length; i++) {
          vals[i] = valsArr[i].strVal(doc);
        }
      }

      @Override
      public String toString(int doc) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(name()).append('(');
        boolean firstTime = true;
        for (FunctionValues vals : valsArr) {
          if (firstTime) {
            firstTime = false;
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
  public void createWeight(Map<Object, Object> context, IndexSearcher searcher) throws IOException {
    for (ValueSource source : sources) {
      source.createWeight(context, searcher);
    }
  }

  @Override
  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append(name()).append('(');
    boolean firstTime = true;
    for (ValueSource source : sources) {
      if (firstTime) {
        firstTime = false;
      } else {
        sb.append(',');
      }
      sb.append(source);
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof VectorValueSource)) return false;

    VectorValueSource that = (VectorValueSource) o;

    return sources.equals(that.sources);
  }

  @Override
  public int hashCode() {
    return sources.hashCode();
  }
}
