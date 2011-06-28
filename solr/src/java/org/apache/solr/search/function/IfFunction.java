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
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.queries.function.DocValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public class IfFunction extends BoolFunction {
  private ValueSource ifSource;
  private ValueSource trueSource;
  private ValueSource falseSource;


  public IfFunction(ValueSource ifSource, ValueSource trueSource, ValueSource falseSource) {
    this.ifSource = ifSource;
    this.trueSource = trueSource;
    this.falseSource = falseSource;
  }

  @Override
  public DocValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    final DocValues ifVals = ifSource.getValues(context, readerContext);
    final DocValues trueVals = trueSource.getValues(context, readerContext);
    final DocValues falseVals = falseSource.getValues(context, readerContext);

    return new DocValues() {
      @Override
      public byte byteVal(int doc) {
        return ifVals.boolVal(doc) ? trueVals.byteVal(doc) : falseVals.byteVal(doc);
      }

      @Override
      public short shortVal(int doc) {
        return ifVals.boolVal(doc) ? trueVals.shortVal(doc) : falseVals.shortVal(doc);
      }

      @Override
      public float floatVal(int doc) {
        return ifVals.boolVal(doc) ? trueVals.floatVal(doc) : falseVals.floatVal(doc);
      }

      @Override
      public int intVal(int doc) {
        return ifVals.boolVal(doc) ? trueVals.intVal(doc) : falseVals.intVal(doc);
      }

      @Override
      public long longVal(int doc) {
        return ifVals.boolVal(doc) ? trueVals.longVal(doc) : falseVals.longVal(doc);
      }

      @Override
      public double doubleVal(int doc) {
        return ifVals.boolVal(doc) ? trueVals.doubleVal(doc) : falseVals.doubleVal(doc);
      }

      @Override
      public String strVal(int doc) {
        return ifVals.boolVal(doc) ? trueVals.strVal(doc) : falseVals.strVal(doc);
      }

      @Override
      public boolean boolVal(int doc) {
        return ifVals.boolVal(doc) ? trueVals.boolVal(doc) : falseVals.boolVal(doc);
      }

      @Override
      public boolean bytesVal(int doc, BytesRef target) {
        return ifVals.boolVal(doc) ? trueVals.bytesVal(doc, target) : falseVals.bytesVal(doc, target);
      }

      @Override
      public Object objectVal(int doc) {
        return ifVals.boolVal(doc) ? trueVals.objectVal(doc) : falseVals.objectVal(doc);
      }

      @Override
      public boolean exists(int doc) {
        return true; // TODO: flow through to any sub-sources?
      }

      @Override
      public ValueFiller getValueFiller() {
        // TODO: we need types of trueSource / falseSource to handle this
        // for now, use float.
        return super.getValueFiller();
      }

      @Override
      public String toString(int doc) {
        return "if(" + ifVals.toString(doc) + ',' + trueVals.toString(doc) + ',' + falseVals.toString(doc) + ')';
      }
    };

  }

  @Override
  public String description() {
    return "if(" + ifSource.description() + ',' + trueSource.description() + ',' + falseSource + ')';
  }

  @Override
  public int hashCode() {
    int h = ifSource.hashCode();
    h = h * 31 + trueSource.hashCode();
    h = h * 31 + falseSource.hashCode();
    return h;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IfFunction)) return false;
    IfFunction other = (IfFunction)o;
    return ifSource.equals(other.ifSource)
        && trueSource.equals(other.trueSource)
        && falseSource.equals(other.falseSource);
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    ifSource.createWeight(context, searcher);
    trueSource.createWeight(context, searcher);
    falseSource.createWeight(context, searcher);
  }
}