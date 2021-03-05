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
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.BoolDocValues;
import org.apache.lucene.search.IndexSearcher;

/** Base class for comparison operators useful within an "if"/conditional. */
public abstract class ComparisonBoolFunction extends BoolFunction {

  private final ValueSource lhs;
  private final ValueSource rhs;
  private final String name;

  public ComparisonBoolFunction(ValueSource lhs, ValueSource rhs, String name) {
    this.lhs = lhs;
    this.rhs = rhs;
    this.name = name;
  }

  /** Perform the comparison, returning true or false */
  public abstract boolean compare(int doc, FunctionValues lhs, FunctionValues rhs)
      throws IOException;

  /** Uniquely identify the operation (ie "gt", "lt" "gte", etc) */
  public String name() {
    return this.name;
  }

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {
    final FunctionValues lhsVal = this.lhs.getValues(context, readerContext);
    final FunctionValues rhsVal = this.rhs.getValues(context, readerContext);
    final String compLabel = this.name();

    return new BoolDocValues(this) {
      @Override
      public boolean boolVal(int doc) throws IOException {
        return compare(doc, lhsVal, rhsVal);
      }

      @Override
      public String toString(int doc) throws IOException {
        return compLabel + "(" + lhsVal.toString(doc) + "," + rhsVal.toString(doc) + ")";
      }

      @Override
      public boolean exists(int doc) throws IOException {
        return lhsVal.exists(doc) && rhsVal.exists(doc);
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this.getClass() != o.getClass()) return false;
    ComparisonBoolFunction other = (ComparisonBoolFunction) o;
    return name().equals(other.name()) && lhs.equals(other.lhs) && rhs.equals(other.rhs);
  }

  @Override
  public int hashCode() {
    int h = this.getClass().hashCode();
    h = h * 31 + this.name().hashCode();
    h = h * 31 + lhs.hashCode();
    h = h * 31 + rhs.hashCode();
    return h;
  }

  @Override
  public String description() {
    return name() + "(" + lhs.description() + "," + rhs.description() + ")";
  }

  @Override
  public void createWeight(Map<Object, Object> context, IndexSearcher searcher) throws IOException {
    lhs.createWeight(context, searcher);
    rhs.createWeight(context, searcher);
  }
}
