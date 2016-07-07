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

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.BoolDocValues;
import org.apache.lucene.search.IndexSearcher;


/**
 * Base class for comparison operators used within if statements
 * To Solr's if function query a 0 is considered "false", all other values are "true"
 */
public abstract class CompareNumericFunction extends BoolFunction {

  private final ValueSource lhs;
  private final ValueSource rhs;
  private final String label;

  public CompareNumericFunction(ValueSource lhs, ValueSource rhs, String label) {
    this.lhs = lhs;
    this.rhs = rhs;
    this.label = label;
  }

  // Perform the comparison, returning true or false
  public abstract boolean compare(double lhs, double rhs);

  // Uniquely identify the operation (ie "gt", "lt" "gte, etc)
  public String getLabel() {
    return this.label;
  }

  // string comparison? Probably should be a seperate function
  // public abstract boolean compareString(String lhs, String rhs);

  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues lhsVal = this.lhs.getValues(context, readerContext);
    final FunctionValues rhsVal = this.rhs.getValues(context, readerContext);
    final String compLabel = this.getLabel();

    return new BoolDocValues(this) {
      @Override
      public boolean boolVal(int doc) {
        return compare(lhsVal.floatVal(doc), rhsVal.floatVal(doc));
      }

      @Override
      public String toString(int doc) {
        return compLabel + "(" + lhsVal.toString(doc) + "," + rhsVal.toString(doc) + ")";
      }
    };


  };

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CompareNumericFunction)) return false;
    CompareNumericFunction other = (CompareNumericFunction)o;
    return getLabel().equals(other.getLabel())
        && lhs.equals(other.lhs)
        && rhs.equals(other.rhs);  }

  @Override
  public int hashCode() {
    int h = getLabel().hashCode();
    h = h * 31 + lhs.hashCode();
    h = h * 31 + rhs.hashCode();
    return h;
  }

  @Override
  public String description() {
      return getLabel() + "(" + lhs.description() + "," + rhs.description() + ")";
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    lhs.createWeight(context, searcher);
    rhs.createWeight(context, searcher);
  }

}
