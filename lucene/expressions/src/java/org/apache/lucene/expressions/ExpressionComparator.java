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
package org.apache.lucene.expressions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorer;

/** A custom comparator for sorting documents by an expression */
class ExpressionComparator extends FieldComparator<Double> implements LeafFieldComparator {
  private final double[] values;
  private double bottom;
  private double topValue;
  
  private ValueSource source;
  private FunctionValues scores;
  private LeafReaderContext readerContext;
  
  public ExpressionComparator(ValueSource source, int numHits) {
    values = new double[numHits];
    this.source = source;
  }
  
  // TODO: change FieldComparator.setScorer to throw IOException and remove this try-catch
  @Override
  public void setScorer(Scorer scorer) {
    // TODO: might be cleaner to lazy-init 'source' and set scorer after?
    assert readerContext != null;
    try {
      Map<String,Object> context = new HashMap<>();
      assert scorer != null;
      context.put("scorer", scorer);
      scores = source.getValues(context, readerContext);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public int compare(int slot1, int slot2) {
    return Double.compare(values[slot1], values[slot2]);
  }
  
  @Override
  public void setBottom(int slot) {
    bottom = values[slot];
  }
  
  @Override
  public void setTopValue(Double value) {
    topValue = value.doubleValue();
  }
  
  @Override
  public int compareBottom(int doc) throws IOException {
    return Double.compare(bottom, scores.doubleVal(doc));
  }
  
  @Override
  public void copy(int slot, int doc) throws IOException {
    values[slot] = scores.doubleVal(doc);
  }
  
  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    this.readerContext = context;
    return this;
  }
  
  @Override
  public Double value(int slot) {
    return Double.valueOf(values[slot]);
  }
  
  @Override
  public int compareTop(int doc) throws IOException {
    return Double.compare(topValue, scores.doubleVal(doc));
  }
}
