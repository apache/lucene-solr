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
package org.apache.solr.client.solrj.io.eval;

import java.io.IOException;

import java.util.Locale;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import java.util.List;
import java.util.ArrayList;
import java.util.TreeSet;

public class TopFeaturesEvaluator extends RecursiveObjectEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  public TopFeaturesEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(2 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly 2 values but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object value1, Object value2) throws IOException {

    int k = ((Number)value2).intValue();

    if(value1 instanceof Matrix) {

      Matrix matrix = (Matrix) value1;
      List<String> features = matrix.getColumnLabels();

      if(features == null) {
        throw new IOException("Matrix column labels cannot be null for topFeatures function.");
      }

      double[][] data = matrix.getData();
      List<List<String>> topFeatures = new ArrayList<>();

      for(int i=0; i<data.length; i++) {
        double[] row = data[i];
        List<String> featuresRow = new ArrayList<>();
        List<Integer> indexes = getMaxIndexes(row, k);
        for(int index : indexes) {
          featuresRow.add(features.get(index));
        }
        topFeatures.add(featuresRow);
      }

      return topFeatures;
    }  else {
      throw new IOException("The topFeatures function expects a matrix as the first parameter");
    }
  }

  private List<Integer> getMaxIndexes(double[] values, int k) {
    TreeSet<Pair> set = new TreeSet<>();
    for(int i=0; i<values.length; i++) {
      if(values[i] > 0){
        set.add(new Pair(i, values[i]));
        if (set.size() > k) {
          set.pollFirst();
        }
      }
    }

    List<Integer> top = new ArrayList<>(k);
    while(set.size() > 0) {
      top.add(set.pollLast().getIndex());
    }

    return top;
  }

  public static class Pair implements Comparable<Pair> {

    private Integer index;
    private Double value;

    public Pair(int _index, Number value) {
      this.index = _index;
      this.value = value.doubleValue();
    }

    public int compareTo(Pair pair) {

      int c = value.compareTo(pair.value);
      if(c==0) {
        return index.compareTo(pair.index);
      } else {
        return c;
      }
    }

    public int getIndex() {
      return this.index;
    }

    public Number getValue() {
      return value;
    }
  }
}
