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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Collections;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class PairSortEvaluator extends RecursiveNumericEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;

  public PairSortEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public Object doWork(Object first, Object second) throws IOException{
    if(null == first){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the first value",toExpression(constructingFactory)));
    }
    if(null == second){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null found for the second value",toExpression(constructingFactory)));
    }
    if(!(first instanceof List<?>)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for the first value, expecting a list of numbers",toExpression(constructingFactory), first.getClass().getSimpleName()));
    }
    if(!(second instanceof List<?>)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for the second value, expecting a list of numbers",toExpression(constructingFactory), first.getClass().getSimpleName()));
    }

    List<Number> l1 = (List<Number>)first;
    List<Number> l2 = (List<Number>)second;

    if(l2.size() != l1.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - first list (%d) has a different size than the second list (%d)",toExpression(constructingFactory), l1.size(), l2.size()));
    }

    List<double[]> pairs = new ArrayList<>();
    for(int idx = 0; idx < l1.size(); ++idx){
      double[] pair = new double[2];
      pair[0]= l1.get(idx).doubleValue();
      pair[1] = l2.get(idx).doubleValue();
      pairs.add(pair);
    }

    Collections.sort(pairs, new PairComp());
    double[][] data = new double[2][pairs.size()];
    for(int i=0; i<pairs.size(); i++) {
      data[0][i] = pairs.get(i)[0];
      data[1][i] = pairs.get(i)[1];
    }

    return new Matrix(data);
  }

  private class PairComp implements Comparator<double[]> {
    public int compare(double[] a, double[] b) {
      if(a[0] > b[0]) {
        return 1;
      } else if(a[0] < b[0]) {
        return -1;
      } else {
        if(a[1] > b[1]) {
          return 1;
        } else if(a[1] < b[1]){
          return -1;
        } else {
          return 0;
        }
      }
    }
  }
}
