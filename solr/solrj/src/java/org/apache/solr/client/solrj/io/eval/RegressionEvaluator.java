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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class RegressionEvaluator extends RecursiveNumericEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public RegressionEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
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
    
    @SuppressWarnings({"unchecked"})
    List<Number> l1 = (List<Number>)first;
    @SuppressWarnings({"unchecked"})
    List<Number> l2 = (List<Number>)second;
    
    if(l2.size() < l1.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - first list (%d) has more values than the second list (%d)",toExpression(constructingFactory), l1.size(), l2.size()));      
    }

    SimpleRegression regression = new SimpleRegression();
    for(int idx = 0; idx < l1.size(); ++idx){
      regression.addData(l1.get(idx).doubleValue(), l2.get(idx).doubleValue());
    }
    
    Map<String, Object> map = new HashMap<>();
    map.put("slope", regression.getSlope());
    map.put("intercept", regression.getIntercept());
    map.put("R", regression.getR());
    map.put("N", regression.getN());
    map.put("RSquared", regression.getRSquare());
    map.put("regressionSumSquares", regression.getRegressionSumSquares());
    map.put("slopeConfidenceInterval", regression.getSlopeConfidenceInterval());
    map.put("interceptStdErr", regression.getInterceptStdErr());
    map.put("totalSumSquares", regression.getTotalSumSquares());
    map.put("significance", regression.getSignificance());
    map.put("meanSquareError", regression.getMeanSquareError());

    return new RegressionTuple(regression, map);
  }
  
  public static class RegressionTuple extends Tuple {
    private SimpleRegression simpleRegression;

    public RegressionTuple(SimpleRegression simpleRegression, Map<?,?> map) {
      super(map);
      this.simpleRegression = simpleRegression;
    }

    public double predict(double value) {
      return this.simpleRegression.predict(value);
    }
  }
}
