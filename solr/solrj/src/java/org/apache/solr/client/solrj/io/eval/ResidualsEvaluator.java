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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ResidualsEvaluator extends RecursiveObjectEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public ResidualsEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }

  @Override
  public Object doWork(Object ... values) throws IOException{
    if(3 != values.length){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - three values expected but found %d",toExpression(constructingFactory), values.length));
    }
    
    if(Arrays.stream(values).filter(value -> null == value).count() > 0){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - null value found",toExpression(constructingFactory)));
    }

    if(!(values[0] instanceof RegressionEvaluator.RegressionTuple)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for the first value, expecting a RegressionTuple",toExpression(constructingFactory), values[0].getClass().getSimpleName()));
    }
    if(!(values[1] instanceof List<?>)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for the second value, expecting a list",toExpression(constructingFactory), values[1].getClass().getSimpleName()));
    }
    if(!(values[2] instanceof List<?>)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - found type %s for the third value, expecting a list",toExpression(constructingFactory), values[2].getClass().getSimpleName()));
    }
    if(((List<?>)values[1]).stream().filter(value -> !(value instanceof Number)).count() > 0){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a list of numbers for the second value",toExpression(constructingFactory)));
    }
    if(((List<?>)values[2]).stream().filter(value -> !(value instanceof Number)).count() > 0){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a list of numbers for the third value",toExpression(constructingFactory)));
    }    
    
    RegressionEvaluator.RegressionTuple regressedTuple = (RegressionEvaluator.RegressionTuple)values[0];
    List<?> l1 = (List<?>)values[1];
    List<?> l2 = (List<?>)values[2];
    
    if(l2.size() < l1.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - first list (%d) has more values than the second list (%d)",toExpression(constructingFactory), l1.size(), l2.size()));      
    }
    
    List<Number> residuals = new ArrayList<>();
    for(int idx = 0; idx < l1.size(); ++idx){
      double value1 = ((Number)l1.get(idx)).doubleValue();
      double value2 = ((Number)l2.get(idx)).doubleValue();
      
      double prediction = regressedTuple.predict(value1);
      double residual = value2 - prediction;
      
      residuals.add(residual);
    }
    
    return residuals;
  }
  
}
