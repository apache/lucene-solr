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
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.math3.util.MathArrays;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class FindDelayEvaluator extends RecursiveNumericEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public FindDelayEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
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

    // Get first and second lists as arrays, where second is in reverse order
    @SuppressWarnings({"unchecked", "rawtypes"})
    double[] firstArray = ((List)first).stream().mapToDouble(value -> ((Number)value).doubleValue()).toArray();
    @SuppressWarnings({"unchecked", "rawtypes"})
    double[] secondArray = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
        ((LinkedList)((List)second).stream().collect(Collectors.toCollection(LinkedList::new))).descendingIterator(),
        Spliterator.ORDERED), false).mapToDouble(value -> ((Number)value).doubleValue()).toArray();
    
    double[] convolution = MathArrays.convolve(firstArray, secondArray);
    double maxValue = -Double.MAX_VALUE;
    double indexOfMaxValue = -1;

    for(int idx = 0; idx < convolution.length; ++idx) {
      double abs = Math.abs(convolution[idx]);
      if(abs > maxValue) {
        maxValue = abs;
        indexOfMaxValue = idx;
      }
    }

    return (indexOfMaxValue + 1) - secondArray.length;

  }
}
