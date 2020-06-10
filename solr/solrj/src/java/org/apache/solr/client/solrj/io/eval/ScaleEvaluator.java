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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.math3.util.MathArrays;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ScaleEvaluator extends RecursiveNumericEvaluator implements TwoValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public ScaleEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);

    if(2 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two values but found %d",expression, containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object first, Object second) throws IOException{
    
    if(null == first || null == second){
      return null;
    }
    
    // we know these are all numbers or lists of numbers
    if(first instanceof List<?>){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting Number as first value but found a list", toExpression(constructingFactory)));
    }
    
    double[] scaleOver;
    if(second instanceof Number){
      scaleOver = Arrays.asList((Number)second).stream().mapToDouble(value -> (value).doubleValue()).toArray();
    }
    else{
      scaleOver = ((List<?>)second).stream().mapToDouble(value -> ((Number)value).doubleValue()).toArray();
    }
      
    return Arrays.stream(MathArrays.scale(((Number)first).doubleValue(), scaleOver)).boxed().collect(Collectors.toList());
  }
}
