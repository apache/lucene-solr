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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class SubtractEvaluator extends RecursiveNumericEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public SubtractEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(containedEvaluators.size() < 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least two values but found %d",expression,containedEvaluators.size()));
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    if(Arrays.stream(values).anyMatch(item -> null == item)){
      return null;
    }
    
    if(0 == values.length){
      return null;
    }
    
    List<BigDecimal> flattenedValues = flatten(Arrays.stream(values).collect(Collectors.toList()));
    
    BigDecimal result = flattenedValues.get(0);
    for(int idx = 1; idx < flattenedValues.size(); ++idx){
      result = subtract(result, flattenedValues.get(idx));
    }
    
    return result;
  }
  
  private List<BigDecimal> flatten(Collection<?> values){
    List<BigDecimal> flattened = new ArrayList<>();
    
    for(Object value : values){
      if(null == value){
        flattened.add(null);
      }
      if(value instanceof Collection<?>){
        flattened.addAll(flatten((Collection<?>)value));
      }
      else if(value instanceof BigDecimal){
        flattened.add((BigDecimal)value);
      }
      else if(value instanceof Number){
        flattened.add(new BigDecimal(value.toString()));
      }
      else{
        throw new StreamEvaluatorException("Numeric value expected but found type %s for value %s", value.getClass().getName(), value.toString());
      }
    }
    
    return flattened;
  }
    
  private BigDecimal subtract(BigDecimal left, Object right) throws IOException{
    
    if(null == right){
      return null;
    }
    
    return left.subtract((BigDecimal)right);
  }
  
}
