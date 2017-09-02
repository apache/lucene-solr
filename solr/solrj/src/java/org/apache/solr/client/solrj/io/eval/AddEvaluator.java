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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class AddEvaluator extends RecursiveNumericEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public AddEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(containedEvaluators.size() < 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least one value but found %d",expression,containedEvaluators.size()));
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
    
    BigDecimal result = BigDecimal.ZERO;
    for(Object value : values){
      result = add(result, value);
    }
    
    return result;
  }
  
  private BigDecimal add(BigDecimal left, Object right) throws IOException{
    if(null == left || null == right){
      return null;
    }
    else if(right instanceof BigDecimal){
      return left.add((BigDecimal)right);
    }
    else if(right instanceof Number){
      return add(left, new BigDecimal(right.toString()));
    }
    else if(right instanceof List){
      return add(left, doWork(((List<?>)right).toArray()));
    }
    else{
      throw new StreamEvaluatorException("Numeric value expected but found type %s for value %s", right.getClass().getName(), right.toString());
    }

  }
  
}
