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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public abstract class RecursiveNumericListEvaluator extends RecursiveEvaluator {
  protected static final long serialVersionUID = 1L;
  
  public RecursiveNumericListEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }
    
  public Object normalizeInputType(Object value) throws StreamEvaluatorException {
    if(null == value){
      return null;
    }
    else if(value instanceof List){
      return ((List<?>)value).stream().map(innerValue -> convertToNumber(innerValue)).collect(Collectors.toList());
    }
    else{
      throw new StreamEvaluatorException("Numeric list value expected but found type %s for value %s", value.getClass().getName(), value.toString());
    }
  }
  
  private BigDecimal convertToNumber(Object value){
    if(null == value){
      return null;
    }
    else if(value instanceof Double){
      if(Double.isNaN((Double)value)){
        return null;
      }
      return new BigDecimal(value.toString());
    } else if (value instanceof String) {
      return new BigDecimal(value.toString());
    }
    else if(value instanceof BigDecimal){
      return (BigDecimal)value;
    }
    else if(value instanceof Number){
      return new BigDecimal(value.toString());
    }
    else{
      throw new StreamEvaluatorException("Numeric value expected but found type %s for value %s", value.getClass().getName(), value.toString());
    }

  }
}
