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
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public abstract class RecursiveNumericEvaluator extends RecursiveEvaluator {
  protected static final long serialVersionUID = 1L;
  
  public RecursiveNumericEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }
    
  public Object normalizeInputType(Object value) throws StreamEvaluatorException {
    if(null == value) {
      return null;
    } else if (value instanceof VectorFunction) {
      return value;
    } else if(value instanceof Double){
      if(Double.isNaN((Double)value)){
        return Double.NaN;
      }
      return new BigDecimal(value.toString());
    }
    else if(value instanceof BigDecimal){
      return value;
    } else if(value instanceof String) {
      return new BigDecimal((String)value);
    }
    else if(value instanceof Number){
      return new BigDecimal(value.toString());
    }
    else if(value instanceof Collection){
      if(value instanceof List) {
        if(((List)value).get(0) instanceof Number) {
          return  value;
        }
      }

      return ((Collection<?>) value).stream().map(innerValue -> normalizeInputType(innerValue)).collect(Collectors.toList());
    }
    else if(value.getClass().isArray()){
      Stream<?> stream = Stream.empty();
      if(value instanceof double[]){
        stream = Arrays.stream((double[])value).boxed();
      }
      else if(value instanceof int[]){
        stream = Arrays.stream((int[])value).boxed();
      }
      else if(value instanceof long[]){
        stream = Arrays.stream((long[])value).boxed();
      }
      else if(value instanceof String[]){
        stream = Arrays.stream((String[])value);
      }      
      return stream.map(innerValue -> normalizeInputType(innerValue)).collect(Collectors.toList());
    }
    else{
      throw new StreamEvaluatorException("Numeric value expected but found type %s for value %s", value.getClass().getName(), value.toString());
    }
  }  
}
