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
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public abstract class RecursiveBooleanEvaluator extends RecursiveEvaluator {
  protected static final long serialVersionUID = 1L;
  
  protected abstract Checker constructChecker(Object value) throws IOException;
  
  public RecursiveBooleanEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }
  
  public Object normalizeInputType(Object value) throws StreamEvaluatorException {
    if(null == value){
      return null;
    }
    else{
      return value;
    }
  }
  
  public Object doWork(Object ... values) throws IOException {
    if(values.length < 2){
      String message = null;
      if(1 == values.length){
        message = String.format(Locale.ROOT,"%s(...) only works with at least 2 values but 1 was provided", constructingFactory.getFunctionName(getClass())); 
      }
      else{
        message = String.format(Locale.ROOT,"%s(...) only works with at least 2 values but 0 were provided", constructingFactory.getFunctionName(getClass()));
      }
      throw new IOException(message);
    }
    
    Checker checker = constructChecker(values[0]);
    if(Arrays.stream(values).anyMatch(result -> null == result)){
      throw new IOException(String.format(Locale.ROOT,"Unable to check %s(...) because a null value was found", constructingFactory.getFunctionName(getClass())));
    }
    if(Arrays.stream(values).anyMatch(result -> !checker.isCorrectType(result))){
      throw new IOException(String.format(Locale.ROOT,"Unable to check %s(...) of differing types [%s]", constructingFactory.getFunctionName(getClass()), Arrays.stream(values).map(item -> item.getClass().getSimpleName()).collect(Collectors.joining(","))));
    }

    for(int idx = 1; idx < values.length; ++idx){
      if(!checker.test(values[idx - 1], values[idx])){
        return false;
      }
    }
    
    return true;
  }
  
  public interface Checker {
    default boolean isNullAllowed(){
      return false;
    }
    boolean isCorrectType(Object value);
    boolean test(Object left, Object right);
  }
  
  public interface NullChecker extends Checker {
    default boolean isNullAllowed(){
      return true;
    }
    default boolean isCorrectType(Object value){
      return true;
    }
    default boolean test(Object left, Object right){
      return null == left && null == right;
    }
  }
  
  public interface BooleanChecker extends Checker {
    default boolean isCorrectType(Object value){
      return value instanceof Boolean;
    }
  }
  
  public interface NumberChecker extends Checker {
    default boolean isCorrectType(Object value){
      return value instanceof Number;
    }
  }
  
  public interface StringChecker extends Checker {
    default boolean isCorrectType(Object value){
      return value instanceof String;
    }
  }

}
