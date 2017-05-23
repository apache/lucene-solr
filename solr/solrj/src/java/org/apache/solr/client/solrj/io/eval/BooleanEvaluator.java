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
import java.util.List;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public abstract class BooleanEvaluator extends ComplexEvaluator {
  protected static final long serialVersionUID = 1L;
  
  public BooleanEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
  }
  
  // restrict result to a Boolean
  public abstract Boolean evaluate(Tuple tuple) throws IOException;
  
  public List<Object> evaluateAll(final Tuple tuple) throws IOException {
    List<Object> results = new ArrayList<Object>();
    for(StreamEvaluator subEvaluator : subEvaluators){
      results.add(subEvaluator.evaluate(tuple));
    }
    
    return results;
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
