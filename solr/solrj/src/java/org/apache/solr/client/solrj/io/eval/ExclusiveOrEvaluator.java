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

public class ExclusiveOrEvaluator extends RecursiveBooleanEvaluator implements ManyValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public ExclusiveOrEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(containedEvaluators.size() < 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least two values but found %d",expression,containedEvaluators.size()));
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

    return 1 == Arrays.stream(values).filter(result -> (boolean)result).count();
  }

  @Override
  protected Checker constructChecker(Object value) throws IOException {
    return new BooleanChecker(){
      @Override
      public boolean test(Object left, Object right) {
        // does nothing useful
        return false;
      }
    };
  }

}
