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
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ExclusiveOrEvaluator extends BooleanEvaluator {
  protected static final long serialVersionUID = 1L;
  
  public ExclusiveOrEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(subEvaluators.size() < 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least two values but found %d",expression,subEvaluators.size()));
    }
  }

  @Override
  public Boolean evaluate(Tuple tuple) throws IOException {
    
    List<Object> results = evaluateAll(tuple);
    
    if(results.size() < 2){
      String message = null;
      if(1 == results.size()){
        message = String.format(Locale.ROOT,"%s(...) only works with at least 2 values but 1 was provided", constructingFactory.getFunctionName(getClass())); 
      }
      else{
        message = String.format(Locale.ROOT,"%s(...) only works with at least 2 values but 0 were provided", constructingFactory.getFunctionName(getClass()));
      }
      throw new IOException(message);
    }
    
    if(results.stream().anyMatch(result -> null == result)){
      throw new IOException(String.format(Locale.ROOT,"Unable to check %s(...) because a null value was found", constructingFactory.getFunctionName(getClass())));
    }
    if(results.stream().anyMatch(result -> !(result instanceof Boolean))){
      throw new IOException(String.format(Locale.ROOT,"Unable to check %s(...) of non-boolean values [%s]", constructingFactory.getFunctionName(getClass()), results.stream().map(item -> item.getClass().getSimpleName()).collect(Collectors.joining(","))));
    }

    return 1 == results.stream().filter(result -> (boolean)result).count();
  }
}
