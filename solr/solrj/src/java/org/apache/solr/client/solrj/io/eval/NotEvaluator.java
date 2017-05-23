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

public class NotEvaluator extends BooleanEvaluator {
  protected static final long serialVersionUID = 1L;
  
  public NotEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(1 != subEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting one value but found %d",expression,subEvaluators.size()));
    }
  }

  @Override
  public Boolean evaluate(Tuple tuple) throws IOException {
    
    List<Object> results = evaluateAll(tuple);
    
    if(1 != results.size()){
      String message = String.format(Locale.ROOT,"%s(...) only works with 1 value but %d were provided", constructingFactory.getFunctionName(getClass()), results.size());
      throw new IOException(message);
    }

    Object result = results.get(0);
    if(null == result){
      throw new IOException(String.format(Locale.ROOT,"Unable to evaluate %s(...) because a null value was found", constructingFactory.getFunctionName(getClass())));
    }
    if(!(result instanceof Boolean)){
      throw new IOException(String.format(Locale.ROOT,"Unable to evaluate %s(...) of a non-boolean value [%s]", constructingFactory.getFunctionName(getClass()), results.stream().map(item -> item.getClass().getSimpleName()).collect(Collectors.joining(","))));
    }
    
    return !((Boolean)result);
  }
}
