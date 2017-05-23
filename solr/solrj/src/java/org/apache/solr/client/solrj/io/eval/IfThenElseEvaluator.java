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

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class IfThenElseEvaluator extends ConditionalEvaluator {
  protected static final long serialVersionUID = 1L;
  
  public IfThenElseEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(3 != subEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting three values but found %d",expression,subEvaluators.size()));
    }
    
    if(!(subEvaluators.get(0) instanceof BooleanEvaluator)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a boolean as the first parameter but found %s",expression,subEvaluators.get(0).getClass().getSimpleName()));
    }

  }

  @Override
  public Object evaluate(Tuple tuple) throws IOException {
    
    List<Object> results = evaluateAll(tuple);
    
    if(3 != results.size()){
      String message = String.format(Locale.ROOT,"%s(...) only works with 3 values but %s were provided", constructingFactory.getFunctionName(getClass()), results.size());
      throw new IOException(message);
    }
    
    if(!(results.get(0) instanceof Boolean)){
      throw new IOException(String.format(Locale.ROOT,"$s(...) only works with a boolean as the first parameter but found %s",results.get(0).getClass().getSimpleName()));
    }
  
    return (boolean)results.get(0) ? results.get(1) : results.get(2);
  }
}
