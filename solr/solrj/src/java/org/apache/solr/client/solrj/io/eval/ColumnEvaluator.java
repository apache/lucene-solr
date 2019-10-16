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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ColumnEvaluator extends RecursiveEvaluator {
  protected static final long serialVersionUID = 1L;
  
  public ColumnEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    init();
  }
  
  public ColumnEvaluator(StreamExpression expression, StreamFactory factory, List<String> ignoredNamedParameters) throws IOException{
    super(expression, factory, ignoredNamedParameters);
    
    init();
  }
  
  private void init() throws IOException{
    if(2 != containedEvaluators.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting exactly 2 parameters but found %d", toExpression(constructingFactory), containedEvaluators.size()));
    }
  }
      
  @Override
  public Object evaluate(Tuple tuple) throws IOException {    
    try{
      
      Object firstLevel = containedEvaluators.get(0).evaluate(tuple);
      
      if(!(firstLevel instanceof List<?>) || ((List<?>) firstLevel).stream().anyMatch(value -> !(value instanceof Tuple))){
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a list of tuples but found %s", toExpression(constructingFactory), firstLevel.getClass().getSimpleName()));
      }

      List<Object> column = new ArrayList<>();
      for(Object innerTuple : (List<?>)firstLevel){
        column.add(containedEvaluators.get(1).evaluate((Tuple)innerTuple));
      }

      return normalizeOutputType(column);
    }
    catch(UncheckedIOException e){
      throw e.getCause();
    }
  }

  @Override
  public Object doWork(Object... values) throws IOException {
    // Nothing to do here
    throw new IOException("This call should never occur");
  }
}
