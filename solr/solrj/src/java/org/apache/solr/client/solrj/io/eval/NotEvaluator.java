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

import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class NotEvaluator extends RecursiveBooleanEvaluator implements OneValueWorker {
  protected static final long serialVersionUID = 1L;
  
  public NotEvaluator(StreamExpression expression, StreamFactory factory) throws IOException{
    super(expression, factory);
    
    if(containedEvaluators.size() < 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting at least one value but found %d",expression,containedEvaluators.size()));
    }
  }
  
  protected Checker constructChecker(Object value) throws IOException{
    return null;
  }
  
  public Object doWork(Object ... values) throws IOException{
    if(1 != values.length){
      throw new IOException(String.format(Locale.ROOT, "Expecting 1 value but found %d", values.length));
    }
    
    return doWork(values[0]);
  }

  public Object doWork(Object value) {
    if(null == value){
      return null;
    }
    else if(value instanceof List){
      return ((List<?>)value).stream().map(innerValue -> doWork(innerValue));
    }
    else{
      // we know it's a boolean
      return !((Boolean)value);
    }
  }

}
