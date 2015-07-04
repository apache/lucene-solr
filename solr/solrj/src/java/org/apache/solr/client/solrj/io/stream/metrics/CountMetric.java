package org.apache.solr.client.solrj.io.stream.metrics;
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

import java.io.IOException;
import java.io.Serializable;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class CountMetric extends Metric implements Serializable {

  private static final long serialVersionUID = 1;

  private long count;
  
  public CountMetric(){
    init("count");
  }
  public CountMetric(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String functionName = expression.getFunctionName();
    String columnName = factory.getValueOperand(expression, 0);
    
    // validate expression contains only what we want.
    if(!"*".equals(columnName)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expected %s(*)", expression, functionName));
    }
    if(1 != expression.getParameters().size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    init(functionName);
    
  }
  
  private void init(String functionName){
    setFunctionName(functionName);
    setIdentifier(functionName, "(*)");
  }

  public void update(Tuple tuple) {
    ++count;
  }

  public double getValue() {
    return count;
  }

  public Metric newInstance() {
    return new CountMetric();
  }
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(getFunctionName()).withParameter("*");
  }
}