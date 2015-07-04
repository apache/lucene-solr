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

public class MeanMetric extends Metric implements Serializable {
  // How'd the MeanMetric get to be so mean?
  // Maybe it was born with it.
  // Maybe it was mayba-mean.
  //
  // I'll see myself out.
  
  private static final long serialVersionUID = 1;

  private String columnName;
  private double doubleSum;
  private long longSum;
  private long count;

  public MeanMetric(String columnName){
    init("avg", columnName);
  }
  public MeanMetric(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String functionName = expression.getFunctionName();
    String columnName = factory.getValueOperand(expression, 0);
    
    // validate expression contains only what we want.
    if(null == columnName){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expected %s(columnName)", expression, functionName));
    }
    if(1 != expression.getParameters().size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    init(functionName, columnName);    
  }
  
  private void init(String functionName, String columnName){
    this.columnName = columnName;
    setFunctionName(functionName);
    setIdentifier(functionName, "(", columnName, ")");
  }
  
  public void update(Tuple tuple) {
    ++count;
    Object o = tuple.get(columnName);
    if(o instanceof Double) {
      Double d = (Double)tuple.get(columnName);
      doubleSum += d.doubleValue();
    } else {
      Long l = (Long)tuple.get(columnName);
      longSum += l.doubleValue();
    }
  }

  public Metric newInstance() {
    return new MeanMetric(columnName);
  }

  public double getValue() {
    double dcount = (double)count;
    if(longSum == 0) {
      double ave = doubleSum/dcount;
      return ave;

    } else {
      double ave = longSum/dcount;
      return ave;
    }
  }
  
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(getFunctionName()).withParameter(columnName);
  }
}