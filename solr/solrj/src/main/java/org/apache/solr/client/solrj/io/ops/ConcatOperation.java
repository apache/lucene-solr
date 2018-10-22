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
package org.apache.solr.client.solrj.io.ops;

import java.io.IOException;
import java.util.Locale;
import java.util.UUID;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * Concatenates fields and adds them to the tuple. Example
 * concat(fields="month,day,year", delim="-", as="id")
 */
public class ConcatOperation implements StreamOperation {

  private static final long serialVersionUID = 1;
  private UUID operationNodeId = UUID.randomUUID();
  
  private String[] fields;
  private String as;
  private String delim;

  public ConcatOperation(String[] fields, String as, String delim) {
    this.fields = fields;
    this.as = as;
    this.delim = delim;
  }

  public ConcatOperation(StreamExpression expression, StreamFactory factory) throws IOException {

    if(3 == expression.getParameters().size()){
      StreamExpressionNamedParameter fieldsParam = factory.getNamedOperand(expression, "fields");
      String fieldsStr = ((StreamExpressionValue)fieldsParam.getParameter()).getValue();
      this.fields = fieldsStr.split(",");
      for(int i=0; i<fields.length; i++) {
        fields[i] = fields[i].trim();
      }

      StreamExpressionNamedParameter asParam = factory.getNamedOperand(expression, "as");
      this.as = ((StreamExpressionValue)asParam.getParameter()).getValue();

      StreamExpressionNamedParameter delim = factory.getNamedOperand(expression, "delim");
      this.delim = ((StreamExpressionValue)delim.getParameter()).getValue();
    } else{
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
  }

  @Override
  public void operate(Tuple tuple) {
    StringBuilder buf = new StringBuilder();
    for(String field : fields) {
      if(buf.length() > 0) {
        buf.append(delim);
      }
      Object value = tuple.get(field);
      if(null == value){ value = "null"; }
      buf.append(value);
    }

    tuple.put(as, buf.toString());
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    StringBuilder sb = new StringBuilder();
    for(String field : fields){
      if(sb.length() > 0){ sb.append(","); }
      sb.append(field);
    }
    expression.addParameter(new StreamExpressionNamedParameter("fields",sb.toString()));
    expression.addParameter(new StreamExpressionNamedParameter("delim",delim));
    expression.addParameter(new StreamExpressionNamedParameter("as",as));
    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(operationNodeId.toString())
      .withExpressionType(ExpressionType.OPERATION)
      .withFunctionName(factory.getFunctionName(getClass()))
      .withImplementingClass(getClass().getName())
      .withExpression(toExpression(factory).toString());
  }
  
}
