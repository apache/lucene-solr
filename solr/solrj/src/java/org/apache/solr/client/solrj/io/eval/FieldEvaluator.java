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
/**
 * 
 */
package org.apache.solr.client.solrj.io.eval;

import java.io.IOException;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class FieldEvaluator extends SimpleEvaluator {
  private static final long serialVersionUID = 1L;
  
  private String fieldName;
  
  public FieldEvaluator(String fieldName) {
    if(fieldName.startsWith("'") && fieldName.endsWith("'") && fieldName.length() > 1){
      fieldName = fieldName.substring(1, fieldName.length() - 1);
    }
    
    this.fieldName = fieldName;
  }
  
  @Override
  public Object evaluate(Tuple tuple) {
    return tuple.get(fieldName); // returns null if field doesn't exist in tuple
  }
  
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpressionValue(fieldName);
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(nodeId.toString())
      .withExpressionType(ExpressionType.EVALUATOR)
      .withImplementingClass(getClass().getName())
      .withExpression(toExpression(factory).toString());
  }

}
