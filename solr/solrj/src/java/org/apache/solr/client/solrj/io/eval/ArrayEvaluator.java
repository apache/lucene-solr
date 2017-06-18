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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class ArrayEvaluator extends ComplexEvaluator implements Expressible {

  private static final long serialVersionUID = 1;
  private String sortOrder;
  
  public ArrayEvaluator(StreamExpression expression, StreamFactory factory) throws IOException {
    super(expression, factory, Arrays.asList("sort"));
    
    sortOrder = extractSortOrder(expression, factory);
  }
  
  private String extractSortOrder(StreamExpression expression, StreamFactory factory) throws IOException{
    StreamExpressionNamedParameter sortParam = factory.getNamedOperand(expression, "sort");
    if(null == sortParam){
      return null; // this is ok
    }
    
    if(sortParam.getParameter() instanceof StreamExpressionValue){
      String sortOrder = ((StreamExpressionValue)sortParam.getParameter()).getValue().trim().toLowerCase(Locale.ROOT);
      if("asc".equals(sortOrder) || "desc".equals(sortOrder)){
        return sortOrder;
      }
    }
    
    throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - invalid 'sort' parameter - expecting either 'asc' or 'desc'", expression));
  }

  public List<Object> evaluate(Tuple tuple) throws IOException {
    List<Object> list = new ArrayList<>();
    for(StreamEvaluator subEvaluator : subEvaluators) {
      Object value = subEvaluator.evaluate(tuple);
      
      // if we want sorting but the evaluated value is not comparable then we have an error
      if(null != sortOrder && !(value instanceof Comparable<?>)){
        String message = String.format(Locale.ROOT,"Failed to evaluate to a comparable object - evaluator '%s' resulted in type '%s' and value '%s'",
            subEvaluator.toExpression(constructingFactory),
            value.getClass().getName(),
            value.toString());
        throw new IOException(message);
      }
      
      list.add(value);
    }
    
    if(null != sortOrder){
      // Because of the type checking above we know that the value is at least Comparable
      Comparator<Comparable> comparator = "asc".equals(sortOrder) ? (left,right) -> left.compareTo(right) : (left,right) -> right.compareTo(left);
      list = list.stream().map(value -> (Comparable<Object>)value).sorted(comparator).collect(Collectors.toList());
    }

    return list;
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(getClass()));
    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(nodeId.toString())
        .withExpressionType(ExpressionType.EVALUATOR)
        .withFunctionName(factory.getFunctionName(getClass()))
        .withImplementingClass(getClass().getName())
        .withExpression(toExpression(factory).toString());
  }
}