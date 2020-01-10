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
import java.util.List;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

public class FieldValueEvaluator extends SourceEvaluator {
  private static final long serialVersionUID = 1L;

  private String fieldName;

  public FieldValueEvaluator(String fieldName) {
    if(fieldName.startsWith("'") && fieldName.endsWith("'") && fieldName.length() > 1){
      fieldName = fieldName.substring(1, fieldName.length() - 1);
    }

    this.fieldName = fieldName;
  }

  @Override
  public Object evaluate(Tuple tuple) throws IOException {
    Object value = tuple.get(fieldName);

    // This is somewhat radical.
    // Here, we allow for the use of the context to provide alternative values
    // when they are not available in the provided tuple. This means that all
    // evaluators can evaluate over both a stream's tuple and the context, and
    // can even evaluate over fields from both of them in the same evaluation
    if(null == value && null != getStreamContext()){
      value = getStreamContext().getLets().get(fieldName);

      // If what's contained in the context is itself an evaluator then
      // we need to evaluate it
      if(value instanceof StreamEvaluator){
        value = ((StreamEvaluator)value).evaluate(tuple);
      }
    }

    // if we have an array then convert to an ArrayList
    // if we have an iterable that is not a list then convert to ArrayList
    // lists are good to go
    if(null != value){
      if(value instanceof Object[]){
        Object[] array = (Object[])value;
        List<Object> list = new ArrayList<Object>(array.length);
        for(Object obj : array){
          list.add(obj);
        }
        return list;
      } else if(value instanceof Matrix) {
        return value;
      } else if(value instanceof VectorFunction) {
        return value;
      } else if(value instanceof Iterable && !(value instanceof List<?>)){
        Iterable<?> iter = (Iterable<?>)value;
        List<Object> list = new ArrayList<Object>();
        for(Object obj : iter){
          list.add(obj);
        }
        return list;
      }
    }

    StreamContext sc = getStreamContext();

    if(sc != null) {sc.getTupleContext().remove("null");}

    if(value == null) {
      if(sc != null) {sc.getTupleContext().put("null", fieldName);}
      if(fieldName.startsWith("\"") && fieldName.endsWith("\"")) {
        return fieldName.substring(1, fieldName.length()-1);
      } else {
        return null;
      }
    }

    return value;
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