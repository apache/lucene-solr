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

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * Replaces some tuple value with another. The replacement value can be either a given value or the 
 * value of another field in the tuple. The expression for a replace operation can be of multiple forms:
 *  replace(fieldA, 0, withValue=100)  // for fieldA if equals 0 then set to 100
 *  replace(fieldA, null, withValue=0) // for fieldA if null then set to 0
 *  replace(fieldA, null, withField=fieldB) // for fieldA if null then set to the value of fieldB (if fieldB is null then fieldA will end up as null)
 *  replace(fieldA, 0, withField=fieldB) // for fieldA if 0 then set to the value of fieldB (if fieldB is 0 then fieldA will end up as 0)
 *  replace(fieldA, "Izzy and Kayden", withValue="my kids")
 *  
 * You can also construct these without the field name in the expression but that does require that you provide the field name during construction.
 * This is most useful during metric calculation because when calculating a metric you have already provided a field name in the metric so there
 * is no reason to have to provide the field name again in the operation
 *  sum(fieldA, replace(null, withValue=0)) // performs the replacement on fieldA
 *  
 * Equality is determined by the standard type .equals() functions.
 */
public class ReplaceOperation implements StreamOperation {

  private static final long serialVersionUID = 1;
  private StreamOperation replacer;  

  public ReplaceOperation(StreamExpression expression, StreamFactory factory) throws IOException {
    this(null, expression, factory);
  }
  
  public ReplaceOperation(String forField, StreamExpression expression, StreamFactory factory) throws IOException {
    
    StreamExpressionNamedParameter withValue = factory.getNamedOperand(expression, "withValue");
    StreamExpressionNamedParameter withField = factory.getNamedOperand(expression, "withField");
    
    if(null != withValue && null == withField){
      replacer = new ReplaceWithValueOperation(forField, expression, factory);
    }
    else if(null != withField && null == withValue){
      replacer = new ReplaceWithFieldOperation(forField, expression, factory);
    }
    else if(null != withValue && null != withField){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting either withValue or withField parameter but found both", expression));
    }
    else{
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting withValue or withField parameter but found neither", expression));
    }
    
  }
    
  @Override
  public void operate(Tuple tuple) {
    replacer.operate(tuple);
  }
  
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return replacer.toExpression(factory);
  }
  
  @Override 
  public Explanation toExplanation(StreamFactory factory) throws IOException{
    return replacer.toExplanation(factory);
  }
  
}
