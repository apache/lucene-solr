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
package org.apache.solr.client.solrj.io.eq;

import java.io.IOException;
import java.util.UUID;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 *  An equality field Equalitor which compares a field of two Tuples and determines if they are equal.
 **/
public class FieldEqualitor implements StreamEqualitor {

  private static final long serialVersionUID = 1;
  private UUID equalitorNodeId = UUID.randomUUID();
  
  private String leftFieldName;
  private String rightFieldName;
  
  public FieldEqualitor(String fieldName) {
    init(fieldName, fieldName);
  }
  public FieldEqualitor(String leftFieldName, String rightFieldName){
    init(leftFieldName, rightFieldName);
  }
  
  private void init(String leftFieldName, String rightFieldName){
    this.leftFieldName = leftFieldName;
    this.rightFieldName = rightFieldName;
  }
  
  public StreamExpressionParameter toExpression(StreamFactory factory){
    StringBuilder sb = new StringBuilder();
    
    sb.append(leftFieldName);
    
    if(!leftFieldName.equals(rightFieldName)){
      sb.append("=");
      sb.append(rightFieldName); 
    }
    
    return new StreamExpressionValue(sb.toString());
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(equalitorNodeId.toString())
      .withExpressionType(ExpressionType.EQUALITOR)
      .withImplementingClass(getClass().getName())
      .withExpression(toExpression(factory).toString());
  }
  
  @SuppressWarnings({"unchecked"})
  public boolean test(Tuple leftTuple, Tuple rightTuple) {

    @SuppressWarnings({"rawtypes"})
    Comparable leftComp = (Comparable)leftTuple.get(leftFieldName);
    @SuppressWarnings({"rawtypes"})
    Comparable rightComp = (Comparable)rightTuple.get(rightFieldName);
    
    if(leftComp == rightComp){ return true; } // if both null then they are equal. if both are same ref then are equal
    if(null == leftComp || null == rightComp){ return false; }
    
    return 0 == leftComp.compareTo(rightComp);
  }
  
  public String getLeftFieldName(){
    return leftFieldName;
  }
  
  public String getRightFieldName(){
    return rightFieldName;
  }
  
  @Override
  public boolean isDerivedFrom(StreamEqualitor base){
    if(null == base){ return false; }
    if(base instanceof FieldEqualitor){
      FieldEqualitor baseEq = (FieldEqualitor)base;
      return leftFieldName.equals(baseEq.leftFieldName) && rightFieldName.equals(baseEq.rightFieldName);
    }
    else if(base instanceof MultipleFieldEqualitor){
      // must equal the first one
      MultipleFieldEqualitor baseEqs = (MultipleFieldEqualitor)base;
      if(baseEqs.getEqs().length > 0){
        return isDerivedFrom(baseEqs.getEqs()[0]);
      }
    }
    
    return false;
  }
  
  @Override
  public boolean isDerivedFrom(StreamComparator base){
    if(null == base){ return false; }
    if(base instanceof FieldComparator){
      FieldComparator baseComp = (FieldComparator)base;
      return leftFieldName.equals(baseComp.getLeftFieldName()) || rightFieldName.equals(baseComp.getRightFieldName());
    }
    else if(base instanceof MultipleFieldComparator){
      // must equal the first one
      MultipleFieldComparator baseComps = (MultipleFieldComparator)base;
      if(baseComps.getComps().length > 0){
        return isDerivedFrom(baseComps.getComps()[0]);
      }
    }
    
    return false;
  }
}