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
package org.apache.solr.client.solrj.io.comp;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 *  An equality field Comparator which compares a field of two Tuples and determines sort order.
 **/
public class FieldComparator implements StreamComparator {

  private static final long serialVersionUID = 1;
  private UUID comparatorNodeId = UUID.randomUUID();
  
  private String leftFieldName;
  private String rightFieldName;
  private final ComparatorOrder order;
  private ComparatorLambda comparator;
  
  public FieldComparator(String fieldName, ComparatorOrder order){
    this(fieldName, fieldName, order);
  }
  
  public FieldComparator(String leftFieldName, String rightFieldName, ComparatorOrder order) {
    this.leftFieldName = leftFieldName;
    this.rightFieldName = rightFieldName;
    this.order = order != null ? order : ComparatorOrder.ASCENDING;
    assignComparator();
  }
  
  public void setLeftFieldName(String leftFieldName){
    this.leftFieldName = leftFieldName;
  }
  public String getLeftFieldName(){
    return leftFieldName;
  }
  
  public void setRightFieldName(String rightFieldName){
    this.rightFieldName = rightFieldName;
  }
  public String getRightFieldName(){
    return rightFieldName;
  }
  
  public ComparatorOrder getOrder(){
    return order;
  }
  
  public boolean hasDifferentFieldNames(){
    return !leftFieldName.equals(rightFieldName);
  }
  
  public StreamExpressionParameter toExpression(StreamFactory factory){
    StringBuilder sb = new StringBuilder();
    
    sb.append(leftFieldName);
    if(hasDifferentFieldNames()){
      sb.append("=");
      sb.append(rightFieldName);
    }
    sb.append(" ");
    sb.append(order);
    
    return new StreamExpressionValue(sb.toString());
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return new Explanation(comparatorNodeId.toString())
      .withExpressionType(ExpressionType.SORTER)
      .withImplementingClass(getClass().getName())
      .withExpression(toExpression(factory).toString());
  }
  
  /*
   * What're we doing here messing around with lambdas for the comparator logic?
   * We want the compare(...) function to run as fast as possible because it will be called many many
   * times over the lifetime of this object. For that reason we want to limit the number of comparisons
   * taking place in the compare(...) function. Because this class supports both ascending and
   * descending comparisons and the logic for each is slightly different, we want to do the 
   *   if(ascending){ compare like this } else { compare like this }
   * check only once - we can do that in the constructor of this class, create a lambda, and then execute 
   * that lambda in the compare function. A little bit of branch prediction savings right here.
   */
  @SuppressWarnings({"unchecked"})
  private void assignComparator(){
    if(ComparatorOrder.DESCENDING == order){
      comparator = (leftTuple, rightTuple) -> {
        @SuppressWarnings({"rawtypes"})
        Comparable leftComp = (Comparable)leftTuple.get(leftFieldName);
        @SuppressWarnings({"rawtypes"})
        Comparable rightComp = (Comparable)rightTuple.get(rightFieldName);

        if(leftComp == rightComp){ return 0; } // if both null then they are equal. if both are same ref then are equal
        if(null == leftComp){ return 1; }
        if(null == rightComp){ return -1; }

        return rightComp.compareTo(leftComp);
      };
    }
    else{
      // See above for black magic reasoning.
      comparator = (leftTuple, rightTuple) -> {
        @SuppressWarnings({"rawtypes"})
        Comparable leftComp = (Comparable)leftTuple.get(leftFieldName);
        @SuppressWarnings({"rawtypes"})
        Comparable rightComp = (Comparable)rightTuple.get(rightFieldName);

        if(leftComp == rightComp){ return 0; } // if both null then they are equal. if both are same ref then are equal
        if(null == leftComp){ return -1; }
        if(null == rightComp){ return 1; }

        return leftComp.compareTo(rightComp);
      };
    }
  }

  public int compare(Tuple leftTuple, Tuple rightTuple) {
    return comparator.compare(leftTuple, rightTuple); 
  }
  
  @Override
  public boolean isDerivedFrom(StreamComparator base){
    if(null == base){ return false; }
    if(base instanceof FieldComparator){
      FieldComparator baseComp = (FieldComparator)base;
      return (leftFieldName.equals(baseComp.leftFieldName) || rightFieldName.equals(baseComp.rightFieldName)) && order == baseComp.order;
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
  
  @Override
  public FieldComparator copyAliased(Map<String,String> aliases){
    return new FieldComparator(
        aliases.containsKey(leftFieldName) ? aliases.get(leftFieldName) : leftFieldName,
        aliases.containsKey(rightFieldName) ? aliases.get(rightFieldName) : rightFieldName,
        order
    );
  }
  
  @Override
  public StreamComparator append(StreamComparator other){
    return new MultipleFieldComparator(this).append(other);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FieldComparator that = (FieldComparator) o;
    return leftFieldName.equals(that.leftFieldName) &&
        rightFieldName.equals(that.rightFieldName) &&
        order == that.order; // comparator is based on the other fields so is not needed in this compare
  }

  @Override
  public int hashCode() {
    return Objects.hash(leftFieldName, rightFieldName, order);
  }
}