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

import java.io.Serializable;
import java.util.Comparator;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 *  An equality field Comparator which compares a field of two Tuples and determines sort order.
 **/
public class FieldComparator extends StreamComparator implements Comparator<Tuple>, ExpressibleComparator, Serializable {

  private static final long serialVersionUID = 1;
  private ComparatorLambda comparator;

  public FieldComparator(String field, ComparatorOrder order) {
    super(field, order);
    assignComparator();
  }
  public FieldComparator(String leftField, String rightField, ComparatorOrder order){
    super(leftField,rightField,order);
    assignComparator();
  }
  
  public StreamExpressionParameter toExpression(StreamFactory factory){
    StringBuilder sb = new StringBuilder();
    
    sb.append(leftField);
    
    if(!leftField.equals(rightField)){
      sb.append("=");
      sb.append(rightField); 
    }
    
    sb.append(" ");
    sb.append(order);
    
    return new StreamExpressionValue(sb.toString());
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
  private void assignComparator(){
    if(ComparatorOrder.DESCENDING == order){
      // What black magic is this type intersection??
      // Because this class is serializable we need to make sure the lambda is also serializable.
      // This can be done by providing this type intersection on the definition of the lambda.
      // Why not do it in the lambda interface? Functional Interfaces don't allow extends clauses
      comparator = (ComparatorLambda & Serializable)(leftTuple, rightTuple) -> {
        Comparable leftComp = (Comparable)leftTuple.get(leftField);
        Comparable rightComp = (Comparable)rightTuple.get(rightField);
        return rightComp.compareTo(leftComp);
      };
    }
    else{
      // See above for black magic reasoning.
      comparator = (ComparatorLambda & Serializable)(leftTuple, rightTuple) -> {
        Comparable leftComp = (Comparable)leftTuple.get(leftField);
        Comparable rightComp = (Comparable)rightTuple.get(rightField);
        return leftComp.compareTo(rightComp);
      };
    }
  }

  public int compare(Tuple leftTuple, Tuple rightTuple) {
    return comparator.compare(leftTuple, rightTuple); 
  }
}