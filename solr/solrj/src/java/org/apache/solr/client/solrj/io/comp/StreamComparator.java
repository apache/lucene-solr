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
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 *  An equality field Comparator which compares a field of two Tuples and determines sort order.
 **/
public class StreamComparator implements Comparator<Tuple>, Expressible, Serializable {

  private static final long serialVersionUID = 1;
  
  private String leftField;
  private String rightField;
  private final ComparatorOrder order;
  private ComparatorLambda comparator;
  
  public StreamComparator(String field, ComparatorOrder order) {
    this.leftField = field;
    this.rightField = field;
    this.order = order;
    assignComparator();
  }
  public StreamComparator(String leftField, String rightField, ComparatorOrder order){
    this.leftField = leftField;
    this.rightField = rightField;
    this.order = order;
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
      comparator = new ComparatorLambda() {
        @Override
        public int compare(Tuple leftTuple, Tuple rightTuple) {
          Comparable leftComp = (Comparable)leftTuple.get(leftField);
          Comparable rightComp = (Comparable)rightTuple.get(rightField);
          
          if(leftComp == rightComp){ return 0; } // if both null then they are equal. if both are same ref then are equal
          if(null == leftComp){ return 1; }
          if(null == rightComp){ return -1; }
          
          return rightComp.compareTo(leftComp);
        }
      };
    }
    else{
      // See above for black magic reasoning.
      comparator = new ComparatorLambda() {
        @Override
        public int compare(Tuple leftTuple, Tuple rightTuple) {
          Comparable leftComp = (Comparable)leftTuple.get(leftField);
          Comparable rightComp = (Comparable)rightTuple.get(rightField);
          
          if(leftComp == rightComp){ return 0; } // if both null then they are equal. if both are same ref then are equal
          if(null == leftComp){ return -1; }
          if(null == rightComp){ return 1; }
          
          return leftComp.compareTo(rightComp);
        }
      };
    }
  }

  public int compare(Tuple leftTuple, Tuple rightTuple) {
    return comparator.compare(leftTuple, rightTuple); 
  }
}