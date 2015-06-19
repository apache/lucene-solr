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

import java.io.Serializable;
import java.util.Comparator;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 *  An equality field Equalitor which compares a field of two Tuples and determines if they are equal.
 **/
public class StreamEqualitor implements Equalitor<Tuple>, Expressible, Serializable {

  private static final long serialVersionUID = 1;
  
  private String leftFieldName;
  private String rightFieldName;
  private StreamComparator comparator;
  
  public StreamEqualitor(String fieldName) {
    init(fieldName, fieldName);
  }
  public StreamEqualitor(String leftFieldName, String rightFieldName){
    init(leftFieldName, rightFieldName);
  }
  
  private void init(String leftFieldName, String rightFieldName){
    this.leftFieldName = leftFieldName;
    this.rightFieldName = rightFieldName;
    this.comparator = new StreamComparator(leftFieldName, rightFieldName, ComparatorOrder.ASCENDING);
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
  
  public boolean test(Tuple leftTuple, Tuple rightTuple) {
    return 0 == comparator.compare(leftTuple, rightTuple); 
  }
}