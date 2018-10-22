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
import java.util.UUID;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 *  An equality Comparator to be used when a stream will only ever return a single field,
 *  ie, it has no sorted order
 **/
public class SingleValueComparator implements StreamComparator {

  private static final long serialVersionUID = 1;
  private UUID comparatorNodeId = UUID.randomUUID();
    
  public StreamExpressionParameter toExpression(StreamFactory factory){
    return null;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {
    return null;
  }
  
  public int compare(Tuple leftTuple, Tuple rightTuple) {
    return -1; // whatever, just keep everything in same order 
  }
  
  @Override
  public boolean isDerivedFrom(StreamComparator base){
    // this doesn't sort, so everything else is a match
    return true;
  }
  
  @Override
  public SingleValueComparator copyAliased(Map<String,String> aliases){
    return this;
  }
  
  @Override
  public StreamComparator append(StreamComparator other){
    return other;
  }
}