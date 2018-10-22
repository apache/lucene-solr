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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * Takes two streams (fullStream and hashStream) and joins them similar to an LeftOuterJoinStream. The difference
 * in a OuterHashJoinStream is that the tuples in the hashStream will all be read and hashed when this stream is
 * opened. This provides a few optimizations iff the hashStream has a relatively small number of documents.
 * The difference between this and a HashJoinStream is that a tuple in the fullStream will be returned even
 * if it doesn't have any matching tuples in the hashStream. 
 * You are expected to provide a set of fields for which the hash will be calculated from. If a tuple from the 
 * hashStream does not contain a value (ie, null) for one of the fields the hash is being computed on then that 
 * tuple will not be considered a match to anything. If a tuple from the fullStream does not contain a value (ie, null) 
 * for one of the fields the hash is being computed on then that tuple will be returned without any joined tuples
 * from the hashStream
 * @since 6.0.0
**/
public class OuterHashJoinStream extends HashJoinStream implements Expressible {
  
  private static final long serialVersionUID = 1L;

  public OuterHashJoinStream(TupleStream fullStream, TupleStream hashStream, List<String> hashOn) throws IOException {
    super(fullStream, hashStream, hashOn);
  }
  
  public OuterHashJoinStream(StreamExpression expression,StreamFactory factory) throws IOException {
    super(expression, factory);
  }
    
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {    
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // streams
    if(hashStream instanceof Expressible && fullStream instanceof Expressible){
      expression.addParameter(((Expressible)fullStream).toExpression(factory));
      expression.addParameter(new StreamExpressionNamedParameter("hashed", ((Expressible)hashStream).toExpression(factory)));
    }
    else{
      throw new IOException("This OuterHashJoinStream contains a non-expressible TupleStream - it cannot be converted to an expression");
    }
    
    // on
    StringBuilder sb = new StringBuilder();
    for(int idx = 0; idx < leftHashOn.size(); ++idx){
      if(sb.length() > 0){ sb.append(","); }
      
      // we know that left and right hashOns are the same size
      String left = leftHashOn.get(idx);
      String right = rightHashOn.get(idx);
      
      if(left.equals(right)){ 
        sb.append(left); 
      }
      else{
        sb.append(left);
        sb.append("=");
        sb.append(right);
      }
    }
    expression.addParameter(new StreamExpressionNamedParameter("on",sb.toString()));
    
    return expression;   
  }

  public Tuple read() throws IOException {
    
    if(null == workingFullTuple){
      Tuple fullTuple = fullStream.read();
      
      // We're at the end of the line
      if(fullTuple.EOF){
        return fullTuple;
      }
      
      // If fullTuple doesn't have a valid hash or the hash cannot be found in the hashedTuples then
      // return the tuple from fullStream.
      // This is an outer join so there is no requirement there be a matching value in the hashed stream
      String fullHash = computeHash(fullTuple, leftHashOn);
      if(null == fullHash || !hashedTuples.containsKey(fullHash)){
        return fullTuple.clone();
      }
      
      workingFullTuple = fullTuple;
      workingFullHash = fullHash;
      workngHashSetIdx = 0;      
    }
  
    // At this point we know we have at least one doc to match on
    // Due to the check at the end, before returning, we know we have at least one to match with left
    List<Tuple> matches = hashedTuples.get(workingFullHash);
    Tuple returnTuple = workingFullTuple.clone();
    returnTuple.merge(matches.get(workngHashSetIdx));
    
    // Increment this so the next time we hit the next matching tuple
    workngHashSetIdx++;
    
    if(workngHashSetIdx >= matches.size()){
      // well, now we've reached all the matches, clear it all out
      workingFullTuple = null;
      workingFullHash = null;
      workngHashSetIdx = 0;
    }
    
    return returnTuple;
    
  }

}
