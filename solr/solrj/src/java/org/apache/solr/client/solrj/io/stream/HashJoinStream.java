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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 * Takes two streams (fullStream and hashStream) and joins them similar to an InnerJoinStream. The difference
 * in a HashJoinStream is that the tuples in the hashStream will all be read and hashed when this stream is
 * opened. This provides a few optimizations iff the hashStream has a relatively small number of documents.
 * You are expected to provide a set of fields for which the hash will be calculated from. If a tuple does
 * not contain a value (ie, null) for one of the fields the hash is being computed on then that tuple will 
 * not be considered a match to anything. Ie, all fields which are part of the hash must have a non-null value.
 * @since 6.0.0
**/
public class HashJoinStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1L;

  protected TupleStream hashStream;
  protected TupleStream fullStream;
  protected List<String> leftHashOn;
  protected List<String> rightHashOn;
  protected HashMap<String, List<Tuple>> hashedTuples;
  
  protected Tuple workingFullTuple = null;
  protected String workingFullHash = null;
  protected int workngHashSetIdx = 0;
  
  public HashJoinStream(TupleStream fullStream, TupleStream hashStream, List<String> hashOn) throws IOException {
    init(fullStream, hashStream, hashOn);
  }
  
  public HashJoinStream(StreamExpression expression,StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter hashStreamExpression = factory.getNamedOperand(expression, "hashed");
    StreamExpressionNamedParameter onExpression = factory.getNamedOperand(expression, "on");
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + 2){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting two streams but found %d",expression, streamExpressions.size()));
    }

    if(null == hashStreamExpression || !(hashStreamExpression.getParameter() instanceof StreamExpression)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'hashed' parameter containing the stream to hash but didn't find one",expression));
    }
    
    if(null == onExpression || !(onExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'on' parameter listing fields to hash on but didn't find one",expression));
    }
    
    String hashOnValue = ((StreamExpressionValue)onExpression.getParameter()).getValue();
    String[] parts = hashOnValue.split(",");
    List<String> hashOn = new ArrayList<String>(parts.length);
    for(String part : parts){
      hashOn.add(part.trim());
    }
    
    init( factory.constructStream(streamExpressions.get(0)),
          factory.constructStream((StreamExpression)hashStreamExpression.getParameter()),
          hashOn
        );
  }
  
  private void init(TupleStream fullStream, TupleStream hashStream, List<String> hashOn) throws IOException {
    this.fullStream = fullStream;
    this.hashStream = hashStream;
    this.hashedTuples = new HashMap<>();
    this.leftHashOn = new ArrayList<>();
    this.rightHashOn = new ArrayList<>();
    
    for(String hasher : hashOn){
      String[] parts = hasher.split("=");
      if(1 == parts.length){
        String field = parts[0].trim();
        leftHashOn.add(field);
        rightHashOn.add(field);
      }
      else if(2 == parts.length){
        leftHashOn.add(parts[0].trim());
        rightHashOn.add(parts[1].trim());
      }
      else{
        throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - invalid 'on' parameter - expecting 1 or more instances if 'field' or 'field=hashedField' but found '%s'",hasher));
      }
    }    
  }
  
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    if(includeStreams){
      // streams
      if(hashStream instanceof Expressible && fullStream instanceof Expressible){
        expression.addParameter(((Expressible)fullStream).toExpression(factory));
        expression.addParameter(new StreamExpressionNamedParameter("hashed", ((Expressible)hashStream).toExpression(factory)));
      }
      else{
        throw new IOException("This HashJoinStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
      expression.addParameter("hashed=<stream>");
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
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    return new StreamExplanation(getStreamNodeId().toString())
      .withChildren(new Explanation[]{
        fullStream.toExplanation(factory),
        hashStream.toExplanation(factory)
      })
      .withFunctionName(factory.getFunctionName(this.getClass()))
      .withImplementingClass(this.getClass().getName())
      .withExpressionType(ExpressionType.STREAM_DECORATOR)
      .withExpression(toExpression(factory, false).toString());    
  }

  public void setStreamContext(StreamContext context) {
    this.hashStream.setStreamContext(context);
    this.fullStream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    l.add(hashStream);
    l.add(fullStream);
    return l;
  }

  public void open() throws IOException {
    hashStream.open();
    fullStream.open();
    
    Tuple tuple = hashStream.read();
    while(!tuple.EOF){
      String hash = computeHash(tuple, rightHashOn);
      if(null != hash){
        if(hashedTuples.containsKey(hash)){
          hashedTuples.get(hash).add(tuple);
        }
        else{
          ArrayList<Tuple> set = new ArrayList<Tuple>();
          set.add(tuple);
          hashedTuples.put(hash, set);
        }
      }
      tuple = hashStream.read();
    }
  }
  
  protected String computeHash(Tuple tuple, List<String> hashOn){
    StringBuilder sb = new StringBuilder();
    for(String part : hashOn){
      Object obj = tuple.get(part);
      if(null == obj){
        return null;
      }
      sb.append(obj.toString());
      sb.append("::"); // this is here to separate fields
    }
    
    return sb.toString();
  }

  public void close() throws IOException {
    hashStream.close();
    fullStream.close();
  }

  public Tuple read() throws IOException {
    
    findNextWorkingFullTuple:
    while(null == workingFullTuple){
      Tuple fullTuple = fullStream.read();
      
      // We're at the end of the line
      if(fullTuple.EOF){
        return fullTuple;
      }
      
      // If fullTuple doesn't have a valid hash or if there is no doc to 
      // join with then retry loop - keep going until we find one
      String fullHash = computeHash(fullTuple, leftHashOn);
      if(null == fullHash || !hashedTuples.containsKey(fullHash)){
        continue findNextWorkingFullTuple;
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

  @Override
  public StreamComparator getStreamSort() {
    return fullStream.getStreamSort();
  }
  
  public int getCost() {
    return 0;
  }
}
