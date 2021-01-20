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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.io.SolrClientCache;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends a commit message to a SolrCloud collection
 * @since 6.3.0
 */
public class CommitStream extends TupleStream implements Expressible {
  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // Part of expression / passed in
  private String collection;
  private String zkHost;
  private boolean waitFlush;
  private boolean waitSearcher;
  private boolean softCommit;
  private int commitBatchSize;
  private TupleStream tupleSource;
  
  private transient SolrClientCache clientCache;
  private long docsSinceCommit;

  public CommitStream(StreamExpression expression, StreamFactory factory) throws IOException {
    
    String collectionName = factory.getValueOperand(expression, 0);
    String zkHost = findZkHost(factory, collectionName, expression);
    int batchSize = factory.getIntOperand(expression, "batchSize", 0);
    boolean waitFlush = factory.getBooleanOperand(expression, "waitFlush", false);
    boolean waitSearcher = factory.getBooleanOperand(expression, "waitSearcher", false);
    boolean softCommit = factory.getBooleanOperand(expression, "softCommit", false);

    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }
    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }
    if(batchSize < 0){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - batchSize cannot be less than 0 but is '%d'",expression,batchSize));
    }

    //Extract underlying TupleStream.
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    if (1 != streamExpressions.size()) {
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }
    StreamExpression sourceStreamExpression = streamExpressions.get(0);
    
    init(collectionName, factory.constructStream(sourceStreamExpression), zkHost, batchSize, waitFlush, waitSearcher, softCommit);
  }
  
  public CommitStream(String collectionName, TupleStream tupleSource, String zkHost, int batchSize, boolean waitFlush, boolean waitSearcher, boolean softCommit) throws IOException {
    if (batchSize < 0) {
      throw new IOException(String.format(Locale.ROOT,"batchSize '%d' cannot be less than 0.", batchSize));
    }
    init(collectionName, tupleSource, zkHost, batchSize, waitFlush, waitSearcher, softCommit);
  }
  
  private void init(String collectionName, TupleStream tupleSource, String zkHost, int batchSize, boolean waitFlush, boolean waitSearcher, boolean softCommit) {
    this.collection = collectionName;
    this.zkHost = zkHost;
    this.commitBatchSize = batchSize;
    this.waitFlush = waitFlush;
    this.waitSearcher = waitSearcher;
    this.softCommit = softCommit;
    this.tupleSource = tupleSource;
  }
  
  @Override
  public void open() throws IOException {
    tupleSource.open();
    clientCache = new SolrClientCache();
    docsSinceCommit = 0;
  }
  
  @Override
  public Tuple read() throws IOException {
    
    Tuple tuple = tupleSource.read();
    if(tuple.EOF){
      if(docsSinceCommit > 0){
        sendCommit();
      }
    }
    else{
      // if the read document contains field 'batchIndexed' then it's a summary
      // document and we can update our count based on it's value. If not then 
      // just increment by 1
      if(tuple.getFields().containsKey(UpdateStream.BATCH_INDEXED_FIELD_NAME) && isInteger(tuple.getString(UpdateStream.BATCH_INDEXED_FIELD_NAME))){
        docsSinceCommit += Integer.parseInt(tuple.getString(UpdateStream.BATCH_INDEXED_FIELD_NAME));
      }
      else{
        docsSinceCommit += 1;
      }
      
      if(commitBatchSize > 0 && docsSinceCommit >= commitBatchSize){
        // if commitBatchSize == 0 then the tuple.EOF above will end up calling sendCommit()
        sendCommit();
      }
    }
    
    return tuple;
  }
  
  private boolean isInteger(String string){
    try{
      Integer.parseInt(string);
      return true;
    }
    catch(NumberFormatException e){
      return false;
    }
  }
  
  @Override
  public void close() throws IOException {
    clientCache.close();
    tupleSource.close();
  }
  
  @Override
  public StreamComparator getStreamSort() {
    return tupleSource.getStreamSort();
  }
  
  @Override
  public List<TupleStream> children() {
    ArrayList<TupleStream> sourceList = new ArrayList<TupleStream>(1);
    sourceList.add(tupleSource);
    return sourceList;
  }
  
  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    expression.addParameter(collection);
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));
    expression.addParameter(new StreamExpressionNamedParameter("batchSize", Integer.toString(commitBatchSize)));
    expression.addParameter(new StreamExpressionNamedParameter("waitFlush", Boolean.toString(waitFlush)));
    expression.addParameter(new StreamExpressionNamedParameter("waitSearcher", Boolean.toString(waitSearcher)));
    expression.addParameter(new StreamExpressionNamedParameter("softCommit", Boolean.toString(softCommit)));
        
    if(includeStreams){
      if(tupleSource instanceof Expressible){
        expression.addParameter(((Expressible)tupleSource).toExpression(factory));
      } else {
        throw new IOException("This CommitStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
    }
    
    return expression;
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    // A commit stream is backward wrt the order in the explanation. This stream is the "child"
    // while the collection we're committing to is the parent.
    
    StreamExplanation explanation = new StreamExplanation(getStreamNodeId() + "-datastore");
    
    explanation.setFunctionName(String.format(Locale.ROOT, "solr (%s)", collection));
    explanation.setImplementingClass("Solr/Lucene");
    explanation.setExpressionType(ExpressionType.DATASTORE);
    explanation.setExpression("Commit into " + collection);
    
    // child is a stream so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId().toString());
    child.setFunctionName(String.format(Locale.ROOT, factory.getFunctionName(getClass())));
    child.setImplementingClass(getClass().getName());
    child.setExpressionType(ExpressionType.STREAM_DECORATOR);
    child.setExpression(toExpression(factory, false).toString());
    child.addChild(tupleSource.toExplanation(factory));
    
    explanation.addChild(child);
    
    return explanation;
  }
  
  @Override
  public void setStreamContext(StreamContext context) {
    if(null != context.getSolrClientCache()){
      this.clientCache = context.getSolrClientCache();
        // this overrides the one created in open
    }
    
    this.tupleSource.setStreamContext(context);
  }
  
  private String findZkHost(StreamFactory factory, String collectionName, StreamExpression expression) {
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");
    if(null == zkHostExpression){
      String zkHost = factory.getCollectionZkHost(collectionName);
      if(zkHost == null) {
        return factory.getDefaultZkHost();
      } else {
        return zkHost;
      }
    } else if(zkHostExpression.getParameter() instanceof StreamExpressionValue){
      return ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }
    
    return null;
  }
  
  private void sendCommit() throws IOException {
    
    try {
      clientCache.getCloudSolrClient(zkHost).commit(collection, waitFlush, waitSearcher, softCommit);
    } catch (SolrServerException | IOException e) {
      log.warn(String.format(Locale.ROOT, "Unable to commit documents to collection '%s' due to unexpected error.", collection), e);
      String className = e.getClass().getName();
      String message = e.getMessage();
      throw new IOException(String.format(Locale.ROOT,"Unexpected error when committing documents to collection %s- %s:%s", collection, className, message));
    }
  }
}
