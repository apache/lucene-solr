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
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.params.ModifiableSolrParams;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.SORT;

/**
 * The ParallelStream decorates a TupleStream implementation and pushes it to N workers for parallel execution.
 * Workers are chosen from a SolrCloud collection.
 * Tuples that are streamed back from the workers are ordered by a Comparator.
 * @since 5.1.0
 **/
public class ParallelStream extends CloudSolrStream implements Expressible {

  private TupleStream tupleStream;
  private int workers;
  private transient StreamFactory streamFactory;

  public ParallelStream(String zkHost,
                        String collection,
                        TupleStream tupleStream,
                        int workers,
                        StreamComparator comp) throws IOException {
    init(zkHost,collection,tupleStream,workers,comp);
  }


  public ParallelStream(String zkHost,
                        String collection,
                        String expressionString,
                        int workers,
                        StreamComparator comp) throws IOException {
    TupleStream tStream = this.streamFactory.constructStream(expressionString);
    init(zkHost,collection, tStream, workers,comp);
  }

  public void setStreamFactory(StreamFactory streamFactory) {
    this.streamFactory = streamFactory;
  }

  public ParallelStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    StreamExpressionNamedParameter workersParam = factory.getNamedOperand(expression, "workers");
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    StreamExpressionNamedParameter sortExpression = factory.getNamedOperand(expression, SORT);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");
    
    // validate expression contains only what we want.

    if(expression.getParameters().size() != streamExpressions.size() + 3 + (null != zkHostExpression ? 1 : 0)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }

    // Workers
    if(null == workersParam || null == workersParam.getParameter() || !(workersParam.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single 'workers' parameter of type positive integer but didn't find one",expression));
    }
    String workersStr = ((StreamExpressionValue)workersParam.getParameter()).getValue();
    int workersInt = 0;
    try{
      workersInt = Integer.parseInt(workersStr);
      if(workersInt <= 0){
        throw new IOException(String.format(Locale.ROOT,"invalid expression %s - workers '%s' must be greater than 0.",expression, workersStr));
      }
    }
    catch(NumberFormatException e){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - workers '%s' is not a valid integer.",expression, workersStr));
    }    

    // Stream
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }
    
    // Sort
    if(null == sortExpression || !(sortExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'sort' parameter telling us how to join the parallel streams but didn't find one",expression));
    }
    
    // zkHost, optional - if not provided then will look into factory list to get
    String zkHost = null;
    if(null == zkHostExpression){
      zkHost = factory.getCollectionZkHost(collectionName);
      if(zkHost == null) {
        zkHost = factory.getDefaultZkHost();
      }
    }
    else if(zkHostExpression.getParameter() instanceof StreamExpressionValue){
      zkHost = ((StreamExpressionValue)zkHostExpression.getParameter()).getValue();
    }
    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }
    
    // We've got all the required items    
    TupleStream stream = factory.constructStream(streamExpressions.get(0));
    StreamComparator comp = factory.constructComparator(((StreamExpressionValue)sortExpression.getParameter()).getValue(), FieldComparator.class);
    streamFactory = factory;
    init(zkHost,collectionName,stream,workersInt,comp);
  }

  private void init(String zkHost,String collection,TupleStream tupleStream,int workers,StreamComparator comp) throws IOException{
    this.zkHost = zkHost;
    this.collection = collection;
    this.workers = workers;
    this.comp = comp;
    this.tupleStream = tupleStream;

    // requires Expressible stream and comparator
    if(! (tupleStream instanceof Expressible)){
      throw new IOException("Unable to create ParallelStream with a non-expressible TupleStream.");
    }
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // collection
    expression.addParameter(collection);
    
    // workers
    expression.addParameter(new StreamExpressionNamedParameter("workers", Integer.toString(workers)));
    
    if(includeStreams){
      if(tupleStream instanceof Expressible){
        expression.addParameter(((Expressible)tupleStream).toExpression(factory));
      }
      else{
        throw new IOException("This ParallelStream contains a non-expressible TupleStream - it cannot be converted to an expression");
      }
    }
    else{
      expression.addParameter("<stream>");
    }
        
    // sort
    expression.addParameter(new StreamExpressionNamedParameter(SORT,comp.toExpression(factory)));
    
    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));
    
    return expression;   
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());
    
    explanation.setFunctionName(factory.getFunctionName(this.getClass()));
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_DECORATOR);
    explanation.setExpression(toExpression(factory, false).toString());
    
    // add a child for each worker
    for(int idx = 0; idx < workers; ++idx){
      explanation.addChild(tupleStream.toExplanation(factory));
    }
    
    return explanation;
  }
  
  public List<TupleStream> children() {
    List<TupleStream> l = new ArrayList<>();
    l.add(tupleStream);
    return l;
  }

  public Tuple read() throws IOException {
    Tuple tuple = _read();

    if(tuple.EOF) {
      /*
      Map<String, Map> metrics = new HashMap();
      Iterator<Entry<String,Tuple>> it = this.eofTuples.entrySet().iterator();
      while(it.hasNext()) {
        Map.Entry<String, Tuple> entry = it.next();
        if(entry.getValue().fields.size() > 1) {
          metrics.put(entry.getKey(), entry.getValue().fields);
        }
      }

      if(metrics.size() > 0) {
        t.setMetrics(metrics);
      }
      */
      return Tuple.EOF();
    }

    return tuple;
  }

  public void setStreamContext(StreamContext streamContext) {
    this.streamContext = streamContext;
    if(streamFactory == null) {
      this.streamFactory = streamContext.getStreamFactory();
    }
    this.tupleStream.setStreamContext(streamContext);
  }

  protected void constructStreams() throws IOException {
    try {
      Object pushStream = ((Expressible) tupleStream).toExpression(streamFactory);

      List<String> shardUrls = getShards(this.zkHost, this.collection, this.streamContext);

      for(int w=0; w<workers; w++) {
        ModifiableSolrParams paramsLoc = new ModifiableSolrParams();
        paramsLoc.set(DISTRIB,"false"); // We are the aggregator.
        paramsLoc.set("numWorkers", workers);
        paramsLoc.set("workerID", w);

        paramsLoc.set("expr", pushStream.toString());
        paramsLoc.set("qt","/stream");

        String url = shardUrls.get(w);
        SolrStream solrStream = new SolrStream(url, paramsLoc);
        solrStream.setStreamContext(streamContext);
        solrStreams.add(solrStream);
      }

      assert(solrStreams.size() == workers);

    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
