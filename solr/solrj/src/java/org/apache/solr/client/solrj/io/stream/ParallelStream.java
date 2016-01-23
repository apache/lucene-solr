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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Base64;

/**
 * The ParallelStream decorates a TupleStream implementation and pushes it to N workers for parallel execution.
 * Workers are chosen from a SolrCloud collection.
 * Tuples that are streamed back from the workers are ordered by a Comparator.
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
    StreamExpressionNamedParameter sortExpression = factory.getNamedOperand(expression, "sort");
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
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single 'workersParam' parameter of type positive integer but didn't find one",expression));
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
  public StreamExpression toExpression(StreamFactory factory) throws IOException {    

    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // collection
    expression.addParameter(collection);
    
    // workers
    expression.addParameter(new StreamExpressionNamedParameter("workers", Integer.toString(workers)));
    
    // stream
    if(tupleStream instanceof Expressible){
      expression.addParameter(((Expressible)tupleStream).toExpression(factory));
    }
    else{
      throw new IOException("This ParallelStream contains a non-expressible TupleStream - it cannot be converted to an expression");
    }
        
    // sort
    expression.addParameter(new StreamExpressionNamedParameter("sort",comp.toExpression(factory)));
    
    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));
    
    return expression;   
  }
  
  public List<TupleStream> children() {
    List l = new ArrayList();
    l.add(tupleStream);
    return l;
  }

  public Tuple read() throws IOException {
    Tuple tuple = _read();

    if(tuple.EOF) {
      Map m = new HashMap();
      m.put("EOF", true);
      Tuple t = new Tuple(m);

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
      return t;
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

      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      Collection<Slice> slices = clusterState.getActiveSlices(this.collection);
      List<Replica> shuffler = new ArrayList();
      for(Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          shuffler.add(replica);
        }
      }

      if(workers > shuffler.size()) {
        throw new IOException("Number of workers exceeds nodes in the worker collection");
      }

      Collections.shuffle(shuffler, new Random());

      for(int w=0; w<workers; w++) {
        HashMap params = new HashMap();
        params.put("distrib","false"); // We are the aggregator.
        params.put("numWorkers", workers);
        params.put("workerID", w);
        params.put("expr", pushStream);
        params.put("qt","/stream");
        Replica rep = shuffler.get(w);
        ZkCoreNodeProps zkProps = new ZkCoreNodeProps(rep);
        String url = zkProps.getCoreUrl();
        SolrStream solrStream = new SolrStream(url, params);
        solrStreams.add(solrStream);
      }

      assert(solrStreams.size() == workers);

    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
