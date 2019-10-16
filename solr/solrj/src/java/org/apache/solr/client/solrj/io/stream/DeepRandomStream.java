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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.Random;
import java.util.LinkedList;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
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
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.common.params.CommonParams.ROWS;
import static org.apache.solr.common.params.CommonParams.SORT;

/**
 * Connects to Zookeeper to pick replicas from a specific collection to send the query to.
 * Under the covers the SolrStream instances send the query to the replicas.
 * SolrStreams are opened using a thread pool, but a single thread is used
 * to iterate and merge Tuples from each SolrStream.
 * @since 5.1.0
 **/

public class DeepRandomStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  protected String zkHost;
  protected String collection;
  protected ModifiableSolrParams params;
  protected Map<String, String> fieldMappings;
  protected StreamComparator comp;
  private boolean trace;
  protected transient Map<String, Tuple> eofTuples;
  protected transient CloudSolrClient cloudSolrClient;
  protected transient List<TupleStream> solrStreams;
  protected transient LinkedList<TupleWrapper> tuples;
  protected transient StreamContext streamContext;


  public DeepRandomStream() {
    //Used by the RandomFacadeStream
  }

  /**
   * @param zkHost         Zookeeper ensemble connection string
   * @param collectionName Name of the collection to operate on
   * @param params         Map&lt;String, String[]&gt; of parameter/value pairs
   * @throws IOException Something went wrong
   */
  public DeepRandomStream(String zkHost, String collectionName, SolrParams params) throws IOException {
    init(collectionName, zkHost, params);
  }

  public DeepRandomStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter aliasExpression = factory.getNamedOperand(expression, "aliases");
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }

    // Validate there are no unknown parameters - zkHost and alias are namedParameter so we don't need to count it twice
    if(expression.getParameters().size() != 1 + namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - unknown operands found",expression));
    }

    // Named parameters - passed directly to solr as solrparams
    if(0 == namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
    }

    ModifiableSolrParams mParams = new ModifiableSolrParams();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") && !namedParam.getName().equals("aliases")){
        mParams.add(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    // Aliases, optional, if provided then need to split
    if(null != aliasExpression && aliasExpression.getParameter() instanceof StreamExpressionValue){
      fieldMappings = new HashMap<>();
      for(String mapping : ((StreamExpressionValue)aliasExpression.getParameter()).getValue().split(",")){
        String[] parts = mapping.trim().split("=");
        if(2 == parts.length){
          fieldMappings.put(parts[0], parts[1]);
        }
        else{
          throw new IOException(String.format(Locale.ROOT,"invalid expression %s - alias expected of the format origName=newName",expression));
        }
      }
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
    /*
    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }
    */

    // We've got all the required items
    init(collectionName, zkHost, mParams);
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException {

    // function name
    StreamExpression expression = new StreamExpression("random");

    // collection
    if(collection.indexOf(',') > -1) {
      expression.addParameter("\""+collection+"\"");
    } else {
      expression.addParameter(collection);
    }

    for (Entry<String, String[]> param : params.getMap().entrySet()) {
      for (String val : param.getValue()) {
        // SOLR-8409: Escaping the " is a special case.
        // Do note that in any other BASE streams with parameters where a " might come into play
        // that this same replacement needs to take place.
        expression.addParameter(new StreamExpressionNamedParameter(param.getKey(),
            val.replace("\"", "\\\"")));
      }
    }

    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));

    // aliases
    if(null != fieldMappings && 0 != fieldMappings.size()){
      StringBuilder sb = new StringBuilder();
      for(Entry<String,String> mapping : fieldMappings.entrySet()){
        if(sb.length() > 0){ sb.append(","); }
        sb.append(mapping.getKey());
        sb.append("=");
        sb.append(mapping.getValue());
      }

      expression.addParameter(new StreamExpressionNamedParameter("aliases", sb.toString()));
    }

    return expression;
  }

  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    StreamExplanation explanation = new StreamExplanation(getStreamNodeId().toString());

    explanation.setFunctionName("random");
    explanation.setImplementingClass(this.getClass().getName());
    explanation.setExpressionType(ExpressionType.STREAM_SOURCE);
    explanation.setExpression(toExpression(factory).toString());

    // child is a datastore so add it at this point
    StreamExplanation child = new StreamExplanation(getStreamNodeId() + "-datastore");
    child.setFunctionName(String.format(Locale.ROOT, "solr (%s)", collection));
    child.setImplementingClass("Solr/Lucene");
    child.setExpressionType(ExpressionType.DATASTORE);

    if(null != params){
      ModifiableSolrParams mParams = new ModifiableSolrParams(params);
      child.setExpression(mParams.getMap().entrySet().stream().map(e -> String.format(Locale.ROOT, "%s=%s", e.getKey(), e.getValue())).collect(Collectors.joining(",")));
    }
    explanation.addChild(child);

    return explanation;
  }

  void init(String collectionName, String zkHost, SolrParams params) throws IOException {
    this.zkHost = zkHost;
    this.collection = collectionName;
    this.params = new ModifiableSolrParams(params);



    if (params.get("q") == null) {
      throw new IOException("q param expected for search function");
    }

    if (params.getParams("fl") == null) {
      throw new IOException("fl param expected for search function");
    }

  }

  public void setFieldMappings(Map<String, String> fieldMappings) {
    this.fieldMappings = fieldMappings;
  }

  public void setTrace(boolean trace) {
    this.trace = trace;
  }

  public void setStreamContext(StreamContext context) {
    this.streamContext = context;
  }

  public void open() throws IOException {
    this.tuples = new LinkedList();
    this.solrStreams = new ArrayList();
    this.eofTuples = Collections.synchronizedMap(new HashMap());
    constructStreams();
    openStreams();
  }

  public List<TupleStream> children() {
    return solrStreams;
  }

  public static Slice[] getSlices(String collectionName, ZkStateReader zkStateReader, boolean checkAlias) throws IOException {
    ClusterState clusterState = zkStateReader.getClusterState();

    Map<String, DocCollection> collectionsMap = clusterState.getCollectionsMap();

    //TODO we should probably split collection by comma to query more than one
    //  which is something already supported in other parts of Solr

    // check for alias or collection

    List<String> allCollections = new ArrayList();
    String[] collectionNames = collectionName.split(",");
    for(String col : collectionNames) {
      List<String> collections = checkAlias
          ? zkStateReader.getAliases().resolveAliases(col)  // if not an alias, returns collectionName
          : Collections.singletonList(collectionName);
      allCollections.addAll(collections);
    }

    // Lookup all actives slices for these collections
    List<Slice> slices = allCollections.stream()
        .map(collectionsMap::get)
        .filter(Objects::nonNull)
        .flatMap(docCol -> Arrays.stream(docCol.getActiveSlicesArr()))
        .collect(Collectors.toList());
    if (!slices.isEmpty()) {
      return slices.toArray(new Slice[slices.size()]);
    }

    // Check collection case insensitive
    for(Entry<String, DocCollection> entry : collectionsMap.entrySet()) {
      if(entry.getKey().equalsIgnoreCase(collectionName)) {
        return entry.getValue().getActiveSlicesArr();
      }
    }

    throw new IOException("Slices not found for " + collectionName);
  }

  protected void constructStreams() throws IOException {
    try {

      List<String> shardUrls = getShards(this.zkHost, this.collection, this.streamContext);

      ModifiableSolrParams mParams = new ModifiableSolrParams(params);
      mParams = adjustParams(mParams);
      mParams.set(DISTRIB, "false"); // We are the aggregator.
      String rows = mParams.get(ROWS);
      int r = Integer.parseInt(rows);
      int newRows = r/shardUrls.size();
      mParams.set(ROWS, Integer.toString(newRows));
      int seed = new Random().nextInt();
      mParams.set(SORT, "random_"+Integer.toString(seed)+" asc");

      int remainder = r - newRows*shardUrls.size();
      for(String shardUrl : shardUrls) {
        ModifiableSolrParams useParams = null;

        if(solrStreams.size() == 0 && remainder > 0) {
          useParams = new ModifiableSolrParams(mParams);
          useParams.set(ROWS, newRows+remainder);
        } else {
          useParams = mParams;
        }

        SolrStream solrStream = new SolrStream(shardUrl, useParams);
        if(streamContext != null) {
          solrStream.setStreamContext(streamContext);
        }
        solrStream.setFieldMappings(this.fieldMappings);
        solrStreams.add(solrStream);

      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void openStreams() throws IOException {
    ExecutorService service = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("DeepRandomStream"));
    try {
      List<Future<TupleWrapper>> futures = new ArrayList();
      for (TupleStream solrStream : solrStreams) {
        StreamOpener so = new StreamOpener((SolrStream) solrStream, comp);
        Future<TupleWrapper> future = service.submit(so);
        futures.add(future);
      }

      try {
        for (Future<TupleWrapper> f : futures) {
          TupleWrapper w = f.get();
          if (w != null) {
            tuples.add(w);
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    } finally {
      service.shutdown();
    }
  }


  public void close() throws IOException {
    if(solrStreams != null) {
      for (TupleStream solrStream : solrStreams) {
        solrStream.close();
      }
    }
  }

  /** Return the stream sort - ie, the order in which records are returned */
  public StreamComparator getStreamSort(){
    return comp;
  }

  public Tuple read() throws IOException {
    return _read();
  }

  protected Tuple _read() throws IOException {
    if(tuples.size() > 0) {
      TupleWrapper tw = tuples.removeFirst();
      Tuple t = tw.getTuple();

      if (trace) {
        t.put("_COLLECTION_", this.collection);
      }

      if(tw.next()) {
        tuples.addLast(tw);
      }
      return t;
    } else {
      Map m = new HashMap();
      if(trace) {
        m.put("_COLLECTION_", this.collection);
      }

      m.put("EOF", true);

      return new Tuple(m);
    }
  }

  protected class TupleWrapper implements Comparable<TupleWrapper> {
    private Tuple tuple;
    private SolrStream stream;
    private StreamComparator comp;

    public TupleWrapper(SolrStream stream, StreamComparator comp) {
      this.stream = stream;
      this.comp = comp;
    }

    public int compareTo(TupleWrapper w) {
      if(this == w) {
        return 0;
      }

      int i = comp.compare(tuple, w.tuple);
      if(i == 0) {
        return 1;
      } else {
        return i;
      }
    }

    public boolean equals(Object o) {
      return this == o;
    }

    public Tuple getTuple() {
      return tuple;
    }

    public boolean next() throws IOException {
      this.tuple = stream.read();

      if(tuple.EOF) {
        eofTuples.put(stream.getBaseUrl(), tuple);
      }

      return !tuple.EOF;
    }
  }

  protected class StreamOpener implements Callable<TupleWrapper> {

    private SolrStream stream;
    private StreamComparator comp;

    public StreamOpener(SolrStream stream, StreamComparator comp) {
      this.stream = stream;
      this.comp = comp;
    }

    public TupleWrapper call() throws Exception {
      stream.open();
      TupleWrapper wrapper = new TupleWrapper(stream, comp);
      if(wrapper.next()) {
        return wrapper;
      } else {
        return null;
      }
    }
  }

  protected ModifiableSolrParams adjustParams(ModifiableSolrParams params) {
    return params;
  }
}
