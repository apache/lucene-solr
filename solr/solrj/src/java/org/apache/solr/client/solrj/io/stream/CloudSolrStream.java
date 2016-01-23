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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;

/**
 * Connects to Zookeeper to pick replicas from a specific collection to send the query to.
 * Under the covers the SolrStream instances send the query to the replicas.
 * SolrStreams are opened using a thread pool, but a single thread is used
 * to iterate and merge Tuples from each SolrStream.
 **/

public class CloudSolrStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  protected String zkHost;
  protected String collection;
  protected Map<String,String> params;
  private Map<String, String> fieldMappings;
  protected StreamComparator comp;
  private int zkConnectTimeout = 10000;
  private int zkClientTimeout = 10000;
  private int numWorkers;
  private int workerID;
  private boolean trace;
  protected transient Map<String, Tuple> eofTuples;
  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;
  protected transient List<TupleStream> solrStreams;
  protected transient TreeSet<TupleWrapper> tuples;
  protected transient StreamContext streamContext;

  // Used by parallel stream
  protected CloudSolrStream(){
    
  }
  public CloudSolrStream(String zkHost, String collectionName, Map params) throws IOException {
    init(collectionName, zkHost, params);
  }

  public CloudSolrStream(StreamExpression expression, StreamFactory factory) throws IOException{   
    // grab all parameters out
    String collectionName = factory.getValueOperand(expression, 0);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter aliasExpression = factory.getNamedOperand(expression, "aliases");
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");
    
    // Validate there are no unknown parameters - zkHost and alias are namedParameter so we don't need to count it twice
    if(expression.getParameters().size() != 1 + namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - unknown operands found",expression));
    }
    
    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as first operand",expression));
    }
        
    // Named parameters - passed directly to solr as solrparams
    if(0 == namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
    }
    
    Map<String,String> params = new HashMap<String,String>();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") && !namedParam.getName().equals("aliases")){
        params.put(namedParam.getName(), namedParam.getParameter().toString().trim());
      }
    }

    // Aliases, optional, if provided then need to split
    if(null != aliasExpression && aliasExpression.getParameter() instanceof StreamExpressionValue){
      fieldMappings = new HashMap<String,String>();
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
    if(null == zkHost){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - zkHost not found for collection '%s'",expression,collectionName));
    }
    
    // We've got all the required items
    init(collectionName, zkHost, params);
  }
  
  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    // functionName(collectionName, param1, param2, ..., paramN, sort="comp", [aliases="field=alias,..."])
    
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // collection
    expression.addParameter(collection);
    
    // parameters
    for(Entry<String,String> param : params.entrySet()){
      expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), param.getValue()));
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
  
  private void init(String collectionName, String zkHost, Map params) throws IOException {
    this.zkHost = zkHost;
    this.collection = collectionName;
    this.params = params;

    // If the comparator is null then it was not explicitly set so we will create one using the sort parameter
    // of the query. While doing this we will also take into account any aliases such that if we are sorting on
    // fieldA but fieldA is aliased to alias.fieldA then the comparater will be against alias.fieldA.
    if(!params.containsKey("fl")){
      throw new IOException("fl param expected for a stream");
    }
    if(!params.containsKey("sort")){
      throw new IOException("sort param expected for a stream");
    }
    this.comp = parseComp((String)params.get("sort"), (String)params.get("fl")); 
  }
  
  public void setFieldMappings(Map<String, String> fieldMappings) {
    this.fieldMappings = fieldMappings;
  }

  public void setTrace(boolean trace) {
    this.trace = trace;
  }

  public void setStreamContext(StreamContext context) {
    this.numWorkers = context.numWorkers;
    this.workerID = context.workerID;
    this.cache = context.getSolrClientCache();
    this.streamContext = context;
  }

  /**
  * Opens the CloudSolrStream
  *
  ***/
  public void open() throws IOException {
    this.tuples = new TreeSet();
    this.solrStreams = new ArrayList();
    this.eofTuples = Collections.synchronizedMap(new HashMap());
    if(this.cache != null) {
      this.cloudSolrClient = this.cache.getCloudSolrClient(zkHost);
    } else {
      this.cloudSolrClient = new CloudSolrClient(zkHost);
      this.cloudSolrClient.connect();
    }
    constructStreams();
    openStreams();
  }


  public Map getEofTuples() {
    return this.eofTuples;
  }

  public List<TupleStream> children() {
    return solrStreams;
  }

  private StreamComparator parseComp(String sort, String fl) throws IOException {

    String[] fls = fl.split(",");
    HashSet fieldSet = new HashSet();
    for(String f : fls) {
      fieldSet.add(f.trim()); //Handle spaces in the field list.
    }

    String[] sorts = sort.split(",");
    StreamComparator[] comps = new StreamComparator[sorts.length];
    for(int i=0; i<sorts.length; i++) {
      String s = sorts[i];

      String[] spec = s.trim().split("\\s+"); //This should take into account spaces in the sort spec.
      
      String fieldName = spec[0].trim();
      String order = spec[1].trim();
      
      if(!fieldSet.contains(spec[0])) {
        throw new IOException("Fields in the sort spec must be included in the field list:"+spec[0]);
      }
      
      // if there's an alias for the field then use the alias
      if(null != fieldMappings && fieldMappings.containsKey(fieldName)){
        fieldName = fieldMappings.get(fieldName);
      }
      
      comps[i] = new FieldComparator(fieldName, order.equalsIgnoreCase("asc") ? ComparatorOrder.ASCENDING : ComparatorOrder.DESCENDING);
    }

    if(comps.length > 1) {
      return new MultipleFieldComparator(comps);
    } else {
      return comps[0];
    }
  }

  protected void constructStreams() throws IOException {

    try {

      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();

      //System.out.println("Connected to zk an got cluster state.");

      Collection<Slice> slices = clusterState.getActiveSlices(this.collection);

      if(slices == null) {

        String colLower = this.collection.toLowerCase(Locale.getDefault());
        //Try case insensitive match
        for(String col : clusterState.getCollections()) {
          if(col.toLowerCase(Locale.getDefault()).equals(colLower)) {
            slices = clusterState.getActiveSlices(col);
            break;
          }
        }

        if(slices == null) {
          throw new Exception("Collection not found:" + this.collection);
        }
      }

      params.put("distrib","false"); // We are the aggregator.

      for(Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        List<Replica> shuffler = new ArrayList();
        for(Replica replica : replicas) {
          shuffler.add(replica);
        }

        Collections.shuffle(shuffler, new Random());
        Replica rep = shuffler.get(0);
        ZkCoreNodeProps zkProps = new ZkCoreNodeProps(rep);
        String url = zkProps.getCoreUrl();
        SolrStream solrStream = new SolrStream(url, params);
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
    ExecutorService service = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("CloudSolrStream"));
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

  /**
   *  Closes the CloudSolrStream
   **/
  public void close() throws IOException {
    if(solrStreams != null) {
      for (TupleStream solrStream : solrStreams) {
        solrStream.close();
      }
    }

    if(cache == null && cloudSolrClient != null) {
      cloudSolrClient.close();
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
    TupleWrapper tw = tuples.pollFirst();
    if(tw != null) {
      Tuple t = tw.getTuple();

      if (trace) {
        t.put("_COLLECTION_", this.collection);
      }

      if(tw.next()) {
        tuples.add(tw);
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
}