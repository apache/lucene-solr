package org.apache.solr.client.solrj.io.stream;

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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicStream extends CloudSolrStream implements Expressible  {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  private static final long serialVersionUID = 1;

  private long count;
  private String id;
  protected long checkpointEvery;

  private Map<String, Long> checkpoints = new HashMap();
  private String checkpointCollection;

  public TopicStream(String zkHost,
                     String checkpointCollection,
                     String collection,
                     String id,
                     long checkpointEvery,
                     Map<String, String> params) {
    init(zkHost,
         checkpointCollection,
         collection,
         id,
         checkpointEvery,
         params);
  }

  private void init(String zkHost,
                    String checkpointCollection,
                    String collection,
                    String id,
                    long checkpointEvery,
                    Map<String, String> params) {
    this.zkHost  = zkHost;
    this.params  = params;
    this.collection = collection;
    this.checkpointCollection = checkpointCollection;
    this.checkpointEvery = checkpointEvery;
    this.id = id;
    this.comp = new FieldComparator("_version_", ComparatorOrder.ASCENDING);
  }

  public TopicStream(StreamExpression expression, StreamFactory factory) throws IOException{
    // grab all parameters out
    String checkpointCollectionName = factory.getValueOperand(expression, 0);
    String collectionName = factory.getValueOperand(expression, 1);
    List<StreamExpressionNamedParameter> namedParams = factory.getNamedOperands(expression);
    StreamExpressionNamedParameter zkHostExpression = factory.getNamedOperand(expression, "zkHost");

    StreamExpressionNamedParameter idParam = factory.getNamedOperand(expression, "id");
    if(null == idParam) {
      throw new IOException("invalid TopicStream id cannot be null");
    }

    StreamExpressionNamedParameter flParam = factory.getNamedOperand(expression, "fl");

    if(null == flParam) {
      throw new IOException("invalid TopicStream fl cannot be null");
    }

    long checkpointEvery = -1;
    StreamExpressionNamedParameter checkpointEveryParam = factory.getNamedOperand(expression, "checkpointEvery");

    if(checkpointEveryParam != null) {
      checkpointEvery = Long.parseLong(((StreamExpressionValue) checkpointEveryParam.getParameter()).getValue());
    }

    //  Checkpoint Collection Name
    if(null == checkpointCollectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - checkpointCollectionName expected as first operand",expression));
    }

    // Collection Name
    if(null == collectionName){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - collectionName expected as second operand",expression));
    }

    // Named parameters - passed directly to solr as solrparams
    if(0 == namedParams.size()){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - at least one named parameter expected. eg. 'q=*:*'",expression));
    }

    Map<String,String> params = new HashMap<String,String>();
    for(StreamExpressionNamedParameter namedParam : namedParams){
      if(!namedParam.getName().equals("zkHost") &&
          !namedParam.getName().equals("id") &&
          !namedParam.getName().equals("checkpointEvery")) {
        params.put(namedParam.getName(), namedParam.getParameter().toString().trim());
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
    init(zkHost,
        checkpointCollectionName,
        collectionName,
        ((StreamExpressionValue) idParam.getParameter()).getValue(),
        checkpointEvery,
        params);
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));

    expression.addParameter(checkpointCollection);
    // collection
    expression.addParameter(collection);

    for(Entry<String,String> param : params.entrySet()) {
      String value = param.getValue();

      // SOLR-8409: This is a special case where the params contain a " character
      // Do note that in any other BASE streams with parameters where a " might come into play
      // that this same replacement needs to take place.
      value = value.replace("\"", "\\\"");

      expression.addParameter(new StreamExpressionNamedParameter(param.getKey(), value));
    }

    // zkHost
    expression.addParameter(new StreamExpressionNamedParameter("zkHost", zkHost));
    expression.addParameter(new StreamExpressionNamedParameter("id", id));
    expression.addParameter(new StreamExpressionNamedParameter("checkpointEvery", Long.toString(checkpointEvery)));

    return expression;
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    return l;
  }

  public void open() throws IOException {
    this.tuples = new TreeSet();
    this.solrStreams = new ArrayList();
    this.eofTuples = Collections.synchronizedMap(new HashMap());

    if(cache != null) {
      cloudSolrClient = cache.getCloudSolrClient(zkHost);
    } else {
      cloudSolrClient = new CloudSolrClient(zkHost);
      this.cloudSolrClient.connect();
    }

    if(checkpoints.size() == 0) {
      getPersistedCheckpoints();
      if(checkpoints.size() == 0) {
        getCheckpoints();
      }
    }

    constructStreams();
    openStreams();
  }


  private void openStreams() throws IOException {

    ExecutorService service = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("TopicStream"));
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
    try {
      persistCheckpoints();
    } finally {

      if(solrStreams != null) {
        for (TupleStream solrStream : solrStreams) {
          solrStream.close();
        }
      }

      if (cache == null) {
        cloudSolrClient.close();
      }
    }
  }

  public Tuple read() throws IOException {
    Tuple tuple = _read();

    if(tuple.EOF) {
      return tuple;
    }

    ++count;
    if(checkpointEvery > -1 && (count % checkpointEvery) == 0) {
      persistCheckpoints();
    }

    long version = tuple.getLong("_version_");
    String slice = tuple.getString("_SLICE_");
    checkpoints.put(slice, version);

    tuple.remove("_SLICE_");
    tuple.remove("_CORE_");

    return tuple;
  }

  public int getCost() {
    return 0;
  }

  private void getCheckpoints() throws IOException {
    this.checkpoints = new HashMap();
    ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
    ClusterState clusterState = zkStateReader.getClusterState();
    Collection<Slice> slices = clusterState.getActiveSlices(collection);

    for(Slice slice : slices) {
      String sliceName = slice.getName();
      long checkpoint = getCheckpoint(slice, clusterState.getLiveNodes());
      this.checkpoints.put(sliceName, checkpoint);
    }
  }

  //Gets the highest version number for the slice.
  private long getCheckpoint(Slice slice, Set<String> liveNodes) throws IOException {
    Collection<Replica> replicas = slice.getReplicas();
    long checkpoint = -1;
    Map params = new HashMap();
    params.put("q","*:*");
    params.put("sort", "_version_ desc");
    params.put("distrib", "false");
    params.put("rows", 1);
    for(Replica replica : replicas) {
      if(replica.getState() == Replica.State.ACTIVE && liveNodes.contains(replica.getNodeName())) {
        String coreUrl = replica.getCoreUrl();
        SolrStream solrStream = new SolrStream(coreUrl, params);

        if(streamContext != null) {
          solrStream.setStreamContext(streamContext);
        }

        try {
          solrStream.open();
          Tuple tuple = solrStream.read();
          if(tuple.EOF) {
            return 0;
          } else {
            checkpoint = tuple.getLong("_version_");
          }
          break;
        } finally {
          solrStream.close();
        }
      }
    }
    return checkpoint;
  }

  private void persistCheckpoints() throws IOException{

    UpdateRequest request = new UpdateRequest();
    request.setParam("collection", checkpointCollection);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", id);

    for(Map.Entry<String, Long> entry : checkpoints.entrySet()) {
      doc.addField("checkpoint_ss", entry.getKey()+"~"+entry.getValue());
    }

    request.add(doc);
    try {
      cloudSolrClient.request(request);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void getPersistedCheckpoints() throws IOException {

    ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
    ClusterState clusterState = zkStateReader.getClusterState();
    Collection<Slice> slices = clusterState.getActiveSlices(checkpointCollection);
    Set<String> liveNodes = clusterState.getLiveNodes();
    OUTER:
    for(Slice slice : slices) {
      Collection<Replica> replicas = slice.getReplicas();
      for(Replica replica : replicas) {
        if(replica.getState() == Replica.State.ACTIVE && liveNodes.contains(replica.getNodeName())){


          HttpSolrClient httpClient = cache.getHttpSolrClient(replica.getCoreUrl());
          try {

            SolrDocument doc = httpClient.getById(id);
            if(doc != null) {
              List<String> checkpoints = (List<String>)doc.getFieldValue("checkpoint_ss");
              for (String checkpoint : checkpoints) {
                String[] pair = checkpoint.split("~");
                this.checkpoints.put(pair[0], Long.parseLong(pair[1]));
              }
            }
          }catch (Exception e) {
            throw new IOException(e);
          }
          break OUTER;
        }
      }
    }
  }

  protected void constructStreams() throws IOException {

    try {

      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      Set<String> liveNodes = clusterState.getLiveNodes();
      //System.out.println("Connected to zk an got cluster state.");

      Collection<Slice> slices = clusterState.getActiveSlices(this.collection);

      if(slices == null) {
        //Try case insensitive match
        for(String col : clusterState.getCollections()) {
          if(col.equalsIgnoreCase(collection)) {
            slices = clusterState.getActiveSlices(col);
            break;
          }
        }

        if(slices == null) {
          throw new Exception("Collection not found:" + this.collection);
        }
      }

      params.put("distrib", "false"); // We are the aggregator.
      String fl = params.get("fl");
      params.put("sort", "_version_ asc");
      fl += ",_version_";
      params.put("fl", fl);

      Random random = new Random();

      for(Slice slice : slices) {
        Map localParams = new HashMap();
        localParams.putAll(params);
        long checkpoint = checkpoints.get(slice.getName());

        Collection<Replica> replicas = slice.getReplicas();
        List<Replica> shuffler = new ArrayList();
        for(Replica replica : replicas) {
          if(replica.getState() == Replica.State.ACTIVE && liveNodes.contains(replica.getNodeName()))
            shuffler.add(replica);
        }

        Replica rep = shuffler.get(random.nextInt(shuffler.size()));
        ZkCoreNodeProps zkProps = new ZkCoreNodeProps(rep);
        String url = zkProps.getCoreUrl();
        SolrStream solrStream = new SolrStream(url, localParams);
        solrStream.setSlice(slice.getName());
        solrStream.setCheckpoint(checkpoint);
        solrStream.setTrace(true);
        if(streamContext != null) {
          solrStream.setStreamContext(streamContext);
        }
        solrStreams.add(solrStream);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}