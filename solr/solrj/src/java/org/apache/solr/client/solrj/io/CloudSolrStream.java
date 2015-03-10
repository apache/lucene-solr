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

package org.apache.solr.client.solrj.io;

import java.io.IOException;
import java.util.Map;
import java.util.Comparator;
import java.util.Random;
import java.util.TreeSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.zookeeper.KeeperException;

/**
* Connects to Zookeeper to pick replicas from a specific collection to send the query to.
* SolrStream instances are used to send the query to the replicas.
* SolrStreams are opened using a Thread pool, but a single thread is used to iterate through each stream's tuples.* *
**/

public class CloudSolrStream extends TupleStream {

  private static final long serialVersionUID = 1;

  protected String zkHost;
  protected String collection;
  protected Map params;
  private Map<String, String> fieldMappings;
  protected TreeSet<TupleWrapper> tuples;
  protected Comparator<Tuple> comp;
  protected List<TupleStream> solrStreams = new ArrayList();
  private int zkConnectTimeout = 10000;
  private int zkClientTimeout = 10000;
  protected transient SolrClientCache cache;
  protected transient CloudSolrClient cloudSolrClient;
  private int numWorkers;
  private int workerID;
  protected Map<String, Tuple> eofTuples = new HashMap();

  public CloudSolrStream(String zkHost, String collection, Map params) {
    this.zkHost = zkHost;
    this.collection = collection;
    this.params = params;
    this.tuples = new TreeSet();
    String sort = (String)params.get("sort");
    this.comp = parseComp(sort);
  }

  //Used by the ParallelStream
  protected CloudSolrStream() {

  }

  public void setComp(Comparator<Tuple> comp) {
    this.comp = comp;
  }

  public void setFieldMappings(Map<String, String> fieldMappings) {
    this.fieldMappings = fieldMappings;
  }

  public void setStreamContext(StreamContext context) {
    this.numWorkers = context.numWorkers;
    this.workerID = context.workerID;
    this.cache = context.clientCache;
  }

  public void open() throws IOException {
    if(this.cache != null) {
      this.cloudSolrClient = this.cache.getCloudSolrClient(zkHost);
    } else {
      this.cloudSolrClient = new CloudSolrClient(zkHost);
      this.cloudSolrClient.connect();
    }
    constructStreams();
    openStreams();
  }



  public List<TupleStream> children() {
    return solrStreams;
  }

  private Comparator<Tuple> parseComp(String sort) {
    String[] sorts = sort.split(",");
    Comparator[] comps = new Comparator[sorts.length];
    for(int i=0; i<sorts.length; i++) {
      String s = sorts[i];
      String[] spec = s.split(" ");
      if(spec[1].trim().equalsIgnoreCase("asc")) {
        comps[i] = new AscFieldComp(spec[0]);
      } else {
        comps[i] = new DescFieldComp(spec[0]);
      }
    }

    if(comps.length > 1) {
      return new MultiComp(comps);
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
      long time = System.currentTimeMillis();
      params.put("distrib","false"); // We are the aggregator.

      for(Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        List<Replica> shuffler = new ArrayList();
        for(Replica replica : replicas) {
          shuffler.add(replica);
        }

        Collections.shuffle(shuffler, new Random(time));
        Replica rep = shuffler.get(0);
        ZkCoreNodeProps zkProps = new ZkCoreNodeProps(rep);
        String url = zkProps.getCoreUrl();
        SolrStream solrStream = new SolrStream(url, params);
        StreamContext context = new StreamContext();
        context.numWorkers = this.numWorkers;
        context.workerID = this.workerID;
        solrStream.setStreamContext(context);
        solrStream.setFieldMappings(this.fieldMappings);
        solrStreams.add(solrStream);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void openStreams() throws IOException {
    ExecutorService service = Executors.newCachedThreadPool(new SolrjNamedThreadFactory("CloudSolrStream"));
    List<Future<TupleWrapper>> futures = new ArrayList();
    for(TupleStream solrStream : solrStreams) {
      StreamOpener so = new StreamOpener((SolrStream)solrStream, comp);
      Future<TupleWrapper> future =  service.submit(so);
      futures.add(future);
    }

    try {
      for(Future<TupleWrapper> f : futures) {
        TupleWrapper w = f.get();
        if(w != null) {
          tuples.add(w);
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    service.shutdown();
  }

  public void close() throws IOException {
    for(TupleStream solrStream : solrStreams) {
      solrStream.close();
    }

    if(cache == null) {
      cloudSolrClient.close();
    }
  }

  public Tuple read() throws IOException {
    return _read();
  }

  protected Tuple _read() throws IOException {
    TupleWrapper tw = tuples.pollFirst();
    if(tw != null) {
      Tuple t = tw.getTuple();
      if(tw.next()) {
        tuples.add(tw);
      }
      return t;
    } else {
      Map m = new HashMap();
      m.put("EOF", true);

      return new Tuple(m);
    }
  }

  protected class TupleWrapper implements Comparable<TupleWrapper> {
    private Tuple tuple;
    private SolrStream stream;
    private Comparator comp;

    public TupleWrapper(SolrStream stream, Comparator comp) {
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
    private Comparator<Tuple> comp;

    public StreamOpener(SolrStream stream, Comparator<Tuple> comp) {
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