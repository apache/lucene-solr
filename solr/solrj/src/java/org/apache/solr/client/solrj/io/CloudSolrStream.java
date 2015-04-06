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
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.SolrjNamedThreadFactory;

/**
 * Connects to Zookeeper to pick replicas from a specific collection to send the query to.
 * Under the covers the SolrStream instances send the query to the replicas.
 * SolrStreams are opened using a thread pool, but a single thread is used
 * to iterate and merge Tuples from each SolrStream.
 **/

public class CloudSolrStream extends TupleStream {

  private static final long serialVersionUID = 1;

  protected String zkHost;
  protected String collection;
  protected Map params;
  private Map<String, String> fieldMappings;
  protected Comparator<Tuple> comp;
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

  public CloudSolrStream(String zkHost, String collection, Map params) throws IOException {
    this.zkHost = zkHost;
    this.collection = collection;
    this.params = params;
    String sort = (String)params.get("sort");
    this.comp = parseComp(sort, params);
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

  private Comparator<Tuple> parseComp(String sort, Map params) throws IOException {

    String fl = (String)params.get("fl");
    String[] fls = fl.split(",");
    HashSet fieldSet = new HashSet();
    for(String f : fls) {
      fieldSet.add(f.trim()); //Handle spaces in the field list.
    }

    String[] sorts = sort.split(",");
    Comparator[] comps = new Comparator[sorts.length];
    for(int i=0; i<sorts.length; i++) {
      String s = sorts[i];
      String[] spec = s.trim().split("\\s+"); //This should take into account spaces in the sort spec.

      if(!fieldSet.contains(spec[0])) {
        throw new IOException("Fields in the sort spec must be included in the field list:"+spec[0]);
      }

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
    ExecutorService service = Executors.newCachedThreadPool(new SolrjNamedThreadFactory("CloudSolrStream"));
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