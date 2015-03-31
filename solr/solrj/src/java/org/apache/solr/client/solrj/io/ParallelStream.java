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
import java.io.ObjectOutputStream;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.TreeSet;
import java.util.Iterator;

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


public class ParallelStream extends CloudSolrStream {

  private TupleStream tupleStream;
  private int workers;
  private String encoded;

  public ParallelStream(String zkHost,
                        String collection,
                        TupleStream tupleStream,
                        int workers,
                        Comparator<Tuple> comp) throws IOException {
    this.zkHost = zkHost;
    this.collection = collection;
    this.workers = workers;
    this.comp = comp;
    this.tupleStream = tupleStream;
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bout);
    out.writeObject(tupleStream);
    byte[] bytes = bout.toByteArray();
    this.encoded = Base64.byteArrayToBase64(bytes, 0, bytes.length);
    this.encoded = URLEncoder.encode(this.encoded, "UTF-8");
    this.tuples = new TreeSet();
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
      t.setMetrics(this.eofTuples);
      return t;
    }

    return tuple;
  }

  public void setStreamContext(StreamContext streamContext) {
    //Note the parallel stream does not set the StreamContext on it's substream.
    //This is because the substream is not actually opened by the ParallelStream.
    this.streamContext = streamContext;
  }

  protected void constructStreams() throws IOException {

    try {
      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      Collection<Slice> slices = clusterState.getActiveSlices(this.collection);
      long time = System.currentTimeMillis();
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

      Collections.shuffle(shuffler, new Random(time));

      for(int w=0; w<workers; w++) {
        HashMap params = new HashMap();
        params.put("distrib","false"); // We are the aggregator.
        params.put("numWorkers", workers);
        params.put("workerID", w);
        params.put("stream", this.encoded);
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
