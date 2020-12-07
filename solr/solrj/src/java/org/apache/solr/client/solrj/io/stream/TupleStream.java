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

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.routing.ReplicaListTransformer;
import org.apache.solr.client.solrj.routing.RequestReplicaListTransformerGenerator;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;


/**
 * @since 5.1.0
 */
public abstract class TupleStream implements Closeable, Serializable, MapWriter {

  private static final long serialVersionUID = 1;
  
  private UUID streamNodeId = UUID.randomUUID();

  public TupleStream() {

  }
  public abstract void setStreamContext(StreamContext context);

  public abstract List<TupleStream> children();

  public abstract void open() throws IOException;

  public abstract void close() throws IOException;

  public abstract Tuple read() throws IOException;

  public abstract StreamComparator getStreamSort();
  
  public abstract Explanation toExplanation(StreamFactory factory) throws IOException;
  
  public int getCost() {
    return 0;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    open();
    ew.put("docs", (IteratorWriter) iw -> {
      try {
        for ( ; ; ) {
          Tuple tuple = read();
          if (tuple != null) {
            iw.add(tuple);
            if (tuple.EOF) {
              close();
              break;
            }
          } else {
            break;
          }
        }
      } catch (Throwable e) {
        close();
        Throwable ex = e;
        while(ex != null) {
          String m = ex.getMessage();
          if(m != null && m.contains("Broken pipe")) {
            throw new IgnoreException();
          }
          ex = ex.getCause();
        }

        if(e instanceof IOException) {
          throw e;
        } else {
          throw new IOException(e);
        }
      }
    });
  }

  public UUID getStreamNodeId(){
    return streamNodeId;
  }

  public static List<String> getShards(String zkHost,
                                       String collection,
                                       StreamContext streamContext)
      throws IOException {
    return getShards(zkHost, collection, streamContext, new ModifiableSolrParams());
  }

  static List<Replica> getReplicas(String zkHost,
                                   String collection,
                                   StreamContext streamContext,
                                   SolrParams requestParams)
      throws IOException {
    List<Replica> replicas = new LinkedList<>();

    //SolrCloud Sharding
    SolrClientCache solrClientCache = (streamContext != null ? streamContext.getSolrClientCache() : null);
    final SolrClientCache localSolrClientCache; // tracks any locally allocated cache that needs to be closed locally
    if (solrClientCache == null) { // streamContext was null OR streamContext.getSolrClientCache() returned null
      solrClientCache = localSolrClientCache = new SolrClientCache();
    } else {
      localSolrClientCache = null;
    }

    if (zkHost == null) {
      throw new IOException(String.format(Locale.ROOT,"invalid expression - zkHost not found for collection '%s'",collection));
    }

    CloudSolrClient cloudSolrClient = solrClientCache.getCloudSolrClient(zkHost);
    ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
    ClusterState clusterState = zkStateReader.getClusterState();
    Slice[] slices = CloudSolrStream.getSlices(collection, zkStateReader, true);
    Set<String> liveNodes = clusterState.getLiveNodes();

    RequestReplicaListTransformerGenerator requestReplicaListTransformerGenerator;
    final ModifiableSolrParams solrParams;
    if (streamContext != null) {
      solrParams = new ModifiableSolrParams(streamContext.getRequestParams());
      requestReplicaListTransformerGenerator = streamContext.getRequestReplicaListTransformerGenerator();
    } else {
      solrParams = new ModifiableSolrParams();
      requestReplicaListTransformerGenerator = null;
    }
    if (requestReplicaListTransformerGenerator == null) {
      requestReplicaListTransformerGenerator = new RequestReplicaListTransformerGenerator();
    }
    solrParams.add(requestParams);

    ReplicaListTransformer replicaListTransformer = requestReplicaListTransformerGenerator.getReplicaListTransformer(solrParams);

    List<Replica> sortedReplicas = new ArrayList<>();
    for(Slice slice : slices) {
      slice.getReplicas().stream().filter(r -> r.isActive(liveNodes)).forEach(sortedReplicas::add);
      replicaListTransformer.transform(sortedReplicas);
      if (sortedReplicas.size() > 0) {
        replicas.add(sortedReplicas.get(0));
      }
      sortedReplicas.clear();
    }

    if (localSolrClientCache != null) {
      localSolrClientCache.close();
    }

    return replicas;
  }

  @SuppressWarnings({"unchecked"})
  public static List<String> getShards(String zkHost,
                                       String collection,
                                       StreamContext streamContext,
                                       SolrParams requestParams)
      throws IOException {

    List<String> shards;
    Map<String, List<String>> shardsMap = streamContext != null ? (Map<String, List<String>>)streamContext.get("shards") : null;
    if(shardsMap != null) {
      //Manual Sharding
      shards = shardsMap.get(collection);
    } else {
      shards = getReplicas(zkHost, collection, streamContext, requestParams).stream()
          .map(Replica::getCoreUrl).collect(Collectors.toList());
    }

    return shards;
  }

  public static class IgnoreException extends IOException {
    public void printStackTrace(PrintWriter pw) {
      pw.print("Early Client Disconnect");
    }

    public String getMessage() {
      return "Early Client Disconnect";
    }
  }
}
