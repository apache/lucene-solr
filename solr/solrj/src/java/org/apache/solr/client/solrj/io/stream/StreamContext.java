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

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

import org.apache.solr.client.solrj.io.ModelCache;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

/**
 *  The StreamContext is passed to TupleStreams using the TupleStream.setStreamContext() method.
 *  The StreamContext is used to pass shared context to concentrically wrapped TupleStreams.
 *
 *  Note: The StreamContext contains the SolrClientCache which is used to cache SolrClients for reuse
 *  across multiple TupleStreams.
 **/


public class StreamContext implements Serializable{

  private Map entries = new HashMap();
  private Map tupleContext = new HashMap();
  private Map<String, Object> lets = new HashMap();
  public int workerID;
  public int numWorkers;
  private SolrClientCache clientCache;
  private ModelCache modelCache;
  private StreamFactory streamFactory;

  public Map<String, Object> getLets(){
    return lets;
  }

  public Object get(Object key) {
    return entries.get(key);
  }

  public void put(Object key, Object value) {
    this.entries.put(key, value);
  }

  public boolean containsKey(Object key) {
    return entries.containsKey(key);
  }

  public Map getEntries() {
    return this.entries;
  }

  public void setSolrClientCache(SolrClientCache clientCache) {
    this.clientCache = clientCache;
  }

  public void setModelCache(ModelCache modelCache) {
    this.modelCache = modelCache;
  }

  public SolrClientCache getSolrClientCache() {
    return this.clientCache;
  }

  public ModelCache getModelCache() {
    return this.modelCache;
  }

  public void setStreamFactory(StreamFactory streamFactory) {
    this.streamFactory = streamFactory;
  }

  public Map getTupleContext() {
    return tupleContext;
  }

  public StreamFactory getStreamFactory() {
    return this.streamFactory;
  }
}