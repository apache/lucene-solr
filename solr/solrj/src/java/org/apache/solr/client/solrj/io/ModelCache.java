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
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.common.params.ModifiableSolrParams;

import static org.apache.solr.common.params.CommonParams.SORT;
import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;


/**
 *  The Model cache keeps a local in-memory copy of models
 */

public class ModelCache {

  private LRU models;
  private String defaultZkHost;
  private SolrClientCache solrClientCache;

  public ModelCache(int size,
                    String defaultZkHost,
                    SolrClientCache solrClientCache) {
    this.models = new LRU(size);
    this.defaultZkHost = defaultZkHost;
    this.solrClientCache = solrClientCache;
  }

  public Tuple getModel(String collection,
                        String modelID,
                        long checkMillis) throws IOException {
    return getModel(defaultZkHost, collection, modelID, checkMillis);
  }

  public Tuple getModel(String zkHost,
                        String collection,
                        String modelID,
                        long checkMillis) throws IOException {
    Model model = null;
    long currentTime = new Date().getTime();
    synchronized (this) {
      model = models.get(modelID);
      if(model != null && ((currentTime - model.getLastChecked()) <= checkMillis)) {
        return model.getTuple();
      }

      if(model != null){
        //model is expired
        models.remove(modelID);
      }
    }

    //Model is not in cache or has expired so fetch the model
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q","name_s:"+modelID);
    params.set("fl", "terms_ss, idfs_ds, weights_ds, iteration_i, _version_");
    params.set(SORT, "iteration_i desc");
    StreamContext streamContext = new StreamContext();
    streamContext.setSolrClientCache(solrClientCache);
    CloudSolrStream stream = new CloudSolrStream(zkHost, collection, params);
    stream.setStreamContext(streamContext);
    Tuple tuple = null;
    try {
      stream.open();
      tuple = stream.read();
      if (tuple.EOF) {
        return null;
      }
    } finally {
      stream.close();
    }

    synchronized (this) {
      //check again to see if another thread has updated the same model
      Model m = models.get(modelID);
      if (m != null) {
        Tuple t = m.getTuple();
        long v = t.getLong(VERSION_FIELD);
        if (v >= tuple.getLong(VERSION_FIELD)) {
          return t;
        } else {
          models.put(modelID, new Model(tuple, currentTime));
          return tuple;
        }
      } else {
        models.put(modelID, new Model(tuple, currentTime));
        return tuple;
      }
    }
  }

  private static class Model {
    private Tuple tuple;
    private long lastChecked;

    public Model(Tuple tuple, long lastChecked) {
      this.tuple = tuple;
      this.lastChecked = lastChecked;
    }

    public Tuple getTuple() {
      return tuple;
    }

    public long getLastChecked() {
      return lastChecked;
    }
  }

  private static class LRU extends LinkedHashMap<String, Model> {

    private int maxSize;

    public LRU(int maxSize) {
      this.maxSize = maxSize;
    }

    public boolean removeEldestEntry(@SuppressWarnings({"rawtypes"})Map.Entry eldest) {
      if(size()> maxSize) {
        return true;
      } else {
        return false;
      }
    }
  }
}
