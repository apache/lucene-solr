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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.HashKey;
import org.apache.solr.client.solrj.io.stream.metrics.Bucket;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;

public class RollupStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private PushBackStream tupleStream;
  private Bucket[] buckets;
  private Metric[] metrics;
  private HashKey currentKey = new HashKey("-");
  private Metric[] currentMetrics;
  private boolean finished = false;

  public RollupStream(TupleStream tupleStream,
                      Bucket[] buckets,
                      Metric[] metrics) {
    this.tupleStream = new PushBackStream(tupleStream);
    this.buckets = buckets;
    this.metrics = metrics;
  }

  public void setStreamContext(StreamContext context) {
    this.tupleStream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList();
    l.add(tupleStream);
    return l;
  }

  public void open() throws IOException {
    tupleStream.open();
  }

  public void close() throws IOException {
    tupleStream.close();
  }

  public Tuple read() throws IOException {

    while(true) {
      Tuple tuple = tupleStream.read();
      if(tuple.EOF) {
        if(!finished) {
          Map map = new HashMap();
          for(Metric metric : currentMetrics) {
            map.put(metric.getName(), metric.getValue());
          }

          for(int i=0; i<buckets.length; i++) {
            map.put(buckets[i].toString(), currentKey.getParts()[i].toString());
          }
          Tuple t = new Tuple(map);
          tupleStream.pushBack(tuple);
          finished = true;
          return t;
        } else {
          return tuple;
        }
      }

      String[] bucketValues = new String[buckets.length];
      for(int i=0; i<buckets.length; i++) {
        bucketValues[i] = buckets[i].getBucketValue(tuple);
      }

      HashKey hashKey = new HashKey(bucketValues);

      if(hashKey.equals(currentKey)) {
        for(Metric bucketMetric : currentMetrics) {
          bucketMetric.update(tuple);
        }
      } else {
        Tuple t = null;
        if(currentMetrics != null) {
          Map map = new HashMap();
          for(Metric metric : currentMetrics) {
            map.put(metric.getName(), metric.getValue());
          }

          for(int i=0; i<buckets.length; i++) {
            map.put(buckets[i].toString(), currentKey.getParts()[i].toString());
          }
          t = new Tuple(map);
        }

        currentMetrics = new Metric[metrics.length];
        currentKey = hashKey;
        for(int i=0; i<metrics.length; i++) {
          Metric bucketMetric = metrics[i].newInstance();
          bucketMetric.update(tuple);
          currentMetrics[i]  = bucketMetric;
        }

        if(t != null) {
          return t;
        }
      }
    }
  }

  public int getCost() {
    return 0;
  }
}