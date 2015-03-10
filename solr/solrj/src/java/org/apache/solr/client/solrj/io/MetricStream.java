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
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.PriorityQueue;

/*
String[] buckets = {"a","b"};
Metric
BucketStream bucketStream = new BucketStream(stream,buckets,metrics,"my-metrics","name");

bucketStream.get(
*/

public class MetricStream extends TupleStream {

  private static final long serialVersionUID = 1;

  private TupleStream tupleStream;
  private Bucket[] buckets;
  private Metric[] metrics;
  private String outKey;
  private Map<HashKey, Metric[]> bucketMap;
  private BucketMetrics[] bucketMetrics;
  private static final HashKey metricsKey = new HashKey("metrics");
  private int topN;
  private Comparator<BucketMetrics> comp;
  private Comparator<BucketMetrics> rcomp;

  public MetricStream(TupleStream tupleStream,
                      Bucket[] buckets,
                      Metric[] metrics,
                      String outKey,
                      Comparator<BucketMetrics> comp,
                      int topN) {
    this.tupleStream = tupleStream;
    this.buckets = buckets;
    this.metrics = metrics;
    this.outKey = outKey;
    this.topN = topN;
    this.rcomp = new ReverseOrdComp(comp);
    this.comp = comp;
    this.bucketMap = new HashMap();
  }

  public MetricStream(TupleStream tupleStream,
                      Metric[] metrics,
                      String outKey) {
    this.tupleStream = tupleStream;
    this.metrics = metrics;
    this.outKey = outKey;
    this.bucketMap = new HashMap();
  }

  public String getOutKey() {
    return this.outKey;
  }

  public BucketMetrics[] getBucketMetrics(){
    return bucketMetrics;
  }

  public void setBucketMetrics(BucketMetrics[] bucketMetrics) {
    this.bucketMetrics = bucketMetrics;
  }

  BucketMetrics[] merge(List<Map> all) {
    Map<HashKey, Metric[]> bucketAccumulator = new HashMap();

    for(Map top : all) {
      List<String> ks = (List<String>)top.get("buckets");
      List<List<Map<String,Double>>> ms = (List<List<Map<String,Double>>>)top.get("metrics");
      for(int i=0; i<ks.size(); i++) {
        String key = ks.get(i);
        List<Map<String,Double>> bucketMs = ms.get(i);

        HashKey hashKey = new HashKey(key);
        if(bucketAccumulator.containsKey(hashKey)) {
          Metric[] mergeMetrics = bucketAccumulator.get(hashKey);
          for(int m=0; m<mergeMetrics.length; m++) {
            mergeMetrics[m].update(bucketMs.get(m));
          }
        } else {
          Metric[] mergedMetrics = new Metric[metrics.length];
          for(int m=0; m<metrics.length; m++) {
            mergedMetrics[m] = metrics[m].newInstance();
            mergedMetrics[m].update(bucketMs.get(m));
           }
          bucketAccumulator.put(hashKey, mergedMetrics);
        }
      }
    }

    Iterator<Map.Entry<HashKey,Metric[]>> it = bucketAccumulator.entrySet().iterator();

    PriorityQueue<BucketMetrics> priorityQueue = new PriorityQueue(topN, rcomp);

    while(it.hasNext()) {
      Map.Entry<HashKey, Metric[]> entry = it.next();
      BucketMetrics bms = new BucketMetrics(entry.getKey(), entry.getValue());
      if(priorityQueue.size() < topN) {
        priorityQueue.add(bms);
      } else {
        BucketMetrics peek = priorityQueue.peek();
        if(comp.compare(bms, peek) < 0) {
          priorityQueue.poll();
          priorityQueue.add(bms);
        }
      }
    }

    int s = priorityQueue.size();
    BucketMetrics[] bucketMetrics = new BucketMetrics[s];

    for(int i=bucketMetrics.length-1; i>=0; i--) {
      BucketMetrics b = priorityQueue.poll();
      bucketMetrics[i]= b;
    }
    return bucketMetrics;
  }

  private class ReverseOrdComp implements Comparator<BucketMetrics>, Serializable {
    private Comparator<BucketMetrics> comp;

    public ReverseOrdComp(Comparator<BucketMetrics> comp) {
      this.comp = comp;
    }

    public int compare(BucketMetrics e1, BucketMetrics e2) {
     return comp.compare(e1,e2)*-1;
    }
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

    Tuple tuple = tupleStream.read();
    if(tuple.EOF) {
      Iterator<Map.Entry<HashKey,Metric[]>> it = bucketMap.entrySet().iterator();

      if(comp == null) {
        //Handle No bucket constructor
        Map.Entry<HashKey, Metric[]> noBucket = it.next();
        BucketMetrics bms = new BucketMetrics(noBucket.getKey(), noBucket.getValue());
        this.bucketMetrics = new BucketMetrics[1];
        this.bucketMetrics[0] = bms;
        List<Map<String, Double>> outMetrics = new ArrayList();
        List<String> outKeys = new ArrayList();
        for(Metric metric : bms.getMetrics()) {
          Map<String, Double> outMetricValues = metric.metricValues();
          String outKey = metric.getName();
          outMetrics.add(outMetricValues);
          outKeys.add(outKey);
        }
        Map outMap = new HashMap();
        outMap.put("buckets",outKeys);
        outMap.put("metrics",outMetrics);
        tuple.set(this.outKey, outMap);
        return tuple;
      }

      PriorityQueue<BucketMetrics> priorityQueue = new PriorityQueue(topN, rcomp);

      while(it.hasNext()) {
        Map.Entry<HashKey, Metric[]> entry = it.next();
        BucketMetrics bms = new BucketMetrics(entry.getKey(), entry.getValue());
        if(priorityQueue.size() < topN) {
          priorityQueue.add(bms);
        } else {
          BucketMetrics peek = priorityQueue.peek();

          if(comp.compare(bms, peek) < 0) {
            priorityQueue.poll();
            priorityQueue.add(bms);
          }

        }
      }

      int s = priorityQueue.size();
      this.bucketMetrics = new BucketMetrics[s];

      for(int i=bucketMetrics.length-1; i>=0; i--) {
        BucketMetrics b = priorityQueue.poll();
        this.bucketMetrics[i]= b;
      }

      List<List<Map<String, Double>>> outMetrics = new ArrayList();
      List<String> outBuckets = new ArrayList();

      for(BucketMetrics bms : this.bucketMetrics) {
        List outBucketMetrics = new ArrayList();
        for(Metric metric : bms.getMetrics()) {
          Map<String, Double> outMetricValues = metric.metricValues();
          outBucketMetrics.add(outMetricValues);
        }
        outBuckets.add(bms.getKey().toString());
        outMetrics.add(outBucketMetrics);
      }

      Map outMap = new HashMap();
      outMap.put("buckets",outBuckets);
      outMap.put("metrics",outMetrics);
      tuple.set(this.outKey, outMap);
      return tuple;
    }

    HashKey hashKey = null;
    if(buckets != null) {
      String[] bucketValues = new String[buckets.length];
      for(int i=0; i<buckets.length; i++) {
        bucketValues[i] = buckets[i].getBucketValue(tuple);
      }
      hashKey = new HashKey(bucketValues);
    } else {
      hashKey = metricsKey;
    }

    Metric[] bucketMetrics = bucketMap.get(hashKey);
    if(bucketMetrics != null) {
      for(Metric bucketMetric : bucketMetrics) {
        bucketMetric.update(tuple);
      }
    } else {
      bucketMetrics = new Metric[metrics.length];

      for(int i=0; i<metrics.length; i++) {
        Metric bucketMetric = metrics[i].newInstance();
        bucketMetric.update(tuple);
        bucketMetrics[i]  = bucketMetric;
      }
      bucketMap.put(hashKey, bucketMetrics);
    }
    return tuple;
  }

  public int getCost() {
    return 0;
  }
}