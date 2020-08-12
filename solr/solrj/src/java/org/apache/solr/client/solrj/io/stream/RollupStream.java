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
import java.util.List;
import java.util.Locale;

import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.comp.HashKey;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.Bucket;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;

/**
 * @since 6.0.0
 */
public class RollupStream extends TupleStream implements Expressible {

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
    init(tupleStream, buckets, metrics);
  }
  
  public RollupStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    List<StreamExpression> metricExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, Metric.class);
    StreamExpressionNamedParameter overExpression = factory.getNamedOperand(expression, "over");
    
    // validate expression contains only what we want.
    if(expression.getParameters().size() != streamExpressions.size() + metricExpressions.size() + 1){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - unknown operands found", expression));
    }
    
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }

    if(null == overExpression || !(overExpression.getParameter() instanceof StreamExpressionValue)){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting single 'over' parameter listing fields to rollup by but didn't find one",expression));
    }
    
    // Construct the metrics
    Metric[] metrics = new Metric[metricExpressions.size()];
    for(int idx = 0; idx < metricExpressions.size(); ++idx){
      metrics[idx] = factory.constructMetric(metricExpressions.get(idx));
    }
    
    // Construct the buckets.
    // Buckets are nothing more than equalitors (I think). We can use equalitors as helpers for creating the buckets, but because
    // I feel I'm missing something wrt buckets I don't want to change the use of buckets in this class to instead be equalitors.    
    StreamEqualitor streamEqualitor = factory.constructEqualitor(((StreamExpressionValue)overExpression.getParameter()).getValue(), FieldEqualitor.class);
    List<FieldEqualitor> flattenedEqualitors = flattenEqualitor(streamEqualitor);
    Bucket[] buckets = new Bucket[flattenedEqualitors.size()];
    for(int idx = 0; idx < flattenedEqualitors.size(); ++idx){
      buckets[idx] = new Bucket(flattenedEqualitors.get(idx).getLeftFieldName());
      // while we're using equalitors we don't support those of the form a=b. Only single field names.
    }
    
    init(factory.constructStream(streamExpressions.get(0)), buckets, metrics);
  }
  
  private List<FieldEqualitor> flattenEqualitor(StreamEqualitor equalitor){
    List<FieldEqualitor> flattenedList = new ArrayList<>();
    
    if(equalitor instanceof FieldEqualitor){
      flattenedList.add((FieldEqualitor)equalitor);
    }
    else if(equalitor instanceof MultipleFieldEqualitor){
      MultipleFieldEqualitor mEqualitor = (MultipleFieldEqualitor)equalitor;
      for(StreamEqualitor subEqualitor : mEqualitor.getEqs()){
        flattenedList.addAll(flattenEqualitor(subEqualitor));
      }
    }
    
    return flattenedList;
  }
  
  private void init(TupleStream tupleStream, Bucket[] buckets, Metric[] metrics){
    this.tupleStream = new PushBackStream(tupleStream);
    this.buckets = buckets;
    this.metrics = metrics;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // stream
    if(includeStreams){
      expression.addParameter(tupleStream.toExpression(factory));
    }
    else{
      expression.addParameter("<stream>");
    }
        
    // over
    StringBuilder overBuilder = new StringBuilder();
    for(Bucket bucket : buckets){
      if(overBuilder.length() > 0){ overBuilder.append(","); }
      overBuilder.append(bucket.toString());
    }
    expression.addParameter(new StreamExpressionNamedParameter("over",overBuilder.toString()));
    
    // metrics
    for(Metric metric : metrics){
      expression.addParameter(metric.toExpression(factory));
    }
    
    return expression;
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    Explanation explanation = new StreamExplanation(getStreamNodeId().toString())
      .withChildren(new Explanation[]{
        tupleStream.toExplanation(factory)
      })
      .withFunctionName(factory.getFunctionName(this.getClass()))
      .withImplementingClass(this.getClass().getName())
      .withExpressionType(ExpressionType.STREAM_DECORATOR)
      .withExpression(toExpression(factory, false).toString());
    
    for(Metric metric : metrics){
      explanation.withHelper(metric.toExplanation(factory));
    }
    
    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    this.tupleStream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    l.add(tupleStream);
    return l;
  }

  public void open() throws IOException {
    tupleStream.open();
  }

  public void close() throws IOException {
    tupleStream.close();
    this.currentMetrics = null;
    this.currentKey = new HashKey("-");
    this.finished = false;
  }

  public Tuple read() throws IOException {

    while(true) {
      Tuple tuple = tupleStream.read();
      if(tuple.EOF) {
        if(!finished) {

          if(currentMetrics == null) {
            return tuple;
          }

          Tuple t = new Tuple();
          for(Metric metric : currentMetrics) {
            t.put(metric.getIdentifier(), metric.getValue());
          }

          for(int i=0; i<buckets.length; i++) {
            t.put(buckets[i].toString(), currentKey.getParts()[i]);
          }
          tupleStream.pushBack(tuple);
          finished = true;
          return t;
        } else {
          return tuple;
        }
      }

      Object[] bucketValues = new Object[buckets.length];
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
          t = new Tuple();
          for(Metric metric : currentMetrics) {
            t.put(metric.getIdentifier(), metric.getValue());
          }

          for(int i=0; i<buckets.length; i++) {
            t.put(buckets[i].toString(), currentKey.getParts()[i]);
          }
        }

        currentKey = hashKey;
        if (metrics != null) {
          currentMetrics = new Metric[metrics.length];
          for(int i=0; i<metrics.length; i++) {
            Metric bucketMetric = metrics[i].newInstance();
            bucketMetric.update(tuple);
            currentMetrics[i]  = bucketMetric;
          }
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

  @Override
  public StreamComparator getStreamSort() {
    return tupleStream.getStreamSort();
  }
}
